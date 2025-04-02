use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use crate::{
    alerts::Alert,
    units::{
        SignedUnit, UncheckedSignedUnit, Unit, UnitStore, UnitStoreStatus, ValidationError,
        Validator as UnitValidator, WrappedUnit,
    },
    Data, Hasher, MultiKeychain, NodeIndex, NodeSubset, Round,
};

/// What can go wrong when validating a unit.
#[derive(Eq, PartialEq)]
pub enum Error<H: Hasher, D: Data, MK: MultiKeychain> {
    Invalid(ValidationError<H, D, MK::Signature>),
    Duplicate(SignedUnit<H, D, MK>),
    Uncommitted(SignedUnit<H, D, MK>),
    NewForker(Box<Alert<H, D, MK::Signature>>),
}

impl<H: Hasher, D: Data, MK: MultiKeychain> Debug for Error<H, D, MK> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use Error::*;
        match self {
            Invalid(e) => write!(f, "Invalid({:?})", e),
            Duplicate(u) => write!(f, "Duplicate({:?})", u.clone().into_unchecked()),
            Uncommitted(u) => write!(f, "Uncommitted({:?})", u.clone().into_unchecked()),
            NewForker(a) => write!(f, "NewForker({:?})", a),
        }
    }
}

impl<H: Hasher, D: Data, MK: MultiKeychain> From<ValidationError<H, D, MK::Signature>>
    for Error<H, D, MK>
{
    fn from(e: ValidationError<H, D, MK::Signature>) -> Self {
        Error::Invalid(e)
    }
}

/// The summary status of the validator.
pub struct ValidatorStatus {
    processing_units: UnitStoreStatus,
    known_forkers: NodeSubset,
}

impl ValidatorStatus {
    /// The highest round among the units that are currently processing.
    pub fn top_round(&self) -> Round {
        self.processing_units.top_round()
    }
}

impl Display for ValidatorStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "processing units: ({}), forkers: {}",
            self.processing_units, self.known_forkers
        )
    }
}

type ValidatorResult<H, D, MK> = Result<SignedUnit<H, D, MK>, Error<H, D, MK>>;

/// A validator that checks basic properties of units and catches forks.
pub struct Validator<H: Hasher, D: Data, MK: MultiKeychain> {
    unit_validator: UnitValidator<MK>,
    processing_units: UnitStore<SignedUnit<H, D, MK>>,
    known_forkers: NodeSubset,
}

impl<H: Hasher, D: Data, MK: MultiKeychain> Validator<H, D, MK> {
    /// A new validator using the provided unit validator under the hood.
    pub fn new(unit_validator: UnitValidator<MK>) -> Self {
        let node_count = unit_validator.node_count();
        Validator {
            unit_validator,
            processing_units: UnitStore::new(node_count),
            known_forkers: NodeSubset::with_size(node_count),
        }
    }

    fn is_forker(&self, node_id: NodeIndex) -> bool {
        self.known_forkers[node_id]
    }

    fn mark_forker<U: WrappedUnit<H, Wrapped = SignedUnit<H, D, MK>>>(
        &mut self,
        forker: NodeIndex,
        store: &UnitStore<U>,
    ) -> Vec<UncheckedSignedUnit<H, D, MK::Signature>> {
        assert!(!self.is_forker(forker), "we shouldn't mark a forker twice");
        self.known_forkers.insert(forker);
        store
            .canonical_units(forker)
            .cloned()
            .map(WrappedUnit::unpack)
            // In principle we can have "canonical" processing units that are forks of store canonical units,
            // but only after we already marked a node as a forker, so not yet.
            // Also note that these units can be from different branches and we still commit to them here.
            // This is somewhat confusing, but not a problem for any theoretical guarantees.
            .chain(self.processing_units.canonical_units(forker).cloned())
            .map(|unit| unit.into_unchecked())
            .collect()
    }

    fn pre_validate<U: WrappedUnit<H, Wrapped = SignedUnit<H, D, MK>>>(
        &mut self,
        unit: UncheckedSignedUnit<H, D, MK::Signature>,
        store: &UnitStore<U>,
    ) -> ValidatorResult<H, D, MK> {
        let unit = self.unit_validator.validate_unit(unit)?;
        let unit_hash = unit.as_signable().hash();
        if store.unit(&unit_hash).is_some() || self.processing_units.unit(&unit_hash).is_some() {
            return Err(Error::Duplicate(unit));
        }
        Ok(unit)
    }

    /// Validate an incoming unit.
    pub fn validate<U: WrappedUnit<H, Wrapped = SignedUnit<H, D, MK>>>(
        &mut self,
        unit: UncheckedSignedUnit<H, D, MK::Signature>,
        store: &UnitStore<U>,
    ) -> ValidatorResult<H, D, MK> {
        use Error::*;
        let unit = self.pre_validate(unit, store)?;
        let unit_coord = unit.as_signable().coord();
        if self.is_forker(unit_coord.creator()) {
            return Err(Uncommitted(unit));
        }
        if let Some(canonical_unit) = store
            .canonical_unit(unit_coord)
            .map(|unit| unit.clone().unpack())
            .or(self.processing_units.canonical_unit(unit_coord).cloned())
        {
            let proof = (canonical_unit.into(), unit.into());
            let committed_units = self.mark_forker(unit_coord.creator(), store);
            return Err(NewForker(Box::new(Alert::new(
                self.unit_validator.index(),
                proof,
                committed_units,
            ))));
        }
        self.processing_units.insert(unit.clone());
        Ok(unit)
    }

    /// Validate a committed unit, it has to be from a forker.
    pub fn validate_committed<U: WrappedUnit<H, Wrapped = SignedUnit<H, D, MK>>>(
        &mut self,
        unit: UncheckedSignedUnit<H, D, MK::Signature>,
        store: &UnitStore<U>,
    ) -> ValidatorResult<H, D, MK> {
        let unit = self.pre_validate(unit, store)?;
        assert!(
            self.is_forker(unit.creator()),
            "We should only receive committed units for known forkers."
        );
        self.processing_units.insert(unit.clone());
        Ok(unit)
    }

    /// The store of units currently being processed.
    pub fn processing_units(&self) -> &UnitStore<SignedUnit<H, D, MK>> {
        &self.processing_units
    }

    /// Signal that a unit finished processing and thus it's copy no longer has to be kept for fork detection.
    /// NOTE: This is only a memory optimization, if the units stay there forever everything still works.
    pub fn finished_processing(&mut self, unit: &H::Hash) {
        self.processing_units.remove(unit)
    }

    /// The status summary of this validator.
    pub fn status(&self) -> ValidatorStatus {
        ValidatorStatus {
            processing_units: self.processing_units.status(),
            known_forkers: self.known_forkers.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        dag::validation::{Error, Validator},
        units::{
            random_full_parent_units_up_to, Unit, UnitStore, Validator as UnitValidator,
            WrappedSignedUnit,
        },
        NodeCount, NodeIndex, Signed,
    };
    use aleph_bft_mock::Keychain;

    #[test]
    fn validates_trivially_correct() {
        let node_count = NodeCount(7);
        let session_id = 0;
        let max_round = 2137;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let mut validator = Validator::new(UnitValidator::new(session_id, keychains[0], max_round));
        for unit in random_full_parent_units_up_to(4, node_count, session_id)
            .iter()
            .flatten()
            .map(|unit| Signed::sign(unit.clone(), &keychains[unit.creator().0]))
        {
            assert_eq!(
                validator.validate(unit.clone().into(), &store),
                Ok(unit.clone())
            );
        }
    }

    #[test]
    fn refuses_processing_duplicates() {
        let node_count = NodeCount(7);
        let session_id = 0;
        let max_round = 2137;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let mut validator = Validator::new(UnitValidator::new(session_id, keychains[0], max_round));
        let unit = random_full_parent_units_up_to(0, node_count, session_id)
            .first()
            .expect("we have the first round")
            .first()
            .expect("we have the initial unit for the zeroth creator")
            .clone();
        let unit = Signed::sign(unit, &keychains[0]);
        assert_eq!(
            validator.validate(unit.clone().into(), &store),
            Ok(unit.clone())
        );
        assert_eq!(
            validator.validate(unit.clone().into(), &store),
            Err(Error::Duplicate(unit.clone()))
        );
    }

    #[test]
    fn refuses_external_duplicates() {
        let node_count = NodeCount(7);
        let session_id = 0;
        let max_round = 2137;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let mut store = UnitStore::new(node_count);
        let mut validator = Validator::new(UnitValidator::new(session_id, keychains[0], max_round));
        let unit = random_full_parent_units_up_to(0, node_count, session_id)
            .first()
            .expect("we have the first round")
            .first()
            .expect("we have the initial unit for the zeroth creator")
            .clone();
        let unit = Signed::sign(unit, &keychains[0]);
        store.insert(WrappedSignedUnit(unit.clone()));
        assert_eq!(
            validator.validate(unit.clone().into(), &store),
            Err(Error::Duplicate(unit.clone()))
        );
    }

    #[test]
    fn detects_processing_fork() {
        let node_count = NodeCount(7);
        let session_id = 0;
        let max_round = 2137;
        let produced_round = 4;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let mut validator = Validator::new(UnitValidator::new(session_id, keychains[0], max_round));
        for unit in random_full_parent_units_up_to(produced_round, node_count, session_id)
            .iter()
            .flatten()
            .map(|unit| Signed::sign(unit.clone(), &keychains[unit.creator().0]))
        {
            assert_eq!(
                validator.validate(unit.clone().into(), &store),
                Ok(unit.clone())
            );
        }
        let fork = random_full_parent_units_up_to(2, node_count, session_id)
            .get(2)
            .expect("we have the requested round")
            .first()
            .expect("we have the unit for the zeroth creator")
            .clone();
        let fork = Signed::sign(fork, &keychains[0]);
        assert!(matches!(
            validator.validate(fork.clone().into(), &store),
            Err(Error::NewForker(_))
        ));
    }

    #[test]
    fn detects_external_fork() {
        let node_count = NodeCount(7);
        let session_id = 0;
        let max_round = 2137;
        let produced_round = 4;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let mut store = UnitStore::new(node_count);
        let mut validator = Validator::new(UnitValidator::new(session_id, keychains[0], max_round));
        for unit in random_full_parent_units_up_to(produced_round, node_count, session_id)
            .iter()
            .flatten()
            .map(|unit| Signed::sign(unit.clone(), &keychains[unit.creator().0]))
        {
            store.insert(WrappedSignedUnit(unit));
        }
        let fork = random_full_parent_units_up_to(2, node_count, session_id)
            .get(2)
            .expect("we have the requested round")
            .first()
            .expect("we have the unit for the zeroth creator")
            .clone();
        let fork = Signed::sign(fork, &keychains[0]);
        assert!(matches!(
            validator.validate(fork.clone().into(), &store),
            Err(Error::NewForker(_))
        ));
    }

    #[test]
    fn refuses_uncommitted() {
        let node_count = NodeCount(7);
        let session_id = 0;
        let max_round = 2137;
        let produced_round = 4;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let mut validator = Validator::new(UnitValidator::new(session_id, keychains[0], max_round));
        let fork = random_full_parent_units_up_to(2, node_count, session_id)
            .get(2)
            .expect("we have the requested round")
            .first()
            .expect("we have the unit for the zeroth creator")
            .clone();
        let fork = Signed::sign(fork, &keychains[0]);
        for unit in random_full_parent_units_up_to(produced_round, node_count, session_id)
            .iter()
            .flatten()
            .filter(|unit| unit.creator() == NodeIndex(0))
            .map(|unit| Signed::sign(unit.clone(), &keychains[unit.creator().0]))
        {
            match unit.round() {
                0..=1 => assert_eq!(
                    validator.validate(unit.clone().into(), &store),
                    Ok(unit.clone())
                ),
                2 => {
                    assert_eq!(
                        validator.validate(unit.clone().into(), &store),
                        Ok(unit.clone())
                    );
                    assert!(matches!(
                        validator.validate(fork.clone().into(), &store),
                        Err(Error::NewForker(_))
                    ))
                }
                3.. => assert_eq!(
                    validator.validate(unit.clone().into(), &store),
                    Err(Error::Uncommitted(unit.clone()))
                ),
            }
        }
    }

    #[test]
    fn accepts_committed() {
        let node_count = NodeCount(7);
        let session_id = 0;
        let max_round = 2137;
        let produced_round = 4;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let mut validator = Validator::new(UnitValidator::new(session_id, keychains[0], max_round));
        let fork = random_full_parent_units_up_to(2, node_count, session_id)
            .get(2)
            .expect("we have the requested round")
            .first()
            .expect("we have the unit for the zeroth creator")
            .clone();
        let fork = Signed::sign(fork, &keychains[0]);
        let units: Vec<_> = random_full_parent_units_up_to(produced_round, node_count, session_id)
            .iter()
            .flatten()
            .filter(|unit| unit.creator() == NodeIndex(0))
            .map(|unit| Signed::sign(unit.clone(), &keychains[unit.creator().0]))
            .collect();
        for unit in units.iter().take(3) {
            assert_eq!(
                validator.validate(unit.clone().into(), &store),
                Ok(unit.clone())
            );
        }
        assert!(matches!(
            validator.validate(fork.clone().into(), &store),
            Err(Error::NewForker(_))
        ));
        assert_eq!(
            validator.validate_committed(fork.clone().into(), &store),
            Ok(fork)
        );
    }
}
