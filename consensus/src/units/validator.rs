use crate::{
    units::{FullUnit, PreUnit, SignedUnit, UncheckedSignedUnit},
    Data, Hasher, Keychain, NodeCount, Round, SessionId, Signature, SignatureError,
};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    result::Result as StdResult,
};

/// All that can be wrong with a unit except control hash issues.
#[derive(Eq, PartialEq, Debug)]
pub enum ValidationError<H: Hasher, D: Data, S: Signature> {
    WrongSignature(UncheckedSignedUnit<H, D, S>),
    WrongSession(FullUnit<H, D>),
    RoundTooHigh(FullUnit<H, D>),
    WrongNumberOfMembers(PreUnit<H>),
    RoundZeroWithParents(PreUnit<H>),
    NotEnoughParents(PreUnit<H>),
    NotDescendantOfPreviousUnit(PreUnit<H>),
}

impl<H: Hasher, D: Data, S: Signature> Display for ValidationError<H, D, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use ValidationError::*;
        match self {
            WrongSignature(usu) => write!(f, "wrongly signed unit: {:?}", usu),
            WrongSession(fu) => write!(f, "unit from wrong session: {:?}", fu),
            RoundTooHigh(fu) => write!(f, "unit with too high round {}: {:?}", fu.round(), fu),
            WrongNumberOfMembers(pu) => write!(
                f,
                "wrong number of members implied by unit {:?}: {:?}",
                pu.n_members(),
                pu
            ),
            RoundZeroWithParents(pu) => write!(f, "zero round unit with parents: {:?}", pu),
            NotEnoughParents(pu) => write!(
                f,
                "nonzero round unit with only {:?} parents: {:?}",
                pu.n_parents(),
                pu
            ),
            NotDescendantOfPreviousUnit(pu) => write!(
                f,
                "nonzero round unit is not descendant of its creator's previous unit: {:?}",
                pu
            ),
        }
    }
}

impl<H: Hasher, D: Data, S: Signature> From<SignatureError<FullUnit<H, D>, S>>
    for ValidationError<H, D, S>
{
    fn from(se: SignatureError<FullUnit<H, D>, S>) -> Self {
        ValidationError::WrongSignature(se.unchecked)
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub struct Validator<K: Keychain> {
    session_id: SessionId,
    keychain: K,
    max_round: Round,
    threshold: NodeCount,
}

type Result<H, D, K> =
    StdResult<SignedUnit<H, D, K>, ValidationError<H, D, <K as Keychain>::Signature>>;

impl<K: Keychain> Validator<K> {
    pub fn new(session_id: SessionId, keychain: K, max_round: Round, threshold: NodeCount) -> Self {
        Validator {
            session_id,
            keychain,
            max_round,
            threshold,
        }
    }

    pub fn validate_unit<H: Hasher, D: Data>(
        &self,
        uu: UncheckedSignedUnit<H, D, K::Signature>,
    ) -> Result<H, D, K> {
        let su = uu.check(&self.keychain)?;
        let full_unit = su.as_signable();
        if full_unit.session_id() != self.session_id {
            // NOTE: this implies malicious behavior as the unit's session_id
            // is incompatible with session_id of the message it arrived in.
            return Err(ValidationError::WrongSession(full_unit.clone()));
        }
        if full_unit.round() > self.max_round {
            return Err(ValidationError::RoundTooHigh(full_unit.clone()));
        }
        self.validate_unit_parents(su)
    }

    fn validate_unit_parents<H: Hasher, D: Data>(
        &self,
        su: SignedUnit<H, D, K>,
    ) -> Result<H, D, K> {
        // NOTE: at this point we cannot validate correctness of the control hash, in principle it could be
        // just a random hash, but we still would not be able to deduce that by looking at the unit only.
        let pre_unit = su.as_signable().as_pre_unit();
        if pre_unit.n_members() != self.keychain.node_count() {
            return Err(ValidationError::WrongNumberOfMembers(pre_unit.clone()));
        }
        let round = pre_unit.round();
        let n_parents = pre_unit.n_parents();
        if round == 0 && n_parents > NodeCount(0) {
            return Err(ValidationError::RoundZeroWithParents(pre_unit.clone()));
        }
        let threshold = self.threshold;
        if round > 0 && n_parents < threshold {
            return Err(ValidationError::NotEnoughParents(pre_unit.clone()));
        }
        let control_hash = &pre_unit.control_hash();
        if round > 0 && !control_hash.parents_mask[pre_unit.creator()] {
            return Err(ValidationError::NotDescendantOfPreviousUnit(
                pre_unit.clone(),
            ));
        }
        Ok(su)
    }
}

#[cfg(test)]
mod tests {
    use super::{ValidationError::*, Validator as GenericValidator};
    use crate::{
        creation::Creator as GenericCreator,
        units::{create_units, creator_set, preunit_to_unchecked_signed_unit, preunit_to_unit},
        NodeCount, NodeIndex,
    };
    use aleph_bft_mock::{Hasher64, Keychain};

    type Validator = GenericValidator<Keychain>;
    type Creator = GenericCreator<Hasher64>;

    #[tokio::test]
    async fn validates_initial_unit() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let round = 0;
        let max_round = 2;
        let creator = Creator::new(creator_id, n_members);
        let keychain = Keychain::new(n_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (preunit, _) = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        let unchecked_unit = preunit_to_unchecked_signed_unit(preunit, session_id, &keychain).await;
        let checked_unit = validator
            .validate_unit(unchecked_unit.clone())
            .expect("Unit should validate.");
        assert_eq!(unchecked_unit, checked_unit.into());
    }

    #[tokio::test]
    async fn detects_wrong_session_id() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let wrong_session_id = 43;
        let round = 0;
        let max_round = 2;
        let creator = Creator::new(creator_id, n_members);
        let keychain = Keychain::new(n_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (preunit, _) = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        let unchecked_unit =
            preunit_to_unchecked_signed_unit(preunit, wrong_session_id, &keychain).await;
        let full_unit = match validator.validate_unit(unchecked_unit.clone()) {
            Ok(_) => panic!("Validated bad unit."),
            Err(WrongSession(full_unit)) => full_unit,
            Err(e) => panic!("Unexpected error from validator: {:?}", e),
        };
        assert_eq!(full_unit, unchecked_unit.into_signable());
    }

    #[tokio::test]
    async fn detects_wrong_number_of_members() {
        let n_members = NodeCount(7);
        let n_plus_one_members = NodeCount(8);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let round = 0;
        let max_round = 2;
        let creator = Creator::new(creator_id, n_members);
        let keychain = Keychain::new(n_plus_one_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (preunit, _) = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        let unchecked_unit =
            preunit_to_unchecked_signed_unit(preunit.clone(), session_id, &keychain).await;
        let other_preunit = match validator.validate_unit(unchecked_unit) {
            Ok(_) => panic!("Validated bad unit."),
            Err(WrongNumberOfMembers(other_preunit)) => other_preunit,
            Err(e) => panic!("Unexpected error from validator: {:?}", e),
        };
        assert_eq!(other_preunit, preunit);
    }

    #[tokio::test]
    async fn detects_below_threshold() {
        let n_members = NodeCount(7);
        // This is the easiest way of testing this I can think off, but it also suggests we are
        // doing something wrong by having multiple sources for what is "threshold".
        let threshold = NodeCount(6);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let round = 1;
        let max_round = 2;
        let mut creators = creator_set(n_members);
        let round_0_units: Vec<_> = create_units(creators.iter(), 0)
            .into_iter()
            .map(|(preunit, _)| preunit_to_unit(preunit, session_id))
            .take(5)
            .collect();
        let creator = &mut creators[0];
        creator.add_units(&round_0_units);
        let keychain = Keychain::new(n_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (preunit, _) = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        let unchecked_unit =
            preunit_to_unchecked_signed_unit(preunit.clone(), session_id, &keychain).await;
        let other_preunit = match validator.validate_unit(unchecked_unit) {
            Ok(_) => panic!("Validated bad unit."),
            Err(NotEnoughParents(other_preunit)) => other_preunit,
            Err(e) => panic!("Unexpected error from validator: {:?}", e),
        };
        assert_eq!(other_preunit, preunit);
    }

    #[tokio::test]
    async fn detects_too_high_round() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let round = 3;
        let max_round = 2;
        let mut creators = creator_set(n_members);
        for round in 0..round {
            let units: Vec<_> = create_units(creators.iter(), round)
                .into_iter()
                .map(|(preunit, _)| preunit_to_unit(preunit, session_id))
                .collect();
            for creator in creators.iter_mut() {
                creator.add_units(&units);
            }
        }
        let creator = &creators[0];
        let keychain = Keychain::new(n_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (preunit, _) = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        let unchecked_unit = preunit_to_unchecked_signed_unit(preunit, session_id, &keychain).await;
        let full_unit = match validator.validate_unit(unchecked_unit.clone()) {
            Ok(_) => panic!("Validated bad unit."),
            Err(RoundTooHigh(full_unit)) => full_unit,
            Err(e) => panic!("Unexpected error from validator: {:?}", e),
        };
        assert_eq!(full_unit, unchecked_unit.into_signable());
    }
}
