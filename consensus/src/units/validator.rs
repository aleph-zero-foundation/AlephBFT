use crate::{
    units::{ControlHash, FullUnit, PreUnit, SignedUnit, UncheckedSignedUnit, Unit},
    Data, Hasher, Keychain, NodeCount, NodeIndex, NodeMap, Round, SessionId, Signature,
    SignatureError,
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
    RoundZeroBadControlHash(PreUnit<H>),
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
            RoundZeroBadControlHash(pu) => {
                write!(f, "zero round unit with wrong control hash: {:?}", pu)
            }
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
}

type Result<H, D, K> =
    StdResult<SignedUnit<H, D, K>, ValidationError<H, D, <K as Keychain>::Signature>>;

impl<K: Keychain> Validator<K> {
    pub fn new(session_id: SessionId, keychain: K, max_round: Round) -> Self {
        Validator {
            session_id,
            keychain,
            max_round,
        }
    }

    pub fn node_count(&self) -> NodeCount {
        self.keychain.node_count()
    }

    pub fn index(&self) -> NodeIndex {
        self.keychain.index()
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

    fn threshold(&self) -> NodeCount {
        self.keychain.node_count().consensus_threshold()
    }

    fn validate_unit_parents<H: Hasher, D: Data>(
        &self,
        su: SignedUnit<H, D, K>,
    ) -> Result<H, D, K> {
        let pre_unit = su.as_signable().as_pre_unit();
        let n_members = pre_unit.n_members();
        if n_members != self.keychain.node_count() {
            return Err(ValidationError::WrongNumberOfMembers(pre_unit.clone()));
        }
        let round = pre_unit.round();
        let n_parents = pre_unit.n_parents();
        match round {
            0 => {
                if n_parents > NodeCount(0) {
                    return Err(ValidationError::RoundZeroWithParents(pre_unit.clone()));
                }
                if pre_unit.control_hash().combined_hash
                    != ControlHash::<H>::combine_hashes(&NodeMap::with_size(n_members))
                {
                    return Err(ValidationError::RoundZeroBadControlHash(pre_unit.clone()));
                }
            }
            // NOTE: at this point we cannot validate correctness of the control hash, in principle it could be
            // just a random hash, but we still would not be able to deduce that by looking at the unit only.
            _ => {
                if n_parents < self.threshold() {
                    return Err(ValidationError::NotEnoughParents(pre_unit.clone()));
                }
                let control_hash = &pre_unit.control_hash();
                if !control_hash.parents_mask[pre_unit.creator()] {
                    return Err(ValidationError::NotDescendantOfPreviousUnit(
                        pre_unit.clone(),
                    ));
                }
            }
        }
        Ok(su)
    }
}

#[cfg(test)]
mod tests {
    use super::{ValidationError::*, Validator as GenericValidator};
    use crate::{
        units::{
            full_unit_to_unchecked_signed_unit, preunit_to_unchecked_signed_unit,
            random_full_parent_units_up_to, random_unit_with_parents, PreUnit,
        },
        NodeCount, NodeIndex,
    };
    use aleph_bft_mock::Keychain;

    type Validator = GenericValidator<Keychain>;

    #[test]
    fn validates_initial_unit() {
        let n_members = NodeCount(7);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychain = Keychain::new(n_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round);
        let full_unit = random_full_parent_units_up_to(0, n_members, session_id)[0][0].clone();
        let unchecked_unit = full_unit_to_unchecked_signed_unit(full_unit, &keychain);
        let checked_unit = validator
            .validate_unit(unchecked_unit.clone())
            .expect("Unit should validate.");
        assert_eq!(unchecked_unit, checked_unit.into());
    }

    #[test]
    fn detects_wrong_initial_control_hash() {
        let n_members = NodeCount(7);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychain = Keychain::new(n_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round);
        let preunit = random_full_parent_units_up_to(0, n_members, session_id)[0][0]
            .as_pre_unit()
            .clone();
        let mut control_hash = preunit.control_hash().clone();
        control_hash.combined_hash = [0, 1, 0, 1, 0, 1, 0, 1];
        let preunit = PreUnit::new(preunit.creator(), preunit.round(), control_hash);
        let unchecked_unit =
            preunit_to_unchecked_signed_unit(preunit.clone(), session_id, &keychain);
        let other_preunit = match validator.validate_unit(unchecked_unit.clone()) {
            Ok(_) => panic!("Validated bad unit."),
            Err(RoundZeroBadControlHash(unit)) => unit,
            Err(e) => panic!("Unexpected error from validator: {:?}", e),
        };
        assert_eq!(other_preunit, preunit);
    }

    #[test]
    fn detects_wrong_session_id() {
        let n_members = NodeCount(7);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let wrong_session_id = 43;
        let max_round = 2;
        let keychain = Keychain::new(n_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round);
        let full_unit =
            random_full_parent_units_up_to(0, n_members, wrong_session_id)[0][0].clone();
        let unchecked_unit = full_unit_to_unchecked_signed_unit(full_unit, &keychain);
        let full_unit = match validator.validate_unit(unchecked_unit.clone()) {
            Ok(_) => panic!("Validated bad unit."),
            Err(WrongSession(full_unit)) => full_unit,
            Err(e) => panic!("Unexpected error from validator: {:?}", e),
        };
        assert_eq!(full_unit, unchecked_unit.into_signable());
    }

    #[test]
    fn detects_wrong_number_of_members() {
        let n_members = NodeCount(7);
        let n_plus_one_members = NodeCount(8);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychain = Keychain::new(n_plus_one_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round);
        let full_unit = random_full_parent_units_up_to(0, n_members, session_id)[0][0].clone();
        let preunit = full_unit.as_pre_unit().clone();
        let unchecked_unit = full_unit_to_unchecked_signed_unit(full_unit, &keychain);
        let other_preunit = match validator.validate_unit(unchecked_unit) {
            Ok(_) => panic!("Validated bad unit."),
            Err(WrongNumberOfMembers(other_preunit)) => other_preunit,
            Err(e) => panic!("Unexpected error from validator: {:?}", e),
        };
        assert_eq!(other_preunit, preunit);
    }

    #[test]
    fn detects_below_threshold() {
        let n_members = NodeCount(7);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let parents = random_full_parent_units_up_to(0, n_members, session_id)[0]
            .iter()
            .take(4)
            .cloned()
            .collect();
        let unit = random_unit_with_parents(creator_id, &parents, 1);
        let preunit = unit.as_pre_unit().clone();
        let keychain = Keychain::new(n_members, creator_id);
        let unchecked_unit = full_unit_to_unchecked_signed_unit(unit, &keychain);
        let validator = Validator::new(session_id, keychain, max_round);
        let other_preunit = match validator.validate_unit(unchecked_unit) {
            Ok(_) => panic!("Validated bad unit."),
            Err(NotEnoughParents(other_preunit)) => other_preunit,
            Err(e) => panic!("Unexpected error from validator: {:?}", e),
        };
        assert_eq!(other_preunit, preunit);
    }

    #[test]
    fn detects_too_high_round() {
        let n_members = NodeCount(7);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychain = Keychain::new(n_members, creator_id);
        let validator = Validator::new(session_id, keychain, max_round);
        let full_unit = random_full_parent_units_up_to(3, n_members, session_id)[3][0].clone();
        let unchecked_unit = full_unit_to_unchecked_signed_unit(full_unit, &keychain);
        let full_unit = match validator.validate_unit(unchecked_unit.clone()) {
            Ok(_) => panic!("Validated bad unit."),
            Err(RoundTooHigh(full_unit)) => full_unit,
            Err(e) => panic!("Unexpected error from validator: {:?}", e),
        };
        assert_eq!(full_unit, unchecked_unit.into_signable());
    }
}
