use crate::{
    signed::{Signable, Signed, UncheckedSigned},
    Data, Hasher, Index, KeyBox, NodeCount, NodeIndex, NodeMap, Round, SessionId,
};
use codec::{Decode, Encode, Error, Input, Output};
use log::error;
use std::{cell::RefCell, collections::HashMap, hash::Hash as StdHash};

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Encode, Decode, StdHash)]
pub(crate) struct UnitCoord {
    round: u16,
    creator: NodeIndex,
}

impl UnitCoord {
    pub fn new(round: Round, creator: NodeIndex) -> Self {
        Self {
            creator,
            round: round as u16,
        }
    }

    pub fn creator(&self) -> NodeIndex {
        self.creator
    }

    pub fn round(&self) -> Round {
        self.round as Round
    }
}

/// Combined hashes of the parents of a unit together with the set of indices of creators of the
/// parents
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ControlHash<H: Hasher> {
    pub(crate) parents_mask: bit_vec::BitVec<u32>,
    pub(crate) combined_hash: H::Hash,
}

impl<H: Hasher> ControlHash<H> {
    pub(crate) fn new(parent_map: &NodeMap<Option<H::Hash>>) -> Self {
        let hash = Self::combine_hashes(&parent_map);
        let parents = parent_map.iter().map(Option::is_some).collect();

        ControlHash {
            parents_mask: parents,
            combined_hash: hash,
        }
    }

    pub(crate) fn combine_hashes(parent_map: &NodeMap<Option<H::Hash>>) -> H::Hash {
        parent_map.using_encoded(H::hash)
    }

    pub(crate) fn parents(&self) -> impl Iterator<Item = NodeIndex> + '_ {
        self.parents_mask
            .iter()
            .enumerate()
            .filter(|(_, b)| *b)
            .map(|(i, _)| i.into())
    }

    pub(crate) fn n_parents(&self) -> NodeCount {
        NodeCount(self.parents().count())
    }

    pub(crate) fn n_members(&self) -> NodeCount {
        NodeCount(self.parents_mask.len())
    }
}

impl<H: Hasher> Encode for ControlHash<H> {
    fn encode_to<T: Output + ?Sized>(&self, dest: &mut T) {
        (self.parents_mask.len() as u32).encode_to(dest);
        self.parents_mask.to_bytes().encode_to(dest);
        self.combined_hash.encode_to(dest);
    }
}

impl<H: Hasher> Decode for ControlHash<H> {
    fn decode<I: Input>(input: &mut I) -> Result<Self, Error> {
        let len = u32::decode(input)?;
        let bytes = Vec::decode(input)?;
        let mut parents = bit_vec::BitVec::from_bytes(&bytes);
        parents.truncate(len as usize);
        let hash = H::Hash::decode(input)?;
        Ok(ControlHash {
            parents_mask: parents,
            combined_hash: hash,
        })
    }
}

/// The simplest type representing a unit, consisting of coordinates and a control hash
#[derive(Clone, Debug, PartialEq, Encode, Decode)]
pub(crate) struct PreUnit<H: Hasher> {
    coord: UnitCoord,
    control_hash: ControlHash<H>,
}

impl<H: Hasher> PreUnit<H> {
    pub(crate) fn new(creator: NodeIndex, round: Round, control_hash: ControlHash<H>) -> Self {
        PreUnit {
            coord: UnitCoord::new(round, creator),
            control_hash,
        }
    }

    pub(crate) fn n_parents(&self) -> NodeCount {
        self.control_hash.n_parents()
    }

    pub(crate) fn n_members(&self) -> NodeCount {
        self.control_hash.n_members()
    }

    pub(crate) fn creator(&self) -> NodeIndex {
        self.coord.creator()
    }
    pub(crate) fn round(&self) -> Round {
        self.coord.round()
    }
    pub(crate) fn control_hash(&self) -> &ControlHash<H> {
        &self.control_hash
    }
}

///
#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct FullUnit<H: Hasher, D: Data> {
    pre_unit: PreUnit<H>,
    data: D,
    session_id: SessionId,
    #[codec(skip)]
    hash: RefCell<Option<H::Hash>>,
}

impl<H: Hasher, D: Data> PartialEq for FullUnit<H, D> {
    fn eq(&self, other: &Self) -> bool {
        self.pre_unit == other.pre_unit
            && self.data == other.data
            && self.session_id == other.session_id
    }
}

impl<H: Hasher, D: Data> FullUnit<H, D> {
    pub(crate) fn new(pre_unit: PreUnit<H>, data: D, session_id: SessionId) -> Self {
        FullUnit {
            pre_unit,
            data,
            session_id,
            hash: RefCell::new(Default::default()),
        }
    }
    pub(crate) fn as_pre_unit(&self) -> &PreUnit<H> {
        &self.pre_unit
    }
    pub(crate) fn creator(&self) -> NodeIndex {
        self.pre_unit.creator()
    }
    pub(crate) fn round(&self) -> Round {
        self.pre_unit.round()
    }
    pub(crate) fn control_hash(&self) -> &ControlHash<H> {
        &self.pre_unit.control_hash()
    }
    pub(crate) fn coord(&self) -> UnitCoord {
        self.pre_unit.coord
    }
    pub(crate) fn data(&self) -> D {
        self.data
    }
    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }
    pub(crate) fn hash(&self) -> H::Hash {
        let hash = *self.hash.borrow();
        match hash {
            Some(hash) => hash,
            None => {
                let hash = self.using_encoded(H::hash);
                *self.hash.borrow_mut() = Some(hash);
                hash
            }
        }
    }
    pub(crate) fn unit(&self) -> Unit<H> {
        Unit::new(self.pre_unit.clone(), self.hash())
    }
}

impl<H: Hasher, D: Data> Signable for FullUnit<H, D> {
    type Hash = H::Hash;
    fn hash(&self) -> H::Hash {
        self.hash()
    }
}

impl<H: Hasher, D: Data> Index for FullUnit<H, D> {
    fn index(&self) -> NodeIndex {
        self.creator()
    }
}

pub(crate) type UncheckedSignedUnit<H, D, S> = UncheckedSigned<FullUnit<H, D>, S>;

pub(crate) type SignedUnit<'a, H, D, KB> = Signed<'a, FullUnit<H, D>, KB>;

#[derive(Clone, Debug, PartialEq, Encode, Decode)]
pub(crate) struct Unit<H: Hasher> {
    pre_unit: PreUnit<H>,
    hash: H::Hash,
}

impl<H: Hasher> Unit<H> {
    pub(crate) fn new(pre_unit: PreUnit<H>, hash: H::Hash) -> Self {
        Unit { pre_unit, hash }
    }
    pub(crate) fn creator(&self) -> NodeIndex {
        self.pre_unit.creator()
    }
    pub(crate) fn round(&self) -> Round {
        self.pre_unit.round()
    }
    pub(crate) fn control_hash(&self) -> &ControlHash<H> {
        self.pre_unit.control_hash()
    }
    pub(crate) fn hash(&self) -> H::Hash {
        self.hash
    }
}

mod store;
pub(crate) use store::*;

#[cfg(test)]
mod tests {
    use crate::{
        nodes::NodeIndex,
        testing::mock::Hasher64,
        units::{ControlHash, FullUnit, PreUnit},
        Hasher,
    };
    use codec::{Decode, Encode};

    #[test]
    fn test_full_unit_hash_is_correct() {
        let ch = ControlHash::<Hasher64>::new(&vec![].into());
        let pre_unit = PreUnit::new(NodeIndex(5), 6, ch);
        let full_unit = FullUnit::new(pre_unit, 7, 8);
        let hash = full_unit.using_encoded(Hasher64::hash);
        assert_eq!(full_unit.hash(), hash);
    }

    #[test]
    fn test_control_hash_codec() {
        let ch = ControlHash::<Hasher64>::new(&vec![Some([0; 8]), None, Some([1; 8])].into());
        let encoded = ch.encode();
        let decoded =
            ControlHash::decode(&mut encoded.as_slice()).expect("should decode correctly");
        assert_eq!(decoded, ch);
    }

    #[test]
    fn test_full_unit_codec() {
        let ch = ControlHash::<Hasher64>::new(&vec![].into());
        let pre_unit = PreUnit::new(NodeIndex(5), 6, ch);
        let full_unit = FullUnit::new(pre_unit, 7, 8);
        full_unit.hash();
        let encoded = full_unit.encode();
        let decoded = FullUnit::decode(&mut encoded.as_slice()).expect("should decode correctly");
        assert_eq!(decoded, full_unit);
    }
}
