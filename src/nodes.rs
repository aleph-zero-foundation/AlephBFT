use codec::{Decode, Encode, Error as CodecError, Error, Input, Output};
use derive_more::{Add, AddAssign, From, Into, Sub, SubAssign, Sum};
use std::{
    iter::FromIterator,
    ops::{Div, Index, IndexMut, Mul},
    vec,
};

/// The index of a node
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash, From)]
pub struct NodeIndex(pub usize);

impl Encode for NodeIndex {
    fn encode_to<T: Output + ?Sized>(&self, dest: &mut T) {
        let val = self.0 as u64;
        let bytes = val.to_le_bytes();
        dest.write(&bytes);
    }
}

impl Decode for NodeIndex {
    fn decode<I: Input>(value: &mut I) -> Result<Self, CodecError> {
        let mut arr = [0u8; 8];
        value.read(&mut arr)?;
        let val: u64 = u64::from_le_bytes(arr);
        Ok(NodeIndex(val as usize))
    }
}

/// Node count -- if necessary this can be then generalized to weights
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Add, Sub, AddAssign, SubAssign, Sum, From, Into,
)]
pub struct NodeCount(pub usize);

// deriving Mul and Div is somehow cumbersome
impl Mul<usize> for NodeCount {
    type Output = Self;
    fn mul(self, rhs: usize) -> Self::Output {
        NodeCount(self.0 * rhs)
    }
}

impl Div<usize> for NodeCount {
    type Output = Self;
    fn div(self, rhs: usize) -> Self::Output {
        NodeCount(self.0 / rhs)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, From, Encode, Decode)]
pub(crate) struct NodeMap<T>(Vec<T>);

impl<T> NodeMap<T> {
    /// Constructs a new node map with a given length.
    pub(crate) fn new_with_len(len: NodeCount) -> Self
    where
        T: Default + Clone,
    {
        let v: Vec<T> = vec![T::default(); len.into()];
        NodeMap(v)
    }

    /// Returns an iterator over all values.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter()
    }

    /// Returns an iterator over all values, by node index.
    pub(crate) fn enumerate(&self) -> impl Iterator<Item = (NodeIndex, &T)> {
        self.iter()
            .enumerate()
            .map(|(idx, value)| (NodeIndex(idx), value))
    }
}

impl<T> IntoIterator for NodeMap<T> {
    type Item = T;
    type IntoIter = vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> Index<NodeIndex> for NodeMap<T> {
    type Output = T;

    fn index(&self, vidx: NodeIndex) -> &T {
        &self.0[vidx.0 as usize]
    }
}

impl<T> IndexMut<NodeIndex> for NodeMap<T> {
    fn index_mut(&mut self, vidx: NodeIndex) -> &mut T {
        &mut self.0[vidx.0 as usize]
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct BoolNodeMap(bit_vec::BitVec<u32>);

#[cfg(test)]
impl BoolNodeMap {
    pub(crate) fn with_capacity(capacity: NodeCount) -> Self {
        BoolNodeMap(bit_vec::BitVec::from_elem(capacity.0, false))
    }

    pub(crate) fn set(&mut self, i: NodeIndex) {
        self.0.set(i.0, true);
    }
}

impl BoolNodeMap {
    pub(crate) fn capacity(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn true_indices(&self) -> impl Iterator<Item = NodeIndex> + '_ {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(i, b)| if b { Some(i.into()) } else { None })
    }
}

impl Encode for BoolNodeMap {
    fn encode_to<T: Output + ?Sized>(&self, dest: &mut T) {
        (self.0.len() as u32).encode_to(dest);
        self.0.to_bytes().encode_to(dest);
    }
}

impl Decode for BoolNodeMap {
    fn decode<I: Input>(input: &mut I) -> Result<Self, Error> {
        let capacity = u32::decode(input)? as usize;
        let bytes = Vec::decode(input)?;
        let mut bv = bit_vec::BitVec::from_bytes(&bytes);
        bv.truncate(capacity);
        Ok(BoolNodeMap(bv))
    }
}

impl FromIterator<bool> for BoolNodeMap {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        BoolNodeMap(bit_vec::BitVec::from_iter(iter))
    }
}

impl Index<NodeIndex> for BoolNodeMap {
    type Output = bool;

    fn index(&self, vidx: NodeIndex) -> &bool {
        &self.0[vidx.0 as usize]
    }
}

#[cfg(test)]
mod tests {
    use crate::nodes::{BoolNodeMap, NodeIndex};
    use codec::{Decode, Encode};
    #[test]
    fn decoding_node_index_works() {
        for i in 0..1000 {
            let node_index = NodeIndex(i);
            let mut encoded: &[u8] = &node_index.encode();
            let decoded = NodeIndex::decode(&mut encoded);
            assert_eq!(node_index, decoded.unwrap());
        }
    }

    #[test]
    fn decoding_bool_node_map_works() {
        let bool_node_map = BoolNodeMap([true, false, true, true, true].iter().cloned().collect());
        let encoded: Vec<_> = bool_node_map.encode();
        let decoded = BoolNodeMap::decode(&mut encoded.as_slice()).expect("decode should work");
        assert_eq!(decoded, bool_node_map);
    }

    #[test]
    fn test_bool_node_map_has_efficient_encoding() {
        let mut bnm = BoolNodeMap::with_capacity(100.into());
        for i in 0..50 {
            bnm.set(i.into())
        }
        assert!(bnm.encode().len() < 20);
    }
}
