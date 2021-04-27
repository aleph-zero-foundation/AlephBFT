use codec::{Decode, Encode, Error as CodecError, Input, Output};
use derive_more::{Add, AddAssign, Display, From, Into, Sub, SubAssign, Sum};
use std::{
    iter::FromIterator,
    ops::{Div, Index, IndexMut, Mul},
    slice, vec,
};

/// The index of a node
#[derive(Copy, Clone, Debug, Display, Default, Eq, PartialEq, Hash, Ord, PartialOrd, From)]
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
    Copy,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Add,
    Sub,
    AddAssign,
    SubAssign,
    Sum,
    From,
    Into,
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

#[derive(Clone, Debug, Default, Eq, PartialEq, From, Hash, Encode, Decode)]
pub struct NodeMap<T>(Vec<T>);

impl<T> NodeMap<T> {
    /// Constructs a new node map with a given length.
    pub fn new_with_len(len: NodeCount) -> Self
    where
        T: Default + Clone,
    {
        let v: Vec<T> = vec![T::default(); len.into()];
        NodeMap(v)
    }

    /// Returns the number of values. This must equal the number of nodes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns an iterator over all values.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter()
    }

    /// Returns an iterator over all values, by node index.
    pub fn enumerate(&self) -> impl Iterator<Item = (NodeIndex, &T)> {
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

impl<T> FromIterator<T> for NodeMap<T> {
    fn from_iter<I: IntoIterator<Item = T>>(ii: I) -> NodeMap<T> {
        NodeMap(ii.into_iter().collect())
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

impl<'a, T> IntoIterator for &'a NodeMap<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::nodes::NodeIndex;
    use codec::{Decode, Encode};
    #[test]
    fn decoding_works() {
        for i in 0..1000 {
            let node_index = NodeIndex(i);
            let mut encoded: &[u8] = &node_index.encode();
            let decoded = NodeIndex::decode(&mut encoded);
            assert_eq!(node_index, decoded.unwrap());
        }
    }
}
