use derive_more::{Add, AddAssign, AsRef, From, Sub, SubAssign, Sum};
use serde::{Deserialize, Serialize};
use std::{
    iter::FromIterator,
    ops::{Div, Index, IndexMut, Mul},
    slice, vec,
};

/// The index of a node
#[derive(
    Copy, Clone, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct NodeIndex(pub(crate) u32);

impl From<u32> for NodeIndex {
    fn from(idx: u32) -> Self {
        NodeIndex(idx)
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
    Serialize,
    Deserialize,
    Add,
    Sub,
    AddAssign,
    SubAssign,
    Sum,
)]
pub struct NodeCount(pub(crate) u32);

impl From<u32> for NodeCount {
    fn from(cnt: u32) -> Self {
        NodeCount(cnt)
    }
}

// deriving Mul and Div is somehow cumbersome
impl Mul<u32> for NodeCount {
    type Output = Self;
    fn mul(self, rhs: u32) -> Self::Output {
        NodeCount(self.0 * rhs)
    }
}

impl Div<u32> for NodeCount {
    type Output = Self;
    fn div(self, rhs: u32) -> Self::Output {
        NodeCount(self.0 * rhs)
    }
}

// This might be ugly, But how to make a vector of size: NodeCount otherwise?
impl From<NodeCount> for usize {
    fn from(cnt: NodeCount) -> usize {
        cnt.0 as usize
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, AsRef, From, Hash)]
pub struct NodeMap<T>(Vec<T>);

impl<T> NodeMap<T> {
    // /// Returns the value for the given node. Panics if the index is out of range.
    // pub(crate) fn get(&self, idx: NodeIndex) -> &T {
    //     &self.0[idx.0 as usize]
    // }

    /// Returns the number of values. This must equal the number of nodes.
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns an iterator over all values.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter()
    }

    /// Returns an iterator over all values, by node index.
    pub(crate) fn enumerate(&self) -> impl Iterator<Item = (NodeIndex, &T)> {
        self.iter()
            .enumerate()
            .map(|(idx, value)| (NodeIndex(idx as u32), value))
    }

    // /// Returns an iterator over all validator indices.
    // pub(crate) fn keys(&self) -> impl Iterator<Item = NodeIndex> {
    //     (0..self.len()).map(|idx| NodeIndex(idx as u32))
    // }
    pub(crate) fn new_with_len(len: NodeCount) -> Self
    where
        T: Default + Clone,
    {
        let v: Vec<T> = vec![T::default(); len.into()];
        return NodeMap(v);
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

// impl<T> NodeMap<Option<T>> {
// 	/// Returns the keys of all validators whose value is `Some`.
// 	pub(crate) fn keys_some(&self) -> impl Iterator<Item = NodeIndex> + '_ {
// 		self.iter_some().map(|(vidx, _)| vidx)
// 	}

// 	/// Returns an iterator over all values that are present, together with their index.
// 	pub(crate) fn iter_some(&self) -> impl Iterator<Item = (NodeIndex, &T)> + '_ {
// 		self.enumerate()
// 			.filter_map(|(vidx, opt)| opt.as_ref().map(|val| (vidx, val)))
// 	}
//}
