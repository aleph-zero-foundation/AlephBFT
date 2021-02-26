use derive_more::{Add, AddAssign, Display, From, Into, Sub, SubAssign, Sum};

use std::{
    iter::FromIterator,
    ops::{Div, Index, IndexMut, Mul},
    slice, vec,
};

/// The index of a node
#[derive(Copy, Clone, Debug, Display, Default, Eq, PartialEq, Hash, Ord, PartialOrd, From)]
pub struct NodeIndex(pub(crate) usize);

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
pub struct NodeCount(pub(crate) usize);

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

#[derive(Clone, Debug, Default, Eq, PartialEq, From, Hash)]
pub struct NodeMap<T>(Vec<T>);

impl<T> NodeMap<T> {
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
            .map(|(idx, value)| (NodeIndex(idx as usize), value))
    }

    pub(crate) fn new_with_len(len: NodeCount) -> Self
    where
        T: Default + Clone,
    {
        let v: Vec<T> = vec![T::default(); len.into()];
        NodeMap(v)
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
        &self.0[vidx.0]
    }
}

impl<T> IndexMut<NodeIndex> for NodeMap<T> {
    fn index_mut(&mut self, vidx: NodeIndex) -> &mut T {
        &mut self.0[vidx.0]
    }
}

impl<'a, T> IntoIterator for &'a NodeMap<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
