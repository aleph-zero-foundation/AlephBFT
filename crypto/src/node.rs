use codec::{Decode, Encode, Error, Input, Output};
use derive_more::{Add, AddAssign, From, Into, Sub, SubAssign, Sum};
use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    ops::{Div, Index as StdIndex, Mul},
    vec,
};

/// The index of a node
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, From, Into)]
pub struct NodeIndex(pub usize);

impl Encode for NodeIndex {
    fn encode_to<T: Output + ?Sized>(&self, dest: &mut T) {
        let val = self.0 as u64;
        let bytes = val.to_le_bytes();
        dest.write(&bytes);
    }
}

impl Decode for NodeIndex {
    fn decode<I: Input>(value: &mut I) -> Result<Self, Error> {
        let mut arr = [0u8; 8];
        value.read(&mut arr)?;
        let val: u64 = u64::from_le_bytes(arr);
        Ok(NodeIndex(val as usize))
    }
}

/// Indicates that an implementor has been assigned some index.
pub trait Index {
    fn index(&self) -> NodeIndex;
}

/// Node count. Right now it doubles as node weight in many places in the code, in the future we
/// might need a new type for that.
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    Add,
    AddAssign,
    From,
    Into,
    Sub,
    SubAssign,
    Sum,
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

impl NodeCount {
    pub fn into_range(self) -> core::ops::Range<NodeIndex> {
        core::ops::Range {
            start: 0.into(),
            end: self.0.into(),
        }
    }

    pub fn into_iterator(self) -> impl Iterator<Item = NodeIndex> {
        (0..self.0).map(NodeIndex)
    }

    /// If this is the total node count, what number of nodes is required for secure consensus.
    pub fn consensus_threshold(&self) -> NodeCount {
        (*self * 2) / 3 + NodeCount(1)
    }
}

/// A container keeping items indexed by NodeIndex.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default, Decode, Encode, From)]
pub struct NodeMap<T>(Vec<Option<T>>);

impl<T> NodeMap<T> {
    /// Constructs a new node map with a given length.
    pub fn with_size(len: NodeCount) -> Self
    where
        T: Clone,
    {
        let v = vec![None; len.into()];
        NodeMap(v)
    }

    pub fn from_hashmap(len: NodeCount, hashmap: HashMap<NodeIndex, T>) -> Self
    where
        T: Clone,
    {
        let v = vec![None; len.into()];
        let mut nm = NodeMap(v);
        for (id, item) in hashmap.into_iter() {
            nm.insert(id, item);
        }
        nm
    }

    pub fn size(&self) -> NodeCount {
        self.0.len().into()
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeIndex, &T)> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(idx, maybe_value)| Some((NodeIndex(idx), maybe_value.as_ref()?)))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (NodeIndex, &mut T)> {
        self.0
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, maybe_value)| Some((NodeIndex(idx), maybe_value.as_mut()?)))
    }

    fn into_iter(self) -> impl Iterator<Item = (NodeIndex, T)>
    where
        T: 'static,
    {
        self.0
            .into_iter()
            .enumerate()
            .filter_map(|(idx, maybe_value)| Some((NodeIndex(idx), maybe_value?)))
    }

    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.iter().map(|(_, value)| value)
    }

    pub fn into_values(self) -> impl Iterator<Item = T>
    where
        T: 'static,
    {
        self.into_iter().map(|(_, value)| value)
    }

    pub fn get(&self, node_id: NodeIndex) -> Option<&T> {
        self.0[node_id.0].as_ref()
    }

    pub fn get_mut(&mut self, node_id: NodeIndex) -> Option<&mut T> {
        self.0[node_id.0].as_mut()
    }

    pub fn insert(&mut self, node_id: NodeIndex, value: T) {
        self.0[node_id.0] = Some(value)
    }

    pub fn delete(&mut self, node_id: NodeIndex) {
        self.0[node_id.0] = None
    }

    pub fn to_subset(&self) -> NodeSubset {
        NodeSubset(self.0.iter().map(Option::is_some).collect())
    }

    pub fn item_count(&self) -> usize {
        self.iter().count()
    }
}

impl<T: 'static> IntoIterator for NodeMap<T> {
    type Item = (NodeIndex, T);
    type IntoIter = Box<dyn Iterator<Item = (NodeIndex, T)>>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.into_iter())
    }
}

impl<'a, T> IntoIterator for &'a NodeMap<T> {
    type Item = (NodeIndex, &'a T);
    type IntoIter = Box<dyn Iterator<Item = (NodeIndex, &'a T)> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl<'a, T> IntoIterator for &'a mut NodeMap<T> {
    type Item = (NodeIndex, &'a mut T);
    type IntoIter = Box<dyn Iterator<Item = (NodeIndex, &'a mut T)> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter_mut())
    }
}

impl<T: fmt::Display> fmt::Display for NodeMap<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        let mut it = self.iter().peekable();
        while let Some((id, item)) = it.next() {
            write!(f, "({}, {})", id.0, item)?;
            if it.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")?;
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct NodeSubset(bit_vec::BitVec<u32>);

impl NodeSubset {
    pub fn with_size(capacity: NodeCount) -> Self {
        NodeSubset(bit_vec::BitVec::from_elem(capacity.0, false))
    }

    pub fn insert(&mut self, i: NodeIndex) {
        self.0.set(i.0, true);
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn elements(&self) -> impl Iterator<Item = NodeIndex> + '_ {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(i, b)| if b { Some(i.into()) } else { None })
    }

    pub fn complement(&self) -> NodeSubset {
        let mut result = self.0.clone();
        result.negate();
        NodeSubset(result)
    }

    pub fn len(&self) -> usize {
        self.elements().count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Encode for NodeSubset {
    fn encode_to<T: Output + ?Sized>(&self, dest: &mut T) {
        (self.0.len() as u32).encode_to(dest);
        self.0.to_bytes().encode_to(dest);
    }
}

impl Decode for NodeSubset {
    fn decode<I: Input>(input: &mut I) -> Result<Self, Error> {
        let capacity = u32::decode(input)? as usize;
        let bytes = Vec::decode(input)?;
        let mut bv = bit_vec::BitVec::from_bytes(&bytes);
        // Length should be capacity rounded up to the closest multiple of 8
        if bv.len() != 8 * ((capacity + 7) / 8) {
            return Err(Error::from(
                "Length of bitvector inconsistent with encoded capacity.",
            ));
        }
        while bv.len() > capacity {
            if bv.pop() != Some(false) {
                return Err(Error::from(
                    "Non-canonical encoding. Trailing bits should be all 0.",
                ));
            }
        }
        bv.truncate(capacity);
        Ok(NodeSubset(bv))
    }
}

impl StdIndex<NodeIndex> for NodeSubset {
    type Output = bool;

    fn index(&self, vidx: NodeIndex) -> &bool {
        &self.0[vidx.0]
    }
}

impl fmt::Display for NodeSubset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut v: Vec<usize> = self.elements().map(|n| n.into()).collect();
        v.sort();
        write!(f, "{:?}", v)
    }
}

#[cfg(test)]
mod tests {

    use crate::node::{NodeIndex, NodeSubset};
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
    fn bool_node_map_decoding_works() {
        for len in 0..12 {
            for mask in 0..(1 << len) {
                let mut bnm = NodeSubset::with_size(len.into());
                for i in 0..len {
                    if (1 << i) & mask != 0 {
                        bnm.insert(i.into());
                    }
                }
                let encoded: Vec<_> = bnm.encode();
                let decoded =
                    NodeSubset::decode(&mut encoded.as_slice()).expect("decode should work");
                assert!(decoded == bnm);
            }
        }
    }

    #[test]
    fn bool_node_map_decoding_deals_with_trailing_zeros() {
        let mut encoded = vec![1, 0, 0, 0];
        encoded.extend(vec![128u8].encode());
        //128 encodes bit-vec 10000000
        let decoded = NodeSubset::decode(&mut encoded.as_slice()).expect("decode should work");
        assert_eq!(decoded, NodeSubset([true].iter().cloned().collect()));

        let mut encoded = vec![1, 0, 0, 0];
        encoded.extend(vec![129u8].encode());
        //129 encodes bit-vec 10000001
        assert!(NodeSubset::decode(&mut encoded.as_slice()).is_err());
    }

    #[test]
    fn bool_node_map_decoding_deals_with_too_long_bitvec() {
        let mut encoded = vec![1, 0, 0, 0];
        encoded.extend(vec![128u8, 0].encode());
        //[128, 0] encodes bit-vec 1000000000000000
        assert!(NodeSubset::decode(&mut encoded.as_slice()).is_err());
    }

    #[test]
    fn decoding_bool_node_map_works() {
        let bool_node_map = NodeSubset([true, false, true, true, true].iter().cloned().collect());
        let encoded: Vec<_> = bool_node_map.encode();
        let decoded = NodeSubset::decode(&mut encoded.as_slice()).expect("decode should work");
        assert_eq!(decoded, bool_node_map);
    }

    #[test]
    fn test_bool_node_map_has_efficient_encoding() {
        let mut bnm = NodeSubset::with_size(100.into());
        for i in 0..50 {
            bnm.insert(i.into())
        }
        assert!(bnm.encode().len() < 20);
    }
}
