use futures::{Sink, Stream};
use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

use crate::skeleton::Message;

//use datasize::DataSize;
use serde::{de::DeserializeOwned, Serialize};

//use crate::NodeRng;

pub trait NodeIdT: Clone + Display + Debug + Send + Eq + Hash + 'static {}
impl<I> NodeIdT for I where I: Clone + Display + Debug + Send + Eq + Hash + 'static {}

/// A hash, as an identifier for a block or unit.
pub trait HashT:
    Eq
    + Ord
    + Copy
    + Clone
    + Default
    + Send
    + Sync
    + Unpin
    + Debug
    + Display
    + Hash
    + Serialize
    + DeserializeOwned
    + From<u32> // TODO remove after adding proper hash impl
{
}

impl<H> HashT for H where
    H: Eq
        + Ord
        + Copy
        + Clone
        + Send
        + Sync
        + Default
        + Unpin
        + Debug
        + Display
        + Hash
        + Serialize
        + DeserializeOwned
        + From<u32> // TODO remove after adding proper hash impl
{
}

pub trait Environment {
    /// Unique identifiers for nodes
    type NodeId: NodeIdT;
    /// Hash type for units.
    type Hash: HashT;
    /// Hash type for blocks.
    type BlockHash: HashT;
    /// The ID of a consensus protocol instance.
    type InstanceId: HashT;

    type Crypto;
    type In: Stream<Item = Message<Self::BlockHash, Self::Hash>> + Send + Unpin;
    type Out: Sink<Message<Self::BlockHash, Self::Hash>, Error = Self::Error> + Send + Unpin;
    type Error: Send + Sync;

    fn finalize_block(&mut self, _h: Self::BlockHash);
    fn check_extends_finalized(&self, _h: Self::BlockHash) -> bool;
    fn best_block(&self) -> Self::BlockHash;
    fn check_available(&self, h: Self::BlockHash) -> bool;
    // sth needed in the future for randomness
    fn crypto(&self) -> Self::Crypto;
    fn consensus_data(&self) -> (Self::Out, Self::In);
    fn hash(data: &[u8]) -> Self::Hash;
}
