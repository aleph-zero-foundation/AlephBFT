use futures::{Sink, Stream};
use std::{
	fmt::{Debug, Display},
	hash::Hash,
};

use crate::skeleton::{Error, Message};

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
	+ Debug
	+ Display
	+ Hash
	+ Serialize
	+ DeserializeOwned
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
		+ Debug
		+ Display
		+ Hash
		+ Serialize
		+ DeserializeOwned
{
}

pub trait Environment {
	/// Unique identifiers for nodes
	type NodeId: NodeIdT;
	/// Unique identifiers for units.
	type Hash: HashT;
	/// The ID of a consensus protocol instance.
	type InstanceId: HashT;

	type Crypto;
	type In: Stream<Item = Message<Self::Hash>> + Send + Unpin;
	type Out: Sink<Message<Self::Hash>, Error = Self::Error> + Send + Unpin;
	type Error: Send + Sync;

	fn finalize_block(&self, _: Self::Hash);
	fn best_block(&self) -> Self::Hash;
	// sth needed in the future for randomness
	fn crypto(&self) -> Self::Crypto;
	fn consensus_data(&self) -> (Self::Out, Self::In);
	fn hash(data: &[u8]) -> Self::Hash;
	// fn verify_signature(
	//     hash: &Self::Hash,
	//     public_key: &Self::ValidatorId,
	//     signature: &<Self::ValidatorSecret as ValidatorSecret>::Signature,
	// ) -> bool;
}
