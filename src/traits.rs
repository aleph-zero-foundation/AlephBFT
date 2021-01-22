use std::{
	fmt::{Debug, Display},
	hash::Hash,
};

use crate::{
	nodes::NodeMap,
};


//use datasize::DataSize;
use serde::{de::DeserializeOwned, Serialize};

//use crate::NodeRng;

pub trait NodeIdT: Clone + Display + Debug + Send + Eq + Hash + 'static {}
impl<I> NodeIdT for I where I: Clone + Display + Debug + Send + Eq + Hash + 'static {}


/// A hash, as an identifier for a block or unit.
pub trait HashT:
	Eq + Ord + Copy + Clone + Debug + Display + Hash + Serialize + DeserializeOwned
{
}
impl<H> HashT for H where
	H: Eq + Ord + Copy + Clone + Debug + Display + Hash + Serialize + DeserializeOwned
{
}


pub trait UnitT<H: HashT>: Clone + Debug + Eq + Ord + Hash {
	fn parent_hashes(&self) -> NodeMap<Option<H>>;
	fn hash(&self) -> H;
}



pub trait Context: Clone + Debug + Eq + Ord + Hash {
	/// Unique identifiers for nodes
	type NodeId: NodeIdT;
	/// Unique identifiers for units.
	type Hash: HashT;
	/// The ID of a consensus protocol instance.
	type InstanceId: HashT;

	type Unit: UnitT<Self::Hash>;

	fn hash(data: &[u8]) -> Self::Hash;

	// fn verify_signature(
	//     hash: &Self::Hash,
	//     public_key: &Self::ValidatorId,
	//     signature: &<Self::ValidatorSecret as ValidatorSecret>::Signature,
	// ) -> bool;
}

