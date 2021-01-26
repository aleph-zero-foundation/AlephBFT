use std::{
	collections::HashMap,
	sync::Arc,
};
use crate::{
	traits::Context,
	nodes::NodeMap,
};


#[derive(Clone)]
pub(crate) struct Vertex<C: Context> {
	parents: NodeMap<Option<Arc<Vertex<C>>>>,
	hash: C::Hash
}


pub(crate) struct Dag<C: Context> {
	vertex_by_hash: HashMap<C::Hash,Arc<Vertex<C>>>,
}

impl<C: Context> Dag<C> {
	pub(crate) fn new() -> Dag<C> {
		Dag {
			vertex_by_hash: HashMap::new(),
		}
	}

	pub(crate) fn contains_hash(&self, hash: &C::Hash) -> bool {
		self.vertex_by_hash.contains_key(hash)
	}

	pub(crate) fn add_vertex(&mut self, hash: C::Hash, parents: NodeMap<Option<Arc<Vertex<C>>>>) {
		let vertex = Vertex {
			parents,
			hash,
		};
		self.vertex_by_hash.insert(vertex.hash, Arc::new(vertex));
	}

}
