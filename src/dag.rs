use crate::{nodes::NodeMap, traits::Environment};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub(crate) struct Vertex<E: Environment> {
	creator: usize,
	parents: NodeMap<Option<Arc<Vertex<E>>>>,
	hash: E::Hash,
}

impl<E: Environment> Vertex<E> {
	pub(crate) fn creator(&self) -> usize {
		self.creator
	}
	pub(crate) fn hash(&self) -> E::Hash {
		self.hash.clone()
	}
}

pub(crate) struct Dag<E: Environment> {
	vertex_by_hash: HashMap<E::Hash, Arc<Vertex<E>>>,
	post_insert: Vec<Box<dyn Fn(Arc<Vertex<E>>) + Send + Sync + 'static>>,
}

impl<E: Environment> Dag<E> {
	pub(crate) fn new() -> Dag<E> {
		Dag {
			vertex_by_hash: HashMap::new(),
			post_insert: vec![],
		}
	}

	pub(crate) fn contains_hash(&self, hash: &E::Hash) -> bool {
		self.vertex_by_hash.contains_key(hash)
	}

	pub(crate) fn add_vertex(
		&mut self,
		hash: E::Hash,
		creator: usize,
		parents: NodeMap<Option<Arc<Vertex<E>>>>,
	) {
		let vertex = Vertex {
			parents,
			hash,
			creator,
		};
		self.vertex_by_hash.insert(vertex.hash, Arc::new(vertex));
	}

	pub(crate) fn register_post_insert_hook(
		&mut self,
		hook: Box<dyn Fn(Arc<Vertex<E>>) + Send + Sync + 'static>,
	) {
		self.post_insert.push(hook);
	}
}
