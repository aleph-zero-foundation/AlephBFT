use std::{
	collections::HashMap,
	sync::{Arc},
};
use crate::{
	traits::{Context, UnitT},
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

	pub(crate) fn contains_unit(&self, u: &C::Unit) -> bool {
		self.vertex_by_hash.contains_key(&u.hash())
	}

	//outputs true if adding succesful
	//TODO: replace with custom error type
	pub(crate) fn add_vertex(&mut self, u: &C::Unit) -> bool {
		if self.contains_unit(u) {
			return false;
		}
		let parents = u.parent_hashes();
		for par_hash in parents.iter() {
			if let Some(hash) =  par_hash {
				if !self.vertex_by_hash.contains_key(hash) {
					return false;
				}
			}
		}
		self.vertex_by_hash.insert(u.hash(), Arc::new(self.vertex_from_unit(u).unwrap()));
		true
	}

	pub(crate) fn vertex_from_unit(&self, u: &C::Unit) -> Option<Vertex<C>> {
		let parent_hashes = u.parent_hashes();
		let mut parent_map: NodeMap<Option<Arc<Vertex<C>>>> = vec![None; parent_hashes.len()].into();
		for (i, par_hash) in parent_hashes.enumerate() {
			if let Some(hash) = par_hash {
				let maybe_parent = self.vertex_by_hash.get(hash);
				match maybe_parent {
					Some(parent) => parent_map[i] = Some(parent.clone()),
					None => return None,
				}
			}
		}
		let vertex = Vertex {
			parents: parent_map,
			hash: u.hash(),
		};
		return Some(vertex);
	}
}
