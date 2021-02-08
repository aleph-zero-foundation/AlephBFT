use crate::{
    nodes::{NodeIndex, NodeMap},
    traits::HashT,
};

#[derive(Clone, Default)]
pub(crate) struct Vertex<H: HashT> {
    creator: NodeIndex,
    parents: NodeMap<Option<H>>,
    hash: H,
    best_block: H,
}

impl<H: HashT> Vertex<H> {
    pub(crate) fn new(
        creator: NodeIndex,
        hash: H,
        parents: NodeMap<Option<H>>,
        best_block: H,
    ) -> Self {
        Vertex {
            creator,
            hash,
            parents,
            best_block,
        }
    }
    pub(crate) fn _creator(&self) -> NodeIndex {
        self.creator
    }
    pub(crate) fn _hash(&self) -> H {
        self.hash
    }
    pub(crate) fn best_block(&self) -> H {
        self.best_block
    }
}
