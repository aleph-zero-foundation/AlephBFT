use aleph_bft_types::{Index, NodeIndex, SignatureSet};
use codec::{Decode, Encode};
use std::hash::Hash;

#[derive(Clone, Eq, PartialEq, Hash, Debug, Default, Encode, Decode)]
pub struct Signature {
    msg: Vec<u8>,
    index: NodeIndex,
}

impl Signature {
    pub fn new(msg: Vec<u8>, index: NodeIndex) -> Self {
        Self { msg, index }
    }

    pub fn msg(&self) -> &Vec<u8> {
        &self.msg
    }
}

impl Index for Signature {
    fn index(&self) -> NodeIndex {
        self.index
    }
}

pub type PartialMultisignature = SignatureSet<Signature>;
