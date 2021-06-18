use aleph_bft::{NodeCount, NodeIndex};
use async_trait::async_trait;
use codec::{Decode, Encode};
use sha3::{Digest, Sha3_256};

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) struct Hasher256;

impl aleph_bft::Hasher for Hasher256 {
    type Hash = [u8; 32];

    fn hash(x: &[u8]) -> Self::Hash {
        let digest = Sha3_256::digest(x);
        digest.into()
    }
}

// This is not cryptographically secure, mocked only for demonstration purposes
#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode)]
pub(crate) struct Signature;

// This is not cryptographically secure, mocked only for demonstration purposes
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct PartialMultisignature {
    signed_by: Vec<NodeIndex>,
}

impl aleph_bft::PartialMultisignature for PartialMultisignature {
    type Signature = Signature;
    fn add_signature(self, _: &Self::Signature, index: NodeIndex) -> Self {
        let Self { mut signed_by } = self;
        for id in &signed_by {
            if *id == index {
                return Self { signed_by };
            }
        }
        signed_by.push(index);
        Self { signed_by }
    }
}

// This is not cryptographically secure, mocked only for demonstration purposes
#[derive(Clone)]
pub(crate) struct KeyBox {
    pub(crate) count: usize,
    pub(crate) index: NodeIndex,
}

#[async_trait]
impl aleph_bft::KeyBox for KeyBox {
    type Signature = Signature;

    fn node_count(&self) -> NodeCount {
        self.count.into()
    }

    async fn sign(&self, _msg: &[u8]) -> Self::Signature {
        Signature {}
    }
    fn verify(&self, _msg: &[u8], _sgn: &Self::Signature, _index: NodeIndex) -> bool {
        true
    }
}

impl aleph_bft::MultiKeychain for KeyBox {
    type PartialMultisignature = PartialMultisignature;
    fn from_signature(&self, _: &Self::Signature, index: NodeIndex) -> Self::PartialMultisignature {
        let signed_by = vec![index];
        PartialMultisignature { signed_by }
    }
    fn is_complete(&self, _: &[u8], partial: &Self::PartialMultisignature) -> bool {
        (self.count * 2) / 3 < partial.signed_by.len()
    }
}

impl aleph_bft::Index for KeyBox {
    fn index(&self) -> NodeIndex {
        self.index
    }
}
