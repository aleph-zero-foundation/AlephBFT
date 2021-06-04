use crate::{
    nodes::{BoolNodeMap, NodeCount, NodeIndex},
    signed::*,
    Index, KeyBox, MultiKeychain, PartialMultisignature, Signable,
};
use codec::{Decode, Encode};

#[derive(Clone, Debug, Default, PartialEq)]
struct TestMessage {
    msg: Vec<u8>,
}

impl Signable for TestMessage {
    type Hash = Vec<u8>;
    fn hash(&self) -> Self::Hash {
        self.msg.clone()
    }
}

fn indexed_test_message(i: usize) -> Indexed<TestMessage> {
    Indexed::new(
        TestMessage {
            msg: "Hello".as_bytes().to_vec(),
        },
        i.into(),
    )
}

#[derive(Debug, Clone)]
pub(crate) struct TestMultiKeychain {
    node_count: NodeCount,
    index: NodeIndex,
}

impl TestMultiKeychain {
    pub(crate) fn new(node_count: NodeCount, index: NodeIndex) -> Self {
        TestMultiKeychain { node_count, index }
    }
}

impl Index for TestMultiKeychain {
    fn index(&self) -> NodeIndex {
        self.index
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct TestSignature {
    pub(crate) msg: Vec<u8>,
    pub(crate) index: NodeIndex,
}

impl KeyBox for TestMultiKeychain {
    type Signature = TestSignature;
    fn sign(&self, msg: &[u8]) -> Self::Signature {
        TestSignature {
            msg: msg.to_vec(),
            index: self.index,
        }
    }
    fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool {
        sgn.msg == msg && sgn.index == index
    }
}

#[test]
fn test_valid_signatures() {
    let node_count: NodeCount = 7.into();
    let keychains: Vec<TestMultiKeychain> = (0_usize..node_count.0)
        .map(|i| TestMultiKeychain {
            node_count,
            index: i.into(),
        })
        .collect();
    for i in 0..node_count.0 {
        for j in 0..node_count.0 {
            let msg = indexed_test_message(i);
            let signed_msg = Signed::sign(msg.clone(), &keychains[i]);
            let unchecked_msg = signed_msg.into_unchecked();
            assert!(
                unchecked_msg.check(&keychains[j]).is_ok(),
                "Signed message should be valid"
            );
        }
    }
}

#[test]
fn test_invalid_signatures() {
    let node_count: NodeCount = 1.into();
    let index: NodeIndex = 0.into();
    let keychain = TestMultiKeychain { node_count, index };
    let msg = indexed_test_message(index.0);
    let signed_msg = Signed::sign(msg, &keychain);
    let mut unchecked_msg = signed_msg.into_unchecked();
    unchecked_msg.signature_mut().index = 1.into();

    assert!(
        unchecked_msg.check(&keychain).is_err(),
        "wrong index makes wrong signature"
    );
}

#[derive(Clone, Debug, Default, Encode, Decode, PartialEq)]
pub(crate) struct TestPartialMultisignature {
    pub(crate) msg: Vec<u8>,
    pub(crate) signers: BoolNodeMap,
}

impl PartialMultisignature for TestPartialMultisignature {
    type Signature = TestSignature;

    fn add_signature(mut self, signature: &Self::Signature, index: NodeIndex) -> Self {
        assert_eq!(self.msg, signature.msg);
        self.signers.set(index);
        self
    }
}

impl MultiKeychain for TestMultiKeychain {
    type PartialMultisignature = TestPartialMultisignature;

    fn from_signature(
        &self,
        signature: &Self::Signature,
        index: NodeIndex,
    ) -> Self::PartialMultisignature {
        let mut signers = BoolNodeMap::with_capacity(self.node_count);
        signers.set(index);
        TestPartialMultisignature {
            msg: signature.msg.clone(),
            signers,
        }
    }

    fn is_complete(&self, msg: &[u8], partial: &Self::PartialMultisignature) -> bool {
        return msg == partial.msg
            && 3 * partial.signers.true_indices().count() > 2 * self.node_count.0;
    }
}

#[test]
fn test_incomplete_multisignature() {
    let msg = indexed_test_message(0);
    let index: NodeIndex = 0.into();
    let node_count: NodeCount = 2.into();
    let keychain = TestMultiKeychain { node_count, index };

    let partial = PartiallyMultisigned::sign(msg, &keychain);
    assert!(
        !partial.is_complete(),
        "One signature does not form a complete multisignature",
    );
}

#[test]
fn test_multisignatures() {
    let mut msg = indexed_test_message(0);
    let node_count: NodeCount = 7.into();
    let keychains: Vec<TestMultiKeychain> = (0..node_count.0)
        .map(|i| TestMultiKeychain {
            node_count,
            index: i.into(),
        })
        .collect();

    let mut partial = PartiallyMultisigned::sign(msg.as_signable().clone(), &keychains[0]);
    for (i, keychain) in keychains.iter().enumerate().skip(1).take(4) {
        let hash = partial.as_unchecked().as_signable().hash().clone();
        assert!(!keychain.is_complete(hash.as_ref(), partial.as_unchecked().signature()));
        *msg.index_mut() = i.into();
        let signed = Signed::sign(msg.clone(), keychain);
        partial = partial.add_signature(signed, keychain);
    }
    assert!(
        partial.is_complete(),
        "5 signatures should form a complete signature {:?}",
        partial
    );
}
