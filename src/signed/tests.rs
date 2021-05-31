use super::*;
use crate::{
    nodes::{BoolNodeMap, NodeCount, NodeIndex},
    KeyBox, MultiKeychain, PartialMultisignature, Signable,
};
use codec::{Decode, Encode};

#[derive(Clone, Debug, PartialEq)]
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
struct TestMultiKeychain {
    node_count: NodeCount,
    index: NodeIndex,
}

impl Index for TestMultiKeychain {
    fn index(&self) -> NodeIndex {
        self.index
    }
}

#[derive(Debug, Clone, Encode, Decode)]
struct TestSignature {
    msg: Vec<u8>,
    index: NodeIndex,
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
            let unchecked_msg = signed_msg.unchecked;
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
    let mut unchecked_msg = signed_msg.unchecked;
    unchecked_msg.signature.index = 1.into();

    assert!(
        unchecked_msg.check(&keychain).is_err(),
        "wrong index makes wrong signature"
    );
}

#[derive(Clone, Debug, Encode, Decode, PartialEq)]
struct TestPartialMultisignature {
    msg: Vec<u8>,
    signers: BoolNodeMap,
}

impl PartialMultisignature for TestPartialMultisignature {
    type Signature = TestSignature;

    fn add_signature(&mut self, signature: &Self::Signature, index: NodeIndex) {
        if self.msg == signature.msg {
            self.signers.set(index);
        }
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

    fn is_complete(&self, partial: &Self::PartialMultisignature) -> bool {
        return 3 * partial.signers.true_indices().count() > 2 * self.node_count.0;
    }

    fn verify_partial(&self, msg: &[u8], partial: &Self::PartialMultisignature) -> bool {
        msg == partial.msg
    }
}

#[test]
fn test_valid_partial_multisignature() {
    let index: NodeIndex = 0.into();
    let node_count: NodeCount = 1.into();
    let keychain = TestMultiKeychain { node_count, index };

    let msg = indexed_test_message(index.0);
    let partial = PartiallyMultisigned::sign(msg, &keychain);
    let unchecked_msg = partial.unchecked;

    assert!(
        unchecked_msg.check_partial(&keychain).is_ok(),
        "Partially signed message should be valid"
    );
}

#[test]
fn test_invalid_partial_multisignature() {
    let index: NodeIndex = 0.into();
    let node_count: NodeCount = 1.into();
    let keychain = TestMultiKeychain { node_count, index };

    let msg = indexed_test_message(index.0);
    let partial = PartiallyMultisigned::sign(msg, &keychain);
    let mut unchecked_msg = partial.unchecked;
    unchecked_msg.signature.msg = "Bye".as_bytes().to_vec();

    assert!(
        unchecked_msg.check_partial(&keychain).is_err(),
        "Wrong signature can't pass partial check",
    );
}

#[test]
fn test_incomplete_multisignature() {
    let msg = indexed_test_message(0);
    let index: NodeIndex = 0.into();
    let node_count: NodeCount = 2.into();
    let keychain = TestMultiKeychain { node_count, index };

    let partial = PartiallyMultisigned::sign(msg, &keychain);
    assert!(
        partial._try_into_complete(&keychain).is_err(),
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

    let mut partial = PartiallyMultisigned::sign(msg.signable.clone(), &keychains[0]);
    for &i in [1, 2, 3, 4].iter() {
        assert!(!keychains[i].is_complete(&partial.unchecked.signature));
        msg.index = i.into();
        let signed = Signed::sign(msg.clone(), &keychains[i]);
        partial.add_signature(signed);
    }
    assert!(
        partial._try_into_complete(&keychains[5]).is_ok(),
        "5 signatures should form a complete signature"
    );
}
