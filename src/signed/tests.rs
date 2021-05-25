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

#[derive(Debug)]
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
fn test_signatures() {
    let node_count: NodeCount = 7.into();
    let keychains: Vec<TestMultiKeychain> = (0_usize..node_count.0)
        .map(|i| TestMultiKeychain {
            node_count,
            index: i.into(),
        })
        .collect();
    for i in 0_usize..7 {
        for j in 0_usize..7 {
            let msg = Indexed::new(
                TestMessage {
                    msg: "Hello".as_bytes().to_vec(),
                },
                i.into(),
            );
            let signed_msg = Signed::sign(&keychains[i], msg.clone());
            let unchecked_msg = signed_msg.unchecked;
            let signed_msg = unchecked_msg
                .check(&keychains[j])
                .expect("Signed message should be valid");
            assert_eq!(signed_msg.as_signable(), &msg)
        }
    }
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
fn test_multisignatures() {
    let mut msg = Indexed::new(
        TestMessage {
            msg: "Hello".as_bytes().to_vec(),
        },
        0.into(),
    );
    let node_count: NodeCount = 7.into();
    let keychains: Vec<TestMultiKeychain> = (0_usize..node_count.0)
        .map(|i| TestMultiKeychain {
            node_count,
            index: i.into(),
        })
        .collect();

    let signed = Signed::sign(&keychains[0], msg.clone());

    let mut partial = signed._into_partially_multisigned();
    for &i in [1, 2, 3, 4].iter() {
        assert!(!keychains[i].is_complete(&partial.unchecked.signature));
        msg.index = i.into();
        let signed = Signed::sign(&keychains[i], msg.clone());
        partial.add_signature(signed);
    }
    assert!(
        partial._try_into_complete(&keychains[5]).is_ok(),
        "5 signatures should form a complete signature"
    );
}
