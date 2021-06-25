use crate::{
    nodes::{NodeCount, NodeIndex},
    signed::*,
    Index, KeyBox, Signable,
};
use async_trait::async_trait;
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

fn test_message() -> TestMessage {
    TestMessage {
        msg: "Hello".as_bytes().to_vec(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct TestSignature {
    pub(crate) msg: Vec<u8>,
    pub(crate) index: NodeIndex,
}

#[derive(Clone, Debug)]
pub(crate) struct TestKeyBox {
    count: NodeCount,
    index: NodeIndex,
}

impl TestKeyBox {
    fn new(count: NodeCount, index: NodeIndex) -> Self {
        TestKeyBox { count, index }
    }
}

impl Index for TestKeyBox {
    fn index(&self) -> NodeIndex {
        self.index
    }
}

#[async_trait]
impl KeyBox for TestKeyBox {
    type Signature = TestSignature;

    fn node_count(&self) -> NodeCount {
        self.count
    }

    async fn sign(&self, msg: &[u8]) -> Self::Signature {
        TestSignature {
            msg: msg.to_vec(),
            index: self.index,
        }
    }

    fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool {
        index == sgn.index && msg == sgn.msg
    }
}

pub(crate) type TestMultiKeychain = DefaultMultiKeychain<TestKeyBox>;

pub(crate) type TestPartialMultisignature = SignatureSet<TestSignature>;

pub(crate) fn test_multi_keychain(node_count: NodeCount, index: NodeIndex) -> TestMultiKeychain {
    let key_box = TestKeyBox::new(node_count, index);
    DefaultMultiKeychain::new(key_box)
}

#[tokio::test]
async fn test_valid_signatures() {
    let node_count: NodeCount = 7.into();
    let keychains: Vec<TestMultiKeychain> = (0_usize..node_count.0)
        .map(|i| test_multi_keychain(node_count, i.into()))
        .collect();
    for i in 0..node_count.0 {
        for j in 0..node_count.0 {
            let msg = test_message();
            let signed_msg = Signed::sign_with_index(msg.clone(), &keychains[i]).await;
            let unchecked_msg = signed_msg.into_unchecked();
            assert!(
                unchecked_msg.check(&keychains[j]).is_ok(),
                "Signed message should be valid"
            );
        }
    }
}

#[tokio::test]
async fn test_invalid_signatures() {
    let node_count: NodeCount = 1.into();
    let index: NodeIndex = 0.into();
    let keychain = test_multi_keychain(node_count, index);
    let msg = test_message();
    let signed_msg = Signed::sign_with_index(msg, &keychain).await;
    let mut unchecked_msg = signed_msg.into_unchecked();
    unchecked_msg.signature_mut().index = 1.into();

    assert!(
        unchecked_msg.check(&keychain).is_err(),
        "wrong index makes wrong signature"
    );
}

#[tokio::test]
async fn test_incomplete_multisignature() {
    let msg = test_message();
    let index: NodeIndex = 0.into();
    let node_count: NodeCount = 2.into();
    let keychain = test_multi_keychain(node_count, index);

    let partial = PartiallyMultisigned::sign(msg, &keychain).await;
    assert!(
        !partial.is_complete(),
        "One signature does not form a complete multisignature",
    );
}

#[tokio::test]
async fn test_multisignatures() {
    let msg = test_message();
    let node_count: NodeCount = 7.into();
    let keychains: Vec<TestMultiKeychain> = (0..node_count.0)
        .map(|i| test_multi_keychain(node_count, i.into()))
        .collect();

    let mut partial = PartiallyMultisigned::sign(msg.clone(), &keychains[0]).await;
    for keychain in keychains.iter().skip(1).take(4) {
        assert!(!partial.is_complete());
        let signed = Signed::sign_with_index(msg.clone(), keychain).await;
        partial = partial.add_signature(signed, keychain);
    }
    assert!(
        partial.is_complete(),
        "5 signatures should form a complete signature {:?}",
        partial
    );
}
