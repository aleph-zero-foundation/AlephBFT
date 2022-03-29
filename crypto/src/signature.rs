use crate::{Index, NodeCount, NodeIndex, NodeMap};
use async_trait::async_trait;
use codec::{Codec, Decode, Encode};
use log::warn;
use std::{fmt::Debug, marker::PhantomData};

/// The type used as a signature.
///
/// The Signature typically does not contain the index of the node who signed the data.
pub trait Signature: Debug + Clone + Codec + Send + Sync + Eq + 'static {}

impl<T: Debug + Clone + Codec + Send + Sync + Eq + 'static> Signature for T {}

/// Abstraction of the signing data and verifying signatures.
///
/// A typical implementation of KeyBox would be a collection of `N` public keys,
/// an index `i` and a single private key corresponding to the public key number `i`.
/// The meaning of sign is then to produce a signature `s` using the given private key,
/// and `verify(msg, s, j)` is to verify whether the signature s under the message msg is
/// correct with respect to the public key of the jth node.
#[async_trait]
pub trait KeyBox: Index + Clone + Send + Sync + 'static {
    type Signature: Signature;

    /// Returns the total number of known public keys.
    fn node_count(&self) -> NodeCount;
    /// Signs a message `msg`.
    async fn sign(&self, msg: &[u8]) -> Self::Signature;
    /// Verifies whether a node with `index` correctly signed the message `msg`.
    /// Should always return false for indices outside the node range.
    fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool;
}

/// A type to which signatures can be aggregated.
///
/// Any signature can be added to multisignature.
/// After adding sufficiently many signatures, the partial multisignature becomes a "complete"
/// multisignature.
/// Whether a multisignature is complete, can be verified with [`MultiKeychain::is_complete`] method.
/// The signature and the index passed to the `add_signature` method are required to be valid.
pub trait PartialMultisignature: Signature {
    type Signature: Signature;
    /// Adds the signature.
    #[must_use = "consumes the original and returns the aggregated signature which should be used"]
    fn add_signature(self, signature: &Self::Signature, index: NodeIndex) -> Self;
}

/// Extends KeyBox with multisigning functionalities.
///
/// A single Signature can be rised to a Multisignature.
/// Allows to verify whether a partial multisignature is complete (and valid).
pub trait MultiKeychain: KeyBox {
    type PartialMultisignature: PartialMultisignature<Signature = Self::Signature>;
    /// Transform a single signature to a multisignature consisting of the signature.
    fn from_signature(
        &self,
        signature: &Self::Signature,
        index: NodeIndex,
    ) -> Self::PartialMultisignature;
    /// Checks if enough signatures have beed added.
    fn is_complete(&self, msg: &[u8], partial: &Self::PartialMultisignature) -> bool;
}

/// A set of signatures of a subset of nodes serving as a (partial) multisignature
pub type SignatureSet<S> = NodeMap<S>;

impl<S: Signature> PartialMultisignature for SignatureSet<S> {
    type Signature = S;

    #[must_use = "consumes the original and returns the aggregated signature which should be used"]
    fn add_signature(mut self, signature: &Self::Signature, index: NodeIndex) -> Self {
        self.insert(index, signature.clone());
        self
    }
}

/// Data which can be signed.
///
/// Signable data should provide a hash of type [`Self::Hash`] which is build from all parts of the
/// data which should be signed. The type [`Self::Hash`] should implement [`AsRef<[u8]>`], and
/// the bytes returned by `hash.as_ref()` are used by a [`MultiKeychain`] to sign the data.
pub trait Signable {
    type Hash: AsRef<[u8]>;
    /// Return a hash for signing.
    fn hash(&self) -> Self::Hash;
}

impl<T: AsRef<[u8]> + Clone> Signable for T {
    type Hash = T;
    fn hash(&self) -> Self::Hash {
        self.clone()
    }
}

/// A pair consisting of an instance of the `Signable` trait and an (arbitrary) signature.
///
/// The method `[UncheckedSigned::check]` can be used to upgrade this `struct` to
/// `[Signed<'a, T, KB>]` which ensures that the signature matches the signed object.
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq, Hash)]
pub struct UncheckedSigned<T: Signable, S: Signature> {
    signable: T,
    signature: S,
}

impl<T: Signable, S: Signature> UncheckedSigned<T, S> {
    pub fn as_signable(&self) -> &T {
        &self.signable
    }

    pub fn into_signable(self) -> T {
        self.signable
    }

    pub fn signature(&self) -> S {
        self.signature.clone()
    }
}

impl<T: Signable, S: Signature> UncheckedSigned<Indexed<T>, S> {
    pub fn as_signable_strip_index(&self) -> &T {
        &self.signable.signable
    }
}

/// Error type returned when a verification of a signature fails.
#[derive(Clone, Debug)]
pub struct SignatureError<T: Signable, S: Signature> {
    pub unchecked: UncheckedSigned<T, S>,
}

impl<T: Signable + Index, S: Signature> UncheckedSigned<T, S> {
    /// Verifies whether the signature matches the key with the index as in the signed data.
    pub fn check<KB: KeyBox<Signature = S>>(
        self,
        key_box: &KB,
    ) -> Result<Signed<T, KB>, SignatureError<T, S>> {
        let index = self.signable.index();
        if !key_box.verify(self.signable.hash().as_ref(), &self.signature, index) {
            return Err(SignatureError { unchecked: self });
        }
        Ok(Signed {
            unchecked: self,
            marker: PhantomData,
        })
    }
}

impl<T: Signable + Index, S: Signature> Index for UncheckedSigned<T, S> {
    fn index(&self) -> NodeIndex {
        self.signable.index()
    }
}

impl<T: Signable, S: PartialMultisignature> UncheckedSigned<T, S> {
    /// Verifies whether the multisignature matches the signed data.
    pub fn check_multi<MK: MultiKeychain<PartialMultisignature = S>>(
        self,
        keychain: &MK,
    ) -> Result<Multisigned<T, MK>, SignatureError<T, S>> {
        if !(keychain.is_complete(self.signable.hash().as_ref(), &self.signature)) {
            return Err(SignatureError { unchecked: self });
        }
        Ok(Multisigned {
            unchecked: self,
            marker: PhantomData,
        })
    }
}

impl<T: Signable, S: Signature> UncheckedSigned<Indexed<T>, S> {
    fn strip_index(self) -> UncheckedSigned<T, S> {
        UncheckedSigned {
            signable: self.signable.strip_index(),
            signature: self.signature,
        }
    }
}

impl<T: Signable, S: Signature> From<UncheckedSigned<Indexed<T>, S>> for UncheckedSigned<T, S> {
    fn from(us: UncheckedSigned<Indexed<T>, S>) -> Self {
        us.strip_index()
    }
}

/// A correctly signed object of type `T`.
///
/// The correctness is guaranteed by storing a (phantom) reference to the `KeyBox` that verified
/// the signature.
#[derive(Debug)]
pub struct Signed<'a, T: Signable + Index, KB: KeyBox> {
    unchecked: UncheckedSigned<T, KB::Signature>,
    marker: PhantomData<&'a KB>,
}

impl<'a, T: Signable + Clone + Index, KB: KeyBox> Clone for Signed<'a, T, KB> {
    fn clone(&self) -> Self {
        Signed {
            unchecked: self.unchecked.clone(),
            marker: PhantomData,
        }
    }
}

impl<'a, T: Signable + Index, KB: KeyBox> Signed<'a, T, KB> {
    /// Create a signed object from a signable. The index of `signable` must match the index of the `key_box`.
    pub async fn sign(signable: T, key_box: &'a KB) -> Signed<'a, T, KB> {
        assert_eq!(signable.index(), key_box.index());
        let signature = key_box.sign(signable.hash().as_ref()).await;
        Signed {
            unchecked: UncheckedSigned {
                signable,
                signature,
            },
            marker: PhantomData,
        }
    }

    /// Get a reference to the signed object.
    pub fn as_signable(&self) -> &T {
        &self.unchecked.signable
    }

    pub fn into_signable(self) -> T {
        self.unchecked.signable
    }

    pub fn into_unchecked(self) -> UncheckedSigned<T, KB::Signature> {
        self.unchecked
    }
}

impl<'a, T: Signable, KB: KeyBox> Signed<'a, Indexed<T>, KB> {
    /// Create a signed object from a signable. The index is added based on the index of the `key_box`.
    pub async fn sign_with_index(signable: T, key_box: &'a KB) -> Signed<'a, Indexed<T>, KB> {
        Signed::sign(Indexed::new(signable, key_box.index()), key_box).await
    }
}

impl<'a, T: Signable, MK: MultiKeychain> Signed<'a, Indexed<T>, MK> {
    /// Transform a singly signed object into a partially multisigned consisting of just the signed object.
    /// Note that depending on the setup, it may yield a complete signature.
    pub fn into_partially_multisigned(self, keychain: &'a MK) -> PartiallyMultisigned<'a, T, MK> {
        let multisignature =
            keychain.from_signature(&self.unchecked.signature, self.unchecked.signable.index);
        let unchecked = UncheckedSigned {
            signable: self.unchecked.signable.strip_index(),
            signature: multisignature,
        };
        if keychain.is_complete(unchecked.signable.hash().as_ref(), &unchecked.signature) {
            PartiallyMultisigned::Complete {
                multisigned: Multisigned {
                    unchecked,
                    marker: PhantomData,
                },
            }
        } else {
            PartiallyMultisigned::Incomplete { unchecked }
        }
    }
}

impl<'a, T: Signable + Index, KB: KeyBox> From<Signed<'a, T, KB>>
    for UncheckedSigned<T, KB::Signature>
{
    fn from(signed: Signed<'a, T, KB>) -> Self {
        signed.into_unchecked()
    }
}

/// A pair consistsing of signable data and a [`NodeIndex`].
///
/// This is a wrapper used for signing data which does not implement the [`Index`] trait.
/// If a node with an index `i` needs to sign some data `signable` which does not
/// implement the [`Index`] trait, it should use the `Signed::sign_with_index` method which will
/// use this wrapper transparently. Note that in the implementation of `Signable` for `Indexed<T>`,
/// the hash is the hash of the underlying data `T`. Therefore, instances of the type
/// [`Signed<'a, Indexed<T>, MK>`] can be aggregated into `Multisigned<'a, T, MK>`
#[derive(Clone, Debug, Decode, Encode, PartialEq, Eq, Hash)]
pub struct Indexed<T: Signable> {
    signable: T,
    index: NodeIndex,
}

impl<T: Signable> Indexed<T> {
    fn new(signable: T, index: NodeIndex) -> Self {
        Indexed { signable, index }
    }

    fn strip_index(self) -> T {
        self.signable
    }
}

impl<T: Signable> Signable for Indexed<T> {
    type Hash = T::Hash;

    fn hash(&self) -> Self::Hash {
        self.signable.hash()
    }
}

impl<T: Signable> Index for Indexed<T> {
    fn index(&self) -> NodeIndex {
        self.index
    }
}

/// Signable data together with a complete multisignature.
///
/// An instance of `Multisigned<'a, T: Signable, MK: MultiKeychain>` consists of a data of type `T`
/// together with a multisignature which is valid and complete according to a multikeychain
/// reference `&'a MK`. The lifetime parameter ensures that the data with a multisignature do not
/// outlive the session.
#[derive(Debug)]
pub struct Multisigned<'a, T: Signable, MK: MultiKeychain> {
    unchecked: UncheckedSigned<T, MK::PartialMultisignature>,
    marker: PhantomData<&'a MK>,
}

impl<'a, T: Signable, MK: MultiKeychain> Multisigned<'a, T, MK> {
    /// Get a reference to the multisigned object.
    pub fn as_signable(&self) -> &T {
        &self.unchecked.signable
    }

    pub fn into_unchecked(self) -> UncheckedSigned<T, MK::PartialMultisignature> {
        self.unchecked
    }
}

impl<'a, T: Signable, MK: MultiKeychain> From<Multisigned<'a, T, MK>>
    for UncheckedSigned<T, MK::PartialMultisignature>
{
    fn from(signed: Multisigned<'a, T, MK>) -> Self {
        signed.into_unchecked()
    }
}

impl<'a, T: Signable + Clone, MK: MultiKeychain> Clone for Multisigned<'a, T, MK> {
    fn clone(&self) -> Self {
        Multisigned {
            unchecked: self.unchecked.clone(),
            marker: self.marker,
        }
    }
}

#[derive(Debug)]
pub struct IncompleteMultisignatureError<'a, T: Signable, MK: MultiKeychain> {
    pub partial: PartiallyMultisigned<'a, T, MK>,
}

/// Signable data together with a valid partial multisignature.
///
/// Instances of this type keep track whether the partial multisignautre is complete or not.
/// If the multisignature is complete, you can get [`Multisigned`] by pattern matching
/// against the variant [`PartiallyMultisigned::Complete`].
#[derive(Debug)]
pub enum PartiallyMultisigned<'a, T: Signable, MK: MultiKeychain> {
    Incomplete {
        unchecked: UncheckedSigned<T, MK::PartialMultisignature>,
    },
    Complete {
        multisigned: Multisigned<'a, T, MK>,
    },
}

impl<'a, T: Signable, MK: MultiKeychain> PartiallyMultisigned<'a, T, MK> {
    /// Create a partially multisigned object.
    pub async fn sign(signable: T, keychain: &'a MK) -> PartiallyMultisigned<'a, T, MK> {
        Signed::sign_with_index(signable, keychain)
            .await
            .into_partially_multisigned(keychain)
    }

    /// Chceck if the partial multisignature is complete.
    pub fn is_complete(&self) -> bool {
        match self {
            PartiallyMultisigned::Incomplete { .. } => false,
            PartiallyMultisigned::Complete { .. } => true,
        }
    }

    /// Get a reference to the multisigned object.
    pub fn as_signable(&self) -> &T {
        match self {
            PartiallyMultisigned::Incomplete { unchecked } => unchecked.as_signable(),
            PartiallyMultisigned::Complete { multisigned } => multisigned.as_signable(),
        }
    }

    /// Return the object that is being signed.
    pub fn into_unchecked(self) -> UncheckedSigned<T, MK::PartialMultisignature> {
        match self {
            PartiallyMultisigned::Incomplete { unchecked } => unchecked,
            PartiallyMultisigned::Complete { multisigned } => multisigned.unchecked,
        }
    }

    /// Adds a signature and checks if multisignature is complete.
    #[must_use = "consumes the original and returns the aggregated signature which should be used"]
    pub fn add_signature(self, signed: Signed<'a, Indexed<T>, MK>, keychain: &'a MK) -> Self {
        if self.as_signable().hash().as_ref() != signed.as_signable().hash().as_ref() {
            warn!(target: "AlephBFT-signed", "Tried to add a signature of a different object");
            return self;
        }
        match self {
            PartiallyMultisigned::Incomplete { mut unchecked } => {
                unchecked.signature = unchecked
                    .signature
                    .add_signature(&signed.unchecked.signature, signed.unchecked.signable.index);
                if keychain.is_complete(unchecked.signable.hash().as_ref(), &unchecked.signature) {
                    PartiallyMultisigned::Complete {
                        multisigned: Multisigned {
                            unchecked,
                            marker: PhantomData,
                        },
                    }
                } else {
                    PartiallyMultisigned::Incomplete { unchecked }
                }
            }
            PartiallyMultisigned::Complete { .. } => self,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        Index, KeyBox, MultiKeychain, NodeCount, NodeIndex, PartialMultisignature,
        PartiallyMultisigned, Signable, SignatureSet, Signed,
    };
    use async_trait::async_trait;
    use codec::{Decode, Encode};
    use std::fmt::Debug;

    /// Keybox wrapper which implements MultiKeychain such that a partial multisignature is a list of
    /// signatures and a partial multisignature is considered complete if it contains more than 2N/3 signatures.
    ///
    /// Note: this way of multisigning is very inefficient, and should be used only for testing.
    #[derive(Debug, Clone)]
    struct DefaultMultiKeychain<KB: KeyBox> {
        key_box: KB,
    }

    impl<KB: KeyBox> DefaultMultiKeychain<KB> {
        // Create a new `DefaultMultiKeychain` using the provided `KeyBox`.
        fn new(key_box: KB) -> Self {
            DefaultMultiKeychain { key_box }
        }

        fn quorum(&self) -> usize {
            2 * self.node_count().0 / 3 + 1
        }
    }

    impl<KB: KeyBox> Index for DefaultMultiKeychain<KB> {
        fn index(&self) -> NodeIndex {
            self.key_box.index()
        }
    }

    #[async_trait::async_trait]
    impl<KB: KeyBox> KeyBox for DefaultMultiKeychain<KB> {
        type Signature = KB::Signature;

        async fn sign(&self, msg: &[u8]) -> Self::Signature {
            self.key_box.sign(msg).await
        }

        fn node_count(&self) -> NodeCount {
            self.key_box.node_count()
        }

        fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool {
            self.key_box.verify(msg, sgn, index)
        }
    }

    impl<KB: KeyBox> MultiKeychain for DefaultMultiKeychain<KB> {
        type PartialMultisignature = SignatureSet<KB::Signature>;

        fn from_signature(
            &self,
            signature: &Self::Signature,
            index: NodeIndex,
        ) -> Self::PartialMultisignature {
            SignatureSet::add_signature(
                SignatureSet::with_size(self.node_count()),
                signature,
                index,
            )
        }

        fn is_complete(&self, msg: &[u8], partial: &Self::PartialMultisignature) -> bool {
            let signature_count = partial.iter().count();
            if signature_count < self.quorum() {
                return false;
            }
            partial
                .iter()
                .all(|(i, sgn)| self.key_box.verify(msg, sgn, i))
        }
    }

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
    struct TestSignature {
        msg: Vec<u8>,
        index: NodeIndex,
    }

    #[derive(Clone, Debug)]
    struct TestKeyBox {
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

    type TestMultiKeychain = DefaultMultiKeychain<TestKeyBox>;

    fn test_multi_keychain(node_count: NodeCount, index: NodeIndex) -> TestMultiKeychain {
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
        unchecked_msg.signature.index = 1.into();

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
}
