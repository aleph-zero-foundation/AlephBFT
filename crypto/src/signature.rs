use crate::{Index, NodeCount, NodeIndex, NodeMap};
use codec::{Codec, Decode, Encode};
use log::warn;
use std::{fmt::Debug, hash::Hash};

/// The type used as a signature.
///
/// The Signature typically does not contain the index of the node who signed the data.
pub trait Signature: Debug + Clone + Codec + Send + Sync + Eq + 'static {}

impl<T: Debug + Clone + Codec + Send + Sync + Eq + 'static> Signature for T {}

/// Abstraction of the signing data and verifying signatures.
///
/// A typical implementation of Keychain would be a collection of `N` public keys,
/// an index `i` and a single private key corresponding to the public key number `i`.
/// The meaning of sign is then to produce a signature `s` using the given private key,
/// and `verify(msg, s, j)` is to verify whether the signature s under the message msg is
/// correct with respect to the public key of the jth node.
pub trait Keychain: Index + Clone + Send + Sync + 'static {
    type Signature: Signature;

    /// Returns the total number of known public keys.
    fn node_count(&self) -> NodeCount;
    /// Signs a message `msg`.
    fn sign(&self, msg: &[u8]) -> Self::Signature;
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

/// Extends Keychain with multisigning functionalities.
///
/// A single Signature can be raised to a Multisignature.
/// Allows to verify whether a partial multisignature is complete (and valid).
pub trait MultiKeychain: Keychain {
    type PartialMultisignature: PartialMultisignature<Signature = Self::Signature>;
    /// Transform a single signature to a multisignature consisting of the signature.
    fn bootstrap_multi(
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
/// `[Signed<T, K>]` which ensures that the signature matches the signed object.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Decode, Encode)]
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
#[derive(Clone, Eq, PartialEq, Hash, Debug, Decode, Encode)]
pub struct SignatureError<T: Signable, S: Signature> {
    pub unchecked: UncheckedSigned<T, S>,
}

impl<T: Signable + Index, S: Signature> UncheckedSigned<T, S> {
    /// Verifies whether the signature matches the key with the index as in the signed data.
    pub fn check<K: Keychain<Signature = S>>(
        self,
        keychain: &K,
    ) -> Result<Signed<T, K>, SignatureError<T, S>> {
        let index = self.signable.index();
        if !keychain.verify(self.signable.hash().as_ref(), &self.signature, index) {
            return Err(SignatureError { unchecked: self });
        }
        Ok(Signed { unchecked: self })
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
        Ok(Multisigned { unchecked: self })
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
#[derive(Eq, PartialEq, Hash, Debug, Decode, Encode)]
pub struct Signed<T: Signable + Index, K: Keychain> {
    unchecked: UncheckedSigned<T, K::Signature>,
}

impl<T: Signable + Clone + Index, K: Keychain> Clone for Signed<T, K> {
    fn clone(&self) -> Self {
        Signed {
            unchecked: self.unchecked.clone(),
        }
    }
}

impl<T: Signable + Index, K: Keychain> Signed<T, K> {
    /// Create a signed object from a signable. The index of `signable` must match the index of the `keychain`.
    pub fn sign(signable: T, keychain: &K) -> Signed<T, K> {
        assert_eq!(signable.index(), keychain.index());
        let signature = keychain.sign(signable.hash().as_ref());
        Signed {
            unchecked: UncheckedSigned {
                signable,
                signature,
            },
        }
    }

    /// Get a reference to the signed object.
    pub fn as_signable(&self) -> &T {
        &self.unchecked.signable
    }

    pub fn into_signable(self) -> T {
        self.unchecked.signable
    }

    pub fn into_unchecked(self) -> UncheckedSigned<T, K::Signature> {
        self.unchecked
    }
}

impl<T: Signable, K: Keychain> Signed<Indexed<T>, K> {
    /// Create a signed object from a signable. The index is added based on the index of the `keychain`.
    pub fn sign_with_index(signable: T, keychain: &K) -> Signed<Indexed<T>, K> {
        Signed::sign(Indexed::new(signable, keychain.index()), keychain)
    }
}

impl<T: Signable, MK: MultiKeychain> Signed<Indexed<T>, MK> {
    /// Transform a singly signed object into a partially multisigned consisting of just the signed object.
    /// Note that depending on the setup, it may yield a complete signature.
    pub fn into_partially_multisigned(self, keychain: &MK) -> PartiallyMultisigned<T, MK> {
        let multisignature =
            keychain.bootstrap_multi(&self.unchecked.signature, self.unchecked.signable.index);
        let unchecked = UncheckedSigned {
            signable: self.unchecked.signable.strip_index(),
            signature: multisignature,
        };
        if keychain.is_complete(unchecked.signable.hash().as_ref(), &unchecked.signature) {
            PartiallyMultisigned::Complete {
                multisigned: Multisigned { unchecked },
            }
        } else {
            PartiallyMultisigned::Incomplete { unchecked }
        }
    }
}

impl<T: Signable + Index, K: Keychain> From<Signed<T, K>> for UncheckedSigned<T, K::Signature> {
    fn from(signed: Signed<T, K>) -> Self {
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
/// [`Signed<Indexed<T>, MK>`] can be aggregated into `Multisigned<T, MK>`
#[derive(Clone, Eq, PartialEq, Hash, Debug, Decode, Encode)]
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

    pub fn as_signable(&self) -> &T {
        &self.signable
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
/// An instance of `Multisigned<T: Signable, MK: MultiKeychain>` consists of a data of type `T`
/// together with a multisignature which is valid and complete according to a multikeychain
/// reference `MK`.
#[derive(Eq, PartialEq, Hash, Debug, Decode, Encode)]
pub struct Multisigned<T: Signable, MK: MultiKeychain> {
    unchecked: UncheckedSigned<T, MK::PartialMultisignature>,
}

impl<T: Signable, MK: MultiKeychain> Multisigned<T, MK> {
    /// Get a reference to the multisigned object.
    pub fn as_signable(&self) -> &T {
        &self.unchecked.signable
    }

    pub fn into_unchecked(self) -> UncheckedSigned<T, MK::PartialMultisignature> {
        self.unchecked
    }
}

impl<T: Signable, MK: MultiKeychain> From<Multisigned<T, MK>>
    for UncheckedSigned<T, MK::PartialMultisignature>
{
    fn from(signed: Multisigned<T, MK>) -> Self {
        signed.into_unchecked()
    }
}

impl<T: Signable + Clone, MK: MultiKeychain> Clone for Multisigned<T, MK> {
    fn clone(&self) -> Self {
        Multisigned {
            unchecked: self.unchecked.clone(),
        }
    }
}

/// Error resulting from multisignature being incomplete.
///
/// ### `Hash` derivation
/// `PartiallyMultisigned` only conditionally implements `Hash`:
/// ```rust
/// # use std::hash::Hasher;
/// # use aleph_bft_crypto::{MultiKeychain, PartiallyMultisigned, Signable};
/// # trait Hash {};
/// impl<'a, T: Hash + Signable, MK: Hash + MultiKeychain>
///    Hash for PartiallyMultisigned<T, MK>
/// where MK::PartialMultisignature: Hash {}
/// ```
/// i.e. Rust automatically adds `where MK::PartialMultisignature: Hash`.
///
/// This is a problem, since such `where` guard cannot be inferred for
/// `IncompleteMultisignatureError` (because we do not use the associate type
/// `MK::PartialMultisignature` of `MK` explicitly here).
///
/// The alternative solution could be to make `Signature: Hash` or add a phantom marker using
/// `MK::PartialMultisignature` type.
#[derive(Clone, Eq, PartialEq, Debug, Decode, Encode)]
pub struct IncompleteMultisignatureError<T: Signable, MK: MultiKeychain> {
    pub partial: PartiallyMultisigned<T, MK>,
}

/// Signable data together with a valid partial multisignature.
///
/// Instances of this type keep track whether the partial multisignautre is complete or not.
/// If the multisignature is complete, you can get [`Multisigned`] by pattern matching
/// against the variant [`PartiallyMultisigned::Complete`].
#[derive(Clone, Eq, PartialEq, Hash, Debug, Decode, Encode)]
pub enum PartiallyMultisigned<T: Signable, MK: MultiKeychain> {
    Incomplete {
        unchecked: UncheckedSigned<T, MK::PartialMultisignature>,
    },
    Complete {
        multisigned: Multisigned<T, MK>,
    },
}

impl<T: Signable, MK: MultiKeychain> PartiallyMultisigned<T, MK> {
    /// Create a partially multisigned object.
    pub fn sign(signable: T, keychain: &MK) -> PartiallyMultisigned<T, MK> {
        Signed::sign_with_index(signable, keychain).into_partially_multisigned(keychain)
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
    pub fn add_signature(self, signed: Signed<Indexed<T>, MK>, keychain: &MK) -> Self {
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
                        multisigned: Multisigned { unchecked },
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
        Index, Keychain, MultiKeychain, NodeCount, NodeIndex, PartialMultisignature,
        PartiallyMultisigned, Signable, SignatureSet, Signed,
    };
    use codec::{Decode, Encode};
    use std::fmt::Debug;

    /// Keychain wrapper which implements MultiKeychain such that a partial multisignature is a list of
    /// signatures and a partial multisignature is considered complete if it contains more than 2N/3 signatures.
    ///
    /// Note: this way of multisigning is very inefficient, and should be used only for testing.
    #[derive(Debug, Clone)]
    struct DefaultMultiKeychain<K: Keychain> {
        keychain: K,
    }

    impl<K: Keychain> DefaultMultiKeychain<K> {
        // Create a new `DefaultMultiKeychain` using the provided `Keychain`.
        fn new(keychain: K) -> Self {
            DefaultMultiKeychain { keychain }
        }
    }

    impl<K: Keychain> Index for DefaultMultiKeychain<K> {
        fn index(&self) -> NodeIndex {
            self.keychain.index()
        }
    }

    impl<K: Keychain> Keychain for DefaultMultiKeychain<K> {
        type Signature = K::Signature;

        fn node_count(&self) -> NodeCount {
            self.keychain.node_count()
        }

        fn sign(&self, msg: &[u8]) -> Self::Signature {
            self.keychain.sign(msg)
        }

        fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool {
            self.keychain.verify(msg, sgn, index)
        }
    }

    impl<K: Keychain> MultiKeychain for DefaultMultiKeychain<K> {
        type PartialMultisignature = SignatureSet<K::Signature>;

        fn bootstrap_multi(
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
            if signature_count < self.node_count().consensus_threshold().0 {
                return false;
            }
            partial
                .iter()
                .all(|(i, sgn)| self.keychain.verify(msg, sgn, i))
        }
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
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
    struct TestKeychain {
        count: NodeCount,
        index: NodeIndex,
    }

    impl TestKeychain {
        fn new(count: NodeCount, index: NodeIndex) -> Self {
            TestKeychain { count, index }
        }
    }

    impl Index for TestKeychain {
        fn index(&self) -> NodeIndex {
            self.index
        }
    }

    impl Keychain for TestKeychain {
        type Signature = TestSignature;

        fn node_count(&self) -> NodeCount {
            self.count
        }

        fn sign(&self, msg: &[u8]) -> Self::Signature {
            TestSignature {
                msg: msg.to_vec(),
                index: self.index,
            }
        }

        fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool {
            index == sgn.index && msg == sgn.msg
        }
    }

    type TestMultiKeychain = DefaultMultiKeychain<TestKeychain>;

    fn test_multi_keychain(node_count: NodeCount, index: NodeIndex) -> TestMultiKeychain {
        let keychain = TestKeychain::new(node_count, index);
        DefaultMultiKeychain::new(keychain)
    }

    #[test]
    fn test_valid_signatures() {
        let node_count: NodeCount = 7.into();
        let keychains: Vec<TestMultiKeychain> = (0_usize..node_count.0)
            .map(|i| test_multi_keychain(node_count, i.into()))
            .collect();
        for i in 0..node_count.0 {
            for j in 0..node_count.0 {
                let msg = test_message();
                let signed_msg = Signed::sign_with_index(msg.clone(), &keychains[i]);
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
        let keychain = test_multi_keychain(node_count, index);
        let msg = test_message();
        let signed_msg = Signed::sign_with_index(msg, &keychain);
        let mut unchecked_msg = signed_msg.into_unchecked();
        unchecked_msg.signature.index = 1.into();

        assert!(
            unchecked_msg.check(&keychain).is_err(),
            "wrong index makes wrong signature"
        );
    }

    #[test]
    fn test_incomplete_multisignature() {
        let msg = test_message();
        let index: NodeIndex = 0.into();
        let node_count: NodeCount = 2.into();
        let keychain = test_multi_keychain(node_count, index);

        let partial = PartiallyMultisigned::sign(msg, &keychain);
        assert!(
            !partial.is_complete(),
            "One signature does not form a complete multisignature",
        );
    }

    #[test]
    fn test_multisignatures() {
        let msg = test_message();
        let node_count: NodeCount = 7.into();
        let keychains: Vec<TestMultiKeychain> = (0..node_count.0)
            .map(|i| test_multi_keychain(node_count, i.into()))
            .collect();

        let mut partial = PartiallyMultisigned::sign(msg.clone(), &keychains[0]);
        for keychain in keychains.iter().skip(1).take(4) {
            assert!(!partial.is_complete());
            let signed = Signed::sign_with_index(msg.clone(), keychain);
            partial = partial.add_signature(signed, keychain);
        }
        assert!(
            partial.is_complete(),
            "5 signatures should form a complete signature {:?}",
            partial
        );
    }
}
