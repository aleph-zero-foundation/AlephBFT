use crate::{Index, NodeCount, NodeIndex, NodeMap};
use async_trait::async_trait;
use codec::Codec;
use std::fmt::Debug;

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
    #[must_use]
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

    #[must_use]
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
