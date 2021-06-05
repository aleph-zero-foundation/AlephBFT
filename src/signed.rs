use crate::{nodes::NodeIndex, Index};
use codec::{Decode, Encode};
use log::debug;
use std::{fmt::Debug, marker::PhantomData};

/// The type used as a signature.
///
/// The Signature typically does not contain the index of the node who signed the data.
pub trait Signature: Debug + Clone + Encode + Decode + Send + 'static {}

impl<T: Debug + Clone + Encode + Decode + Send + 'static> Signature for T {}

/// Abstraction of the signing data and verifying signatures.
///
/// A typical implementation of KeyBox would be a collection of `N` public keys,
/// an index `i` and a single private key corresponding to the public key number `i`.
/// The meaning of sign is then to produce a signature `s` using the given private key,
/// and `verify(msg, s, j)` is to verify whether the signature s under the message msg is
/// correct with respect to the public key of the jth node.
pub trait KeyBox: Index + Clone + Send + Sync + 'static {
    type Signature: Signature;
    /// Signs a message `msg`.
    fn sign(&self, msg: &[u8]) -> Self::Signature;
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
pub trait PartialMultisignature: Debug + Clone + Encode + Decode + Send + 'static {
    type Signature: Signature;
    /// Adds the signature.
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
/// The methods `[UncheckedSigned::check_with_index]` and `[UncheckedSigned::check]` can be used
/// to upgrade this `struct` to `[Signed<'a, T, KB>]` which ensures that the signature matches the
/// signed object, and the method `[UncheckedSigned::check_partial]` can be used to upgrade to
/// `[PartiallyMultisigned<'a, T, MK>]`.
#[derive(Clone, Debug, Decode, Encode)]
pub struct UncheckedSigned<T: Signable, S> {
    signable: T,
    signature: S,
}

/// A pair consisting of an instance of the `Signable` trait and an unchecked signature.
///
/// The method [`UncheckedSigned::check`] can be used
/// to upgrade this `struct` to [`Signed<'a, T, KB>`] which ensures that the signature matches the
/// signed object, and the method [`UncheckedSigned::check_multi`] can be used to upgrade to
/// [`Multisigned<'a, T, MK>`].
impl<T: Signable, S: Signature> UncheckedSigned<T, S> {
    pub(crate) fn as_signable(&self) -> &T {
        &self.signable
    }
}

#[cfg(test)]
impl<T: Signable, S: Signature> UncheckedSigned<T, S> {
    pub(crate) fn new(signable: T, signature: S) -> Self {
        UncheckedSigned {
            signable,
            signature,
        }
    }
    pub(crate) fn as_signable_mut(&mut self) -> &mut T {
        &mut self.signable
    }
    pub(crate) fn signature(&self) -> &S {
        &self.signature
    }
    pub(crate) fn signature_mut(&mut self) -> &mut S {
        &mut self.signature
    }
}

impl<T: Signable, S: Signature> Signable for UncheckedSigned<T, S> {
    type Hash = Vec<u8>;

    fn hash(&self) -> Self::Hash {
        self.signature.encode()
    }
}

/// Error type returned when a verification of a signature fails.
#[derive(Clone, Debug)]
pub struct SignatureError<T: Signable, S> {
    unchecked: UncheckedSigned<T, S>,
}

impl<T: Signable + Index, S: Clone> UncheckedSigned<T, S> {
    /// Verifies whether the signature matches the key with the index as in the signed data.
    pub(crate) fn check<KB: KeyBox<Signature = S>>(
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

impl<T: Signable + Index, S: Clone> Index for UncheckedSigned<T, S> {
    fn index(&self) -> NodeIndex {
        self.signable.index()
    }
}

impl<T: Signable, S: Clone> UncheckedSigned<T, S> {
    /// Verifies whether the multisignature matches the signed data.
    pub(crate) fn check_multi<MK: MultiKeychain<PartialMultisignature = S>>(
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

/// A pair consisting of an object and a matching signature
///
/// An instance of `Signed<'a, T, KB>` stores an object `t: T`, a signature `s: KB::Signature`,
/// and a reference `kb: &'a KB`, with the requirement that there exists some node index
/// `i: NodeIndex` such that `kb.verify(&t.bytes_to_sign(), s, i)` return true. The index
/// `i` is not stored explicitly, but usually, either it is a part of the signed object `t`,
/// or is known from the context.
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
    pub fn sign(signable: T, key_box: &'a KB) -> Self {
        assert_eq!(signable.index(), key_box.index());
        let signature = key_box.sign(signable.hash().as_ref());
        Signed {
            unchecked: UncheckedSigned {
                signable,
                signature,
            },
            marker: PhantomData,
        }
    }

    pub(crate) fn as_signable(&self) -> &T {
        &self.unchecked.signable
    }

    pub(crate) fn into_unchecked(self) -> UncheckedSigned<T, KB::Signature> {
        self.unchecked
    }
}

impl<'a, T: Signable, MK: MultiKeychain> Signed<'a, Indexed<T>, MK> {
    /// Transform a singly signed object into a partially multisigned consisting of just the signed object.
    /// Note that depending on the setup, it may yield a complete signature.
    pub fn into_partially_multisigned(self, keychain: &'a MK) -> PartiallyMultisigned<'a, T, MK> {
        let multisignature =
            keychain.from_signature(&self.unchecked.signature, self.unchecked.signable.index);
        let unchecked = UncheckedSigned {
            signable: self.unchecked.signable.signable,
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
        signed.unchecked
    }
}

/// A pair consistsing of signable data and a [`NodeIndex`].
///
/// This is a wrapper used for signing data which does not implement the [`Index`] trait.
/// If a node with an index `i` needs to sign some data `signable` which does not
/// implement the [`Index`] trait, then a wrapped instance `signable`: `Indexed::new(signable, i)`
/// should be signed instead. Note that in the implementation of `Signable` for `Indexed<T>`,
/// the hash is the hash of the underlying data `T`. Therefore, instances of the type
/// [`Signed<'a, Indexed<T>, MK>`] can be aggregated into `Multisigned<'a, T, MK>`
#[derive(Clone, Encode, Decode, Debug, PartialEq)]
pub struct Indexed<T: Signable> {
    signable: T,
    index: NodeIndex,
}

impl<T: Signable> Indexed<T> {
    pub(crate) fn new(signable: T, index: NodeIndex) -> Self {
        Indexed { signable, index }
    }
    pub(crate) fn as_signable(&self) -> &T {
        &self.signable
    }
}

#[cfg(test)]
impl<T: Signable> Indexed<T> {
    pub(crate) fn index_mut(&mut self) -> &mut NodeIndex {
        &mut self.index
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
    pub(crate) fn as_signable(&self) -> &T {
        &self.unchecked.signable
    }

    pub(crate) fn into_unchecked(self) -> UncheckedSigned<T, MK::PartialMultisignature> {
        self.unchecked
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
pub(crate) struct IncompleteMultisignatureError<'a, T: Signable, MK: MultiKeychain> {
    pub(crate) partial: PartiallyMultisigned<'a, T, MK>,
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
    pub fn sign(signable: T, keychain: &'a MK) -> Self {
        let indexed = Indexed::new(signable, keychain.index());
        let signed = Signed::sign(indexed, keychain);
        signed.into_partially_multisigned(keychain)
    }

    /// Chceck if the partial multisignature is complete.
    pub fn is_complete(&self) -> bool {
        match self {
            PartiallyMultisigned::Incomplete { .. } => false,
            PartiallyMultisigned::Complete { .. } => true,
        }
    }

    /// Return a reference to the object that is being signed.
    pub fn as_unchecked(&self) -> &UncheckedSigned<T, MK::PartialMultisignature> {
        match self {
            PartiallyMultisigned::Incomplete { unchecked } => unchecked,
            PartiallyMultisigned::Complete { multisigned } => &multisigned.unchecked,
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
    pub fn add_signature(self, signed: Signed<'a, Indexed<T>, MK>, keychain: &'a MK) -> Self {
        if self.as_unchecked().signable.hash().as_ref() != signed.as_signable().hash().as_ref() {
            debug!(target: "AlephBFT-add_signature", "Tried to add a signature of a different object");
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
