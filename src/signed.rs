use crate::{nodes::NodeIndex, Index};
use codec::{Decode, Encode};
use log::debug;
use std::{fmt::Debug, marker::PhantomData};

/// The type used as a signature. The Signature typically does not contain the index of the node who
/// signed the data.
pub trait Signature: Debug + Clone + Encode + Decode + Send + 'static {}

impl<T: Debug + Clone + Encode + Decode + Send + 'static> Signature for T {}

/// Abstraction of the signing data and verifying signatures. Typically, consists of a private key
/// of the node and the public keys of all nodes.
pub trait KeyBox: Index {
    type Signature: Signature;
    fn sign(&self, msg: &[u8]) -> Self::Signature;
    fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool;
}

/// A type to which Signatures can be aggregated.
/// A single Signature can be rised to a Multisignature, and any signature can be added to
/// multisignature.
/// After adding sufficiently many signatures, the partial multisignature becomes a "complete"
/// multisignature.
/// Whether a multisignature is complete, can be verified with `[MultiKeychain::is_complete]` method.
/// The signature and the index passed to the `add_signature` method are required to be valid.
pub trait PartialMultisignature: Debug + Clone + Encode + Decode {
    type Signature: Signature;
    fn add_signature(&mut self, signature: &Self::Signature, index: NodeIndex);
}

/// Extends KeyBox with multisigning functionalities. Allows to verify whether a partial multisignature
/// is valid (or complete).
pub trait MultiKeychain: KeyBox {
    type PartialMultisignature: PartialMultisignature<Signature = Self::Signature>;
    fn from_signature(
        &self,
        signature: &Self::Signature,
        index: NodeIndex,
    ) -> Self::PartialMultisignature;
    fn is_complete(&self, partial: &Self::PartialMultisignature) -> bool;
    fn verify_partial(&self, msg: &[u8], partial: &Self::PartialMultisignature) -> bool;
}

pub trait Signable {
    type Hash: AsRef<[u8]>;
    fn hash(&self) -> Self::Hash;
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

#[cfg(test)]
impl<T: Signable, S: Signature> UncheckedSigned<T, S> {
    pub(crate) fn as_signable(&mut self) -> &mut T {
        &mut self.signable
    }
}

#[derive(Clone, Debug)]
pub struct SignatureError<T: Signable, S> {
    unchecked: UncheckedSigned<T, S>,
}

impl<T: Signable + Index, S: Clone> UncheckedSigned<T, S> {
    /// Verifies whether the signature matches the key with the given index.
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

impl<T: Signable, S: Clone> UncheckedSigned<T, S> {
    pub fn check_partial<MK: MultiKeychain<PartialMultisignature = S>>(
        self,
        keychain: &MK,
    ) -> Result<PartiallyMultisigned<T, MK>, SignatureError<T, S>> {
        if !keychain.verify_partial(self.signable.hash().as_ref(), &self.signature) {
            return Err(SignatureError { unchecked: self });
        }
        Ok(PartiallyMultisigned {
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
    pub fn sign(signable: T, key_box: &'a KB) -> Self {
        assert_eq!(signable.index(), key_box.index());
        let signature = key_box.sign(&signable.hash().as_ref());
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
}

impl<'a, T: Signable, MK: MultiKeychain> Signed<'a, Indexed<T>, MK> {
    fn _into_partially_multisigned(self, key_box: &'a MK) -> PartiallyMultisigned<'a, T, MK> {
        let multisignature =
            key_box.from_signature(&self.unchecked.signature, self.unchecked.signable.index);
        PartiallyMultisigned {
            unchecked: UncheckedSigned {
                signable: self.unchecked.signable.signable,
                signature: multisignature,
            },
            marker: PhantomData,
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

#[derive(Clone, Debug, PartialEq)]
pub struct Indexed<T: Signable> {
    signable: T,
    index: NodeIndex,
}

impl<T: Signable> Indexed<T> {
    pub fn new(signable: T, index: NodeIndex) -> Self {
        Indexed { signable, index }
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

#[derive(Debug)]
pub struct PartiallyMultisigned<'a, T: Signable, MK: MultiKeychain> {
    unchecked: UncheckedSigned<T, MK::PartialMultisignature>,
    marker: PhantomData<&'a MK>,
}

pub struct Multisigned<'a, T: Signable, MK: MultiKeychain> {
    pub unchecked: UncheckedSigned<T, MK::PartialMultisignature>,
    marker: PhantomData<&'a MK>,
}

#[derive(Debug)]
pub struct IncompleteMultisignatureError<'a, T: Signable, MK: MultiKeychain> {
    pub partial: PartiallyMultisigned<'a, T, MK>,
}

impl<'a, T: Signable, MK: MultiKeychain> PartiallyMultisigned<'a, T, MK> {
    pub fn sign(signable: T, keychain: &'a MK) -> Self {
        let signature = keychain.sign(signable.hash().as_ref());
        let multisignature = keychain.from_signature(&signature, keychain.index());
        PartiallyMultisigned {
            unchecked: UncheckedSigned {
                signable,
                signature: multisignature,
            },
            marker: PhantomData,
        }
    }
    pub fn add_signature(&mut self, signed: Signed<'a, Indexed<T>, MK>) {
        if self.unchecked.signable.hash().as_ref() != signed.as_signable().hash().as_ref() {
            debug!("Tried to add a signature of a different object");
            return;
        }
        self.unchecked
            .signature
            .add_signature(&signed.unchecked.signature, signed.as_signable().index);
    }

    fn _try_into_complete(
        self,
        keychain: &'a MK,
    ) -> Result<Multisigned<'a, T, MK>, IncompleteMultisignatureError<'a, T, MK>> {
        if !keychain.is_complete(&self.unchecked.signature) {
            return Err(IncompleteMultisignatureError { partial: self });
        }
        Ok(Multisigned {
            unchecked: self.unchecked,
            marker: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests;
