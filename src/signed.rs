use crate::{nodes::NodeIndex, Index, KeyBox};
use codec::{Decode, Encode};

pub trait Signable {
    type Hash: AsRef<[u8]>;
    fn hash(&self) -> Self::Hash;
}

/// A pair consisting of an instance of the `Signable` trait and an (arbitrary) signature.
///
/// The methods `[UncheckedSigned::check_with_index]` and `[UncheckedSigned::check]` can be used
/// to upgrade this `struct` to `[CheckedSigned<T, S>]` which ensures that the signature matches the
/// signed object.
#[derive(Clone, Debug, Decode, Encode)]
pub struct UncheckedSigned<T: Signable, S> {
    signable: T,
    signature: S,
}

#[derive(Clone, Debug)]
pub(crate) struct SignatureError<T: Signable, S> {
    unchecked: UncheckedSigned<T, S>,
}

impl<T: Signable, S> UncheckedSigned<T, S> {
    /// Verifies whether the signature matches the key with the given index.
    pub(crate) fn check_with_index<KB: KeyBox<Signature = S>>(
        self,
        key_box: &KB,
        index: NodeIndex,
    ) -> Result<Signed<T, KB>, SignatureError<T, KB::Signature>> {
        if !key_box.verify(self.signable.hash().as_ref(), &self.signature, index) {
            return Err(SignatureError { unchecked: self });
        }
        Ok(Signed {
            unchecked: self,
            key_box,
        })
    }
}

impl<T: Signable + Index, S> UncheckedSigned<T, S> {
    /// Verifies, whether the signature matches the key with the index of the signed object.
    pub(crate) fn check<KB: KeyBox<Signature = S>>(
        self,
        key_box: &KB,
    ) -> Result<Signed<T, KB>, SignatureError<T, S>> {
        let index = self.signable.index();
        self.check_with_index(key_box, index)
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
pub struct Signed<'a, T: Signable, KB: KeyBox> {
    unchecked: UncheckedSigned<T, KB::Signature>,
    key_box: &'a KB,
}

impl<'a, T: Signable + Clone, KB: KeyBox> Clone for Signed<'a, T, KB> {
    fn clone(&self) -> Self {
        Signed {
            unchecked: self.unchecked.clone(),
            key_box: self.key_box,
        }
    }
}

impl<'a, T: Signable, KB: KeyBox> Signed<'a, T, KB> {
    pub fn sign(key_box: &'a KB, signable: T) -> Self {
        let signature = key_box.sign(&signable.hash().as_ref());
        Signed {
            unchecked: UncheckedSigned {
                signable,
                signature,
            },
            key_box,
        }
    }

    pub(crate) fn into_unchecked(self) -> UncheckedSigned<T, KB::Signature> {
        self.unchecked
    }

    pub(crate) fn as_signable(&self) -> &T {
        &self.unchecked.signable
    }
}

impl<'a, T: Signable, KB: KeyBox> From<Signed<'a, T, KB>> for UncheckedSigned<T, KB::Signature> {
    fn from(signed: Signed<'a, T, KB>) -> Self {
        signed.into_unchecked()
    }
}
