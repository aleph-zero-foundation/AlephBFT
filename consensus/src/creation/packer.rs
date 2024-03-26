use crate::{
    units::{FullUnit, PreUnit, SignedUnit},
    Data, Hasher, MultiKeychain, SessionId, Signed,
};

/// The component responsible for packing Data into PreUnits,
/// and signing the outcome, thus creating SignedUnits that are sent back to Runway.
pub struct Packer<MK: MultiKeychain> {
    keychain: MK,
    session_id: SessionId,
}

impl<MK: MultiKeychain> Packer<MK> {
    pub fn new(keychain: MK, session_id: SessionId) -> Self {
        Packer {
            keychain,
            session_id,
        }
    }

    pub fn pack<H: Hasher, D: Data>(
        &self,
        preunit: PreUnit<H>,
        data: Option<D>,
    ) -> SignedUnit<H, D, MK> {
        Signed::sign(
            FullUnit::new(preunit, data, self.session_id),
            &self.keychain,
        )
    }
}
