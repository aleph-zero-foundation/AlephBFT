use crate::{dag::Request, units::UncheckedSignedUnit, Data, Hasher, Signature};

/// Task that needs to be performed to ensure successful unit dissemination, either requesting or broadcasting a unit.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum DisseminationTask<H: Hasher, D: Data, S: Signature> {
    /// Perform a request.
    Request(Request<H>),
    /// Broadcast a unit.
    /// TODO(A0-4567): This should soon only contain the hash.
    Broadcast(UncheckedSignedUnit<H, D, S>),
}
