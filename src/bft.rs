use crate::{units::UncheckedSignedUnit, Data, Hasher, NodeIndex};
use codec::{Decode, Encode};

#[derive(Clone, Debug, Decode, Encode)]
pub(crate) struct ForkProof<H: Hasher, D: Data, S> {
    pub(crate) u1: UncheckedSignedUnit<H, D, S>,
    pub(crate) u2: UncheckedSignedUnit<H, D, S>,
}

#[derive(Clone, Debug, Decode, Encode)]
pub(crate) struct Alert<H: Hasher, D: Data, S> {
    pub(crate) sender: NodeIndex,
    pub(crate) forker: NodeIndex,
    pub(crate) proof: ForkProof<H, D, S>,
    pub(crate) legit_units: Vec<UncheckedSignedUnit<H, D, S>>,
}
