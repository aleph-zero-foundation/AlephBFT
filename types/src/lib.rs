mod dataio;
mod network;
mod tasks;

pub use aleph_bft_crypto::{
    IncompleteMultisignatureError, Index, Indexed, KeyBox, MultiKeychain, Multisigned, NodeCount,
    NodeIndex, NodeMap, NodeSubset, PartialMultisignature, PartiallyMultisigned, Signable,
    Signature, SignatureError, SignatureSet, Signed, UncheckedSigned,
};
pub use dataio::{DataProvider, FinalizationHandler};
pub use network::{Network, Recipient};
pub use tasks::{SpawnHandle, TaskHandle};

use codec::Codec;
use std::{fmt::Debug, hash::Hash as StdHash};

/// Data type that we want to order.
pub trait Data: Eq + Clone + Send + Sync + Debug + StdHash + Codec + 'static {}

impl<T> Data for T where T: Eq + Clone + Send + Sync + Debug + StdHash + Codec + 'static {}

/// A hasher, used for creating identifiers for blocks or units.
pub trait Hasher: Eq + Clone + Send + Sync + Debug + 'static {
    /// A hash, as an identifier for a block or unit.
    type Hash: AsRef<[u8]> + Eq + Ord + Copy + Clone + Send + Sync + Debug + StdHash + Codec;

    fn hash(s: &[u8]) -> Self::Hash;
}

/// The number of a session for which the consensus is run.
pub type SessionId = u64;

/// An asynchronous round of the protocol.
pub type Round = u16;
