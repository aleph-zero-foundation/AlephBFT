//! Implements the Aleph BFT Consensus protocol as a "finality gadget". The [Member] struct
//! requires access to a network layer, a cryptographic primitive, and a data provider that
//! gives appropriate access to the set of available data that we need to make consensus on.

mod alerts;
mod config;
mod consensus;
mod creation;
mod extender;
mod member;
mod network;
mod runway;
mod terminal;
mod units;

#[cfg(test)]
mod testing;

pub use aleph_bft_types::{
    Data, DataProvider, FinalizationHandler, Hasher, IncompleteMultisignatureError, Index, Indexed,
    Keychain, MultiKeychain, Multisigned, Network, NodeCount, NodeIndex, NodeMap, NodeSubset,
    PartialMultisignature, PartiallyMultisigned, Recipient, Round, SessionId, Signable, Signature,
    SignatureError, SignatureSet, Signed, SpawnHandle, TaskHandle, UncheckedSigned,
};
pub use config::{default_config, exponential_slowdown, Config, DelayConfig};
pub use member::{run_session, LocalIO};
pub use network::NetworkData;

type Receiver<T> = futures::channel::mpsc::UnboundedReceiver<T>;
type Sender<T> = futures::channel::mpsc::UnboundedSender<T>;
