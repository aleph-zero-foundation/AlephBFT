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
mod signed;
mod terminal;
mod units;

pub use aleph_bft_types::{
    Data, DataProvider, FinalizationHandler, Hasher, Index, KeyBox, MultiKeychain, Network,
    NodeCount, NodeIndex, NodeMap, NodeSubset, PartialMultisignature, Recipient, Round, SessionId,
    Signable, Signature, SignatureSet, SpawnHandle, TaskHandle,
};
pub use config::{default_config, exponential_slowdown, Config, DelayConfig};
pub use member::run_session;
pub use network::NetworkData;

pub use signed::*;
pub mod rmc;
#[cfg(test)]
pub mod testing;

type Receiver<T> = futures::channel::mpsc::UnboundedReceiver<T>;
type Sender<T> = futures::channel::mpsc::UnboundedSender<T>;
