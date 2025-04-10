//! Implements the Aleph BFT Consensus protocol as a "finality gadget". The [run_session] function
//! requires access to a network layer, a cryptographic primitive, and a data provider that
//! gives appropriate access to the set of available data that we need to make consensus on.

mod alerts;
mod collection;
mod config;
mod consensus;
mod creation;
mod dag;
mod dissemination;
mod extension;
mod interface;
mod network;
mod terminator;
mod units;

mod backup;
mod task_queue;
#[cfg(test)]
mod testing;

pub use aleph_bft_types::{
    Data, DataProvider, FinalizationHandler, Hasher, IncompleteMultisignatureError, Index, Indexed,
    Keychain, MultiKeychain, Multisigned, Network, NodeCount, NodeIndex, NodeMap, NodeSubset,
    OrderedUnit, PartialMultisignature, PartiallyMultisigned, Recipient, Round, SessionId,
    Signable, Signature, SignatureError, SignatureSet, Signed, SpawnHandle, TaskHandle,
    UncheckedSigned, UnitFinalizationHandler,
};
pub use config::{
    create_config, default_config, default_delay_config, exponential_slowdown, Config, DelayConfig,
};
pub use consensus::run_session;
pub use interface::LocalIO;
pub use network::NetworkData;
pub use terminator::{handle_task_termination, Terminator};

type Receiver<T> = futures::channel::mpsc::UnboundedReceiver<T>;
type Sender<T> = futures::channel::mpsc::UnboundedSender<T>;
