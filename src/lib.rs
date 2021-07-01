//! Implements the Aleph BFT Consensus protocol as a "finality gadget". The [Member] struct
//! requires access to a network layer, a cryptographic primitive, and a data provider that
//! gives appropriate access to the set of available data that we need to make consensus on.

use codec::{Decode, Encode};
use futures::{channel::mpsc, Future};
use std::{fmt::Debug, hash::Hash as StdHash, pin::Pin};

use crate::nodes::NodeMap;

pub use config::{default_config, Config, DelayConfig};
pub use member::Member;
pub use network::{Network, NetworkData};
pub use nodes::{NodeCount, NodeIndex};

mod alerts;
mod consensus;
mod creator;
mod extender;
mod member;
mod network;
mod nodes;
mod signed;
pub use signed::*;
mod config;
pub mod rmc;
mod terminal;
mod testing;
mod units;

/// The number of a session for which the consensus is run.
pub type SessionId = u64;

/// Type responsible for fetching missing data.
pub type Fetch<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send>>;

/// Enum representing state of the data in a data base.
pub enum DataState<E> {
    Available,
    Missing(Fetch<E>),
}

/// The source of data items that consensus should order.
///
/// AlephBFT internally calls [`DataIO::get_data`] whenever a new unit is created and data needs to be placed inside.
/// The [`DataIO::send_ordered_batch`] method is called whenever a new round has been decided and thus a new batch of units
/// (or more precisely the data they carry) is available. Finally, [`DataIO::check_availability is used to validate and check availability of data.
/// The meaning of the latter might be unclear if we think of `Data` as being the actual data that is being ordered,
/// but in applications one often wants to use hashes of data (for instance block hashes) in which case it is crucial
/// for security that there is access to the actual data, cryptographically represented by a hash.
/// It is assumed that the implementation of DataIO makes best effort of fetch the data in case it is unavailable.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/aleph_bft_api.html for a discussion
/// and examples of how this trait can be implemented.
pub trait DataIO<Data> {
    type Error: Debug + 'static;
    /// Outputs a new data item to be ordered
    fn get_data(&self) -> Data;
    /// Returns future that indicates when the data becomes available or None if the data is already available.
    /// For applications where `Data` is actually a real data and not some reprentation of it, this may be trivially
    /// implemented to return always `None`.
    fn check_availability(&self, data: &Data) -> DataState<Self::Error>;
    /// Takes a new ordered batch of data item.
    fn send_ordered_batch(&mut self, data: OrderedBatch<Data>) -> Result<(), Self::Error>;
}

/// Indicates that an implementor has been assigned some index.
pub trait Index {
    fn index(&self) -> NodeIndex;
}

/// A hasher, used for creating identifiers for blocks or units.
pub trait Hasher: Eq + Clone + Send + Sync + Debug + 'static {
    /// A hash, as an identifier for a block or unit.
    type Hash: AsRef<[u8]>
        + Eq
        + Ord
        + Copy
        + Clone
        + Send
        + Sync
        + Debug
        + StdHash
        + Encode
        + Decode;

    fn hash(s: &[u8]) -> Self::Hash;
}

/// Data type that we want to order.
pub trait Data: Eq + Clone + Send + Sync + Debug + StdHash + Encode + Decode + 'static {}

impl<T> Data for T where T: Eq + Clone + Send + Sync + Debug + StdHash + Encode + Decode + 'static {}

/// An asynchronous round of the protocol.
pub type Round = u16;

/// Type for sending a new ordered batch of data items.
pub type OrderedBatch<Data> = Vec<Data>;

/// A handle for waiting the task's completion.
pub type TaskHandle = Pin<Box<dyn Future<Output = Result<(), ()>> + Send>>;

/// An abstraction for an execution engine for Rust's asynchronous tasks.
pub trait SpawnHandle: Clone + Send + 'static {
    /// Run a new task.
    fn spawn(&self, name: &'static str, task: impl Future<Output = ()> + Send + 'static);
    /// Run a new task and returns a handle to it. If there is some error or panic during
    /// execution of the task, the handle should return an error.
    fn spawn_essential(
        &self,
        name: &'static str,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> TaskHandle;
}

pub(crate) type Receiver<T> = mpsc::UnboundedReceiver<T>;
pub(crate) type Sender<T> = mpsc::UnboundedSender<T>;
