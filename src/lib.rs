//! Implements the Aleph BFT Consensus protocol as a "finality gadget". The [Member] struct
//! requires access to a network layer, a cryptographic primitive, and a data provided that
//! gives appropriate access to the set of available data that we need to make consensus on.

use codec::{Decode, Encode};
use futures::{channel::mpsc, Future};
use std::{fmt::Debug, hash::Hash as StdHash};

use crate::nodes::{NodeCount, NodeIndex, NodeMap};

pub use config::{default_config, Config};
pub use member::Member;
pub use network::{Network, NetworkData};

mod bft;
mod consensus;
mod creator;
mod extender;
mod member;
mod network;
pub mod nodes;
mod signed;
pub use signed::*;
mod config;
mod terminal;
mod testing;
mod units;

pub trait DataIO<Data> {
    type Error: Debug;
    fn get_data(&self) -> Data;
    fn send_ordered_batch(&mut self, data: OrderedBatch<Data>) -> Result<(), Self::Error>;
}

pub type SessionId = u64;

pub trait Index {
    fn index(&self) -> NodeIndex;
}

/// A hasher, used for creating identifiers for blocks or units.
pub trait Hasher: Eq + Clone + Send + Sync + Debug + 'static {
    /// A hash, as an identifier for a block or unit.
    type Hash: AsRef<[u8]> + Eq + Ord + Copy + Clone + Send + Debug + StdHash + Encode + Decode;

    fn hash(s: &[u8]) -> Self::Hash;
}

/// Data type that we want to order.
pub trait Data: Eq + Clone + Send + Sync + Debug + StdHash + Encode + Decode + 'static {}

impl<T> Data for T where T: Eq + Clone + Send + Sync + Debug + StdHash + Encode + Decode + 'static {}

/// A round.
pub type Round = usize;

/// Type used in NotificationOut::MissingUnits to give additional info about the missing units that might
/// help the Environment to fetch them (currently this is the node_ix of the unit whose parents are missing).
#[derive(Clone, Debug, PartialEq, Encode, Decode)]
pub struct RequestAuxData {
    child_creator: NodeIndex,
}

impl RequestAuxData {
    fn new(child_creator: NodeIndex) -> Self {
        RequestAuxData { child_creator }
    }

    pub fn child_creator(&self) -> NodeIndex {
        self.child_creator
    }
}
/// Type for sending a new ordered batch of units
pub type OrderedBatch<Data> = Vec<Data>;

pub trait SpawnHandle: Clone + Send + 'static {
    fn spawn(&self, name: &'static str, task: impl Future<Output = ()> + Send + 'static);
}

pub(crate) type Receiver<T> = mpsc::UnboundedReceiver<T>;
pub(crate) type Sender<T> = mpsc::UnboundedSender<T>;
