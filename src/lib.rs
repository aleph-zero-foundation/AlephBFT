//! Implements the Aleph BFT Consensus protocol as a "finality gadget". The [Consensus] struct
//! requires access to an [Environment] object which black-boxes the network layer and gives
//! appropriate access to the set of available blocks that we need to make consensus on.

use codec::Encode;
use futures::{Sink, Stream};
use log::{debug, error};
use parking_lot::Mutex;
use std::{
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
};
use tokio::sync::mpsc;

use crate::{
    creator::Creator,
    extender::Extender,
    finalizer::Finalizer,
    nodes::{NodeCount, NodeIndex, NodeMap},
    syncer::Syncer,
    terminal::Terminal,
};

mod creator;
mod extender;
mod finalizer;
mod nodes;
mod syncer;
mod terminal;
mod testing;

pub trait MyIndex {
    fn my_index(&self) -> Option<NodeIndex>;
}
pub trait NodeIdT: Clone + Display + Debug + Send + Eq + Hash + Encode + MyIndex + 'static {}

impl<I> NodeIdT for I where
    I: Clone + Display + Debug + Send + Eq + Hash + Encode + MyIndex + 'static
{
}

/// A hash, as an identifier for a block or unit.
pub trait HashT: Eq + Ord + Copy + Clone + Send + Sync + Debug + Display + Hash + Encode {}

impl<H> HashT for H where H: Eq + Ord + Copy + Clone + Send + Sync + Debug + Display + Hash + Encode {}

/// A trait that describes the interaction of the [Consensus] component with the external world.
pub trait Environment {
    /// Unique identifiers for nodes
    type NodeId: NodeIdT;
    /// Hash type for units.
    type Hash: HashT;
    /// Hash type for blocks.
    type BlockHash: HashT;
    /// The ID of a consensus protocol instance.
    type InstanceId: HashT;
    type Hashing: Fn(&[u8]) -> Self::Hash + Send + Sync + 'static;

    type Crypto;
    type In: Stream<Item = NotificationIn<Self::BlockHash, Self::Hash>> + Send + Unpin;
    type Out: Sink<NotificationOut<Self::BlockHash, Self::Hash>, Error = Self::Error> + Send + Unpin;
    type Error: Send + Sync + std::fmt::Debug;

    /// Supposed to be called whenever a new block is finalized according to the protocol.
    /// If [finalize_block] is called first with `h1` and then with `h2` then necessarily
    /// `h2` must be a strict descendant of `h1`.
    fn finalize_block(&mut self, _h: Self::BlockHash);

    /// Checks if a particular block has been already finalized.
    fn check_extends_finalized(&self, _h: Self::BlockHash) -> bool;

    /// Outputs a block that the node should vote for. There is no concrete specification on
    /// what [best_block] should be only that it should guarantee that its output is always a
    /// descendant of the most recently finalized block (to guarantee liveness).
    fn best_block(&self) -> Self::BlockHash;

    /// Checks whether a given block is "available" meaning that its content has been downloaded
    /// by the current node. This is required as consensus messages from other committee members
    /// referring to unavailable blocks must be ignored (at least till the block shows).
    fn check_available(&self, h: Self::BlockHash) -> bool;

    /// Outputs two channel endpoints: transmitter of outgoing messages and receiver if incoming
    /// messages.
    fn consensus_data(&self) -> (Self::Out, Self::In);
    fn hash(data: &[u8]) -> Self::Hash;
    fn hashing() -> Self::Hashing;
}

pub enum Error {}

pub type Round = usize;
pub type EpochId = usize;

/// Type used in NotificationOut::MissingUnits to give additional info about the missing units that might
/// help the Environment to fetch them (currently this is the node_ix of the unit whose parents are missing).
#[derive(Clone, Debug, PartialEq)]
pub struct RequestAuxData {
    child_creator: NodeIndex,
}

impl RequestAuxData {
    fn new(child_creator: NodeIndex) -> Self {
        RequestAuxData { child_creator }
    }
}

/// Type for incoming notifications: Environment to Consensus.
#[derive(Clone, Debug, PartialEq)]
pub enum NotificationIn<B: HashT, H: HashT> {
    /// A notification carrying a single unit. This might come either from multicast or
    /// from a response to a request. This is of no importance at this layer.
    NewUnit(Unit<B, H>),
    // TODO: ResponseParents(H, Vec<B, H>) and Alert() notifications
}

/// Type for outgoing notifications: Consensus to Environment.
#[derive(Clone, Debug, PartialEq)]
pub enum NotificationOut<B: HashT, H: HashT> {
    // Notification about a unit created by this Consensus Node. Environment is meant to
    // disseminate this unit among other nodes.
    CreatedUnit(Unit<B, H>),
    /// Notification that some units are needed but missing. The role of the Environment
    /// is to fetch these unit (somehow). Auxiliary data is provided to help handle this request.
    MissingUnits(Vec<(Round, NodeIndex)>, RequestAuxData),
    // TODO: RequestParents(H) and Alert() notifications
}

#[derive(Clone)]
pub struct Config<NI: NodeIdT> {
    pub(crate) node_id: NI,
    n_members: NodeCount,
    epoch_id: EpochId,
}

impl<NI: NodeIdT> Config<NI> {
    pub fn new(node_id: NI, n_members: NodeCount, epoch_id: EpochId) -> Self {
        Config {
            node_id,
            n_members,
            epoch_id,
        }
    }
}

pub struct Consensus<E: Environment + 'static> {
    conf: Config<E::NodeId>,
    creator: Option<Creator<E>>,
    terminal: Option<Terminal<E>>,
    extender: Option<Extender<E>>,
    syncer: Option<Syncer<E>>,
    finalizer: Option<Finalizer<E>>,
}

pub(crate) type Receiver<T> = mpsc::UnboundedReceiver<T>;
pub(crate) type Sender<T> = mpsc::UnboundedSender<T>;

impl<E: Environment + Send + Sync + 'static> Consensus<E> {
    pub fn new(conf: Config<E::NodeId>, env: E) -> Self {
        let (o, i) = env.consensus_data();
        let env = Arc::new(Mutex::new(env));

        let e = env.clone();
        let env_finalize = Box::new(move |h| e.lock().finalize_block(h));
        let e = env.clone();
        let env_extends_finalized = Box::new(move |h| e.lock().check_extends_finalized(h));

        let (finalizer, batch_tx) =
            Finalizer::<E>::new(conf.node_id.clone(), env_finalize, env_extends_finalized);

        let n_members = conf.n_members;

        let (electors_tx, electors_rx) = mpsc::unbounded_channel();
        let extender = Some(Extender::<E>::new(
            conf.node_id.clone(),
            electors_rx,
            batch_tx,
            n_members,
        ));

        let (syncer, requests_tx, incoming_units_rx, created_units_tx) =
            Syncer::<E>::new(conf.node_id.clone(), o, i);

        let (parents_tx, parents_rx) = mpsc::unbounded_channel();

        let e = env.clone();
        let best_block = Box::new(move || e.lock().best_block());
        let hashing = Box::new(E::hashing());
        let creator = Some(Creator::<E>::new(
            conf.clone(),
            parents_rx,
            created_units_tx,
            best_block,
            hashing,
        ));

        let check_available = Box::new(move |h| env.lock().check_available(h));

        let mut terminal = Terminal::<E>::new(
            conf.node_id.clone(),
            incoming_units_rx,
            requests_tx.clone(),
            check_available,
        );

        // send a multicast request
        let my_ix = conf.node_id.my_index().unwrap();
        terminal.register_post_insert_hook(Box::new(move |u| {
            if my_ix == u.creator() {
                // send unit u corresponding to v
                let send_result = requests_tx.send(NotificationOut::CreatedUnit(u.into()));
                if let Err(e) = send_result {
                    error!(target:"rush-init", "Unable to place a Multicast request: {:?}.", e);
                }
            }
        }));
        // send a new parent candidate to the creator
        terminal.register_post_insert_hook(Box::new(move |u| {
            let send_result = parents_tx.send(u.into());
            if let Err(e) = send_result {
                error!(target:"rush-terminal", "Unable to send a unit to Creator: {:?}.", e);
            }
        }));
        // try to extend the partial order after adding a unit to the dag
        terminal.register_post_insert_hook(Box::new(move |u| {
            let send_result = electors_tx.send(u.into());
            if let Err(e) = send_result {
                error!(target:"rush-terminal", "Unable to send a unit to Extender: {:?}.", e);
            }
        }));
        let finalizer = Some(finalizer);
        let syncer = Some(syncer);
        let terminal = Some(terminal);

        Consensus {
            conf,
            creator,
            terminal,
            extender,
            syncer,
            finalizer,
        }
    }
}

// This is to be called from within substrate
impl<E: Environment> Consensus<E> {
    pub async fn run(mut self) {
        debug!(target: "rush-init", "{} Starting all services...", self.conf.node_id);
        let mut creator = self.creator.take().unwrap();
        let _creator_handle = tokio::spawn(async move { creator.create().await });
        let mut terminal = self.terminal.take().unwrap();
        let _terminal_handle = tokio::spawn(async move { terminal.run().await });
        let mut extender = self.extender.take().unwrap();
        let _extender_handle = tokio::spawn(async move { extender.extend().await });
        let mut syncer = self.syncer.take().unwrap();
        let _syncer_handle = tokio::spawn(async move { syncer.sync().await });
        let mut finalizer = self.finalizer.take().unwrap();
        let _finalizer_handle = tokio::spawn(async move { finalizer.finalize().await });

        debug!(target: "rush-init", "{} All services started.", self.conf.node_id);

        // TODO add close signal
    }
}

#[derive(Clone, Debug, Default, PartialEq, Encode)]
pub struct ControlHash<H: HashT> {
    // TODO we need to optimize it for it to take O(N) bits of memory not O(N) words.
    pub parents: NodeMap<bool>,
    pub hash: H,
}

impl<H: HashT> ControlHash<H> {
    fn new<Hashing: Fn(&[u8]) -> H>(parent_map: NodeMap<Option<H>>, hashing: &Hashing) -> Self {
        let mut bytes = vec![];
        parent_map
            .iter()
            .flatten()
            .map(|h| h.encode())
            .for_each(|b| bytes.extend(b));

        let hash = hashing(&bytes);
        let parents = parent_map.iter().map(|h| h.is_some()).collect();

        ControlHash { parents, hash }
    }

    pub(crate) fn n_parents(&self) -> NodeCount {
        NodeCount(self.parents.iter().filter(|&b| *b).count())
    }

    pub(crate) fn n_members(&self) -> NodeCount {
        NodeCount(self.parents.len())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Unit<B: HashT, H: HashT> {
    pub(crate) creator: NodeIndex,
    pub(crate) round: Round,
    pub(crate) epoch_id: EpochId,
    pub(crate) hash: H,
    pub(crate) control_hash: ControlHash<H>,
    pub(crate) best_block: B,
}

impl<B: HashT, H: HashT> Unit<B, H> {
    pub(crate) fn hash(&self) -> H {
        self.hash
    }
    pub(crate) fn creator(&self) -> NodeIndex {
        self.creator
    }
    pub(crate) fn round(&self) -> Round {
        self.round
    }

    pub(crate) fn new_from_parents<Hashing: Fn(&[u8]) -> H>(
        creator: NodeIndex,
        round: Round,
        epoch_id: EpochId,
        parents: NodeMap<Option<H>>,
        best_block: B,
        hashing: &Hashing,
    ) -> Self {
        let mut v = vec![];
        v.extend(creator.0.to_le_bytes().to_vec());
        v.extend(round.to_le_bytes().to_vec());
        v.extend(epoch_id.to_le_bytes().to_vec());
        v.extend(parents.encode());
        v.extend(best_block.encode());
        let control_hash = ControlHash::new(parents, &hashing);
        Unit {
            creator,
            round,
            epoch_id,
            hash: hashing(&v),
            control_hash,
            best_block,
        }
    }

    pub(crate) fn _new(
        creator: NodeIndex,
        round: Round,
        epoch_id: EpochId,
        hash: H,
        control_hash: ControlHash<H>,
        best_block: B,
    ) -> Self {
        Unit {
            creator,
            round,
            epoch_id,
            hash,
            control_hash,
            best_block,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::environment::{self, BlockHash, Network, NodeId};

    fn init_log() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::max())
            .is_test(true)
            .try_init();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn dummy() {
        init_log();
        let net = Network::new();
        let n_nodes = 16;
        let mut finalized_blocks_rxs = Vec::new();
        let mut handles = Vec::new();

        for node_ix in 0..n_nodes {
            let (mut env, rx) = environment::Environment::new(NodeId(node_ix), net.clone());
            finalized_blocks_rxs.push(rx);
            env.gen_chain(vec![(0.into(), vec![1.into()])]);
            let conf = Config::new(node_ix.into(), n_nodes.into(), 0);
            handles.push(tokio::spawn(Consensus::new(conf, env).run()));
        }

        for mut rx in finalized_blocks_rxs.drain(..) {
            let h = futures::executor::block_on(async { rx.recv().await.unwrap().hash() });
            assert_eq!(h, BlockHash(1));
        }
    }
}
