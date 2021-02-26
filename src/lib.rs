//! Implements the Aleph BFT Consensus protocol as a "finality gadget". The [Consensus] struct
//! requires access to an [Environment] object which black-boxes the network layer and gives
//! appropriate access to the set of available blocks that we need to make consensus on.

use codec::{Encode, Output};
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

pub trait NodeIdT: Clone + Display + Debug + Send + Eq + Hash + Encode + 'static {}

impl<I> NodeIdT for I where I: Clone + Display + Debug + Send + Eq + Hash + Encode + 'static {}

/// A hash, as an identifier for a block or unit.
pub trait HashT:
    Eq + Ord + Copy + Clone + Default + Send + Sync + Debug + Display + Hash + Encode
{
}

impl<H> HashT for H where
    H: Eq + Ord + Copy + Clone + Send + Sync + Default + Debug + Display + Hash + Encode
{
}

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
    type In: Stream<Item = Message<Self::BlockHash, Self::Hash>> + Send + Unpin;
    type Out: Sink<Message<Self::BlockHash, Self::Hash>, Error = Self::Error> + Send + Unpin;
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

/// Type for Consensus messages.
#[derive(Clone, Debug, PartialEq)]
pub enum Message<B: HashT, H: HashT> {
    /// The most common message: multicasting a unit to all committee memebers.
    Multicast(Unit<B, H>),
    /// Request for a particular list of units (specified by (round, creator)) to a particular node.
    FetchRequest(Vec<(Round, NodeIndex)>, NodeIndex),
    /// Response to a FetchRequest.
    FetchResponse(Vec<Unit<B, H>>, NodeIndex),
    SyncMessage,
    SyncResponse,
    Alert,
}

#[derive(Clone)]
pub struct ConsensusConfig<E: Environment> {
    pub(crate) node_id: E::NodeId,
    //TODO: we need to get rid of my_ix field here when adding support for non-committee nodes
    my_ix: NodeIndex,
    n_members: NodeCount,
    epoch_id: EpochId,
}

impl<E: Environment> ConsensusConfig<E> {
    pub fn new(
        node_id: E::NodeId,
        my_ix: NodeIndex,
        n_members: NodeCount,
        epoch_id: EpochId,
    ) -> Self {
        ConsensusConfig {
            node_id,
            my_ix,
            n_members,
            epoch_id,
        }
    }
}

pub struct Consensus<E: Environment + 'static> {
    conf: ConsensusConfig<E>,
    creator: Option<Creator<E>>,
    terminal: Option<Terminal<E>>,
    extender: Option<Extender<E>>,
    syncer: Option<Syncer<E>>,
    finalizer: Option<Finalizer<E>>,
}

pub(crate) type Receiver<T> = mpsc::UnboundedReceiver<T>;
pub(crate) type Sender<T> = mpsc::UnboundedSender<T>;

impl<E: Environment + Send + Sync + 'static> Consensus<E> {
    pub fn new(conf: ConsensusConfig<E>, env: E) -> Self {
        let (o, i) = env.consensus_data();
        let env = Arc::new(Mutex::new(env));

        let e = env.clone();
        let env_finalize = Box::new(move |h| e.lock().finalize_block(h));
        let e = env.clone();
        let env_extends_finalized = Box::new(move |h| e.lock().check_extends_finalized(h));

        let (finalizer, batch_tx) =
            Finalizer::<E>::new(conf.node_id.clone(), env_finalize, env_extends_finalized);

        let my_ix = conf.my_ix;
        let n_members = conf.n_members;
        let epoch_id = conf.epoch_id;

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
            conf.node_id.clone(),
            my_ix,
            parents_rx,
            created_units_tx,
            epoch_id,
            n_members,
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
        terminal.register_post_insert_hook(Box::new(move |u| {
            if my_ix == u.creator() {
                // send unit u corresponding to v
                let send_result = requests_tx.send(Message::Multicast(u.into()));
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
            terminal,
            extender,
            creator,
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
    //TODO need to actually compute the hash instead of return default
    fn new(parent_map: NodeMap<Option<H>>) -> Self {
        let hash = H::default();
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

impl<B: HashT, H: HashT> Encode for Unit<B, H> {
    fn encode_to<T: Output>(&self, dest: &mut T) {
        let mut bytes = self.epoch_id.to_le_bytes().to_vec();
        bytes.append(&mut self.creator.0.to_le_bytes().to_vec());
        bytes.append(&mut self.round.to_le_bytes().to_vec());
        bytes.append(&mut self.hash.encode());
        bytes.append(&mut self.control_hash.encode());
        bytes.append(&mut self.best_block.encode());
        Encode::encode_to(&bytes, dest)
    }
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
        hashing: Hashing,
    ) -> Self {
        let mut v = vec![];
        v.extend(creator.0.to_le_bytes().to_vec());
        v.extend(round.to_le_bytes().to_vec());
        v.extend(epoch_id.to_le_bytes().to_vec());
        v.extend(parents.encode());
        v.extend(best_block.encode());
        let control_hash = ControlHash::new(parents);
        Unit {
            creator,
            round,
            epoch_id,
            hash: (hashing(&v)),
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
    use crate::testing::environment::{self, BlockHash, Network};

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
        let n_nodes = 4;
        let mut finalized_blocks_rxs = Vec::new();
        let mut handles = Vec::new();

        for node_ix in 0..n_nodes {
            let (mut env, rx) = environment::Environment::new(net.clone());
            finalized_blocks_rxs.push(rx);
            env.gen_chain(vec![(0.into(), vec![1.into()])]);
            let conf = ConsensusConfig::new(node_ix.into(), node_ix.into(), n_nodes.into(), 0);
            handles.push(tokio::spawn(Consensus::new(conf, env).run()));
        }

        for mut rx in finalized_blocks_rxs.drain(..) {
            let h = futures::executor::block_on(async { rx.recv().await.unwrap().hash() });
            assert_eq!(h, BlockHash(1));
        }
    }
}
