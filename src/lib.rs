//! Consensus crate level documentation is about to show here

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

pub trait NodeIdT: Clone + Display + Debug + Send + Eq + Hash + 'static {}

impl<I> NodeIdT for I where I: Clone + Display + Debug + Send + Eq + Hash + 'static {}

/// A hash, as an identifier for a block or unit.
pub trait HashT:
    // TODO remove From<u32> after adding proper hash impl
    Eq + Ord + Copy + Clone + Default + Send + Sync + Debug + Display + Hash + From<u32>
{
}

impl<H> HashT for H where
    // TODO remove From<u32> after adding proper hash impl
    H: Eq + Ord + Copy + Clone + Send + Sync + Default + Debug + Display + Hash + From<u32>
{
}

pub trait Environment {
    /// Unique identifiers for nodes
    type NodeId: NodeIdT;
    /// Hash type for units.
    type Hash: HashT;
    /// Hash type for blocks.
    type BlockHash: HashT;
    /// The ID of a consensus protocol instance.
    type InstanceId: HashT;

    type Crypto;
    type In: Stream<Item = Message<Self::BlockHash, Self::Hash>> + Send + Unpin;
    type Out: Sink<Message<Self::BlockHash, Self::Hash>, Error = Self::Error> + Send + Unpin;
    type Error: Send + Sync + std::fmt::Debug;

    fn finalize_block(&mut self, _h: Self::BlockHash);
    fn check_extends_finalized(&self, _h: Self::BlockHash) -> bool;
    fn best_block(&self) -> Self::BlockHash;
    fn check_available(&self, h: Self::BlockHash) -> bool;
    fn consensus_data(&self) -> (Self::Out, Self::In);
    fn hash(data: &[u8]) -> Self::Hash;
}

pub enum Error {}

pub type Round = usize;
pub type EpochId = usize;

#[derive(Clone, Debug, PartialEq)]
pub enum Message<B: HashT, H: HashT> {
    Multicast(Unit<B, H>),
    // request for a particular list of units (specified by (round, creator)) to a particular node
    FetchRequest(Vec<(Round, NodeIndex)>, NodeIndex),
    // requested units by a given request id
    FetchResponse(Vec<Unit<B, H>>, NodeIndex),
    SyncMessage,
    SyncResponse,
    Alert,
}

#[derive(Clone)]
pub struct ConsensusConfig {
    pub(crate) ix: NodeIndex,
    n_members: NodeCount,
    epoch_id: EpochId,
}

impl ConsensusConfig {
    pub fn new(ix: NodeIndex, n_members: NodeCount, epoch_id: EpochId) -> Self {
        ConsensusConfig {
            ix,
            n_members,
            epoch_id,
        }
    }
}

pub struct Consensus<E: Environment + 'static> {
    _conf: ConsensusConfig,
    creator: Option<Creator<E>>,
    terminal: Option<Terminal<E>>,
    extender: Option<Extender<E>>,
    syncer: Option<Syncer<E>>,
    finalizer: Option<Finalizer<E>>,
}

pub(crate) type Receiver<T> = mpsc::UnboundedReceiver<T>;
pub(crate) type Sender<T> = mpsc::UnboundedSender<T>;

impl<E: Environment + Send + Sync + 'static> Consensus<E> {
    pub fn new(conf: ConsensusConfig, env: E) -> Self {
        let (o, i) = env.consensus_data();
        let env = Arc::new(Mutex::new(env));

        let e = env.clone();
        let env_finalize = Box::new(move |h| e.lock().finalize_block(h));
        let e = env.clone();
        let env_extends_finalized = Box::new(move |h| e.lock().check_extends_finalized(h));

        let (finalizer, batch_tx) = Finalizer::<E>::new(env_finalize, env_extends_finalized);

        let my_ix = conf.ix;
        let n_members = conf.n_members;
        let epoch_id = conf.epoch_id;

        let (electors_tx, electors_rx) = mpsc::unbounded_channel();
        let extender = Some(Extender::<E>::new(electors_rx, batch_tx, n_members));

        let (syncer, requests_tx, incoming_units_rx, created_units_tx) = Syncer::<E>::new(o, i);

        let (parents_tx, parents_rx) = mpsc::unbounded_channel();

        let e = env.clone();
        let best_block = Box::new(move || e.lock().best_block());
        let creator = Some(Creator::<E>::new(
            parents_rx,
            created_units_tx,
            epoch_id,
            my_ix,
            n_members,
            best_block,
        ));

        let check_available = Box::new(move |h| env.lock().check_available(h));

        let mut terminal = Terminal::<E>::new(
            conf.ix,
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
        terminal.register_post_insert_hook(Box::new(
            move |u| if electors_tx.send(u.into()).is_err() {},
        ));
        let finalizer = Some(finalizer);
        let syncer = Some(syncer);
        let terminal = Some(terminal);

        Consensus {
            _conf: conf,
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
        debug!(target: "rush-init", "Starting all services...",);
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

        debug!(target: "rush-init", "All services started.",);

        // TODO add close signal
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ControlHash<H: HashT> {
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

    pub(crate) fn compute_hash(
        creator: NodeIndex,
        round: Round,
        _epoch_id: EpochId,
        control_hash: &ControlHash<H>,
    ) -> H {
        //TODO: need to write actual hashing here

        let n_parents = control_hash.n_parents().0;

        ((round * n_parents + creator.0) as u32).into()
    }

    pub(crate) fn new_from_parents(
        creator: NodeIndex,
        round: Round,
        epoch_id: EpochId,
        parents: NodeMap<Option<H>>,
        best_block: B,
    ) -> Self {
        let control_hash = ControlHash::new(parents);
        Unit {
            creator,
            round,
            epoch_id,
            hash: Self::compute_hash(creator, round, epoch_id, &control_hash),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn dummy() {
        let net = Network::new();
        let n_nodes = 2;
        let mut finalized_blocks_rxs = Vec::new();
        let mut handles = Vec::new();

        for node_ix in 0..n_nodes {
            let (mut env, rx) = environment::Environment::new(net.clone());
            finalized_blocks_rxs.push(rx);
            env.gen_chain(vec![(0.into(), vec![1.into()])]);
            let conf = ConsensusConfig::new(node_ix.into(), n_nodes.into(), 0);
            handles.push(tokio::spawn(Consensus::new(conf.clone(), env).run()));
        }

        for mut rx in finalized_blocks_rxs.drain(..) {
            let h = futures::executor::block_on(async { rx.recv().await.unwrap().hash() });
            assert_eq!(h, BlockHash(1));
        }
    }
}
