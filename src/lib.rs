//! Implements the Aleph BFT Consensus protocol as a "finality gadget". The [Consensus] struct
//! requires access to an [Environment] object which black-boxes the network layer and gives
//! appropriate access to the set of available blocks that we need to make consensus on.

use codec::{Decode, Encode};
use futures::{Future, Sink, Stream};
use log::{debug, error};
use std::{
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Duration,
};

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
pub mod nodes;
mod syncer;
mod terminal;
mod testing;

pub trait MyIndex {
    fn my_index(&self) -> Option<NodeIndex>;
}
pub trait NodeIdT:
    Clone + Debug + Display + Send + Eq + Hash + Encode + Decode + MyIndex + 'static
{
}

impl<I> NodeIdT for I where
    I: Clone + Debug + Display + Send + Eq + Hash + Encode + Decode + MyIndex + 'static
{
}

/// A hash, as an identifier for a block or unit.
pub trait HashT:
    Eq + Ord + Copy + Clone + Send + Sync + Debug + Display + Hash + Encode + Decode
{
}

impl<H> HashT for H where
    H: Eq + Ord + Copy + Clone + Send + Sync + Debug + Display + Hash + Encode + Decode
{
}

pub type Hashing<H> = Box<dyn Fn(&[u8]) -> H + Send + Sync + 'static>;

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

    type Crypto;
    type In: Stream<Item = NotificationIn<Self::BlockHash, Self::Hash>> + Send + Unpin;
    type Out: Sink<NotificationOut<Self::BlockHash, Self::Hash>, Error = Self::Error> + Send + Unpin;
    type Error: Send + Sync + std::fmt::Debug;

    /// Supposed to be called whenever a new block is finalized according to the protocol.
    /// If [finalize_block] is called first with `h1` and then with `h2` then necessarily
    /// `h2` must be a strict descendant of `h1`.
    fn finalize_block(&self, _h: Self::BlockHash);

    /// Checks if a particular block has been already finalized.
    fn check_extends_finalized(&self, _h: Self::BlockHash) -> bool;

    /// Outputs a block that the node should vote for. There is no concrete specification on
    /// what [best_block] should be only that it should guarantee that its output is always a
    /// descendant of the most recently finalized block (to guarantee liveness).
    fn best_block(&self) -> Self::BlockHash;

    /// Outputs two channel endpoints: transmitter of outgoing messages and receiver if incoming
    /// messages.
    fn consensus_data(&self) -> (Self::Out, Self::In);
    fn hashing() -> Hashing<Self::Hash>;
}

pub enum Error {}

/// A round.
pub type Round = usize;

/// A newtype of an epoch id.
///
/// This will be eventually moved to the Environment.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Decode, Encode)]
pub struct EpochId(pub u64);

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

/// Type for incoming notifications: Environment to Consensus.
#[derive(Clone, Debug, PartialEq)]
pub enum NotificationIn<B: HashT, H: HashT> {
    /// A notification carrying a single unit. This might come either from multicast or
    /// from a response to a request. This is of no importance at this layer.
    NewUnits(Vec<Unit<B, H>>),
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
    create_lag: Duration,
}

impl<NI: NodeIdT> Config<NI> {
    pub fn new(node_id: NI, n_members: NodeCount, epoch_id: EpochId, create_lag: Duration) -> Self {
        Config {
            node_id,
            n_members,
            epoch_id,
            create_lag,
        }
    }
}

pub trait SpawnHandle {
    fn spawn(&self, name: &'static str, task: impl Future<Output = ()> + Send + 'static);
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
    pub fn new(conf: Config<E::NodeId>, env: Arc<E>) -> Self {
        let (o, i) = env.consensus_data();

        let e = env.clone();
        let env_finalize = Box::new(move |h| e.finalize_block(h));
        let e = env.clone();
        let env_extends_finalized = Box::new(move |h| e.check_extends_finalized(h));

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

        let best_block = Box::new(move || env.best_block());
        let creator = Some(Creator::<E>::new(
            conf.clone(),
            parents_rx,
            created_units_tx,
            best_block,
            Box::new(E::hashing()),
        ));

        let mut terminal = Terminal::<E>::new(
            conf.node_id.clone(),
            incoming_units_rx,
            requests_tx.clone(),
            Box::new(E::hashing()),
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

impl<E: Environment> Consensus<E> {
    pub async fn run(mut self, spawn_handle: impl SpawnHandle, exit: oneshot::Receiver<()>) {
        debug!(target: "rush-root", "{} Starting all services...", self.conf.node_id);

        let (creator_exit, exit_rx) = oneshot::channel();
        let mut creator = self.creator.take().unwrap();
        spawn_handle.spawn(
            "consensus/creator",
            async move { creator.create(exit_rx).await },
        );

        let (terminal_exit, exit_rx) = oneshot::channel();
        let mut terminal = self.terminal.take().unwrap();
        spawn_handle.spawn(
            "consensus/terminal",
            async move { terminal.run(exit_rx).await },
        );

        let (extender_exit, exit_rx) = oneshot::channel();
        let mut extender = self.extender.take().unwrap();
        spawn_handle.spawn("consensus/extender", async move {
            extender.extend(exit_rx).await
        });

        let (syncer_exit, exit_rx) = oneshot::channel();
        let mut syncer = self.syncer.take().unwrap();
        spawn_handle.spawn(
            "consensus/syncer",
            async move { syncer.sync(exit_rx).await },
        );

        let (finalizer_exit, exit_rx) = oneshot::channel();
        let mut finalizer = self.finalizer.take().unwrap();
        spawn_handle.spawn("consensus/finalizer", async move {
            finalizer.finalize(exit_rx).await
        });

        debug!(target: "rush-root", "{} All services started.", self.conf.node_id);

        let _ = exit.await;
        // we stop no matter if received Ok or Err
        let _ = creator_exit.send(());
        let _ = terminal_exit.send(());
        let _ = extender_exit.send(());
        let _ = syncer_exit.send(());
        let _ = finalizer_exit.send(());

        debug!(target: "rush-root", "{} All services started.", self.conf.node_id);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Encode, Decode)]
pub struct ControlHash<H: HashT> {
    // TODO we need to optimize it for it to take O(N) bits of memory not O(N) words.
    pub parents: NodeMap<bool>,
    pub hash: H,
}

impl<H: HashT> ControlHash<H> {
    fn new<Hashing: Fn(&[u8]) -> H>(parent_map: &NodeMap<Option<H>>, hashing: &Hashing) -> Self {
        let hash = Self::combine_hashes(&parent_map, hashing);
        let parents = parent_map.iter().map(|h| h.is_some()).collect();

        ControlHash { parents, hash }
    }

    pub(crate) fn combine_hashes<Hashing: Fn(&[u8]) -> H>(
        parent_map: &NodeMap<Option<H>>,
        hashing: &Hashing,
    ) -> H {
        parent_map.using_encoded(hashing)
    }

    pub(crate) fn n_parents(&self) -> NodeCount {
        NodeCount(self.parents.iter().filter(|&b| *b).count())
    }

    pub(crate) fn n_members(&self) -> NodeCount {
        NodeCount(self.parents.len())
    }
}

type UnitRound = u64;

#[derive(Clone, Debug, Default, PartialEq, Encode, Decode)]
pub struct Unit<B: HashT, H: HashT> {
    pub(crate) creator: NodeIndex,
    round: UnitRound,
    pub(crate) epoch_id: EpochId,
    pub(crate) hash: H,
    pub(crate) control_hash: ControlHash<H>,
    pub(crate) best_block: B,
}

impl<B: HashT, H: HashT> Unit<B, H> {
    pub fn hash(&self) -> H {
        self.hash
    }
    pub fn creator(&self) -> NodeIndex {
        self.creator
    }
    pub fn round(&self) -> Round {
        self.round as Round
    }
    pub fn epoch_id(&self) -> EpochId {
        self.epoch_id
    }

    pub(crate) fn new_from_parents<Hashing: Fn(&[u8]) -> H>(
        creator: NodeIndex,
        round: Round,
        epoch_id: EpochId,
        parents: NodeMap<Option<H>>,
        best_block: B,
        hashing: &Hashing,
    ) -> Self {
        let control_hash = ControlHash::new(&parents, &hashing);
        let hash = (
            creator.0 as u64,
            round as u64,
            epoch_id.0 as u64,
            parents,
            best_block,
        )
            .using_encoded(hashing);
        Unit {
            creator,
            round: round as u64,
            epoch_id,
            hash,
            control_hash,
            best_block,
        }
    }

    pub fn new(
        creator: NodeIndex,
        round: Round,
        epoch_id: EpochId,
        hash: H,
        control_hash: ControlHash<H>,
        best_block: B,
    ) -> Self {
        Unit {
            creator,
            round: round as UnitRound,
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
    use crate::testing::environment::{self, BlockHash, Chain, Network, NodeId};
    use parking_lot::Mutex;

    fn init_log() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::max())
            .is_test(true)
            .try_init();
    }

    #[derive(Default, Clone)]
    struct Spawner {
        handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    }

    impl SpawnHandle for Spawner {
        fn spawn(&self, _name: &str, task: impl Future<Output = ()> + Send + 'static) {
            self.handles.lock().push(tokio::spawn(task))
        }
    }

    impl Spawner {
        async fn wait(&self) {
            for h in self.handles.lock().iter_mut() {
                let _ = h.await;
            }
        }
    }

    #[tokio::test(max_threads = 1)]
    async fn dummy() {
        init_log();
        let net = Network::new();
        let n_nodes = 16;
        let mut finalized_blocks_rxs = vec![];
        let mut exits = vec![];
        let spawner = Spawner::default();

        for node_ix in 0..n_nodes {
            let (env, rx) = environment::Environment::new(NodeId(node_ix), net.clone());
            finalized_blocks_rxs.push(rx);
            env.gen_chain(vec![(0.into(), vec![1.into()])]);
            let conf = Config::new(
                node_ix.into(),
                n_nodes.into(),
                EpochId(0),
                Duration::from_millis(10),
            );
            let (exit_tx, exit_rx) = oneshot::channel();
            exits.push(exit_tx);
            spawner.spawn(
                "consensus",
                Consensus::new(conf, env).run(spawner.clone(), exit_rx),
            );
        }

        for mut rx in finalized_blocks_rxs.drain(..) {
            let h = rx.recv().await.unwrap().hash();
            assert_eq!(h, BlockHash(1));
        }

        exits.into_iter().for_each(|tx| {
            let _ = tx.send(());
        });
        spawner.wait().await;
    }

    #[tokio::test(max_threads = 1)]
    async fn finalize_blocks_random_chain() {
        init_log();
        let net = Network::new();
        let n_nodes = 7;
        let n_blocks = 10usize;
        let mut finalized_blocks_rxs = Vec::new();
        let mut exits = vec![];
        let spawner = Spawner::default();

        for node_ix in 0..n_nodes {
            let node_chain = Arc::new(Mutex::new(Chain::new()));
            let seed_delay = node_ix as u64;
            let _ = tokio::spawn(Chain::grow_chain(node_chain.clone(), 5, seed_delay));
            let (env, rx) = environment::Environment::new_with_chain(
                NodeId(node_ix),
                net.clone(),
                node_chain.clone(),
            );
            finalized_blocks_rxs.push(rx);
            let conf = Config::new(
                node_ix.into(),
                n_nodes.into(),
                EpochId(0),
                Duration::from_millis(15),
            );
            let (exit_tx, exit_rx) = oneshot::channel();
            exits.push(exit_tx);
            spawner.spawn(
                "consensus",
                Consensus::new(conf, env).run(spawner.clone(), exit_rx),
            );
        }
        let mut blocks_per_node = Vec::new();
        for mut rx in finalized_blocks_rxs.drain(..) {
            let mut initial_blocks = Vec::new();
            for _ in 0..n_blocks {
                let block = rx.recv().await;
                initial_blocks.push(block.unwrap().hash());
            }
            blocks_per_node.push(initial_blocks);
        }
        let blocks_node_0 = blocks_per_node[0].clone();
        for blocks in blocks_per_node {
            assert_eq!(blocks, blocks_node_0);
        }

        exits.into_iter().for_each(|tx| {
            let _ = tx.send(());
        });
        spawner.wait().await;
    }
}
