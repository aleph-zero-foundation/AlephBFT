//! Implements the Aleph BFT Consensus protocol as a "finality gadget". The [Consensus] struct
//! requires access to an [Environment] object which black-boxes the network layer and gives
//! appropriate access to the set of available blocks that we need to make consensus on.

use codec::{Decode, Encode};
use futures::{Future, Sink, Stream};
use log::{debug, error};
use std::{
    fmt::{Debug, Display},
    hash::Hash,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Duration,
};

use crate::{creator::Creator, extender::Extender, syncer::Syncer, terminal::Terminal};

pub use crate::nodes::{NodeCount, NodeIndex, NodeMap};

mod creator;
mod extender;
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

pub enum Error {}

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

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct UnitCoord {
    pub creator: NodeIndex,
    pub round: u64,
}

impl<H: HashT> From<Unit<H>> for UnitCoord {
    fn from(unit: Unit<H>) -> Self {
        UnitCoord {
            creator: unit.creator(),
            round: unit.round() as u64,
        }
    }
}

impl<H: HashT> From<&Unit<H>> for UnitCoord {
    fn from(unit: &Unit<H>) -> Self {
        UnitCoord {
            creator: unit.creator(),
            round: unit.round() as u64,
        }
    }
}

impl From<(usize, NodeIndex)> for UnitCoord {
    fn from(coord: (usize, NodeIndex)) -> Self {
        UnitCoord {
            creator: coord.1,
            round: coord.0 as u64,
        }
    }
}

/// Type for incoming notifications: Environment to Consensus.
#[derive(Clone, Debug, PartialEq)]
pub enum NotificationIn<H: HashT> {
    /// A notification carrying a single unit. This might come either from multicast or
    /// from a response to a request. This is of no importance at this layer.
    NewUnits(Vec<Unit<H>>),
    /// Response to a request to decode parents when the control hash is wrong.
    UnitParents(H, Vec<H>),
}

/// Type for outgoing notifications: Consensus to Environment.
#[derive(Clone, Debug, PartialEq)]
pub enum NotificationOut<H: HashT> {
    /// Notification about a preunit created by this Consensus Node. Environment is meant to
    /// disseminate this preunit among other nodes.
    CreatedPreUnit(PreUnit<H>),
    /// Notification that some units are needed but missing. The role of the Environment
    /// is to fetch these unit (somehow). Auxiliary data is provided to help handle this request.
    MissingUnits(Vec<UnitCoord>, RequestAuxData),
    /// Notification that Consensus has parents incompatible with the control hash.
    WrongControlHash(H),
    /// Notification that a new unit has been added to the DAG, list of decoded parents provided
    AddedToDag(H, Vec<H>),
}

/// Type for sending a new ordered batch of units
pub type OrderedBatch<H> = Vec<H>;

#[derive(Clone)]
pub struct Config<NI: NodeIdT> {
    pub(crate) node_id: NI,
    n_members: NodeCount,
    create_lag: Duration,
}

impl<NI: NodeIdT> Config<NI> {
    pub fn new(node_id: NI, n_members: NodeCount, create_lag: Duration) -> Self {
        Config {
            node_id,
            n_members,
            create_lag,
        }
    }
}

pub trait SpawnHandle {
    fn spawn(&self, name: &'static str, task: impl Future<Output = ()> + Send + 'static);
}

pub struct Consensus<H: HashT, NI: NodeIdT> {
    conf: Config<NI>,
    creator: Option<Creator<H, NI>>,
    terminal: Option<Terminal<H, NI>>,
    extender: Option<Extender<H, NI>>,
    syncer: Option<Syncer<H, NI>>,
}

pub(crate) type Receiver<T> = mpsc::UnboundedReceiver<T>;
pub(crate) type Sender<T> = mpsc::UnboundedSender<T>;

impl<H: HashT + 'static, NI: NodeIdT> Consensus<H, NI> {
    pub fn new(
        conf: Config<NI>,
        ntfct_env_rx: impl Stream<Item = NotificationIn<H>> + Send + Unpin + 'static,
        ntfct_env_tx: impl Sink<NotificationOut<H>, Error = Box<dyn std::error::Error>>
            + Send
            + Unpin
            + 'static,
        ordered_batch_tx: Sender<OrderedBatch<H>>,
        hashing: impl Fn(&[u8]) -> H + Send + Copy + 'static,
    ) -> Self {
        let n_members = conf.n_members;

        let (electors_tx, electors_rx) = mpsc::unbounded_channel();
        let extender = Some(Extender::<H, NI>::new(
            conf.node_id.clone(),
            n_members,
            electors_rx,
            ordered_batch_tx,
        ));

        let (syncer, ntfct_common_tx, ntfct_term_rx) =
            Syncer::new(conf.node_id.clone(), ntfct_env_tx, ntfct_env_rx);

        let (parents_tx, parents_rx) = mpsc::unbounded_channel();
        let new_units_tx = ntfct_common_tx.clone();
        let creator = Some(Creator::new(
            conf.clone(),
            parents_rx,
            new_units_tx,
            hashing,
        ));

        let mut terminal = Terminal::new(
            conf.node_id.clone(),
            hashing,
            ntfct_term_rx,
            ntfct_common_tx,
        );

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
        let syncer = Some(syncer);
        let terminal = Some(terminal);

        Consensus {
            conf,
            creator,
            terminal,
            extender,
            syncer,
        }
    }
}

impl<H: HashT + 'static, NI: NodeIdT> Consensus<H, NI> {
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

        debug!(target: "rush-root", "{} All services started.", self.conf.node_id);

        let _ = exit.await;
        // we stop no matter if received Ok or Err
        let _ = creator_exit.send(());
        let _ = terminal_exit.send(());
        let _ = extender_exit.send(());
        let _ = syncer_exit.send(());

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
    fn new(parent_map: &NodeMap<Option<H>>, hashing: impl Fn(&[u8]) -> H) -> Self {
        let hash = Self::combine_hashes(&parent_map, hashing);
        let parents = parent_map.iter().map(|h| h.is_some()).collect();

        ControlHash { parents, hash }
    }

    pub fn combine_hashes(parent_map: &NodeMap<Option<H>>, hashing: impl Fn(&[u8]) -> H) -> H {
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
pub struct PreUnit<H: HashT> {
    pub creator: NodeIndex,
    round: UnitRound,
    pub control_hash: ControlHash<H>,
}

impl<H: HashT> PreUnit<H> {
    pub fn creator(&self) -> NodeIndex {
        self.creator
    }

    pub fn round(&self) -> Round {
        self.round as Round
    }

    pub(crate) fn new_from_parents(
        creator: NodeIndex,
        round: Round,
        parents: NodeMap<Option<H>>,
        hashing: impl Fn(&[u8]) -> H,
    ) -> Self {
        let control_hash = ControlHash::new(&parents, &hashing);
        PreUnit {
            creator,
            round: round as u64,
            control_hash,
        }
    }

    pub fn new(creator: NodeIndex, round: Round, control_hash: ControlHash<H>) -> Self {
        PreUnit {
            creator,
            round: round as UnitRound,
            control_hash,
        }
    }
}

impl<H: HashT> From<PreUnit<H>> for NotificationOut<H> {
    fn from(pu: PreUnit<H>) -> Self {
        NotificationOut::CreatedPreUnit(pu)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Encode, Decode)]
pub struct Unit<H: HashT> {
    pub(crate) creator: NodeIndex,
    round: UnitRound,
    pub(crate) hash: H,
    pub(crate) control_hash: ControlHash<H>,
}

impl<H: HashT> Unit<H> {
    pub fn hash(&self) -> H {
        self.hash
    }

    pub fn creator(&self) -> NodeIndex {
        self.creator
    }
    pub fn round(&self) -> Round {
        self.round as Round
    }

    pub fn new(creator: NodeIndex, round: Round, hash: H, control_hash: ControlHash<H>) -> Self {
        Unit {
            creator,
            round: round as UnitRound,
            hash,
            control_hash,
        }
    }

    pub fn new_from_preunit(pu: PreUnit<H>, hash: H) -> Self {
        Unit {
            creator: pu.creator,
            round: pu.round,
            hash,
            control_hash: pu.control_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::mock::{hashing, Hash, Network, NodeId};
    use futures::{channel, sink::SinkExt, stream::StreamExt};
    use parking_lot::Mutex;
    use std::sync::Arc;

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
    async fn small() {
        init_log();
        let net = Network::new();
        let n_nodes = 16;
        let mut exits = vec![];
        let mut batch_rxs = vec![];
        let spawner = Spawner::default();

        for node_ix in 0..n_nodes {
            let (o, i) = net.consensus_data(node_ix.into());
            let conf =
                Config::<NodeId>::new(node_ix.into(), n_nodes.into(), Duration::from_millis(10));
            let (exit_tx, exit_rx) = oneshot::channel();
            exits.push(exit_tx);
            let (batch_tx, batch_rx) = mpsc::unbounded_channel();
            batch_rxs.push(batch_rx);
            spawner.spawn(
                "consensus",
                Consensus::new(conf, i, o, batch_tx, hashing).run(spawner.clone(), exit_rx),
            );
        }

        let mut batches = vec![];
        for mut rx in batch_rxs.drain(..) {
            let batch = rx.recv().await.unwrap();
            assert!(!batch.is_empty());
            batches.push(batch);
        }

        // TODO add better checks
        for node_ix in 1..n_nodes {
            assert_eq!(batches[0], batches[node_ix]);
        }

        exits.into_iter().for_each(|tx| {
            let _ = tx.send(());
        });
        spawner.wait().await;
    }

    #[tokio::test(max_threads = 1)]
    async fn catches_wrong_control_hash() {
        init_log();
        let n_nodes = 4;
        let spawner = Spawner::default();
        let node_ix = 0;
        let (mut tx_in, rx_in) = channel::mpsc::unbounded();
        let (tx_out, mut rx_out) = channel::mpsc::unbounded();
        let tx_out = tx_out.sink_map_err(|e| e.into());
        let conf = Config::<NodeId>::new(node_ix.into(), n_nodes.into(), Duration::from_millis(10));
        let (exit_tx, exit_rx) = oneshot::channel();
        let (batch_tx, _batch_rx) = mpsc::unbounded_channel();
        spawner.spawn(
            "consensus",
            Consensus::new(conf, rx_in, tx_out, batch_tx, hashing).run(spawner.clone(), exit_rx),
        );
        let mut bad_pu =
            PreUnit::new_from_parents(1.into(), 0, (vec![None; n_nodes]).into(), hashing);
        let bad_control_hash = Hash(1111111);
        assert!(
            bad_control_hash != bad_pu.control_hash.hash,
            "Bad control hash cannot be the correct one."
        );
        bad_pu.control_hash.hash = bad_control_hash;
        let bad_hash = Hash(1234567);
        let bad_unit = Unit::new_from_preunit(bad_pu, bad_hash);
        let _ = tx_in.send(NotificationIn::NewUnits(vec![bad_unit])).await;
        loop {
            let notification = rx_out.next().await.unwrap();
            debug!("notification {:?}", notification);
            if let NotificationOut::WrongControlHash(h) = notification {
                assert_eq!(h, bad_hash, "Expected notification for our bad unit.");
                break;
            }
        }

        let _ = exit_tx.send(());
        spawner.wait().await;
    }

    // #[tokio::test(max_threads = 1)]
    // async fn finalize_blocks_random_chain() {
    //     init_log();
    //     let net = Network::new();
    //     let n_nodes = 7;
    //     let n_blocks = 10usize;
    //     let mut finalized_blocks_rxs = Vec::new();
    //     let mut exits = vec![];
    //     let spawner = Spawner::default();

    //     for node_ix in 0..n_nodes {
    //         let node_chain = Arc::new(Mutex::new(Chain::new()));
    //         let seed_delay = node_ix as u64;
    //         let _ = tokio::spawn(Chain::grow_chain(node_chain.clone(), 5, seed_delay));
    //         let (env, rx) = environment::Environment::new_with_chain(
    //             NodeId(node_ix),
    //             net.clone(),
    //             node_chain.clone(),
    //         );
    //         finalized_blocks_rxs.push(rx);
    //         let conf = Config::new(node_ix.into(), n_nodes.into(), Duration::from_millis(15));
    //         let (exit_tx, exit_rx) = oneshot::channel();
    //         exits.push(exit_tx);
    //         spawner.spawn(
    //             "consensus",
    //             Consensus::new(conf, env).run(spawner.clone(), exit_rx),
    //         );
    //     }
    //     let mut blocks_per_node = Vec::new();
    //     for mut rx in finalized_blocks_rxs.drain(..) {
    //         let mut initial_blocks = Vec::new();
    //         for _ in 0..n_blocks {
    //             let block = rx.recv().await;
    //             initial_blocks.push(block.unwrap().hash());
    //         }
    //         blocks_per_node.push(initial_blocks);
    //     }
    //     let blocks_node_0 = blocks_per_node[0].clone();
    //     for blocks in blocks_per_node {
    //         assert_eq!(blocks, blocks_node_0);
    //     }

    //     exits.into_iter().for_each(|tx| {
    //         let _ = tx.send(());
    //     });
    //     spawner.wait().await;
    // }
}
