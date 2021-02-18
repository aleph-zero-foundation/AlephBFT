use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::{
    creator::Creator,
    extender::Extender,
    finalizer::Finalizer,
    nodes::{NodeCount, NodeIndex, NodeMap},
    terminal::Terminal,
    traits::{Environment, HashT},
};

pub enum Error {}

pub type UnitCoord = (u32, NodeIndex);

#[derive(Clone, Debug, PartialEq)]
pub enum Message<B: HashT, H: HashT> {
    Multicast(Unit<B, H>),
    // request for a particular list of units (specified by (round, creator)) to a particular node
    FetchRequest(Vec<UnitCoord>, NodeIndex),
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
    epoch_id: u32,
}

impl ConsensusConfig {
    pub fn new(ix: NodeIndex, n_members: NodeCount, epoch_id: u32) -> Self {
        ConsensusConfig {
            ix,
            n_members,
            epoch_id,
        }
    }
}

pub struct Consensus<E: Environment + 'static> {
    _conf: ConsensusConfig,
    _env: Arc<Mutex<E>>,
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

        let e = env.clone();
        let check_available = Box::new(move |h| e.lock().check_available(h));

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
                let _ = requests_tx.send(Message::Multicast(u.into()));
            }
        }));
        // send a new parent candidate to the creator
        terminal.register_post_insert_hook(Box::new(move |u| {
            let _ = parents_tx.send(u.into());
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
            _env: env,
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

        //join all the above
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
        NodeCount(self.parents.iter().filter(|&b| *b).count() as u32)
    }

    pub(crate) fn n_members(&self) -> NodeCount {
        NodeCount(self.parents.len() as u32)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Unit<B: HashT, H: HashT> {
    pub(crate) creator: NodeIndex,
    pub(crate) round: u32,
    pub(crate) epoch_id: u32, //we probably want a custom type for that
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
    pub(crate) fn round(&self) -> u32 {
        self.round
    }

    pub(crate) fn compute_hash(
        creator: NodeIndex,
        round: u32,
        _epoch_id: u32,
        control_hash: &ControlHash<H>,
    ) -> H {
        //TODO: need to write actual hashing here

        let n_parents = control_hash.n_parents().0;

        (round * n_parents + creator.0).into()
    }

    pub(crate) fn new_from_parents(
        creator: NodeIndex,
        round: u32,
        epoch_id: u32,
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
        round: u32,
        epoch_id: u32,
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

struct Syncer<E: Environment> {
    // outgoing messages
    messages_tx: E::Out,
    // incoming messages
    messages_rx: E::In,
    // channel for sending units to the terminal
    units_tx: Sender<Unit<E::BlockHash, E::Hash>>,
    // channel for receiving messages to the outside world
    requests_rx: Receiver<Message<E::BlockHash, E::Hash>>,
}

impl<E: Environment> Syncer<E> {
    fn new(
        messages_tx: E::Out,
        messages_rx: E::In,
    ) -> (
        Self,
        Sender<Message<E::BlockHash, E::Hash>>,
        Receiver<Unit<E::BlockHash, E::Hash>>,
        Sender<Unit<E::BlockHash, E::Hash>>,
    ) {
        let (units_tx, units_rx) = mpsc::unbounded_channel();
        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        (
            Syncer {
                messages_tx,
                messages_rx,
                units_tx: units_tx.clone(),
                requests_rx,
            },
            requests_tx,
            units_rx,
            units_tx,
        )
    }
    async fn sync(&mut self) {
        loop {
            tokio::select! {
                Some(m) = self.requests_rx.recv() => {
                    let _ = self.messages_tx.send(m).await;
                }
                Some(m) = self.messages_rx.next() => {
                    match m {
                        Message::Multicast(u) => if self.units_tx.send(u).is_err() {},
                        Message::FetchResponse(units, _) => units
                            .into_iter()
                            .for_each(|u| if self.units_tx.send(u).is_err() {}),
                        _ => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::testing::environment::{self, dispatch_nodes, BlockHash, Network};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn dummy() {
        let net = Network::new();
        let (env, mut finalized_blocks) = environment::Environment::new(net);
        env.gen_chain(vec![(0.into(), vec![1.into()])]);
        let n_nodes = 2;
        let conf = ConsensusConfig::new(0.into(), n_nodes.into(), 0);

        let check = Box::new(move || {
            let h = futures::executor::block_on(finalized_blocks.recv())
                .unwrap()
                .hash();
            assert_eq!(h, BlockHash(1));
        });

        dispatch_nodes(n_nodes, conf, env, check).await;
    }
}
