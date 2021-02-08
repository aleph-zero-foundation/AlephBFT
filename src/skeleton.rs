use futures::prelude::*;
use parking_lot::Mutex;
use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};
use tokio::sync::mpsc;

use crate::{
    creator::Creator,
    dag::Vertex,
    nodes::{NodeCount, NodeIndex, NodeMap},
    terminal::Terminal,
    traits::{Environment, HashT},
};

pub enum Error {}

#[derive(Clone, Debug, PartialEq)]
pub enum Message<H: HashT> {
    Multicast(CHUnit<H>),
    // request for a particular list of units (specified by (round, creator)) to a particular node
    FetchRequest(Vec<(u32, NodeIndex)>, NodeIndex),
    // requested units by a given request id
    FetchResponse(Vec<CHUnit<H>>, NodeIndex),
    SyncMessage,
    SyncResponse,
    Alert,
}

pub struct ConsensusConfig {
    ix: NodeIndex,
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

type JoinHandle<E> = tokio::task::JoinHandle<Result<(), <E as Environment>::Error>>;

pub struct Consensus<E: Environment + 'static> {
    _conf: ConsensusConfig,
    _env: Arc<Mutex<E>>,
    _creator: JoinHandle<E>,
    _terminal: JoinHandle<E>,
    _extender: JoinHandle<E>,
    _syncer: JoinHandle<E>,
    _finalizer: JoinHandle<E>,
}

pub(crate) type Receiver<T> = mpsc::UnboundedReceiver<T>;
pub(crate) type Sender<T> = mpsc::UnboundedSender<T>;

impl<E: Environment + Send + Sync + 'static> Consensus<E> {
    pub fn new(conf: ConsensusConfig, env: E) -> Self {
        let (o, i) = env.consensus_data();
        let env = Arc::new(Mutex::new(env));

        let e = env.clone();
        let (finalizer, batch_tx) =
            Finalizer::<E>::new(Box::new(move |h| e.lock().finalize_block(h)));

        let (electors_tx, electors_rx) = mpsc::unbounded_channel();
        let extender = Extender::<E>::new(electors_rx, batch_tx);

        let (syncer, requests_tx, incoming_units_rx, created_units_tx) = Syncer::<E>::new(o, i);

        let (parents_tx, parents_rx) = mpsc::unbounded_channel();
        let my_ix = conf.ix;
        let n_members = conf.n_members;
        let epoch_id = conf.epoch_id;
        let e = env.clone();
        let best_block = Box::new(move || e.lock().best_block());
        let creator = Creator::<E>::new(
            parents_rx,
            created_units_tx,
            epoch_id,
            my_ix,
            n_members,
            best_block,
        );

        let mut terminal = Terminal::<E>::new(conf.ix, incoming_units_rx, requests_tx.clone());
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

        let creator = tokio::spawn(creator);
        let terminal = tokio::spawn(terminal);
        let extender = tokio::spawn(extender);
        let syncer = tokio::spawn(syncer);
        let finalizer = tokio::spawn(finalizer);

        Consensus {
            _conf: conf,
            _env: env,
            _terminal: terminal,
            _extender: extender,
            _creator: creator,
            _syncer: syncer,
            _finalizer: finalizer,
        }
    }
}

// This is to be called from within substrate
impl<E: Environment> Future for Consensus<E> {
    type Output = Result<(), E::Error>;

    fn poll(self: Pin<&mut Self>, _: &mut task::Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
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
        let mut parents = NodeMap::new_with_len(NodeCount(parent_map.len() as u32));
        for (i, maybe_hash) in parent_map.enumerate() {
            if let Some(_h) = maybe_hash {
                parents[i] = true;
            // hash = H(hash || _h);
            } else {
                parents[i] = false;
            }
        }
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
pub struct CHUnit<H: HashT> {
    pub(crate) creator: NodeIndex,
    pub(crate) round: u32,
    pub(crate) epoch_id: u32, //we probably want a custom type for that
    pub(crate) hash: H,
    pub(crate) control_hash: ControlHash<H>,
    pub(crate) best_block: H,
}

impl<H: HashT> CHUnit<H> {
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
        _creator: NodeIndex,
        _round: u32,
        _epoch_id: u32,
        _parents: NodeMap<Option<H>>,
    ) -> H {
        //TODO: need to write actual hashing here
        H::default()
    }

    pub(crate) fn new_from_parents(
        creator: NodeIndex,
        round: u32,
        epoch_id: u32,
        parents: NodeMap<Option<H>>,
        best_block: H,
    ) -> Self {
        CHUnit {
            creator,
            round,
            epoch_id,
            hash: Self::compute_hash(creator, round, epoch_id, parents.clone()),
            control_hash: ControlHash::new(parents),
            best_block,
        }
    }

    pub(crate) fn _new(
        creator: NodeIndex,
        round: u32,
        epoch_id: u32,
        hash: H,
        control_hash: ControlHash<H>,
        best_block: H,
    ) -> Self {
        CHUnit {
            creator,
            round,
            epoch_id,
            hash,
            control_hash,
            best_block,
        }
    }
}

// a process responsible for extending the partial order
struct Extender<E: Environment> {
    electors: Receiver<Vertex<E::Hash>>,
    finalizer_tx: Sender<Vec<E::Hash>>,
}

impl<E: Environment> Extender<E> {
    fn new(electors: Receiver<Vertex<E::Hash>>, finalizer_tx: Sender<Vec<E::Hash>>) -> Self {
        Extender {
            electors,
            finalizer_tx,
        }
    }
    fn new_head(&mut self, _v: Vertex<E::Hash>) -> bool {
        false
    }
    fn next_batch(&self) {}
}

impl<E: Environment> Future for Extender<E> {
    type Output = Result<(), E::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        while let Poll::Ready(Some(v)) = self.electors.poll_recv(cx) {
            let _ = (*self).finalizer_tx.send(vec![v.best_block()]); // just for tests; remove at the earliest convenience
            while self.new_head(v.clone()) {
                self.next_batch();
            }
        }
        Poll::Pending
    }
}

struct Finalizer<E: Environment> {
    batch_rx: Receiver<Vec<E::Hash>>,
    finalizer: Box<dyn Fn(E::Hash) + Sync + Send + 'static>,
}

impl<E: Environment> Finalizer<E> {
    fn new(
        finalizer: Box<dyn Fn(E::Hash) + Send + Sync + 'static>,
    ) -> (Self, Sender<Vec<E::Hash>>) {
        let (batch_tx, batch_rx) = mpsc::unbounded_channel();
        (
            Finalizer {
                batch_rx,
                finalizer,
            },
            batch_tx,
        )
    }
}

impl<E: Environment> Future for Finalizer<E> {
    type Output = Result<(), E::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        while let Poll::Ready(Some(batch)) = self.batch_rx.poll_recv(cx) {
            for h in batch {
                ((*self).finalizer)(h);
            }
        }
        Poll::Pending
    }
}

struct Syncer<E: Environment> {
    // outgoing messages
    messages_tx: E::Out,
    // incoming messages
    messages_rx: E::In,
    // channel for sending units to the terminal
    units_tx: Sender<CHUnit<E::Hash>>,
    // channel for receiving messages to the outside world
    requests_rx: Receiver<Message<E::Hash>>,
}

impl<E: Environment> Syncer<E> {
    fn new(
        messages_tx: E::Out,
        messages_rx: E::In,
    ) -> (
        Self,
        Sender<Message<E::Hash>>,
        Receiver<CHUnit<E::Hash>>,
        Sender<CHUnit<E::Hash>>,
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
}

impl<E: Environment> Future for Syncer<E> {
    type Output = Result<(), E::Error>;

    // TODO there is a theoretical possibility of starving the sender part by the receiver (very unlikely)
    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        futures::ready!(Sink::poll_ready(Pin::new(&mut self.messages_tx), cx))?;
        while let Poll::Ready(Some(m)) = self.requests_rx.poll_recv(cx) {
            Sink::start_send(Pin::new(&mut self.messages_tx), m)?;
        }
        let _ = Sink::poll_flush(Pin::new(&mut self.messages_tx), cx)?;

        while let Poll::Ready(Some(m)) = Stream::poll_next(Pin::new(&mut self.messages_rx), cx) {
            match m {
                Message::Multicast(u) => if self.units_tx.send(u).is_err() {},
                Message::FetchResponse(units, _) => units
                    .into_iter()
                    .for_each(|u| if self.units_tx.send(u).is_err() {}),
                _ => {}
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::environment::{self, Hash, Network};

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn dummy() {
        let net = Network::new(1);
        let (env, mut finalized_blocks) = environment::Environment::new(net);
        env.gen_chain(vec![(0.into(), vec![1.into()])]);
        let conf = ConsensusConfig::new(0.into(), 1.into(), 0);
        let c = Consensus::new(conf, env);
        let _ = tokio::spawn(c);

        assert_eq!(finalized_blocks.recv().await.unwrap().hash(), Hash(1));
    }
}
