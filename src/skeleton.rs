use futures::prelude::*;
use parking_lot::RwLock;
use std::{
	pin::Pin,
	sync::Arc,
	task::{self, Poll},
};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::{
	dag::{Dag, Vertex},
	traits::{Environment, HashT},
};

pub enum Error {}

pub enum Message<H: HashT> {
	Multicast(Unit<H>),
	// request of a given id of units of given hashes
	FetchMessage(Vec<H>, u32),
	// requested units by a given request id
	FetchResponse(Vec<Unit<H>>, u32),
	SyncMessage,
	SyncResponse,
	Alert,
}

pub struct ConsensusConfig {
	pid: usize,
	n_members: usize,
}

type JoinHandle = tokio::task::JoinHandle<Result<(), Error>>;
pub struct Consensus<E: Environment + 'static> {
	_conf: ConsensusConfig,
	_env: E,
	_dag: Arc<RwLock<Dag<E>>>,
	_runtime: Runtime,
	_creator: JoinHandle,
	_terminal: JoinHandle,
	_adders: Vec<JoinHandle>,
	_extender: JoinHandle,
	_syncer: JoinHandle,
}

type Receiver<T> = mpsc::UnboundedReceiver<T>;
type Sender<T> = mpsc::UnboundedSender<T>;

impl<E: Environment + 'static> Consensus<E> {
	pub fn new(conf: ConsensusConfig, env: E) -> Self {
		let dag = Arc::new(RwLock::new(Dag::new()));

		let (electors_tx, electors_rx) = mpsc::unbounded_channel();
		let extender = Extender::new(electors_rx);
		{
			// try to extend the partial order after adding a unit to the dag
			let mut dag = dag.write();
			dag.register_post_insert_hook(Box::new(move |v| if electors_tx.send(v).is_err() {}));
		}

		let (o, i) = env.consensus_data();
		let (syncer, requests_tx, incoming_units_rx) = Syncer::<E>::new(Box::new(o), Box::new(i));

		let mcast_request_tx = requests_tx.clone();
		let (parents_tx, parents_rx) = mpsc::unbounded_channel();
		let creator = Creator::new(parents_rx, Arc::clone(&dag), mcast_request_tx);
		let my_pid = conf.pid.clone();
		{
			// send a new parent candidate to the creator
			let mut dag = dag.write();
			dag.register_post_insert_hook(Box::new(move |v| {
				if my_pid != v.creator() {
					let _ = parents_tx.send(v);
				}
			}));
		}

		let mut adders = vec![];
		let mut ready = vec![];
		let (added_tx, added_rx) = mpsc::unbounded_channel();
		for pid in 0..conf.n_members {
			let (adder, ready_tx) = Adder::new(pid, added_tx.clone(), Arc::clone(&dag));
			ready.push(ready_tx);
			adders.push(adder);
		}

		let terminal = Terminal::<E>::new(
			conf.n_members,
<<<<<<< HEAD
			incoming_units_rx,
=======
			incomming_units_rx,
>>>>>>> 6fb6f5a (more complete skeleton)
			requests_tx,
			added_rx,
			ready,
		);

		// NOTE: it's possible to run adders as one future and see if spawning them in parallel gives us anything
		let rt = Runtime::new().unwrap();
		let creator = rt.spawn(creator);
		let terminal = rt.spawn(terminal);
		let extender = rt.spawn(extender);
		let adders = adders.into_iter().map(|adder| rt.spawn(adder)).collect();
<<<<<<< HEAD
		let syncer = rt.spawn(syncer);
=======
		let mcast_sender = rt.spawn(mcast_sender);
>>>>>>> 6fb6f5a (more complete skeleton)

		Consensus {
			_conf: conf,
			_env: env,
			_dag: dag,
			_runtime: rt,
			_terminal: terminal,
			_adders: adders,
			_extender: extender,
			_creator: creator,
<<<<<<< HEAD
			_syncer: syncer,
=======
			_mcast_sender: mcast_sender,
>>>>>>> 6fb6f5a (more complete skeleton)
		}
	}
}

// This is to be called from within substrate
impl<E: Environment> Future for Consensus<E> {
	type Output = Result<(), Error>;

	fn poll(self: Pin<&mut Self>, _: &mut task::Context<'_>) -> Poll<Self::Output> {
		Poll::Pending
	}
}

// Terminal is responsible for managing units that cannot be added to the dag yet
struct Terminal<E: Environment> {
	n_members: usize,
	new_units_rx: Receiver<Unit<E::Hash>>,
	requests_tx: Sender<Message<E::Hash>>,
	added: Receiver<Unit<E::Hash>>,
	_waiting_list: Vec<Unit<E::Hash>>,
	ready: Vec<Sender<Unit<E::Hash>>>,
}

impl<E: Environment> Terminal<E> {
	fn new(
		n_members: usize,
		new_units_rx: Receiver<Unit<E::Hash>>,
		requests_tx: Sender<Message<E::Hash>>,
		added: Receiver<Unit<E::Hash>>,
		ready: Vec<Sender<Unit<E::Hash>>>,
	) -> Self {
		Terminal {
			n_members,
			new_units_rx,
			requests_tx,
			added,
			_waiting_list: vec![],
			ready,
		}
	}

	fn add_to_waiting(&mut self, u: Unit<E::Hash>) {
		// if u has all its parents in dag, we send it to the corresponding adder to try to add it to the dag
		let _ = self.ready[u.creator].send(u);
	}

	fn notify_waiting(&mut self, _u: Unit<E::Hash>) {
		// we may mark ready all units that has u as last waiting parrent
		// for all such units v we do
		// self.ready[v.creator].send(u);
	}

<<<<<<< HEAD
	fn process_incoming(&mut self, cx: &mut task::Context) -> Result<(), Error> {
=======
	fn process_incomming(&mut self, cx: &mut task::Context) -> Result<(), Error> {
>>>>>>> 6fb6f5a (more complete skeleton)
		while let Poll::Ready(Some(u)) = self.added.poll_recv(cx) {
			self.notify_waiting(u);
		}

		while let Poll::Ready(Some(u)) = self.new_units_rx.poll_recv(cx) {
			self.add_to_waiting(u);
		}
		Ok(())
	}
}

impl<E: Environment> Unpin for Terminal<E> {}

impl<E: Environment> Future for Terminal<E> {
	type Output = Result<(), Error>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
<<<<<<< HEAD
		self.process_incoming(cx)?;
=======
		self.process_incomming(cx)?;
>>>>>>> 6fb6f5a (more complete skeleton)
		Poll::Pending
	}
}

#[derive(Clone, Default)]
pub struct Unit<H: HashT> {
	creator: usize,
	hash: H,
}

impl<H: HashT> Unit<H> {
	fn hash(&self) -> H {
		self.hash.clone()
	}
}

// a queue for units that has all parents in the dag an possible may be added to the dag
struct Adder<E: Environment> {
	pid: usize,
	added_tx: Sender<Unit<E::Hash>>,
<<<<<<< HEAD
	incoming: Receiver<Unit<E::Hash>>,
=======
	incomming: Receiver<Unit<E::Hash>>,
>>>>>>> 6fb6f5a (more complete skeleton)
	dag: Arc<RwLock<Dag<E>>>,
}

impl<E: Environment> Adder<E> {
	fn new(
		pid: usize,
		added_tx: Sender<Unit<E::Hash>>,
		dag: Arc<RwLock<Dag<E>>>,
	) -> (Self, Sender<Unit<E::Hash>>) {
		let (ready_tx, ready_rx) = mpsc::unbounded_channel();
		(
			Adder {
				pid,
				added_tx,
<<<<<<< HEAD
				incoming: ready_rx,
=======
				incomming: ready_rx,
>>>>>>> 6fb6f5a (more complete skeleton)
				dag,
			},
			ready_tx,
		)
	}
}

impl<E: Environment> Future for Adder<E> {
	type Output = Result<(), Error>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
<<<<<<< HEAD
		while let Poll::Ready(Some(u)) = self.incoming.poll_recv(cx) {
=======
		while let Poll::Ready(Some(u)) = self.incomming.poll_recv(cx) {
>>>>>>> 6fb6f5a (more complete skeleton)
			// process u and if it is successfully added to dag, notify the terminal
			let _ = self.added_tx.send(u.clone());
			self.dag
				.write()
				.add_vertex(u.hash(), u.creator, vec![None].into())
		}
		Poll::Pending
	}
}

// a process responsible for creating new units
struct Creator<E: Environment> {
	parents_rx: Receiver<Arc<Vertex<E>>>,
	mcast_request_tx: Sender<Message<E::Hash>>,
	dag: Arc<RwLock<Dag<E>>>,
}

impl<E: Environment> Creator<E> {
	fn new(
		parents_rx: Receiver<Arc<Vertex<E>>>,
		dag: Arc<RwLock<Dag<E>>>,
		mcast_request_tx: Sender<Message<E::Hash>>,
	) -> Self {
		Creator {
			parents_rx,
			mcast_request_tx,
			dag,
		}
	}
}

impl<E: Environment> Future for Creator<E> {
	type Output = Result<(), Error>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
		while let Poll::Ready(Some(_v)) = self.parents_rx.poll_recv(cx) {
			// check if u may be a parent and if yes check if we are ready to produce a new unit
			// if so, send it to dag
			let _ = self
				.mcast_request_tx
				.send(Message::Multicast(Unit::<E::Hash>::default()));
		}
		Poll::Pending
	}
}

// a process responsible for extending the partial order
struct Extender<E: Environment> {
	electors: Receiver<Arc<Vertex<E>>>,
}

impl<E: Environment> Extender<E> {
	fn new(electors: Receiver<Arc<Vertex<E>>>) -> Self {
		Extender { electors }
	}
	fn new_head(&mut self, _v: Arc<Vertex<E>>) -> bool {
		false
	}
	fn finalize_next_batch(&self) {}
}

impl<E: Environment> Future for Extender<E> {
	type Output = Result<(), Error>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
		while let Poll::Ready(Some(v)) = self.electors.poll_recv(cx) {
			while self.new_head(v.clone()) {
				self.finalize_next_batch();
			}
		}
		Poll::Pending
	}
}

struct Syncer<E: Environment> {
	// outgoing messages
	messages_tx: Box<E::Out>,
<<<<<<< HEAD
	// incoming messages
=======
	// incomming messages
>>>>>>> 6fb6f5a (more complete skeleton)
	messages_rx: Box<E::In>,
	// channel for sending units to the terminal
	units_tx: Sender<Unit<E::Hash>>,
	// channel for receiving messages to the outside world
	requests_rx: Receiver<Message<E::Hash>>,
	to_send: Option<Unit<E::Hash>>,
}

impl<E: Environment> Syncer<E> {
	fn new(
		messages_tx: Box<E::Out>,
		messages_rx: Box<E::In>,
	) -> (Self, Sender<Message<E::Hash>>, Receiver<Unit<E::Hash>>) {
		let (units_tx, units_rx) = mpsc::unbounded_channel();
		let (requests_tx, requests_rx) = mpsc::unbounded_channel();
		(
			Syncer {
				messages_tx,
				messages_rx,
				units_tx,
				requests_rx,
				to_send: None,
			},
			requests_tx,
			units_rx,
		)
	}
}

impl<E: Environment> Future for Syncer<E> {
	type Output = Result<(), Error>;

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
