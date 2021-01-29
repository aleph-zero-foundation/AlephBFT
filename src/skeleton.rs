use futures::prelude::*;
use std::{
	collections::HashMap,
	pin::Pin,
	task::{self, Poll},
};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::{
	dag::{Dag, Vertex},
	nodes::{NodeIndex, NodeMap},
	traits::{Environment, HashT},
};

pub enum Error {}

#[derive(Clone, Debug, PartialEq)]
pub enum Message<H: HashT> {
	Multicast(Unit<H>),
	// request of a given id of units of given hashes
	FetchRequest(Vec<NodeIndex>, NodeIndex),
	// requested units by a given request id
	FetchResponse(Vec<Unit<H>>, NodeIndex),
	SyncMessage,
	SyncResponse,
	Alert,
}

pub struct ConsensusConfig {
	ix: NodeIndex,
	n_members: u32,
}

type JoinHandle<E: Environment> = tokio::task::JoinHandle<Result<(), E::Error>>;
pub struct Consensus<E: Environment + 'static> {
	_conf: ConsensusConfig,
	_env: E,
	_runtime: Runtime,
	_creator: JoinHandle<E>,
	_terminal: JoinHandle<E>,
	_extender: JoinHandle<E>,
	_syncer: JoinHandle<E>,
}

type Receiver<T> = mpsc::UnboundedReceiver<T>;
type Sender<T> = mpsc::UnboundedSender<T>;

impl<E: Environment + 'static> Consensus<E> {
	pub fn new(conf: ConsensusConfig, env: E) -> Self {
		let (electors_tx, electors_rx) = mpsc::unbounded_channel();
		let extender = Extender::<E>::new(electors_rx);

		let (o, i) = env.consensus_data();
		let (syncer, requests_tx, incoming_units_rx, created_units_tx) = Syncer::<E>::new(o, i);

		let (parents_tx, parents_rx) = mpsc::unbounded_channel();
		let creator = Creator::<E>::new(parents_rx, created_units_tx);
		let my_ix = conf.ix.clone();

		let mut terminal = Terminal::<E>::new(
			conf.ix,
			conf.n_members,
			incoming_units_rx,
			requests_tx.clone(),
		);
		// send a multicast request
		terminal.register_post_insert_hook(Box::new(move |u| {
			if my_ix == u.creator() {
				// send unit u corresponding to v
				let _ = requests_tx.send(Message::Multicast(u));
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

		let rt = Runtime::new().unwrap();
		let creator = rt.spawn(creator);
		let terminal = rt.spawn(terminal);
		let extender = rt.spawn(extender);
		let syncer = rt.spawn(syncer);

		Consensus {
			_conf: conf,
			_env: env,
			_runtime: rt,
			_terminal: terminal,
			_extender: extender,
			_creator: creator,
			_syncer: syncer,
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

// Terminal is responsible for:
// - managing units that cannot be added to the dag yet, i.e fetching missing parents
// - checking control hashes
// - TODO checking for potential forks and raising alarms
// - TODO updating randomness source
struct Terminal<E: Environment + 'static> {
	ix: NodeIndex,
	n_members: u32,
	// common channel for units from outside world and the ones we create, possibly split into two so that we prioritize ours
	new_units_rx: Receiver<Unit<E::Hash>>,
	requests_tx: Sender<Message<E::Hash>>,
	waiting_list: Vec<Unit<E::Hash>>,
	post_insert: Vec<Box<dyn Fn(Unit<E::Hash>) + Send + Sync + 'static>>,
	dag: Dag<E>,
	unit_bag: HashMap<E::Hash, Unit<E::Hash>>,
}

enum ConvertError {
	WrongControlHash,
	MissingParents(Vec<NodeIndex>),
}

impl<E: Environment + 'static> Terminal<E> {
	fn new(
		ix: NodeIndex,
		n_members: u32,
		new_units_rx: Receiver<Unit<E::Hash>>,
		requests_tx: Sender<Message<E::Hash>>,
	) -> Self {
		Terminal {
			ix,
			n_members,
			new_units_rx,
			requests_tx,
			waiting_list: vec![],
			post_insert: vec![],
			dag: Dag::<E>::new(),
			unit_bag: HashMap::new(),
		}
	}

	// try to convert unit to vertex which may fail if a parent is missing or all parents are there but the control hash is wrong
	fn unit_to_vertex(&self, _u: &Unit<E::Hash>) -> Result<Vertex<E::Hash>, ConvertError> {
		Ok(Vertex::default())
	}

	fn process_incoming(&mut self, cx: &mut task::Context) -> Result<(), E::Error> {
		while let Poll::Ready(Some(u)) = self.new_units_rx.poll_recv(cx) {
			if self.unit_bag.contains_key(&u.hash()) {
				continue;
			}

			match self.unit_to_vertex(&u) {
				Ok(v) => {
					// TODO check for forks
					self.unit_bag.insert(u.hash, u.clone());
					// TODO update waiting_list and check if we may now add units from there
					self.dag.add_vertex(v);
					// send v to Extender, Syncer, and Creator
					self.post_insert.iter().for_each(|f| f(u.clone()));
				}
				Err(ConvertError::WrongControlHash) => {
					// TODO probably fork, act mericilessly
				}
				Err(ConvertError::MissingParents(list)) => {
					// TODO decide on a policy for choosing to should we call for missing parents,
					let _ = self
						.requests_tx
						.send(Message::FetchRequest(list, u.creator()));
					self.waiting_list.push(u);
				}
			}
		}
		Ok(())
	}

	pub(crate) fn register_post_insert_hook(
		&mut self,
		hook: Box<dyn Fn(Unit<E::Hash>) + Send + Sync + 'static>,
	) {
		self.post_insert.push(hook);
	}
}

impl<E: Environment> Unpin for Terminal<E> {}

impl<E: Environment> Future for Terminal<E> {
	type Output = Result<(), E::Error>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
		self.process_incoming(cx)?;
		Poll::Pending
	}
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ControlHash<H: HashT> {
	parents: NodeMap<Option<NodeIndex>>,
	hash: H,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Unit<H: HashT> {
	creator: NodeIndex,
	hash: H,
	control_hash: ControlHash<H>,
	parents: NodeMap<Option<H>>,
}

impl<H: HashT> Into<Vertex<H>> for Unit<H> {
	fn into(self) -> Vertex<H> {
		Vertex::new(self.creator, self.hash, self.parents)
	}
}

impl<H: HashT> Unit<H> {
	fn hash(&self) -> H {
		self.hash.clone()
	}
	fn creator(&self) -> NodeIndex {
		self.creator
	}
	fn control_hash(&self) -> ControlHash<H> {
		self.control_hash.clone()
	}
}

// a process responsible for creating new units
struct Creator<E: Environment> {
	parents_rx: Receiver<Vertex<E::Hash>>,
	new_units_tx: Sender<Unit<E::Hash>>,
}

impl<E: Environment> Creator<E> {
	fn new(parents_rx: Receiver<Vertex<E::Hash>>, new_units_tx: Sender<Unit<E::Hash>>) -> Self {
		Creator {
			parents_rx,
			new_units_tx,
		}
	}
}

impl<E: Environment> Future for Creator<E> {
	type Output = Result<(), E::Error>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
		while let Poll::Ready(Some(_v)) = self.parents_rx.poll_recv(cx) {
			// check if v may be a parent and if yes check if we are ready to produce a new unit
			// if so, send it to the terminal
			let _ = self.new_units_tx.send(Unit::<E::Hash>::default());
		}
		Poll::Pending
	}
}

// a process responsible for extending the partial order
struct Extender<E: Environment> {
	electors: Receiver<Vertex<E::Hash>>,
}

impl<E: Environment> Extender<E> {
	fn new(electors: Receiver<Vertex<E::Hash>>) -> Self {
		Extender { electors }
	}
	fn new_head(&mut self, _v: Vertex<E::Hash>) -> bool {
		false
	}
	fn finalize_next_batch(&self) {}
}

impl<E: Environment> Future for Extender<E> {
	type Output = Result<(), E::Error>;

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
	messages_tx: E::Out,
	// incoming messages
	messages_rx: E::In,
	// channel for sending units to the terminal
	units_tx: Sender<Unit<E::Hash>>,
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
		Receiver<Unit<E::Hash>>,
		Sender<Unit<E::Hash>>,
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
