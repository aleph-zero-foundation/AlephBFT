use crate::{
    config::Config,
    network::{NetworkHub, Recipient},
    runway::{InitializedRunway, RunwayFacade},
    signed::Signature,
    units::{PreUnit, UncheckedSignedUnit, Unit, UnitCoord},
    Data, DataIO, Hasher, MultiKeychain, Network, NodeCount, NodeIndex, Receiver, Sender,
    SpawnHandle,
};
use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    future::ready,
    pin_mut,
    stream::iter,
    Future, FutureExt, Stream, StreamExt,
};
use futures_timer::Delay;
use log::{debug, error, info, trace, warn};
use rand::Rng;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    fmt::Debug,
    iter::repeat,
    marker::PhantomData,
    time,
};

pub(crate) fn into_infinite_stream<F: Future>(f: F) -> impl Stream<Item = ()> {
    f.then(|_| ready(iter(repeat(())))).flatten_stream()
}

/// A message concerning units, either about new units or some requests for them.
#[derive(Debug, Encode, Decode, Clone)]
pub(crate) enum UnitMessage<H: Hasher, D: Data, S: Signature> {
    /// For disseminating newly created units.
    NewUnit(UncheckedSignedUnit<H, D, S>),
    /// Request for a unit by its coord.
    RequestCoord(NodeIndex, UnitCoord),
    /// Response to a request by coord.
    ResponseCoord(UncheckedSignedUnit<H, D, S>),
    /// Request for the full list of parents of a unit.
    RequestParents(NodeIndex, H::Hash),
    /// Response to a request for a full list of parents.
    ResponseParents(H::Hash, Vec<UncheckedSignedUnit<H, D, S>>),
}

impl<H: Hasher, D: Data, S: Signature> UnitMessage<H, D, S> {
    pub(crate) fn included_data(&self) -> Vec<D> {
        match self {
            Self::NewUnit(uu) => vec![uu.as_signable().data().clone()],
            Self::RequestCoord(_, _) => Vec::new(),
            Self::ResponseCoord(uu) => vec![uu.as_signable().data().clone()],
            Self::RequestParents(_, _) => Vec::new(),
            Self::ResponseParents(_, units) => units
                .iter()
                .map(|uu| uu.as_signable().data().clone())
                .collect(),
        }
    }
}

/// Type for incoming notifications: Member to Consensus.
#[derive(Clone, PartialEq)]
pub(crate) enum NotificationIn<H: Hasher> {
    /// A notification carrying a single unit. This might come either from multicast or
    /// from a response to a request. This is of no importance at this layer.
    NewUnits(Vec<Unit<H>>),
    /// Response to a request to decode parents when the control hash is wrong.
    UnitParents(H::Hash, Vec<H::Hash>),
}

/// Type for outgoing notifications: Consensus to Member.
#[derive(Debug, PartialEq)]
pub(crate) enum NotificationOut<H: Hasher> {
    /// Notification about a preunit created by this Consensus Node. Member is meant to
    /// disseminate this preunit among other nodes.
    CreatedPreUnit(PreUnit<H>, Vec<H::Hash>),
    /// Notification that some units are needed but missing. The role of the Member
    /// is to fetch these unit (somehow).
    MissingUnits(Vec<UnitCoord>),
    /// Notification that Consensus has parents incompatible with the control hash.
    WrongControlHash(H::Hash),
    /// Notification that a new unit has been added to the DAG, list of decoded parents provided
    AddedToDag(H::Hash, Vec<H::Hash>),
}

#[derive(Eq, PartialEq)]
pub(crate) enum Task<H: Hasher> {
    // Request the unit with the given (creator, round) coordinates.
    CoordRequest(UnitCoord),
    // Request parents of the unit with the given hash and NodeIndex.
    ParentsRequest(H::Hash, NodeIndex),
    // Broadcast the unit with the given index (local storage).
    UnitMulticast(usize),
}

#[derive(Eq, PartialEq)]
struct ScheduledTask<H: Hasher> {
    task: Task<H>,
    scheduled_time: time::Instant,
    // The number of times the task was performed so far.
    counter: usize,
}

impl<H: Hasher> ScheduledTask<H> {
    fn new(task: Task<H>, scheduled_time: time::Instant) -> Self {
        ScheduledTask {
            task,
            scheduled_time,
            counter: 0,
        }
    }
}

impl<H: Hasher> Ord for ScheduledTask<H> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we want earlier times to come first when used in max-heap, hence the below:
        other.scheduled_time.cmp(&self.scheduled_time)
    }
}

impl<H: Hasher> PartialOrd for ScheduledTask<H> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A representation of a committee member responsible for establishing the consensus.
///
/// It depends on the following objects (for more detailed description of the above obejcts, see their references):
/// - [`Hasher`] - an abstraction for creating identifiers for units, alerts, and other internal objects,
/// - [`DataIO`] - an abstraction for a component that outputs data items and allows to input ordered data items,
/// - [`MultiKeychain`] - an abstraction for digitally signing arbitrary data and verifying signatures,
/// - [`Network`] - an abstraction for a network connecting the committee members,
/// - [`SpawnHandle`] - an abstraction for an executor of asynchronous tasks.
///
/// For a detailed description of the consensus implemented in Member see
/// [docs for devs](https://cardinal-cryptography.github.io/AlephBFT/index.html)
/// or the [original paper](https://arxiv.org/abs/1908.05156).
pub struct Member<'a, D, DP, MK, SH>
where
    D: Data,
    DP: DataIO<D> + Send,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    config: Config,
    data_io: DP,
    keybox: &'a MK,
    spawn_handle: SH,
    _phantom: PhantomData<D>,
}

impl<'a, D, DP, MK, SH> Member<'a, D, DP, MK, SH>
where
    D: Data,
    DP: DataIO<D> + Send,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    /// Create a new instance of the Member for a given session. Under the hood, the Member implementation
    /// makes an extensive use of asynchronous features of Rust, so creating a new Member doesn't start it.
    /// See [`Member::run_session`].
    pub fn new(data_io: DP, keybox: &'a MK, config: Config, spawn_handle: SH) -> Self {
        Member {
            config,
            data_io,
            keybox,
            spawn_handle,
            _phantom: PhantomData,
        }
    }
}

struct InitializedMember<H, D, MK, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    config: Config,
    task_queue: BinaryHeap<ScheduledTask<H>>,
    requested_coords: HashSet<UnitCoord>,
    n_members: NodeCount,
    spawn_handle: SH,
    scheduled_units: Vec<UncheckedSignedUnit<H, D, MK::Signature>>,
    runway_facade: RunwayFacade<H, D, MK>,
    unit_messages_for_network: Sender<(UnitMessage<H, D, MK::Signature>, Recipient)>,
    unit_messages_from_network: Receiver<UnitMessage<H, D, MK::Signature>>,
}

impl<H, D, MK, SH> InitializedMember<H, D, MK, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    fn new(
        config: Config,
        spawn_handle: SH,
        runway_facade: RunwayFacade<H, D, MK>,
        unit_messages_for_network: Sender<(UnitMessage<H, D, MK::Signature>, Recipient)>,
        unit_messages_from_network: Receiver<UnitMessage<H, D, MK::Signature>>,
    ) -> Self {
        let n_members = config.n_members;
        Self {
            config,
            task_queue: BinaryHeap::new(),
            requested_coords: HashSet::new(),
            n_members,
            spawn_handle,
            scheduled_units: Vec::new(),
            runway_facade,
            unit_messages_for_network,
            unit_messages_from_network,
        }
    }
}

impl<H, D, MK, SH> InitializedMember<H, D, MK, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    async fn run<N: Network<H, D, MK::Signature, MK::PartialMultisignature> + 'static>(
        mut self,
        runway_future: impl Future<Output = ()>,
        mut network: NetworkHub<H, D, MK::Signature, MK::PartialMultisignature, N>,
        mut exit: oneshot::Receiver<()>,
    ) {
        info!(target: "AlephBFT-member", "{:?} Spawning network.", self.index());
        let index = self.index();
        let (network_exit, exit_stream) = oneshot::channel();
        let network_handle = self
            .spawn_handle
            .spawn_essential("member/network", async move {
                network.run(exit_stream).await;
            });
        let mut network_handle = into_infinite_stream(network_handle).fuse();
        info!(target: "AlephBFT-member", "{:?} Network spawned.", self.index());

        info!(target: "AlephBFT-member", "{:?} Initializing Runway.", self.index());

        let runway_future = into_infinite_stream(runway_future).fuse();
        pin_mut!(runway_future);

        let ticker_delay = self.config.delay_config.tick_interval;
        let mut ticker = Delay::new(ticker_delay).fuse();

        info!(target: "AlephBFT-member", "{:?} Runway initialized.", index);

        loop {
            futures::select! {
                _ = runway_future.next() => {
                    error!(target: "AlephBFT-member", "{:?} Runway terminated early.", index);
                    break;
                },

                event = self.runway_facade.next_outgoing_message().fuse() => match event {
                    Some((message, recipient)) => {
                        self.on_unit_message_from_units(message, recipient);
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from Runway closed.", index);
                        break;
                    },
                },

                event = self.unit_messages_from_network.next().fuse() => match event {
                    Some(message) => {
                        self.runway_facade.enqueue_message(message);
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from network closed.", index);
                        break;
                    },
                },

                _ = network_handle.next() => {
                    debug!(target: "AlephBFT-member", "{:?} network task terminated early.", index);
                    break;
                },

                _ = &mut ticker => {
                    self.trigger_tasks();
                    ticker = Delay::new(ticker_delay).fuse();
                },

                _ = &mut exit => break,
            }
        }
        self.runway_facade.stop().await;
        runway_future.next().await;
        if network_exit.send(()).is_err() {
            debug!(target: "AlephBFT-member", "{:?} network already stopped.", index);
        }
        network_handle.next().await.unwrap();
        debug!(target: "AlephBFT-member", "{:?} Member stopped.", index);
    }
}

impl<H, D, MK, SH> InitializedMember<H, D, MK, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    fn on_create(&mut self, u: UncheckedSignedUnit<H, D, MK::Signature>) {
        let index = self.scheduled_units.len();
        self.scheduled_units.push(u);
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::UnitMulticast(index), curr_time);
        self.task_queue.push(task);
    }

    fn on_request_coord(&mut self, coord: UnitCoord) {
        trace!(target: "AlephBFT-member", "{:?} Dealing with missing coord notification {:?}.", self.index(), coord);
        if !self.requested_coords.insert(coord) {
            return;
        }
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::CoordRequest(coord), curr_time);
        self.task_queue.push(task);
        self.trigger_tasks();
    }

    fn on_request_parents(&mut self, u_hash: H::Hash, peer_id: NodeIndex) {
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::ParentsRequest(u_hash, peer_id), curr_time);
        self.task_queue.push(task);
        self.trigger_tasks();
    }

    // Pulls tasks from the priority queue (sorted by scheduled time) and sends them to random peers
    // as long as they are scheduled at time <= curr_time
    fn trigger_tasks(&mut self) {
        while let Some(request) = self.task_queue.peek() {
            let curr_time = time::Instant::now();
            if request.scheduled_time > curr_time {
                break;
            }
            let mut request = self.task_queue.pop().expect("The element was peeked");
            if let Some((message, recipient, delay)) =
                self.task_details(&request.task, request.counter)
            {
                self.unit_messages_for_network
                    .unbounded_send((message, recipient))
                    .expect("Channel to network should be open");
                request.scheduled_time += delay;
                request.counter += 1;
                self.task_queue.push(request);
            }
        }
    }

    fn random_peer(&self) -> NodeIndex {
        rand::thread_rng()
            .gen_range(0..self.n_members.into())
            .into()
    }

    fn index(&self) -> NodeIndex {
        self.config.node_ix
    }

    fn send_unit_message(&mut self, message: UnitMessage<H, D, MK::Signature>, peer_id: NodeIndex) {
        self.unit_messages_for_network
            .unbounded_send((message, Recipient::Node(peer_id)))
            .expect("Channel to network should be open")
    }

    /// Given a task and the number of times it was performed, returns `None` if the task is no longer active, or
    /// `Some((message, recipient, delay))` if the task is active and the task is to send `message` to `recipient`
    /// and should be rescheduled after `delay`.
    fn task_details(
        &mut self,
        task: &Task<H>,
        counter: usize,
    ) -> Option<(UnitMessage<H, D, MK::Signature>, Recipient, time::Duration)> {
        // preferred_recipient is Everyone if the message is supposed to be broadcast,
        // and Node(node_id) if the message should be send to one peer (node_id when
        // the task is done for the first time or a random peer otherwise)
        let (message, preferred_recipient) = match task {
            Task::CoordRequest(coord) => {
                if !self.runway_facade.missing_coords(coord) {
                    return None;
                }
                let message = UnitMessage::RequestCoord(self.index(), *coord);
                let preferred_recipient = Recipient::Node(coord.creator());
                (message, preferred_recipient)
            }
            Task::ParentsRequest(hash, preferred_id) => {
                if !self.runway_facade.missing_parents(hash) {
                    return None;
                }
                let message = UnitMessage::RequestParents(self.index(), *hash);
                (message, Recipient::Node(*preferred_id))
            }
            Task::UnitMulticast(index) => {
                let signed_unit = self
                    .scheduled_units
                    .get(*index)
                    .cloned()
                    .expect("unit should be stored");
                let message = UnitMessage::NewUnit(signed_unit.into());
                let preferred_recipient = Recipient::Everyone;
                (message, preferred_recipient)
            }
        };
        let (recipient, delay) = match preferred_recipient {
            Recipient::Everyone => (
                Recipient::Everyone,
                (self.config.delay_config.unit_broadcast_delay)(counter),
            ),
            Recipient::Node(preferred_id) => {
                let recipient = if counter == 0 {
                    preferred_id
                } else {
                    self.random_peer()
                };
                (
                    Recipient::Node(recipient),
                    self.config.delay_config.requests_interval,
                )
            }
        };
        Some((message, recipient, delay))
    }

    fn on_unit_message_from_units(
        &mut self,
        message: UnitMessage<H, D, MK::Signature>,
        recipient: Recipient,
    ) {
        match message {
            UnitMessage::NewUnit(u) => self.on_create(u),
            UnitMessage::RequestCoord(_, coord) => self.on_request_coord(coord),
            UnitMessage::RequestParents(peer_id, hash) => self.on_request_parents(hash, peer_id),
            UnitMessage::ResponseCoord(_) => match recipient {
                Recipient::Node(peer_id) => {
                    self.send_unit_message(message, peer_id);
                }
                _ => {
                    warn!(target: "AlephBFT-member", "{:?} Missing Recipient for a ResponseCoord message.", self.index());
                }
            },
            UnitMessage::ResponseParents(_, _) => match recipient {
                Recipient::Node(peer_id) => {
                    self.send_unit_message(message, peer_id);
                }
                _ => {
                    warn!(target: "AlephBFT-member", "{:?} Missing Recipient for a ResponseParents message.", self.index());
                }
            },
        }
    }
}

impl<'a, D, DP, MK, SH> Member<'a, D, DP, MK, SH>
where
    D: Data,
    DP: DataIO<D> + Send + 'static,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    /// Actually start the Member as an async task. It stops establishing consensus for new data items after
    /// reaching the threshold specified in [`Config::max_round`] or upon receiving a stop signal from `exit`.
    pub async fn run_session<
        H: Hasher,
        N: Network<H, D, MK::Signature, MK::PartialMultisignature> + 'static,
    >(
        self,
        network: N,
        exit: oneshot::Receiver<()>,
    ) {
        let index = self.config.node_ix;
        info!(target: "AlephBFT-member", "{:?} Spawning party for a session.", index);

        let (alert_messages_for_alerter, alert_messages_from_network) = mpsc::unbounded();
        let (alert_messages_for_network, alert_messages_from_alerter) = mpsc::unbounded();
        let (unit_messages_for_units, unit_messages_from_network) = mpsc::unbounded();
        let (unit_messages_for_network, unit_messages_from_units) = mpsc::unbounded();

        let network_hub = NetworkHub::new(
            network,
            unit_messages_from_units,
            unit_messages_for_units,
            alert_messages_from_alerter,
            alert_messages_for_alerter,
        );

        let runway = InitializedRunway::new(
            self.config.clone(),
            self.keybox.clone(),
            self.data_io,
            self.spawn_handle.clone(),
            alert_messages_for_network,
            alert_messages_from_network,
        );
        let (runway_facade, runway_future) = runway.start();

        let initialized_member = InitializedMember::new(
            self.config,
            self.spawn_handle,
            runway_facade,
            unit_messages_for_network,
            unit_messages_from_network,
        );

        info!(target: "AlephBFT-member", "{:?} Running member.", index);

        initialized_member
            .run(runway_future, network_hub, exit)
            .await;

        info!(target: "AlephBFT-member", "{:?} Run ended.", index);
    }
}
