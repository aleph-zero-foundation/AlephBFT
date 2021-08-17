use crate::{
    config::Config,
    network::{NetworkHub, Recipient},
    runway::{
        InitializedRunway, Request, Response, RunwayFacade, RunwayNotification,
        RunwayNotificationIn, RunwayNotificationOut, TrackedRequest,
    },
    signed::Signature,
    units::{UncheckedSignedUnit, UnitCoord},
    utils::into_infinite_stream,
    Data, DataIO, Hasher, MultiKeychain, Network, NodeCount, NodeIndex, Receiver, Sender,
    SpawnHandle,
};
use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    pin_mut, Future, FutureExt, StreamExt,
};
use futures_timer::Delay;
use log::{debug, error, info, trace};
use rand::Rng;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    fmt::Debug,
    marker::PhantomData,
    time,
};

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

// #[derive(Eq, PartialEq)]
pub(crate) enum Task<H: Hasher, D: Data, S: Signature> {
    // Request the unit with the given (creator, round) coordinates.
    CoordRequest(UnitCoord, TrackedRequest),
    // Request parents of the unit with the given hash and Recipient.
    ParentsRequest(H::Hash, Recipient, TrackedRequest),
    // Broadcast the given unit.
    UnitMulticast(UncheckedSignedUnit<H, D, S>),
}

impl<H: Hasher, D: Data, S: Signature + PartialEq> PartialEq for Task<H, D, S> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Task::CoordRequest(self_coord, _), Task::CoordRequest(other_coord, _)) => {
                self_coord.eq(other_coord)
            }
            (
                Task::ParentsRequest(self_hash, self_recipient, _),
                Task::ParentsRequest(other_hash, other_recipient, _),
            ) => self_hash.eq(other_hash) && self_recipient.eq(other_recipient),
            (Task::UnitMulticast(self_unit), Task::UnitMulticast(other_unit)) => {
                self_unit.eq(other_unit)
            }
            _ => false,
        }
    }
}

impl<H: Hasher, D: Data, S: Signature + PartialEq> Eq for Task<H, D, S> {
    fn assert_receiver_is_total_eq(&self) {}
}

#[derive(Eq, PartialEq)]
struct ScheduledTask<H: Hasher, D: Data, S: Signature + PartialEq> {
    task: Task<H, D, S>,
    scheduled_time: time::Instant,
    // The number of times the task was performed so far.
    counter: usize,
}

impl<H: Hasher, D: Data, S: Signature + Eq + PartialEq> ScheduledTask<H, D, S> {
    fn new(task: Task<H, D, S>, scheduled_time: time::Instant) -> Self {
        ScheduledTask {
            task,
            scheduled_time,
            counter: 0,
        }
    }
}

impl<H: Hasher, D: Data, S: Signature + Eq + PartialEq> Ord for ScheduledTask<H, D, S> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we want earlier times to come first when used in max-heap, hence the below:
        other.scheduled_time.cmp(&self.scheduled_time)
    }
}

impl<H: Hasher, D: Data, S: Signature + Eq + PartialEq> PartialOrd for ScheduledTask<H, D, S> {
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
    MK::Signature: Eq + PartialEq,
    SH: SpawnHandle,
{
    config: Config,
    task_queue: BinaryHeap<ScheduledTask<H, D, MK::Signature>>,
    requested_coords: HashSet<UnitCoord>,
    n_members: NodeCount,
    spawn_handle: SH,
    runway_facade: RunwayFacade<H, D, MK>,
    unit_messages_for_network: Sender<(UnitMessage<H, D, MK::Signature>, Recipient)>,
    unit_messages_from_network: Receiver<UnitMessage<H, D, MK::Signature>>,
}

impl<H, D, MK, SH> InitializedMember<H, D, MK, SH>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    MK::Signature: Eq + PartialEq,
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
            runway_facade,
            unit_messages_for_network,
            unit_messages_from_network,
        }
    }

    fn on_create(&mut self, u: UncheckedSignedUnit<H, D, MK::Signature>) {
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::UnitMulticast(u), curr_time);
        self.task_queue.push(task);
    }

    fn on_request_coord(&mut self, coord: UnitCoord, tracker: TrackedRequest) {
        trace!(target: "AlephBFT-member", "{:?} Dealing with missing coord notification {:?}.", self.index(), coord);
        if !self.requested_coords.insert(coord) {
            return;
        }
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::CoordRequest(coord, tracker), curr_time);
        self.task_queue.push(task);
        self.trigger_tasks();
    }

    fn on_request_parents(
        &mut self,
        u_hash: H::Hash,
        recipient: Recipient,
        tracker: TrackedRequest,
    ) {
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::ParentsRequest(u_hash, recipient, tracker), curr_time);
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

    fn send_unit_message(
        &mut self,
        message: UnitMessage<H, D, MK::Signature>,
        recipient: Recipient,
    ) {
        self.unit_messages_for_network
            .unbounded_send((message, recipient))
            .expect("Channel to network should be open")
    }

    /// Given a task and the number of times it was performed, returns `None` if the task is no longer active, or
    /// `Some((message, recipient, delay))` if the task is active and the task is to send `message` to `recipient`
    /// and should be rescheduled after `delay`.
    fn task_details(
        &mut self,
        task: &Task<H, D, MK::Signature>,
        counter: usize,
    ) -> Option<(UnitMessage<H, D, MK::Signature>, Recipient, time::Duration)> {
        // preferred_recipient is Everyone if the message is supposed to be broadcast,
        // and Node(node_id) if the message should be send to one peer (node_id when
        // the task is done for the first time or a random peer otherwise)
        let (message, preferred_recipient) = match task {
            Task::CoordRequest(coord, tracker) => {
                if tracker.is_satisfied() {
                    return None;
                }
                let message = UnitMessage::RequestCoord(self.index(), *coord);
                let preferred_recipient = Recipient::Node(coord.creator());
                (message, preferred_recipient)
            }
            Task::ParentsRequest(hash, preferred_recipient, tracker) => {
                if tracker.is_satisfied() {
                    return None;
                }
                let message = UnitMessage::RequestParents(self.index(), *hash);
                let preferred_recipient = preferred_recipient.clone();
                (message, preferred_recipient)
            }
            Task::UnitMulticast(signed_unit) => {
                let message = UnitMessage::NewUnit(signed_unit.clone());
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

    fn on_unit_message_from_units(&mut self, message: RunwayNotificationOut<H, D, MK::Signature>) {
        match message {
            RunwayNotification::NewUnit(u) => self.on_create(u),
            RunwayNotification::Request((request, recipient, tracker)) => match request {
                Request::RequestCoord(coord) => self.on_request_coord(coord, tracker),
                crate::runway::Request::RequestParents(u_hash) => {
                    self.on_request_parents(u_hash, recipient, tracker)
                }
            },
            RunwayNotification::Response((response, recipient)) => match response {
                Response::ResponseCoord(u) => {
                    let message = UnitMessage::ResponseCoord(u);
                    self.send_unit_message(message, Recipient::Node(recipient))
                }
                Response::ResponseParents(u_hash, parents) => {
                    let message = UnitMessage::ResponseParents(u_hash, parents);
                    self.send_unit_message(message, Recipient::Node(recipient))
                }
            },
        }
    }

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
                    Some(message) => {
                        self.on_unit_message_from_units(message);
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from Runway closed.", index);
                        break;
                    },
                },

                event = self.unit_messages_from_network.next() => match event {
                    Some(message) => {
                        self.send_notification_to_runway(message)
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
        self.runway_facade.stop();
        runway_future.next().await.unwrap();
        if network_exit.send(()).is_err() {
            debug!(target: "AlephBFT-member", "{:?} network already stopped.", index);
        }
        network_handle.next().await.unwrap();
        debug!(target: "AlephBFT-member", "{:?} Member stopped.", index);
    }

    fn send_notification_to_runway(&mut self, message: UnitMessage<H, D, MK::Signature>) {
        let notification = RunwayNotificationIn::from(message);
        self.runway_facade.enqueue_notification(notification)
    }
}

impl<'a, D, DP, MK, SH> Member<'a, D, DP, MK, SH>
where
    D: Data,
    DP: DataIO<D> + Send + 'static,
    MK: MultiKeychain,
    MK::Signature: Eq + PartialEq,
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
