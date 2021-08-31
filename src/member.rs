use crate::{
    config::Config,
    network::{self, Recipient},
    runway::{self, Request, Response, RunwayIO, RunwayNotificationIn, RunwayNotificationOut},
    signed::Signature,
    units::{UncheckedSignedUnit, UnitCoord},
    Data, DataIO, Hasher, MultiKeychain, Network, NodeCount, NodeIndex, Receiver, Sender,
    SpawnHandle,
};
use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    future::FusedFuture,
    pin_mut, FutureExt, StreamExt,
};
use futures_timer::Delay;
use log::{debug, error, info, trace};
use rand::Rng;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    convert::TryFrom,
    fmt::Debug,
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

#[derive(Eq, PartialEq)]
enum Task<H: Hasher, D: Data, S: Signature> {
    // Request the unit with the given (creator, round) coordinates.
    CoordRequest(UnitCoord),
    // Request parents of the unit with the given hash and Recipient.
    ParentsRequest(H::Hash, Recipient),
    // Broadcast the given unit.
    UnitMulticast(UncheckedSignedUnit<H, D, S>),
}

#[derive(Eq, PartialEq)]
struct ScheduledTask<H: Hasher, D: Data, S: Signature> {
    task: Task<H, D, S>,
    scheduled_time: time::Instant,
    // The number of times the task was performed so far.
    counter: usize,
}

impl<H: Hasher, D: Data, S: Signature> ScheduledTask<H, D, S> {
    fn new(task: Task<H, D, S>, scheduled_time: time::Instant) -> Self {
        ScheduledTask {
            task,
            scheduled_time,
            counter: 0,
        }
    }
}

impl<H: Hasher, D: Data, S: Signature> Ord for ScheduledTask<H, D, S> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we want earlier times to come first when used in max-heap, hence the below:
        other.scheduled_time.cmp(&self.scheduled_time)
    }
}

impl<H: Hasher, D: Data, S: Signature> PartialOrd for ScheduledTask<H, D, S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct Member<H, D, S>
where
    H: Hasher,
    D: Data,
    S: Signature,
{
    config: Config,
    task_queue: BinaryHeap<ScheduledTask<H, D, S>>,
    not_resolved_parents: HashSet<H::Hash>,
    not_resolved_coords: HashSet<UnitCoord>,
    n_members: NodeCount,
    unit_messages_for_network: Sender<(UnitMessage<H, D, S>, Recipient)>,
    unit_messages_from_network: Receiver<UnitMessage<H, D, S>>,
    messages_from_network: Sender<RunwayNotificationIn<H, D, S>>,
    messages_for_network: Receiver<RunwayNotificationOut<H, D, S>>,
    resolved_requests: Receiver<Request<H>>,
}

impl<H, D, S> Member<H, D, S>
where
    H: Hasher,
    D: Data,
    S: Signature,
{
    fn new(
        config: Config,
        unit_messages_for_network: Sender<(UnitMessage<H, D, S>, Recipient)>,
        unit_messages_from_network: Receiver<UnitMessage<H, D, S>>,
        messages_from_network: Sender<RunwayNotificationIn<H, D, S>>,
        messages_for_network: Receiver<RunwayNotificationOut<H, D, S>>,
        resolved_requests: Receiver<Request<H>>,
    ) -> Self {
        let n_members = config.n_members;
        Self {
            config,
            task_queue: BinaryHeap::new(),
            not_resolved_parents: HashSet::new(),
            not_resolved_coords: HashSet::new(),
            n_members,
            unit_messages_for_network,
            unit_messages_from_network,
            messages_from_network,
            messages_for_network,
            resolved_requests,
        }
    }

    fn on_create(&mut self, u: UncheckedSignedUnit<H, D, S>) {
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::UnitMulticast(u), curr_time);
        self.task_queue.push(task);
    }

    fn on_request_coord(&mut self, coord: UnitCoord) {
        trace!(target: "AlephBFT-member", "{:?} Dealing with missing coord notification {:?}.", self.index(), coord);
        if !self.not_resolved_coords.insert(coord) {
            return;
        }
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::CoordRequest(coord), curr_time);
        self.task_queue.push(task);
        self.trigger_tasks();
    }

    fn on_request_parents(&mut self, u_hash: H::Hash, recipient: Recipient) {
        if !self.not_resolved_parents.insert(u_hash) {
            return;
        }
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::ParentsRequest(u_hash, recipient), curr_time);
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

    fn send_unit_message(&mut self, message: UnitMessage<H, D, S>, recipient: Recipient) {
        self.unit_messages_for_network
            .unbounded_send((message, recipient))
            .expect("Channel to network should be open")
    }

    /// Given a task and the number of times it was performed, returns `None` if the task is no longer active, or
    /// `Some((message, recipient, delay))` if the task is active and the task is to send `message` to `recipient`
    /// and should be rescheduled after `delay`.
    fn task_details(
        &mut self,
        task: &Task<H, D, S>,
        counter: usize,
    ) -> Option<(UnitMessage<H, D, S>, Recipient, time::Duration)> {
        // preferred_recipient is Everyone if the message is supposed to be broadcast,
        // and Node(node_id) if the message should be send to one peer (node_id when
        // the task is done for the first time or a random peer otherwise)
        let (message, preferred_recipient) = match task {
            Task::CoordRequest(coord) => {
                if !self.not_resolved_coords.contains(coord) {
                    return None;
                }
                let message = UnitMessage::RequestCoord(self.index(), *coord);
                let preferred_recipient = Recipient::Node(coord.creator());
                (message, preferred_recipient)
            }
            Task::ParentsRequest(hash, preferred_recipient) => {
                if !self.not_resolved_parents.contains(hash) {
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

    fn on_unit_message_from_units(&mut self, message: RunwayNotificationOut<H, D, S>) {
        match message {
            RunwayNotificationOut::NewUnit(u) => self.on_create(u),
            RunwayNotificationOut::Request(request, recipient) => match request {
                Request::RequestCoord(coord) => self.on_request_coord(coord),
                Request::RequestParents(u_hash) => self.on_request_parents(u_hash, recipient),
            },
            RunwayNotificationOut::Response(response, recipient) => match response {
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

    async fn run(mut self, mut exit: oneshot::Receiver<()>) {
        let ticker_delay = self.config.delay_config.tick_interval;
        let mut ticker = Delay::new(ticker_delay).fuse();

        loop {
            futures::select! {
                event = self.messages_for_network.next() => match event {
                    Some(message) => {
                        self.on_unit_message_from_units(message);
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from Runway closed.", self.index());
                        break;
                    },
                },

                event = self.resolved_requests.next() => match event {
                    Some(request) => match request {
                        Request::RequestCoord(coord) => { self.not_resolved_coords.remove(&coord); },
                        Request::RequestParents(u_hash) => { self.not_resolved_parents.remove(&u_hash); },
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Resolved-requests stream from Runway closed.", self.index());
                        break;
                    }
                },

                event = self.unit_messages_from_network.next() => match event {
                    Some(message) => {
                        if self.send_notification_to_runway(message).is_err() {
                            error!(target: "AlephBFT-member", "{:?} Unable to convert a UnitMessage into an instance of RunwayNotificationIn.", self.index());
                        }
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from network closed.", self.index());
                        break;
                    },
                },

                _ = &mut ticker => {
                    self.trigger_tasks();
                    ticker = Delay::new(ticker_delay).fuse();
                },

                _ = &mut exit => break,
            }
        }
        debug!(target: "AlephBFT-member", "{:?} Member stopped.", self.index());
    }

    fn send_notification_to_runway(&mut self, message: UnitMessage<H, D, S>) -> Result<(), ()> {
        match RunwayNotificationIn::try_from(message) {
            Ok(notification) => {
                self.messages_from_network
                    .unbounded_send(notification)
                    .expect("Sender to runway with RunwayNotificationIn messages should be open");
                Ok(())
            }
            Err(_) => Err(()),
        }
    }
}

/// Starts the consensus algorithm as an async task. It stops establishing consensus for new data items after
/// reaching the threshold specified in [`Config::max_round`] or upon receiving a stop signal from `exit`.
/// For a detailed description of the consensus implemented by `run_session` see
/// [docs for devs](https://cardinal-cryptography.github.io/AlephBFT/index.html)
/// or the [original paper](https://arxiv.org/abs/1908.05156).
pub async fn run_session<
    H: Hasher,
    D: Data,
    DP: DataIO<D>,
    N: Network<H, D, MK::Signature, MK::PartialMultisignature> + 'static,
    SH: SpawnHandle,
    MK: MultiKeychain,
>(
    config: Config,
    network: N,
    data_io: DP,
    keybox: MK,
    spawn_handle: SH,
    mut exit: oneshot::Receiver<()>,
) {
    let index = config.node_ix;
    info!(target: "AlephBFT-member", "{:?} Spawning party for a session.", index);

    let (alert_messages_for_alerter, alert_messages_from_network) = mpsc::unbounded();
    let (alert_messages_for_network, alert_messages_from_alerter) = mpsc::unbounded();
    let (unit_messages_for_units, unit_messages_from_network) = mpsc::unbounded();
    let (unit_messages_for_network, unit_messages_from_units) = mpsc::unbounded();
    let (runway_messages_for_runway, runway_messages_from_network) = mpsc::unbounded();
    let (runway_messages_for_network, runway_messages_from_runway) = mpsc::unbounded();
    let (resolved_requests_tx, resolved_requests_rx) = mpsc::unbounded();

    info!(target: "AlephBFT-member", "{:?} Spawning network.", index);
    let (network_exit, exit_stream) = oneshot::channel();

    let network_handle = spawn_handle.spawn_essential("member/network", async move {
        network::run(
            network,
            unit_messages_from_units,
            unit_messages_for_units,
            alert_messages_from_alerter,
            alert_messages_for_alerter,
            exit_stream,
        )
        .await
    });
    let network_handle = network_handle.fuse();
    pin_mut!(network_handle);
    info!(target: "AlephBFT-member", "{:?} Network spawned.", index);

    info!(target: "AlephBFT-member", "{:?} Initializing Runway.", index);
    let (runway_exit, exit_stream) = oneshot::channel();
    let runway_io = RunwayIO {
        alert_messages_for_network,
        alert_messages_from_network,
        unit_messages_from_network: runway_messages_from_network,
        unit_messages_for_network: runway_messages_for_network,
        resolved_requests: resolved_requests_tx,
    };
    let runway_handle = runway::run(
        config.clone(),
        keybox.clone(),
        data_io,
        spawn_handle.clone(),
        runway_io,
        exit_stream,
    );
    let runway_handle = runway_handle.fuse();
    pin_mut!(runway_handle);
    info!(target: "AlephBFT-member", "{:?} Runway initialized.", index);

    info!(target: "AlephBFT-member", "{:?} Initializing Member.", index);
    let member = Member::new(
        config,
        unit_messages_for_network,
        unit_messages_from_network,
        runway_messages_for_runway,
        runway_messages_from_runway,
        resolved_requests_rx,
    );
    let (member_exit, exit_stream) = oneshot::channel();
    let member_handle = member.run(exit_stream).fuse();
    pin_mut!(member_handle);
    info!(target: "AlephBFT-member", "{:?} Member initialized.", index);

    futures::select! {
        _ = network_handle => {
            error!(target: "AlephBFT-member", "{:?} Network-hub terminated early.", index);
        },

        _ = runway_handle => {
            error!(target: "AlephBFT-member", "{:?} Runway terminated early.", index);
        },

        _ = member_handle => {
            error!(target: "AlephBFT-member", "{:?} Member terminated early.", index);
        },

        _ = &mut exit => {
            info!(target: "AlephBFT-member", "{:?} exit channel was called.", index);
        },
    }

    if runway_exit.send(()).is_err() {
        debug!(target: "AlephBFT-member", "{:?} Runway already stopped.", index);
    }
    if !runway_handle.is_terminated() {
        runway_handle.await;
    }
    if member_exit.send(()).is_err() {
        debug!(target: "AlephBFT-member", "{:?} Member already stopped.", index);
    }
    if !member_handle.is_terminated() {
        member_handle.await;
    }
    if network_exit.send(()).is_err() {
        debug!(target: "AlephBFT-member", "{:?} Network-hub already stopped.", index);
    }
    if !network_handle.is_terminated() {
        network_handle.await.unwrap();
    }

    info!(target: "AlephBFT-member", "{:?} Run ended.", index);
}
