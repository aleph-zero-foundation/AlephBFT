use crate::{
    network,
    runway::{
        self, NetworkIO, NewestUnitResponse, Request, Response, RunwayIO, RunwayNotificationIn,
        RunwayNotificationOut,
    },
    units::{UncheckedSignedUnit, UnitCoord},
    Config, Data, DataProvider, FinalizationHandler, Hasher, MultiKeychain, Network, NodeCount,
    NodeIndex, Receiver, Recipient, Sender, Signature, SpawnHandle, UncheckedSigned,
};
use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    future::FusedFuture,
    pin_mut, FutureExt, StreamExt,
};
use futures_timer::Delay;
use log::{debug, error, info, trace, warn};
use network::NetworkData;
use rand::Rng;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    convert::TryInto,
    fmt::{self, Debug},
    io::{Read, Write},
    marker::PhantomData,
    time,
    time::Duration,
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
    /// Request by a node for the newest unit created by them, together with a u64 salt
    RequestNewest(NodeIndex, u64),
    /// Response to RequestNewest: (our index, maybe unit, salt) signed by us
    ResponseNewest(UncheckedSigned<NewestUnitResponse<H, D, S>, S>),
}

impl<H: Hasher, D: Data, S: Signature> UnitMessage<H, D, S> {
    pub(crate) fn included_data(&self) -> Vec<D> {
        match self {
            Self::NewUnit(uu) => uu.as_signable().included_data(),
            Self::RequestCoord(_, _) => Vec::new(),
            Self::ResponseCoord(uu) => uu.as_signable().included_data(),
            Self::RequestParents(_, _) => Vec::new(),
            Self::ResponseParents(_, units) => units
                .iter()
                .flat_map(|uu| uu.as_signable().included_data())
                .collect(),
            UnitMessage::RequestNewest(_, _) => Vec::new(),
            UnitMessage::ResponseNewest(response) => response.as_signable().included_data(),
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
enum Task<H: Hasher, D: Data, S: Signature> {
    // Request the unit with the given (creator, round) coordinates.
    CoordRequest(UnitCoord),
    // Request parents of the unit with the given hash and Recipient.
    ParentsRequest(H::Hash, Recipient),
    // Broadcast the given unit.
    UnitMulticast(UncheckedSignedUnit<H, D, S>),
    // Request the newest unit created by node itself.
    RequestNewest(u64),
}

#[derive(Eq, PartialEq, Debug)]
struct ScheduledTask<H: Hasher, D: Data, S: Signature> {
    task: Task<H, D, S>,
    scheduled_time: time::Instant,
    // The number of times the task was performed so far.
    counter: usize,
}

impl<H: Hasher, D: Data, S: Signature> fmt::Display for ScheduledTask<H, D, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ScheduledTask({:?}, counter {})",
            self.task, self.counter
        )
    }
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

#[derive(Clone)]
pub struct LocalIO<D: Data, DP: DataProvider<D>, FH: FinalizationHandler<D>, US: Write, UL: Read> {
    data_provider: DP,
    finalization_handler: FH,
    unit_saver: US,
    unit_loader: UL,
    _phantom: PhantomData<D>,
}

impl<D: Data, DP: DataProvider<D>, FH: FinalizationHandler<D>, US: Write, UL: Read>
    LocalIO<D, DP, FH, US, UL>
{
    pub fn new(
        data_provider: DP,
        finalization_handler: FH,
        unit_saver: US,
        unit_loader: UL,
    ) -> LocalIO<D, DP, FH, US, UL> {
        LocalIO {
            data_provider,
            finalization_handler,
            unit_saver,
            unit_loader,
            _phantom: PhantomData,
        }
    }
}

struct MemberStatus<'a, H, D, S>
where
    H: Hasher,
    D: Data,
    S: Signature,
{
    task_queue: &'a BinaryHeap<ScheduledTask<H, D, S>>,
    not_resolved_parents: &'a HashSet<H::Hash>,
    not_resolved_coords: &'a HashSet<UnitCoord>,
}

impl<'a, H, D, S> MemberStatus<'a, H, D, S>
where
    H: Hasher,
    D: Data,
    S: Signature,
{
    fn new(
        task_queue: &'a BinaryHeap<ScheduledTask<H, D, S>>,
        not_resolved_parents: &'a HashSet<H::Hash>,
        not_resolved_coords: &'a HashSet<UnitCoord>,
    ) -> Self {
        Self {
            task_queue,
            not_resolved_parents,
            not_resolved_coords,
        }
    }
}

impl<'a, H, D, S> fmt::Display for MemberStatus<'a, H, D, S>
where
    H: Hasher,
    D: Data,
    S: Signature,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut count_coord_request: usize = 0;
        let mut count_parents_request: usize = 0;
        let mut count_unit_multicast: usize = 0;
        let mut count_request_newest: usize = 0;
        for task in self.task_queue.iter().map(|st| &st.task) {
            match task {
                Task::CoordRequest(_) => count_coord_request += 1,
                Task::ParentsRequest(_, _) => count_parents_request += 1,
                Task::UnitMulticast(_) => count_unit_multicast += 1,
                Task::RequestNewest(_) => count_request_newest += 1,
            }
        }
        let long_time_pending_tasks: Vec<_> = self
            .task_queue
            .iter()
            .filter(|st| match st.task {
                Task::UnitMulticast(_) => false,
                _ => st.counter >= 5,
            })
            .collect();
        write!(f, "Member status report: ")?;
        write!(f, "task queue content: ")?;
        write!(
            f,
            "CoordRequest - {}, ParentsRequest - {}, UnitMulticast - {}, RequestNewest - {}",
            count_coord_request, count_parents_request, count_unit_multicast, count_request_newest,
        )?;
        if !self.not_resolved_coords.is_empty() {
            write!(
                f,
                "; not_resolved_coords.len() - {}",
                self.not_resolved_coords.len()
            )?;
        }
        if !self.not_resolved_parents.is_empty() {
            write!(
                f,
                "; not_resolved_parents.len() - {}",
                self.not_resolved_parents.len()
            )?;
        }
        if !long_time_pending_tasks.is_empty() {
            write!(f, "; pending tasks with counter >= 5 -")?;
            for task in long_time_pending_tasks.iter() {
                write!(f, " {},", task)?;
            }
        }
        write!(f, ".")?;
        Ok(())
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
    newest_unit_resolved: bool,
    n_members: NodeCount,
    unit_messages_for_network: Sender<(UnitMessage<H, D, S>, Recipient)>,
    unit_messages_from_network: Receiver<UnitMessage<H, D, S>>,
    notifications_for_runway: Sender<RunwayNotificationIn<H, D, S>>,
    notifications_from_runway: Receiver<RunwayNotificationOut<H, D, S>>,
    resolved_requests: Receiver<Request<H>>,
    exiting: bool,
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
        notifications_for_runway: Sender<RunwayNotificationIn<H, D, S>>,
        notifications_from_runway: Receiver<RunwayNotificationOut<H, D, S>>,
        resolved_requests: Receiver<Request<H>>,
    ) -> Self {
        let n_members = config.n_members;
        Self {
            config,
            task_queue: BinaryHeap::new(),
            not_resolved_parents: HashSet::new(),
            not_resolved_coords: HashSet::new(),
            newest_unit_resolved: false,
            n_members,
            unit_messages_for_network,
            unit_messages_from_network,
            notifications_for_runway,
            notifications_from_runway,
            resolved_requests,
            exiting: false,
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

    fn on_request_newest(&mut self, salt: u64) {
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(Task::RequestNewest(salt), curr_time);
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
                self.send_unit_message(message, recipient);
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
        if self
            .unit_messages_for_network
            .unbounded_send((message, recipient))
            .is_err()
        {
            warn!(target: "AlephBFT-member", "{:?} Channel to network should be open", self.index());
            self.exiting = true;
        }
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
            Task::RequestNewest(salt) => {
                if self.newest_unit_resolved {
                    return None;
                }
                let message = UnitMessage::RequestNewest(self.index(), *salt);
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
                Request::Coord(coord) => self.on_request_coord(coord),
                Request::Parents(u_hash) => self.on_request_parents(u_hash, recipient),
                Request::NewestUnit(salt) => self.on_request_newest(salt),
            },
            RunwayNotificationOut::Response(response, recipient) => match response {
                Response::Coord(u) => {
                    let message = UnitMessage::ResponseCoord(u);
                    self.send_unit_message(message, Recipient::Node(recipient))
                }
                Response::Parents(u_hash, parents) => {
                    let message = UnitMessage::ResponseParents(u_hash, parents);
                    self.send_unit_message(message, Recipient::Node(recipient))
                }
                Response::NewestUnit(response) => {
                    let requester = response.as_signable().requester();
                    let message = UnitMessage::ResponseNewest(response);
                    self.send_unit_message(message, Recipient::Node(requester))
                }
            },
        }
    }

    fn status_report(&self) {
        let status = MemberStatus::new(
            &self.task_queue,
            &self.not_resolved_parents,
            &self.not_resolved_coords,
        );
        info!(target: "AlephBFT-member", "{}", status);
    }

    async fn run(mut self, mut exit: oneshot::Receiver<()>) {
        let ticker_delay = self.config.delay_config.tick_interval;
        let mut ticker = Delay::new(ticker_delay).fuse();
        let status_ticker_delay = Duration::from_secs(10);
        let mut status_ticker = Delay::new(status_ticker_delay).fuse();

        loop {
            futures::select! {
                event = self.notifications_from_runway.next() => match event {
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
                        Request::Coord(coord) => { self.not_resolved_coords.remove(&coord); },
                        Request::Parents(u_hash) => { self.not_resolved_parents.remove(&u_hash); },
                        Request::NewestUnit(_) => {
                            self.newest_unit_resolved = true;
                        }
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Resolved-requests stream from Runway closed.", self.index());
                        break;
                    }
                },

                event = self.unit_messages_from_network.next() => match event {
                    Some(message) => match message.try_into() {
                        Ok(notification) => self.send_notification_to_runway(notification),
                        Err(_) => error!(target: "AlephBFT-member", "{:?} Unable to convert a UnitMessage into an instance of RunwayNotificationIn.", self.index()),
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

                _ = &mut status_ticker => {
                    self.status_report();
                    status_ticker = Delay::new(status_ticker_delay).fuse();
                },

                _ = &mut exit => {
                    info!(target: "AlephBFT-member", "{:?} received exit signal", self.index());
                    self.exiting = true;
                },
            }
            if self.exiting {
                info!(target: "AlephBFT-member", "{:?} Member decided to exit.", self.index());
                break;
            }
        }
        debug!(target: "AlephBFT-member", "{:?} Member stopped.", self.index());
    }

    fn send_notification_to_runway(&mut self, notification: RunwayNotificationIn<H, D, S>) {
        if self
            .notifications_for_runway
            .unbounded_send(notification)
            .is_err()
        {
            warn!(target: "AlephBFT-member", "{:?} Sender to runway with RunwayNotificationIn messages should be open", self.index());
            self.exiting = true;
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
    DP: DataProvider<D>,
    FH: FinalizationHandler<D>,
    US: Write,
    UL: Read,
    N: Network<NetworkData<H, D, MK::Signature, MK::PartialMultisignature>> + 'static,
    SH: SpawnHandle,
    MK: MultiKeychain,
>(
    config: Config,
    local_io: LocalIO<D, DP, FH, US, UL>,
    network: N,
    keychain: MK,
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
    let network_io = NetworkIO {
        alert_messages_for_network,
        alert_messages_from_network,
        unit_messages_from_network: runway_messages_from_network,
        unit_messages_for_network: runway_messages_for_network,
        resolved_requests: resolved_requests_tx,
    };
    let runway_io = RunwayIO::new(
        local_io.data_provider,
        local_io.finalization_handler,
        local_io.unit_saver,
        local_io.unit_loader,
    );
    let runway_handle = runway::run(
        config.clone(),
        runway_io,
        keychain.clone(),
        spawn_handle.clone(),
        network_io,
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

    info!(target: "AlephBFT-member", "{:?} Run ending.", index);
    if runway_exit.send(()).is_err() {
        debug!(target: "AlephBFT-member", "{:?} Runway already stopped.", index);
    }
    if !runway_handle.is_terminated() {
        runway_handle.await;
        debug!(target: "AlephBFT-member", "{:?} Runway stopped.", index);
    }

    if member_exit.send(()).is_err() {
        debug!(target: "AlephBFT-member", "{:?} Member already stopped.", index);
    }
    if !member_handle.is_terminated() {
        member_handle.await;
        debug!(target: "AlephBFT-member", "{:?} Member stopped.", index);
    }

    if network_exit.send(()).is_err() {
        debug!(target: "AlephBFT-member", "{:?} Network-hub already stopped.", index);
    }
    if !network_handle.is_terminated() {
        if let Err(()) = network_handle.await {
            warn!(target: "AlephBFT-member", "{:?} Network task stopped with an error", index);
        }
        debug!(target: "AlephBFT-member", "{:?} Network stopped.", index);
    }

    info!(target: "AlephBFT-member", "{:?} Run ended.", index);
}
