use crate::{
    member::Task::{CoordRequest, ParentsRequest, RequestNewest, UnitBroadcast},
    network,
    runway::{
        self, NetworkIO, NewestUnitResponse, Request, Response, RunwayIO, RunwayNotificationIn,
        RunwayNotificationOut,
    },
    task_queue::TaskQueue,
    units::{UncheckedSignedUnit, UnitCoord},
    Config, Data, DataProvider, FinalizationHandler, Hasher, MultiKeychain, Network, NodeCount,
    NodeIndex, Receiver, Recipient, Round, Sender, Signature, SpawnHandle, UncheckedSigned,
};
use aleph_bft_types::NodeMap;
use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    future::FusedFuture,
    pin_mut, FutureExt, StreamExt,
};
use futures_timer::Delay;
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use network::NetworkData;
use rand::Rng;
use std::{
    collections::HashSet,
    convert::TryInto,
    fmt::{self, Debug},
    io::{Read, Write},
    marker::PhantomData,
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
    // Rebroadcast a given unit periodically (cancelled after a more recent unit by the same creator is received)
    UnitBroadcast(UncheckedSignedUnit<H, D, S>),
    // Request the newest unit created by node itself.
    RequestNewest(u64),
}

#[derive(Eq, PartialEq, Debug)]
struct RepeatableTask<H: Hasher, D: Data, S: Signature> {
    task: Task<H, D, S>,
    counter: usize,
}

impl<H: Hasher, D: Data, S: Signature> fmt::Display for RepeatableTask<H, D, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RepeatableTask({:?}, counter {})",
            self.task, self.counter
        )
    }
}

impl<H: Hasher, D: Data, S: Signature> RepeatableTask<H, D, S> {
    fn new(task: Task<H, D, S>) -> Self {
        Self { task, counter: 0 }
    }
}

enum TaskDetails<H: Hasher, D: Data, S: Signature> {
    Cancel,
    Perform {
        message: UnitMessage<H, D, S>,
        recipient: Recipient,
        reschedule: Duration,
    },
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

struct MemberStatus<'a, H: Hasher, D: Data, S: Signature> {
    task_queue: &'a TaskQueue<RepeatableTask<H, D, S>>,
    not_resolved_parents: &'a HashSet<H::Hash>,
    not_resolved_coords: &'a HashSet<UnitCoord>,
}

impl<'a, H: Hasher, D: Data, S: Signature> MemberStatus<'a, H, D, S> {
    fn new(
        task_queue: &'a TaskQueue<RepeatableTask<H, D, S>>,
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

impl<'a, H: Hasher, D: Data, S: Signature> fmt::Display for MemberStatus<'a, H, D, S>
where
    H: Hasher,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut count_coord_request: usize = 0;
        let mut count_parents_request: usize = 0;
        let mut count_request_newest: usize = 0;
        let mut count_rebroadcast: usize = 0;
        for task in self.task_queue.iter().map(|st| &st.task) {
            match task {
                CoordRequest(_) => count_coord_request += 1,
                ParentsRequest(_, _) => count_parents_request += 1,
                RequestNewest(_) => count_request_newest += 1,
                UnitBroadcast(_) => count_rebroadcast += 1,
            }
        }
        let long_time_pending_tasks: Vec<_> = self
            .task_queue
            .iter()
            .filter(|st| st.counter >= 5)
            .collect();
        write!(f, "Member status report: ")?;
        write!(f, "task queue content: ")?;
        write!(
            f,
            "CoordRequest - {}, ParentsRequest - {}, UnitBroadcast - {}, RequestNewest - {}",
            count_coord_request, count_parents_request, count_rebroadcast, count_request_newest,
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

        static ITEMS_PRINT_LIMIT: usize = 10;

        if !long_time_pending_tasks.is_empty() {
            write!(f, "; pending tasks with counter >= 5 -")?;
            write!(f, " {}", {
                long_time_pending_tasks
                    .iter()
                    .take(ITEMS_PRINT_LIMIT)
                    .join(", ")
            })?;

            if let Some(remaining) = long_time_pending_tasks.len().checked_sub(ITEMS_PRINT_LIMIT) {
                write!(f, " and {remaining} more")?
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
    task_queue: TaskQueue<RepeatableTask<H, D, S>>,
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
    top_units: NodeMap<Round>,
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
            task_queue: TaskQueue::new(),
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
            top_units: NodeMap::with_size(n_members),
        }
    }

    fn on_create(&mut self, u: UncheckedSignedUnit<H, D, S>) {
        self.send_unit_message(UnitMessage::NewUnit(u), Recipient::Everyone);
    }

    fn on_unit_discovered(&mut self, new_unit: UncheckedSignedUnit<H, D, S>) {
        let unit_creator = new_unit.as_signable().creator();
        let unit_round = new_unit.as_signable().round();
        if self
            .top_units
            .get(unit_creator)
            .map(|round| round < &unit_round)
            .unwrap_or(true)
        {
            self.top_units.insert(unit_creator, unit_round);
            let task = RepeatableTask::new(UnitBroadcast(new_unit));
            let delay = self.delay(&task.task);
            self.task_queue.schedule_in(task, delay)
        }
    }

    fn on_request_coord(&mut self, coord: UnitCoord) {
        trace!(target: "AlephBFT-member", "{:?} Dealing with missing coord notification {:?}.", self.index(), coord);
        if !self.not_resolved_coords.insert(coord) {
            return;
        }

        self.task_queue
            .schedule_now(RepeatableTask::new(CoordRequest(coord)));
        self.trigger_tasks();
    }

    fn on_request_parents(&mut self, u_hash: H::Hash, recipient: Recipient) {
        if !self.not_resolved_parents.insert(u_hash) {
            return;
        }

        self.task_queue
            .schedule_now(RepeatableTask::new(ParentsRequest(u_hash, recipient)));
        self.trigger_tasks();
    }

    fn on_request_newest(&mut self, salt: u64) {
        self.task_queue
            .schedule_now(RepeatableTask::new(RequestNewest(salt)));
        self.trigger_tasks();
    }

    fn trigger_tasks(&mut self) {
        while let Some(mut task) = self.task_queue.pop_due_task() {
            match self.task_details(&task.task, task.counter) {
                TaskDetails::Cancel => (),
                TaskDetails::Perform {
                    message,
                    recipient,
                    reschedule,
                } => {
                    self.send_unit_message(message, recipient);
                    task.counter += 1;
                    self.task_queue.schedule_in(task, reschedule)
                }
            }
        }
    }

    fn random_peer(&self) -> Recipient {
        let node = rand::thread_rng()
            .gen_range(0..self.n_members.into())
            .into();
        Recipient::Node(node)
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

    /// Given a task and the number of times it was performed, returns `Cancel` if the task is no longer active,
    /// `Delay(Duration)` if the task is active, but cannot be performed right now, and
    /// `Perform { message, recipient, reschedule }` if the task is to send `message` to `recipient` and it should
    /// be rescheduled after `reschedule`.
    fn task_details(&mut self, task: &Task<H, D, S>, counter: usize) -> TaskDetails<H, D, S> {
        match self.still_valid(task) {
            false => TaskDetails::Cancel,
            true => TaskDetails::Perform {
                message: self.message(task),
                recipient: self.recipient(task, counter),
                reschedule: self.delay(task),
            },
        }
    }

    fn message(&self, task: &Task<H, D, S>) -> UnitMessage<H, D, S> {
        match task {
            CoordRequest(coord) => UnitMessage::RequestCoord(self.index(), *coord),
            ParentsRequest(hash, _) => UnitMessage::RequestParents(self.index(), *hash),
            UnitBroadcast(unit) => UnitMessage::NewUnit(unit.clone()),
            RequestNewest(salt) => UnitMessage::RequestNewest(self.index(), *salt),
        }
    }

    fn recipient(&self, task: &Task<H, D, S>, counter: usize) -> Recipient {
        match (self.preferred_recipient(task), counter) {
            (Recipient::Everyone, _) => Recipient::Everyone,
            (recipient, 0) => recipient,
            (_, _) => self.random_peer(),
        }
    }

    fn preferred_recipient(&self, task: &Task<H, D, S>) -> Recipient {
        match task {
            CoordRequest(coord) => Recipient::Node(coord.creator()),
            ParentsRequest(_, preferred_recipient) => preferred_recipient.clone(),
            UnitBroadcast(_) => Recipient::Everyone,
            RequestNewest(_) => Recipient::Everyone,
        }
    }

    fn still_valid(&self, task: &Task<H, D, S>) -> bool {
        match task {
            CoordRequest(coord) => self.not_resolved_coords.contains(coord),
            ParentsRequest(hash, _preferred_recipient) => self.not_resolved_parents.contains(hash),
            RequestNewest(_) => !self.newest_unit_resolved,
            UnitBroadcast(unit) => {
                Some(&unit.as_signable().round())
                    == self.top_units.get(unit.as_signable().creator())
            }
        }
    }

    /// Most tasks use `requests_interval` (see [crate::DelayConfig]) as their delay.
    /// The exception is [Task::UnitBroadcast] - this one picks a random delay between
    /// `unit_rebroadcast_interval_min` and `unit_rebroadcast_interval_max`.
    fn delay(&self, task: &Task<H, D, S>) -> Duration {
        match task {
            UnitBroadcast(_) => {
                let low = self.config.delay_config.unit_rebroadcast_interval_min;
                let high = self.config.delay_config.unit_rebroadcast_interval_max;
                let millis = rand::thread_rng().gen_range(low.as_millis()..high.as_millis());
                Duration::from_millis(millis as u64)
            }
            _ => self.config.delay_config.requests_interval,
        }
    }

    fn on_unit_message_from_units(&mut self, message: RunwayNotificationOut<H, D, S>) {
        match message {
            RunwayNotificationOut::NewSelfUnit(u) => self.on_create(u),
            RunwayNotificationOut::NewAnyUnit(u) => self.on_unit_discovered(u),
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
