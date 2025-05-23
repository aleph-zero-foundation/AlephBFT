use crate::{
    config::{DelaySchedule, RecipientCountSchedule},
    dag::{DagUnit, Request},
    dissemination::{Addressed, DisseminationMessage, ReconstructionRequest, LOG_TARGET},
    task_queue::TaskQueue,
    units::{SignedUnit, Unit, UnitCoord, UnitStore, WrappedUnit},
    Data, DelayConfig, Hasher, MultiKeychain, NodeCount, NodeIndex, NodeMap, Recipient, Round,
    Signature,
};
use itertools::Itertools;
use log::trace;
use rand::{prelude::SliceRandom, Rng};
use std::{
    collections::HashSet,
    fmt::{Display, Formatter, Result as FmtResult},
    time::Duration,
};

/// Task that needs to be performed to ensure successful unit dissemination, either requesting or broadcasting a unit.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum DisseminationTask<H: Hasher> {
    /// Perform a request.
    Request(Request<H>),
    /// Broadcast a unit.
    Broadcast(H::Hash),
}

enum TaskDetails<H: Hasher, D: Data, S: Signature> {
    Cancel,
    Delay(Duration),
    BetterInstead {
        better_request: Request<H>,
        delay: Duration,
    },
    Perform {
        message: Addressed<DisseminationMessage<H, D, S>>,
        delay: Duration,
    },
}

#[derive(Eq, PartialEq, Debug, Clone)]
struct RepeatableTask<H: Hasher> {
    task: DisseminationTask<H>,
    counter: usize,
}

impl<H: Hasher> RepeatableTask<H> {
    fn new(task: DisseminationTask<H>) -> Self {
        Self { task, counter: 0 }
    }
}

impl<H: Hasher> Display for RepeatableTask<H> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "RepeatableTask({:?}, counter {})",
            self.task, self.counter
        )
    }
}

/// Manager for tasks. Controls which tasks are performed and when.
pub struct Manager<H: Hasher> {
    own_id: NodeIndex,
    peers: Vec<Recipient>,
    tick_interval: Duration,
    unit_rebroadcast_interval_min: Duration,
    unit_rebroadcast_interval_max: Duration,
    coord_request_delay: DelaySchedule,
    coord_request_recipients: RecipientCountSchedule,
    parent_request_delay: DelaySchedule,
    parent_request_recipients: RecipientCountSchedule,
    task_queue: TaskQueue<RepeatableTask<H>>,
    missing_coords: HashSet<UnitCoord>,
    top_rounds: NodeMap<Round>,
}

pub struct ManagerStatus<H: Hasher> {
    coord_request_count: usize,
    parent_request_count: usize,
    rebroadcast_count: usize,
    long_time_pending_tasks: Vec<RepeatableTask<H>>,
}

impl<H: Hasher> ManagerStatus<H> {
    fn new(task_queue: &TaskQueue<RepeatableTask<H>>) -> Self {
        use DisseminationTask::*;
        let mut coord_request_count: usize = 0;
        let mut parent_request_count: usize = 0;
        let mut rebroadcast_count: usize = 0;
        for task in task_queue.iter().map(|st| &st.task) {
            match task {
                Request(ReconstructionRequest::Coord(_)) => coord_request_count += 1,
                Request(ReconstructionRequest::ParentsOf(_)) => parent_request_count += 1,
                Broadcast(_) => rebroadcast_count += 1,
            }
        }
        let long_time_pending_tasks = task_queue
            .iter()
            .filter(|st| st.counter >= 5)
            .cloned()
            .collect();
        ManagerStatus {
            coord_request_count,
            parent_request_count,
            rebroadcast_count,
            long_time_pending_tasks,
        }
    }

    fn longest_pending_tasks(&self) -> Vec<&RepeatableTask<H>> {
        const ITEMS_PRINT_LIMIT: usize = 10;
        self.long_time_pending_tasks
            .iter()
            .fold(Vec::new(), |mut highest, task| {
                if let Some(index) = highest
                    .iter()
                    .position(|ancient_task| ancient_task.counter < task.counter)
                {
                    highest.insert(index, task);
                } else if highest.len() < ITEMS_PRINT_LIMIT {
                    highest.push(task);
                }
                if highest.len() > ITEMS_PRINT_LIMIT {
                    highest.pop();
                }
                highest
            })
    }
}

impl<H: Hasher> Display for ManagerStatus<H> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "task queue content: ")?;
        write!(
            f,
            "CoordRequest - {}, ParentsRequest - {}, UnitBroadcast - {}",
            self.coord_request_count, self.parent_request_count, self.rebroadcast_count,
        )?;
        if !self.long_time_pending_tasks.is_empty() {
            write!(
                f,
                "; {} pending tasks with counter >= 5,",
                self.long_time_pending_tasks.len()
            )?;
            write!(f, "longest pending: {}", {
                self.longest_pending_tasks().iter().join(", ")
            })?;
        }
        Ok(())
    }
}

impl<H: Hasher> Manager<H> {
    /// Create a new Manager.
    pub fn new(own_id: NodeIndex, n_members: NodeCount, delay_config: DelayConfig) -> Self {
        let DelayConfig {
            tick_interval,
            unit_rebroadcast_interval_min,
            unit_rebroadcast_interval_max,
            coord_request_delay,
            coord_request_recipients,
            parent_request_delay,
            parent_request_recipients,
            ..
        } = delay_config;
        let peers = (0..n_members.0)
            .map(NodeIndex)
            .filter(|x| *x != own_id)
            .map(Recipient::Node)
            .collect();
        Manager {
            own_id,
            peers,
            tick_interval,
            unit_rebroadcast_interval_min,
            unit_rebroadcast_interval_max,
            coord_request_delay,
            coord_request_recipients,
            parent_request_delay,
            parent_request_recipients,
            task_queue: TaskQueue::new(),
            missing_coords: HashSet::new(),
            top_rounds: NodeMap::with_size(n_members),
        }
    }

    fn index(&self) -> NodeIndex {
        self.own_id
    }

    /// When should `trigger_tasks` be called next.
    pub fn next_tick(&self) -> Duration {
        self.tick_interval
    }

    fn broadcast_delay(&self) -> Duration {
        let low = self.unit_rebroadcast_interval_min;
        let high = self.unit_rebroadcast_interval_max;
        let millis = rand::thread_rng().gen_range(low.as_millis()..high.as_millis());
        Duration::from_millis(millis as u64)
    }

    fn random_peers(&self, n: usize) -> Vec<Recipient> {
        self.peers
            .choose_multiple(&mut rand::thread_rng(), n)
            .cloned()
            .collect()
    }

    fn coord_request_details<D: Data, MK: MultiKeychain>(
        &mut self,
        coord: UnitCoord,
        counter: usize,
        stored_units: &UnitStore<DagUnit<H, D, MK>>,
        processing_units: &UnitStore<SignedUnit<H, D, MK>>,
    ) -> TaskDetails<H, D, MK::Signature> {
        use Request::*;
        use TaskDetails::*;
        if stored_units.canonical_unit(coord).is_some() {
            self.missing_coords.remove(&coord);
            return Cancel;
        }
        let delay = (self.coord_request_delay)(counter);
        if processing_units.canonical_unit(coord).is_some() {
            return Delay(delay);
        }
        let top_round_for_creator = stored_units.top_round_for(coord.creator()).unwrap_or(0);
        const FUTURE_REQUEST_LIMIT: Round = 10;
        if coord.round().saturating_sub(top_round_for_creator) > FUTURE_REQUEST_LIMIT {
            return BetterInstead {
                better_request: Coord(UnitCoord::new(
                    top_round_for_creator + FUTURE_REQUEST_LIMIT / 2,
                    coord.creator(),
                )),
                delay,
            };
        }
        Perform {
            message: Addressed::new(
                DisseminationMessage::Request(self.index(), Coord(coord)),
                self.random_peers((self.coord_request_recipients)(counter)),
            ),
            delay,
        }
    }

    fn request_details<D: Data, MK: MultiKeychain>(
        &mut self,
        request: &Request<H>,
        counter: usize,
        stored_units: &UnitStore<DagUnit<H, D, MK>>,
        processing_units: &UnitStore<SignedUnit<H, D, MK>>,
    ) -> TaskDetails<H, D, MK::Signature> {
        use Request::*;
        use TaskDetails::*;
        match request {
            Coord(coord) => {
                self.coord_request_details(*coord, counter, stored_units, processing_units)
            }
            ParentsOf(hash) => match stored_units.unit(hash) {
                Some(_) => Cancel,
                None => Perform {
                    message: Addressed::new(
                        DisseminationMessage::Request(self.index(), request.clone()),
                        self.random_peers((self.parent_request_recipients)(counter)),
                    ),
                    delay: (self.parent_request_delay)(counter),
                },
            },
        }
    }

    fn task_details<D: Data, MK: MultiKeychain>(
        &mut self,
        task: &RepeatableTask<H>,
        stored_units: &UnitStore<DagUnit<H, D, MK>>,
        processing_units: &UnitStore<SignedUnit<H, D, MK>>,
    ) -> TaskDetails<H, D, MK::Signature> {
        use DisseminationTask::*;
        use TaskDetails::*;
        match &task.task {
            Request(request) => {
                self.request_details(request, task.counter, stored_units, processing_units)
            }
            Broadcast(hash) => match stored_units.unit(hash) {
                Some(unit) => match self.top_rounds.get(unit.creator()) == Some(&unit.round()) {
                    true => Perform {
                        message: Addressed::broadcast(DisseminationMessage::Unit(
                            unit.clone().unpack().into(),
                        )),
                        delay: self.broadcast_delay(),
                    },
                    false => Cancel,
                },
                // This should never happen, as we never remove units from the store.
                None => Cancel,
            },
        }
    }

    /// Trigger all the ready tasks and get all the messages that should be sent now.
    pub fn trigger_tasks<D: Data, MK: MultiKeychain>(
        &mut self,
        stored_units: &UnitStore<DagUnit<H, D, MK>>,
        processing_units: &UnitStore<SignedUnit<H, D, MK>>,
    ) -> Vec<Addressed<DisseminationMessage<H, D, MK::Signature>>> {
        trace!(target: LOG_TARGET, "Checking for due tasks.");
        use TaskDetails::*;
        let mut result = Vec::new();
        while let Some(mut task) = self.task_queue.pop_due_task() {
            trace!(target: LOG_TARGET, "Triggering due task: {:?}", task);
            match self.task_details(&task, stored_units, processing_units) {
                Cancel => {
                    trace!(target: LOG_TARGET, "Task outdated, dropped.");
                }
                Delay(delay) => {
                    trace!(target: LOG_TARGET, "Task pending verification, delayed by {:?}.", delay);
                    self.task_queue.schedule_in(task, delay)
                }
                BetterInstead {
                    better_request,
                    delay,
                } => {
                    trace!(target: LOG_TARGET, "Requesting {:?} instead of executing task, original delayed by {:?}.", better_request, delay);
                    self.add_request(better_request);
                    self.task_queue.schedule_in(task, delay)
                }
                Perform { message, delay } => {
                    trace!(target: LOG_TARGET, "Executing task by sending {:?}, and rescheduling it delayed by {:?}.", message, delay);
                    result.push(message);
                    task.counter += 1;
                    self.task_queue.schedule_in(task, delay)
                }
            }
        }
        trace!(target: LOG_TARGET, "Due tasks resulted in sending {} messages.", result.len());
        result
    }

    /// Add a request to be performed according to the appropriate schedule.
    pub fn add_request(&mut self, request: Request<H>) {
        trace!(target: LOG_TARGET, "Handling newly created request: {:?}", request);
        if let Request::Coord(coord) = request {
            if !self.missing_coords.insert(coord) {
                return;
            }
        }
        trace!(target: LOG_TARGET, "Scheduling newly created request: {:?}", request);
        self.task_queue
            .schedule_now(RepeatableTask::new(DisseminationTask::Request(request)));
    }

    /// Add a unit that should potentially be broadcast. Returns a message for immediate broadcast
    /// of own units.
    pub fn add_unit<D: Data, MK: MultiKeychain>(
        &mut self,
        unit: &DagUnit<H, D, MK>,
    ) -> Option<Addressed<DisseminationMessage<H, D, MK::Signature>>> {
        trace!(target: LOG_TARGET, "New unit with hash {:?} at {}.", unit.hash(), unit.coord());
        let hash = unit.hash();
        let round = unit.round();
        let creator = unit.creator();
        if self
            .top_rounds
            .get(creator)
            .map(|r| round <= *r)
            .unwrap_or(false)
        {
            return None;
        }
        self.top_rounds.insert(creator, round);
        self.task_queue.schedule_in(
            RepeatableTask::new(DisseminationTask::Broadcast(hash)),
            self.broadcast_delay(),
        );
        trace!(target: LOG_TARGET, "Scheduled broadcast for unit with hash {:?}.", unit.hash());
        match creator == self.index() {
            true => Some(Addressed::broadcast(DisseminationMessage::Unit(
                unit.clone().unpack().into(),
            ))),
            false => None,
        }
    }

    pub fn status(&self) -> ManagerStatus<H> {
        ManagerStatus::new(&self.task_queue)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dag::Request,
        dissemination::{task::Manager, DisseminationMessage},
        testing::gen_delay_config,
        units::{
            random_full_parent_reconstrusted_units_up_to, Unit, UnitCoord, UnitStore, WrappedUnit,
        },
        NodeCount, NodeIndex, Recipient,
    };
    use aleph_bft_mock::{Hasher64, Keychain};
    use std::thread::sleep;

    #[test]
    fn correct_tick_interval() {
        let node_ix = NodeIndex(7);
        let node_count = NodeCount(20);
        let delay_config = gen_delay_config();
        let manager: Manager<Hasher64> = Manager::new(node_ix, node_count, delay_config.clone());

        assert_eq!(manager.next_tick(), delay_config.tick_interval);
    }

    #[test]
    fn broadcasts_own_unit() {
        let node_ix = NodeIndex(7);
        let node_count = NodeCount(20);
        let delay_config = gen_delay_config();
        let mut manager = Manager::new(node_ix, node_count, delay_config.clone());

        let session_id = 43;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();

        let units =
            random_full_parent_reconstrusted_units_up_to(0, node_count, session_id, &keychains)
                .pop()
                .expect("just created initial round");
        let own_unit = units[node_ix.0].clone();

        let message = manager
            .add_unit(&own_unit)
            .expect("should immediately broadcast");
        assert_eq!(message.recipients(), &vec![Recipient::Everyone]);
        match message.message() {
            DisseminationMessage::Unit(unit) => {
                assert_eq!(unit, &own_unit.clone().unpack().into_unchecked())
            }
            m => panic!("Unexpected message: {:?}", m),
        }

        let mut store = UnitStore::new(node_count);
        let processing_store = UnitStore::new(node_count);
        store.insert(own_unit.clone());
        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
        sleep(delay_config.unit_rebroadcast_interval_max);
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(message.recipients(), &vec![Recipient::Everyone]);
        match message.message() {
            DisseminationMessage::Unit(unit) => {
                assert_eq!(unit, &own_unit.unpack().into_unchecked())
            }
            m => panic!("Unexpected message: {:?}", m),
        }
    }

    #[test]
    fn broadcasts_other_unit() {
        let node_ix = NodeIndex(7);
        let node_count = NodeCount(20);
        let delay_config = gen_delay_config();
        let mut manager = Manager::new(node_ix, node_count, delay_config.clone());

        let session_id = 43;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();

        let units =
            random_full_parent_reconstrusted_units_up_to(0, node_count, session_id, &keychains)
                .pop()
                .expect("just created initial round");
        let other_unit = units[0].clone();

        assert!(manager.add_unit(&other_unit).is_none());

        let mut store = UnitStore::new(node_count);
        let processing_store = UnitStore::new(node_count);
        store.insert(other_unit.clone());
        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
        sleep(delay_config.unit_rebroadcast_interval_max);
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(message.recipients(), &vec![Recipient::Everyone]);
        match message.message() {
            DisseminationMessage::Unit(unit) => {
                assert_eq!(unit, &other_unit.unpack().into_unchecked())
            }
            m => panic!("Unexpected message: {:?}", m),
        }
    }

    #[test]
    fn doesnt_broadcast_old_unit() {
        let node_ix = NodeIndex(7);
        let node_count = NodeCount(20);
        let delay_config = gen_delay_config();
        let mut manager = Manager::new(node_ix, node_count, delay_config.clone());

        let session_id = 43;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();

        let units =
            random_full_parent_reconstrusted_units_up_to(1, node_count, session_id, &keychains);
        let other_unit = units[0][0].clone();
        let other_unit_descendant = units[1][0].clone();

        assert!(manager.add_unit(&other_unit).is_none());
        assert!(manager.add_unit(&other_unit_descendant).is_none());

        let mut store = UnitStore::new(node_count);
        let processing_store = UnitStore::new(node_count);
        store.insert(other_unit.clone());
        store.insert(other_unit_descendant.clone());
        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
        sleep(delay_config.unit_rebroadcast_interval_max);
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(message.recipients(), &vec![Recipient::Everyone]);
        match message.message() {
            DisseminationMessage::Unit(unit) => {
                assert_eq!(unit, &other_unit_descendant.unpack().into_unchecked())
            }
            m => panic!("Unexpected message: {:?}", m),
        }
    }

    #[test]
    fn requests_coord() {
        let node_ix = NodeIndex(7);
        let node_count = NodeCount(20);
        let delay_config = gen_delay_config();
        let mut manager = Manager::new(node_ix, node_count, delay_config.clone());

        let session_id = 43;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();

        let units =
            random_full_parent_reconstrusted_units_up_to(0, node_count, session_id, &keychains)
                .pop()
                .expect("just created initial round");
        let coord = units[0].coord();
        let unit_to_make_typing_easier = units[1].clone();

        let mut store = UnitStore::new(node_count);
        store.insert(unit_to_make_typing_easier);
        let processing_store = UnitStore::new(node_count);
        manager.add_request(Request::Coord(coord));
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(
            message.recipients().len(),
            (delay_config.coord_request_recipients)(0)
        );
        match message.message() {
            DisseminationMessage::Request(requesting_node, request) => {
                assert_eq!(requesting_node, &node_ix);
                assert_eq!(request, &Request::Coord(coord));
            }
            m => panic!("Unexpected message: {:?}", m),
        }

        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
        sleep((delay_config.coord_request_delay)(0));
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(
            message.recipients().len(),
            (delay_config.coord_request_recipients)(1)
        );
        match message.message() {
            DisseminationMessage::Request(requesting_node, request) => {
                assert_eq!(requesting_node, &node_ix);
                assert_eq!(request, &Request::Coord(coord));
            }
            m => panic!("Unexpected message: {:?}", m),
        }
    }

    #[test]
    fn requests_ancient_coord_when_far_behind() {
        let node_ix = NodeIndex(7);
        let node_count = NodeCount(20);
        let delay_config = gen_delay_config();
        let mut manager = Manager::new(node_ix, node_count, delay_config.clone());

        let session_id = 43;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();

        let units =
            random_full_parent_reconstrusted_units_up_to(0, node_count, session_id, &keychains)
                .pop()
                .expect("just created initial round");
        let unit_to_make_typing_easier = units[1].clone();

        let mut store = UnitStore::new(node_count);
        store.insert(unit_to_make_typing_easier);
        let processing_store = UnitStore::new(node_count);
        manager.add_request(Request::Coord(UnitCoord::new(2137, NodeIndex(0))));
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(
            message.recipients().len(),
            (delay_config.coord_request_recipients)(0)
        );
        match message.message() {
            DisseminationMessage::Request(requesting_node, request) => {
                assert_eq!(requesting_node, &node_ix);
                match request {
                    Request::Coord(coord) => {
                        assert_eq!(coord.creator(), NodeIndex(0));
                        assert!(coord.round() < 100);
                    }
                    r => panic!("Unexpected request: {:?}", r),
                }
            }
            m => panic!("Unexpected message: {:?}", m),
        }

        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
        sleep((delay_config.coord_request_delay)(0));
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(
            message.recipients().len(),
            (delay_config.coord_request_recipients)(1)
        );
        match message.message() {
            DisseminationMessage::Request(requesting_node, request) => {
                assert_eq!(requesting_node, &node_ix);
                match request {
                    Request::Coord(coord) => {
                        assert_eq!(coord.creator(), NodeIndex(0));
                        assert!(coord.round() < 100);
                    }
                    r => panic!("Unexpected request: {:?}", r),
                }
            }
            m => panic!("Unexpected message: {:?}", m),
        }
    }

    #[test]
    fn stops_requesting_coord_when_has_unit() {
        let node_ix = NodeIndex(7);
        let node_count = NodeCount(20);
        let delay_config = gen_delay_config();
        let mut manager = Manager::new(node_ix, node_count, delay_config.clone());

        let session_id = 43;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();

        let units =
            random_full_parent_reconstrusted_units_up_to(0, node_count, session_id, &keychains)
                .pop()
                .expect("just created initial round");
        let unit = units[0].clone();
        let coord = unit.coord();
        let unit_to_make_typing_easier = units[1].clone();

        let mut store = UnitStore::new(node_count);
        store.insert(unit_to_make_typing_easier);
        let processing_store = UnitStore::new(node_count);
        manager.add_request(Request::Coord(coord));
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(
            message.recipients().len(),
            (delay_config.coord_request_recipients)(0)
        );
        match message.message() {
            DisseminationMessage::Request(requesting_node, request) => {
                assert_eq!(requesting_node, &node_ix);
                assert_eq!(request, &Request::Coord(coord));
            }
            m => panic!("Unexpected message: {:?}", m),
        }

        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
        sleep((delay_config.coord_request_delay)(0));
        store.insert(unit);
        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
    }

    #[test]
    fn requests_parents() {
        let node_ix = NodeIndex(7);
        let node_count = NodeCount(20);
        let delay_config = gen_delay_config();
        let mut manager = Manager::new(node_ix, node_count, delay_config.clone());

        let session_id = 43;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();

        let units =
            random_full_parent_reconstrusted_units_up_to(0, node_count, session_id, &keychains)
                .pop()
                .expect("just created initial round");
        let hash = units[0].hash();
        let unit_to_make_typing_easier = units[1].clone();

        let mut store = UnitStore::new(node_count);
        store.insert(unit_to_make_typing_easier);
        let processing_store = UnitStore::new(node_count);
        manager.add_request(Request::ParentsOf(hash));
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(
            message.recipients().len(),
            (delay_config.parent_request_recipients)(0)
        );
        match message.message() {
            DisseminationMessage::Request(requesting_node, request) => {
                assert_eq!(requesting_node, &node_ix);
                assert_eq!(request, &Request::ParentsOf(hash));
            }
            m => panic!("Unexpected message: {:?}", m),
        }

        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
        sleep((delay_config.parent_request_delay)(0));
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(
            message.recipients().len(),
            (delay_config.parent_request_recipients)(1)
        );
        match message.message() {
            DisseminationMessage::Request(requesting_node, request) => {
                assert_eq!(requesting_node, &node_ix);
                assert_eq!(request, &Request::ParentsOf(hash));
            }
            m => panic!("Unexpected message: {:?}", m),
        }
    }

    #[test]
    fn stops_requesting_parents_when_has_unit() {
        let node_ix = NodeIndex(7);
        let node_count = NodeCount(20);
        let delay_config = gen_delay_config();
        let mut manager = Manager::new(node_ix, node_count, delay_config.clone());

        let session_id = 43;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();

        let units =
            random_full_parent_reconstrusted_units_up_to(0, node_count, session_id, &keychains)
                .pop()
                .expect("just created initial round");
        let unit = units[0].clone();
        let hash = unit.hash();
        let unit_to_make_typing_easier = units[1].clone();

        let mut store = UnitStore::new(node_count);
        store.insert(unit_to_make_typing_easier);
        let processing_store = UnitStore::new(node_count);
        manager.add_request(Request::ParentsOf(hash));
        let mut messages = manager.trigger_tasks(&store, &processing_store);
        assert_eq!(messages.len(), 1);
        let message = messages.pop().expect("just checked");
        assert_eq!(
            message.recipients().len(),
            (delay_config.parent_request_recipients)(0)
        );
        match message.message() {
            DisseminationMessage::Request(requesting_node, request) => {
                assert_eq!(requesting_node, &node_ix);
                assert_eq!(request, &Request::ParentsOf(hash));
            }
            m => panic!("Unexpected message: {:?}", m),
        }

        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
        sleep((delay_config.parent_request_delay)(0));
        store.insert(unit);
        assert!(manager.trigger_tasks(&store, &processing_store).is_empty());
    }
}
