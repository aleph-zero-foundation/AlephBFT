use std::collections::{hash_map::Entry, BinaryHeap, HashMap, VecDeque};

use crate::{
    extender::ExtenderUnit,
    nodes::{NodeCount, NodeIndex, NodeMap},
    Environment, HashT, Message, Receiver, Round, Sender, Unit,
};
use log::{debug, error};
use std::cmp::Ordering;
use tokio::time;

pub const FETCH_INTERVAL: time::Duration = time::Duration::from_secs(4);
pub const BLOCK_AVAILABLE_INTERVAL: time::Duration = time::Duration::from_millis(50);
pub const TICK_INTERVAL: time::Duration = time::Duration::from_millis(10);

/// An enum describing the status of a Unit in the Terminal pipeline.
#[derive(Clone, Debug, PartialEq)]
pub enum UnitStatus {
    ReconstructingParents,
    WrongControlHash,
    WaitingParentsInDag,
    WaitingBlockAvailable,
    InDag,
}

impl Default for UnitStatus {
    fn default() -> UnitStatus {
        UnitStatus::ReconstructingParents
    }
}

/// A Unit struct used in the Terminal. It stores a copy of a unit and apart from that some
/// information on its status, i.e., already reconstructed parents etc.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TerminalUnit<B: HashT, H: HashT> {
    unit: Unit<B, H>,
    parents: NodeMap<Option<H>>,
    n_miss_par_store: NodeCount,
    n_miss_par_dag: NodeCount,
    status: UnitStatus,
}

impl<B: HashT, H: HashT> From<TerminalUnit<B, H>> for ExtenderUnit<B, H> {
    fn from(u: TerminalUnit<B, H>) -> ExtenderUnit<B, H> {
        ExtenderUnit::new(
            u.unit.creator,
            u.unit.round,
            u.unit.hash,
            u.parents,
            u.unit.best_block,
        )
    }
}

impl<B: HashT, H: HashT> From<TerminalUnit<B, H>> for Unit<B, H> {
    fn from(u: TerminalUnit<B, H>) -> Unit<B, H> {
        u.unit
    }
}

impl<B: HashT, H: HashT> TerminalUnit<B, H> {
    // creates a unit from a Control Hash Unit, that has no parents reconstructed yet
    pub(crate) fn blank_from_unit(unit: &Unit<B, H>) -> Self {
        let n_members = unit.control_hash.n_members();
        let n_parents = unit.control_hash.n_parents();
        TerminalUnit {
            unit: unit.clone(),
            parents: NodeMap::new_with_len(n_members),
            n_miss_par_store: n_parents,
            n_miss_par_dag: n_parents,
            status: UnitStatus::ReconstructingParents,
        }
    }
    pub(crate) fn creator(&self) -> NodeIndex {
        self.unit.creator
    }

    pub(crate) fn round(&self) -> Round {
        self.unit.round
    }

    pub(crate) fn verify_control_hash(&self) -> bool {
        // this will be called only after all parents have been reconstructed
        // TODO: implement this
        true
    }
}

// This enum could be simplified to just one option and consequently gotten rid off. However,
// this has been made slightly more general (and thus complex) to add Alerts easily in the future.
pub enum TerminalEvent<H: HashT> {
    ParentsReconstructed(H),
    ReadyForAvailabilityCheck(H),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Task {
    CheckBlockAvailable,
    RequestParents,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ScheduledTask<H: HashT> {
    unit: H,
    scheduled_time: time::Instant,
    task: Task,
}

impl<H: HashT> ScheduledTask<H> {
    fn new(unit: H, scheduled_time: time::Instant, task: Task) -> Self {
        ScheduledTask {
            unit,
            scheduled_time,
            task,
        }
    }
}

impl<H: HashT> Ord for ScheduledTask<H> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we want earlier times to come first when used in max-heap, hence the below:
        other.scheduled_time.cmp(&self.scheduled_time)
    }
}

impl<H: HashT> PartialOrd for ScheduledTask<H> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A process whose goal is to receive new units and place them in our local Dag. Towards this end
/// this process must orchestrate fetching units from other nodes in case we are missing them and
/// manage the `Alert` mechanism which makes sure `horizontal spam` (unit fork spam) is not possible.
/// Importantly, our local Dag is a set of units that are *guaranteed* to be sooner or later
/// received by all honest nodes in the network.
/// The Terminal receives new units via the new_units_rx channel endpoint and pushes requests for units
/// to the requests_tx channel endpoint.
pub(crate) struct Terminal<E: Environment + 'static> {
    _ix: NodeIndex,
    // A channel for receiving new units (they might also come from the local node).
    new_units_rx: Receiver<Unit<E::BlockHash, E::Hash>>,
    // A channel to push unit requests.
    requests_tx: Sender<Message<E::BlockHash, E::Hash>>,
    // An oracle for checking if a given block hash h is available. This is needed to check for a new unit
    // U whether the block it carries is available, and hence the unit U should be accepted (or held aside
    // till the block is available).
    check_available: Box<dyn Fn(E::BlockHash) -> bool + Sync + Send + 'static>,
    // A Queue that is necessary to deal with cascading updates to the Dag/Store
    event_queue: VecDeque<TerminalEvent<E::Hash>>,
    // This queue contains "notes" that remind us to check on a particular unit, whether it already
    // has all the parents. If not then repeat a request.
    scheduled_task_queue: BinaryHeap<ScheduledTask<E::Hash>>,
    post_insert: Vec<Box<dyn Fn(TerminalUnit<E::BlockHash, E::Hash>) + Send + Sync + 'static>>,
    // Here we store all the units -- the one in Dag and the ones "hanging".
    unit_store: HashMap<E::Hash, TerminalUnit<E::BlockHash, E::Hash>>,

    // TODO: get rid of HashMaps below and just use Vec<Vec<E::Hash>> for efficiency

    // In this Map, for each pair (r, pid) we store the first unit made by pid at round r that we ever received.
    // In case of forks, we still store only the first one -- others are ignored (but stored in store anyway of course).
    unit_by_coord: HashMap<(Round, NodeIndex), E::Hash>,
    // This stores, for a pair (r, pid) the list of all units (by hash) that await a unit made by pid at
    // round r, as their parent. Once such a unit arrives, we notify all these children.
    children_coord: HashMap<(Round, NodeIndex), Vec<E::Hash>>,
    // The same as above, but this time we await for a unit (with a particular hash) to be added to the Dag.
    // Once this happens, we notify all the children.
    children_hash: HashMap<E::Hash, Vec<E::Hash>>,
    // This is a ticker that is useful in forcing to fire "poll" at least every TICK_INTERVAL.
    request_ticker: time::Interval,
}

impl<E: Environment + 'static> Terminal<E> {
    pub(crate) fn new(
        _ix: NodeIndex,
        new_units_rx: Receiver<Unit<E::BlockHash, E::Hash>>,
        requests_tx: Sender<Message<E::BlockHash, E::Hash>>,
        check_available: Box<dyn Fn(E::BlockHash) -> bool + Sync + Send + 'static>,
    ) -> Self {
        Terminal {
            _ix,
            new_units_rx,
            requests_tx,
            check_available,
            event_queue: VecDeque::new(),
            scheduled_task_queue: BinaryHeap::new(),
            post_insert: Vec::new(),
            unit_store: HashMap::new(),
            unit_by_coord: HashMap::new(),
            children_coord: HashMap::new(),
            children_hash: HashMap::new(),
            request_ticker: time::interval(TICK_INTERVAL),
        }
    }

    // Reconstruct the parent of a unit u (given by hash u_hash) at position pid as p (given by hash p_hash)
    fn reconstruct_parent(&mut self, u_hash: &E::Hash, pid: NodeIndex, p_hash: &E::Hash) {
        let p_status = self.unit_store.get(p_hash).unwrap().status.clone();
        let u = self.unit_store.get_mut(u_hash).unwrap();
        // the above unwraps must succeed, should probably add some debug messages here...

        u.parents[pid] = Some(*p_hash);
        u.n_miss_par_store -= NodeCount(1);
        if u.n_miss_par_store == NodeCount(0) {
            u.status = UnitStatus::WaitingParentsInDag;
            self.event_queue
                .push_back(TerminalEvent::ParentsReconstructed(*u_hash));
        }
        if p_status == UnitStatus::InDag {
            u.n_miss_par_dag -= NodeCount(1);
        } else {
            self.add_hash_trigger(p_hash, u_hash);
        }
    }

    // update u's count (u given by hash u_hash) of parents present in Dag and trigger the event of
    // adding u itself to the Dag, if all parents present and control hash verifies
    fn new_parent_in_dag(&mut self, u_hash: &E::Hash) {
        let u = self.unit_store.get_mut(u_hash).unwrap();
        u.n_miss_par_dag -= NodeCount(1);
        if u.n_miss_par_store == NodeCount(0) {
            self.event_queue
                .push_back(TerminalEvent::ParentsReconstructed(*u_hash));
        }
    }

    fn add_coord_trigger(&mut self, round: Round, pid: NodeIndex, u_hash: E::Hash) {
        let coord = (round, pid);
        if !self.children_coord.contains_key(&coord) {
            self.children_coord.insert((round, pid), Vec::new());
        }
        let wait_list = self.children_coord.get_mut(&coord).unwrap();
        wait_list.push(u_hash);
    }

    fn add_hash_trigger(&mut self, p_hash: &E::Hash, u_hash: &E::Hash) {
        if !self.children_hash.contains_key(p_hash) {
            self.children_hash.insert(*p_hash, Vec::new());
        }
        let wait_list = self.children_hash.get_mut(p_hash).unwrap();
        wait_list.push(*u_hash);
    }

    fn update_on_store_add(&mut self, u: Unit<E::BlockHash, E::Hash>) {
        let u_hash = u.hash;
        let (u_round, pid) = (u.round, u.creator);
        //TODO: should check for forks at this very place and possibly trigger Alarm
        self.unit_by_coord.insert((u_round, pid), u_hash);
        if let Some(children) = self.children_coord.remove(&(u_round, pid)) {
            for v_hash in children {
                self.reconstruct_parent(&v_hash, pid, &u_hash);
            }
        }
        if u_round == 0 {
            self.event_queue
                .push_back(TerminalEvent::ParentsReconstructed(u_hash));
        } else {
            for (i, b) in u.control_hash.parents.enumerate() {
                if *b {
                    let coord = (u_round - 1, i);
                    let maybe_hash = self.unit_by_coord.get(&coord).cloned();
                    match maybe_hash {
                        Some(v_hash) => self.reconstruct_parent(&u_hash, i, &v_hash),
                        None => {
                            self.add_coord_trigger(u_round - 1, i, u_hash);
                        }
                    }
                }
            }
        }
    }

    fn update_on_dag_add(&mut self, u_hash: &E::Hash) {
        if let Some(children) = self.children_hash.remove(u_hash) {
            for v_hash in children {
                self.new_parent_in_dag(&v_hash);
            }
        }
        let u = self.unit_store.get(u_hash).unwrap();
        self.post_insert.iter().for_each(|f| f(u.clone()));
    }

    fn add_to_store(&mut self, u: Unit<E::BlockHash, E::Hash>) {
        debug!(target: "rush-terminal", "Adding a unit {:?} to store", u.hash());
        if let Entry::Vacant(entry) = self.unit_store.entry(u.hash()) {
            entry.insert(TerminalUnit::<E::BlockHash, E::Hash>::blank_from_unit(&u));
            let curr_time = time::Instant::now();
            self.scheduled_task_queue.push(ScheduledTask::new(
                u.hash(),
                curr_time,
                Task::RequestParents,
            ));
            self.update_on_store_add(u);
        }
    }

    fn check_availability(&mut self, u_hash: &E::Hash) {
        let u = self.unit_store.get_mut(u_hash).unwrap();
        if (self.check_available)(u.unit.best_block) {
            u.status = UnitStatus::InDag;
            debug!(target: "rush-terminal", "Adding unit {:?} to Dag.", u_hash);
            self.update_on_dag_add(u_hash);
        } else {
            debug!(target: "rush-terminal", "Block in unit {:?} not available.", u_hash);
            u.status = UnitStatus::WaitingBlockAvailable;
            let curr_time = time::Instant::now();
            self.scheduled_task_queue.push(ScheduledTask::new(
                *u_hash,
                curr_time + BLOCK_AVAILABLE_INTERVAL,
                Task::CheckBlockAvailable,
            ));
        }
    }

    // This drains the event queue. Note that new events might be added to the queue as the result of
    // handling other events -- for instance when a unit u is waiting for its parent p, and this parent p waits
    // for his parent pp. In this case adding pp to the Dag, will trigger adding p, which in turns triggers
    // adding u.
    fn handle_events(&mut self) {
        while let Some(event) = self.event_queue.pop_front() {
            match event {
                TerminalEvent::ParentsReconstructed(u_hash) => {
                    let u = self.unit_store.get_mut(&u_hash).unwrap();
                    if u.verify_control_hash() {
                        self.event_queue
                            .push_back(TerminalEvent::ReadyForAvailabilityCheck(u_hash));
                    } else {
                        u.status = UnitStatus::WrongControlHash;
                        // TODO: should trigger some immediate action here
                    }
                }
                TerminalEvent::ReadyForAvailabilityCheck(u_hash) => {
                    self.check_availability(&u_hash);
                }
            }
        }
    }

    fn process_scheduled_tasks(&mut self) {
        let curr_time = time::Instant::now();
        while !self.scheduled_task_queue.is_empty() {
            if self.scheduled_task_queue.peek().unwrap().scheduled_time > curr_time {
                // if there is a more rustic version of this loop control -- let me know :)
                break;
            }
            let note = self.scheduled_task_queue.pop().unwrap();
            let u = self.unit_store.get(&note.unit).unwrap();
            match note.task {
                Task::RequestParents => {
                    if u.status == UnitStatus::ReconstructingParents {
                        // This means there are some unavailable parents for this unit...
                        for (i, b) in u.unit.control_hash.parents.enumerate() {
                            if *b && u.parents[i].is_none() {
                                let send_result = self
                                    .requests_tx
                                    .send(Message::FetchRequest(vec![(u.round() - 1, i)], i));
                                if let Err(e) = send_result {
                                    error!(target: "rush-terminal", "Unable to place a Fetch request: {:?}.", e);
                                }
                            }
                        }
                        // We might not be done, so we schedule the same task after FETCH_INTERVAL seconds.
                        self.scheduled_task_queue.push(ScheduledTask::new(
                            note.unit,
                            curr_time + FETCH_INTERVAL,
                            Task::RequestParents,
                        ));
                    }
                }
                Task::CheckBlockAvailable => {
                    if u.status == UnitStatus::WaitingBlockAvailable {
                        if (self.check_available)(u.unit.best_block) {
                            self.event_queue
                                .push_back(TerminalEvent::ReadyForAvailabilityCheck(note.unit));
                        } else {
                            // The block is not there yet, we check again after BLOCK_AVAILABLE_INTERVAL seconds.
                            self.scheduled_task_queue.push(ScheduledTask::new(
                                note.unit,
                                curr_time + BLOCK_AVAILABLE_INTERVAL,
                                Task::CheckBlockAvailable,
                            ));
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn register_post_insert_hook(
        &mut self,
        hook: Box<dyn Fn(TerminalUnit<E::BlockHash, E::Hash>) + Send + Sync + 'static>,
    ) {
        self.post_insert.push(hook);
    }

    pub(crate) async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(u) = self.new_units_rx.recv() => {
                    self.add_to_store(u);

                }
                _ = self.request_ticker.tick() =>{},
            };
            self.process_scheduled_tasks();
            self.handle_events();
        }
    }
}
