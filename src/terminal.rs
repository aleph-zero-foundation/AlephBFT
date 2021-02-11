use std::collections::{hash_map::Entry, HashMap, VecDeque};

use crate::{
    dag::Vertex,
    nodes::{NodeCount, NodeIndex, NodeMap},
    skeleton::{Message, Receiver, Sender, Unit},
    traits::{Environment, HashT},
};
use tokio::time;

pub const REPEAT_INTERVAL: time::Duration = time::Duration::from_secs(4);
pub const TICK_INTERVAL: time::Duration = time::Duration::from_millis(100);

#[derive(Clone, Debug, PartialEq)]
pub enum UnitStatus {
    ReconstructingParents,
    WrongControlHash,
    WaitingParentsInDag,
    InDag,
}

impl Default for UnitStatus {
    fn default() -> UnitStatus {
        UnitStatus::ReconstructingParents
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TerminalUnit<H: HashT> {
    unit: Unit<H>,
    parents: NodeMap<Option<H>>,
    n_miss_par_store: NodeCount,
    n_miss_par_dag: NodeCount,
    status: UnitStatus,
}

impl<H: HashT> From<TerminalUnit<H>> for Vertex<H> {
    fn from(u: TerminalUnit<H>) -> Vertex<H> {
        Vertex::new(u.unit.creator, u.unit.hash, u.parents, u.unit.best_block)
    }
}

impl<H: HashT> From<TerminalUnit<H>> for Unit<H> {
    fn from(u: TerminalUnit<H>) -> Unit<H> {
        u.unit
    }
}

impl<H: HashT> TerminalUnit<H> {
    // creates a unit from a Control Hash Unit, that has no parents reconstructed yet
    pub(crate) fn blank_from_unit(unit: &Unit<H>) -> Self {
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

    pub(crate) fn round(&self) -> u32 {
        self.unit.round
    }

    pub(crate) fn verify_control_hash(&self) -> bool {
        // this will be called only after all parents have been reconstructed
        // TODO: implement this
        true
    }
}

/// This enum could be simplified to just one option and consequently gotten rid off. However,
/// this has been made slightly more general (and thus complex) to add Alerts easily in the future.
pub enum TerminalEvent<H: HashT> {
    ParentsReconstructed(H),
    ReadyForDag(H),
}

struct RequestNote<H: HashT> {
    unit: H,
    scheduled_time: time::Instant,
}

impl<H: HashT> RequestNote<H> {
    fn new(unit: H, scheduled_time: time::Instant) -> Self {
        RequestNote {
            unit,
            scheduled_time,
        }
    }
}

// Terminal is responsible for:
// - managing units that cannot be added to the dag yet, i.e fetching missing parents
// - checking control hashes
// - TODO checking for potential forks and raising alarms
// - TODO updating randomness source
pub(crate) struct Terminal<E: Environment + 'static> {
    _ix: NodeIndex,
    // common channel for units from outside world and the ones we create
    new_units_rx: Receiver<Unit<E::Hash>>,
    requests_tx: Sender<Message<E::Hash>>,
    // Queue that is necessary to deal with cascading updates to the Dag/Store
    event_queue: VecDeque<TerminalEvent<E::Hash>>,
    // This queue contains "notes" that remind us to check on a particular unit, whether it already
    // has all the parents. If not then repeat a request.
    note_queue: VecDeque<RequestNote<E::Hash>>,
    post_insert: Vec<Box<dyn Fn(TerminalUnit<E::Hash>) + Send + Sync + 'static>>,
    // Here we store all the units -- the one in Dag and the ones "hanging".
    unit_store: HashMap<E::Hash, TerminalUnit<E::Hash>>,

    // TODO: custom type for round number?
    // TODO: get rid of HashMaps below and just use Vec<Vec<E::Hash>> for efficiency

    // In this Map, for each pair (r, pid) we store the first unit made by pid at round r that we ever received.
    // In case of forks, we still store only the first one -- others are ignored (but stored in store anyway of course).
    unit_by_coord: HashMap<(u32, NodeIndex), E::Hash>,
    // This stores, for a pair (r, pid) the list of all units (by hash) that await a unit made by pid at
    // round r, as their parent. Once such a unit arrives, we notify all these children.
    children_coord: HashMap<(u32, NodeIndex), Vec<E::Hash>>,
    // The same as above, but this time we await for a unit (with a particular hash) to be added to the Dag.
    // Once this happens, we notify all the children.
    children_hash: HashMap<E::Hash, Vec<E::Hash>>,
    /// This is a ticker that is useful in forcing to fire "poll" at least every 100ms.
    request_ticker: time::Interval,
}

impl<E: Environment + 'static> Terminal<E> {
    pub(crate) fn new(
        _ix: NodeIndex,
        new_units_rx: Receiver<Unit<E::Hash>>,
        requests_tx: Sender<Message<E::Hash>>,
    ) -> Self {
        Terminal {
            _ix,
            new_units_rx,
            requests_tx,
            event_queue: VecDeque::new(),
            note_queue: VecDeque::new(),
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

    fn add_coord_trigger(&mut self, round: u32, pid: NodeIndex, u_hash: E::Hash) {
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

    fn update_on_store_add(&mut self, u: Unit<E::Hash>) {
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

    fn add_to_store(&mut self, u: Unit<E::Hash>) {
        if let Entry::Vacant(entry) = self.unit_store.entry(u.hash()) {
            entry.insert(TerminalUnit::<E::Hash>::blank_from_unit(&u));
            // below we push to the front of the queue because we want these requests (if applicable) to be
            // performed right away. This is perhaps not super clean, but the invariant of events in queue
            // being sorted by time is still (pretty much) preserved.
            let curr_time = time::Instant::now();
            self.note_queue
                .push_front(RequestNote::new(u.hash(), curr_time));
            self.update_on_store_add(u);
        }
    }

    fn add_to_dag(&mut self, u_hash: &E::Hash) {
        let u = self.unit_store.get_mut(u_hash).unwrap();
        u.status = UnitStatus::InDag;
        self.update_on_dag_add(u_hash);
    }

    /// This drains the event queue. Note that new events might be added to the queue as the result of
    /// handling other events -- for instance when a unit u is waiting for its parent p, and this parent p waits
    /// for his parent pp. In this case adding pp to the Dag, will trigger adding p, which in turns triggers
    /// adding u.
    fn handle_events(&mut self) {
        while let Some(event) = self.event_queue.pop_front() {
            match event {
                TerminalEvent::ParentsReconstructed(u_hash) => {
                    let u = self.unit_store.get_mut(&u_hash).unwrap();
                    if u.verify_control_hash() {
                        self.event_queue
                            .push_back(TerminalEvent::ReadyForDag(u_hash));
                    } else {
                        u.status = UnitStatus::WrongControlHash;
                        // might trigger some immediate action here, but it is not necessary as each "hanging" unit is periodically
                        // checked and appropriate fetch requests are made.
                    }
                }
                TerminalEvent::ReadyForDag(u_hash) => self.add_to_dag(&u_hash),
            }
        }
    }

    fn process_hanging_units(&mut self) {
        let curr_time = time::Instant::now();
        while !self.note_queue.is_empty() {
            if self.note_queue.front().unwrap().scheduled_time > curr_time {
                // if there is a more rustic version of this loop control -- let me know :)
                break;
            }
            let note = self.note_queue.pop_front().unwrap();
            let u = self.unit_store.get(&note.unit).unwrap();
            if u.round() == 0 {
                continue;
            }
            match u.status {
                UnitStatus::ReconstructingParents => {
                    // This means there are some unavailable parents for this units...
                    for (i, b) in u.unit.control_hash.parents.enumerate() {
                        if *b && u.parents[i].is_none() {
                            let _ = self
                                .requests_tx
                                .send(Message::FetchRequest(vec![(u.round() - 1, i)], i));
                        }
                    }
                    // The line below adds a note to the queue that we should wake up and look at this unit again
                    // after REPEAT_INTERVAL=4seconds.
                    self.note_queue
                        .push_back(RequestNote::new(note.unit, curr_time + REPEAT_INTERVAL));
                }
                UnitStatus::WaitingParentsInDag => {
                    // We are done here, all parents are in the store
                    continue;
                }
                UnitStatus::InDag => {
                    // We are done here, the unit is in Dag
                    continue;
                }
                UnitStatus::WrongControlHash => {
                    // TODO: need to introduce a new type of requests for this, and then write logic
                    // Should also add a note to the queue (as the status means the issue not resolved yet)
                }
            }
        }
    }

    pub(crate) fn register_post_insert_hook(
        &mut self,
        hook: Box<dyn Fn(TerminalUnit<E::Hash>) + Send + Sync + 'static>,
    ) {
        self.post_insert.push(hook);
    }

    pub(crate) async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(u) = self.new_units_rx.recv() => {
                    self.add_to_store(u);
                    self.handle_events();
                }
                _ = self.request_ticker.tick() =>
                    self.process_hanging_units(),
            };
        }
    }
}
