use std::collections::{hash_map::Entry, HashMap, VecDeque};

use crate::{
    extender::ExtenderUnit,
    nodes::{NodeCount, NodeIndex, NodeMap},
    ControlHash, Environment, HashT, Hashing, NotificationOut, Receiver, RequestAuxData, Round,
    Sender, Unit,
};
use log::{debug, error};
use std::future::Future;

/// An enum describing the status of a Unit in the Terminal pipeline.
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
            u.unit.round(),
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

    pub(crate) fn _round(&self) -> Round {
        self.unit.round()
    }

    pub(crate) fn _hash(&self) -> H {
        self.unit.hash
    }

    pub(crate) fn verify_control_hash<Hashing: Fn(&[u8]) -> H>(&self, hashing: &Hashing) -> bool {
        // this will be called only after all parents have been reconstructed

        self.unit.control_hash.hash == ControlHash::combine_hashes(&self.parents, hashing)
    }
}

// This enum could be simplified to just one option and consequently gotten rid off. However,
// this has been made slightly more general (and thus complex) to add Alerts easily in the future.
pub enum TerminalEvent<H: HashT> {
    ParentsReconstructed(H),
    ParentsInDag(H),
    BlockAvailable(H),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Task<H: HashT> {
    BlockAvailable(H),
}

type SyncClosure<X, Y> = Box<dyn Fn(X) -> Y + Sync + Send + 'static>;

/// A process whose goal is to receive new units and place them in our local Dag. Towards this end
/// this process must orchestrate fetching units from other nodes in case we are missing them and
/// manage the `Alert` mechanism which makes sure `horizontal spam` (unit fork spam) is not possible.
/// Importantly, our local Dag is a set of units that are *guaranteed* to be sooner or later
/// received by all honest nodes in the network.
/// The Terminal receives new units via the new_units_rx channel endpoint and pushes requests for units
/// to the requests_tx channel endpoint.
pub(crate) struct Terminal<E: Environment + 'static> {
    node_id: E::NodeId,
    // A channel for receiving new units (they might also come from the local node).
    new_units_rx: Receiver<Unit<E::BlockHash, E::Hash>>,
    // A channel to push unit requests.
    requests_tx: Sender<NotificationOut<E::BlockHash, E::Hash>>,
    // An oracle for checking if a given block hash h is available. This is needed to check for a new unit
    // U whether the block it carries is available, and hence the unit U should be accepted (or held aside
    // till the block is available).
    check_available: SyncClosure<
        E::BlockHash,
        Box<dyn Future<Output = Result<(), E::Error>> + Send + Sync + Unpin>,
    >,
    // A Queue that is necessary to deal with cascading updates to the Dag/Store
    event_queue: VecDeque<TerminalEvent<E::Hash>>,
    task_queue: Receiver<Task<E::Hash>>,
    task_notifier: Sender<Task<E::Hash>>,
    post_insert: Vec<SyncClosure<TerminalUnit<E::BlockHash, E::Hash>, ()>>,
    // Here we store all the units -- the one in Dag and the ones "hanging".
    unit_store: HashMap<E::Hash, TerminalUnit<E::BlockHash, E::Hash>>,
    // Hashing function
    hashing: Hashing<E::Hash>,

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
}

impl<E: Environment + 'static> Terminal<E> {
    pub(crate) fn new(
        node_id: E::NodeId,
        new_units_rx: Receiver<Unit<E::BlockHash, E::Hash>>,
        requests_tx: Sender<NotificationOut<E::BlockHash, E::Hash>>,
        check_available: SyncClosure<
            E::BlockHash,
            Box<dyn Future<Output = Result<(), E::Error>> + Send + Sync + Unpin>,
        >,
        hashing: Hashing<E::Hash>,
    ) -> Self {
        let (task_notifier, task_queue) = tokio::sync::mpsc::unbounded_channel();
        Terminal {
            node_id,
            new_units_rx,
            requests_tx,
            check_available,
            event_queue: VecDeque::new(),
            task_queue,
            task_notifier,
            post_insert: Vec::new(),
            unit_store: HashMap::new(),
            hashing,
            unit_by_coord: HashMap::new(),
            children_coord: HashMap::new(),
            children_hash: HashMap::new(),
        }
    }

    // Reconstruct the parent of a unit u (given by hash u_hash) at position pid as p (given by hash p_hash)
    fn reconstruct_parent(&mut self, u_hash: &E::Hash, pid: NodeIndex, p_hash: &E::Hash) {
        let u = self.unit_store.get_mut(u_hash).unwrap();
        // the above unwraps must succeed, should probably add some debug messages here...

        u.parents[pid] = Some(*p_hash);
        u.n_miss_par_store -= NodeCount(1);
        if u.n_miss_par_store == NodeCount(0) {
            self.event_queue
                .push_back(TerminalEvent::ParentsReconstructed(*u_hash));
        }
    }

    // update u's count (u given by hash u_hash) of parents present in Dag and trigger the event of
    // adding u itself to the Dag
    fn new_parent_in_dag(&mut self, u_hash: &E::Hash) {
        let u = self.unit_store.get_mut(u_hash).unwrap();
        u.n_miss_par_dag -= NodeCount(1);
        if u.n_miss_par_dag == NodeCount(0) {
            self.event_queue
                .push_back(TerminalEvent::ParentsInDag(*u_hash));
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
        let (u_round, pid) = (u.round(), u.creator);
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
            let mut coords_to_request = Vec::new();
            for (i, b) in u.control_hash.parents.enumerate() {
                if *b {
                    let coord = (u_round - 1, i);
                    let maybe_hash = self.unit_by_coord.get(&coord).cloned();
                    match maybe_hash {
                        Some(v_hash) => self.reconstruct_parent(&u_hash, i, &v_hash),
                        None => {
                            self.add_coord_trigger(u_round - 1, i, u_hash);
                            coords_to_request.push((u.round() - 1, i));
                        }
                    }
                }
            }
            if !coords_to_request.is_empty() {
                let aux_data = RequestAuxData::new(u.creator);
                debug!(target: "rush-terminal", "{} Missing coords {:?} aux {:?}", self.node_id, coords_to_request, aux_data);
                let send_result = self
                    .requests_tx
                    .send(NotificationOut::MissingUnits(coords_to_request, aux_data));
                if let Err(e) = send_result {
                    error!(target: "rush-terminal", "{} Unable to place a Fetch request: {:?}.", self.node_id, e);
                }
            }
        }
    }

    fn update_on_dag_add(&mut self, u_hash: &E::Hash) {
        let u = self.unit_store.get(u_hash).unwrap();
        self.post_insert.iter().for_each(|f| f(u.clone()));
        if let Some(children) = self.children_hash.remove(u_hash) {
            for v_hash in children {
                self.new_parent_in_dag(&v_hash);
            }
        }
    }

    fn add_to_store(&mut self, u: Unit<E::BlockHash, E::Hash>) {
        debug!(target: "rush-terminal", "{} Adding to store {:?} r {:?} ix {:?}", self.node_id, u.hash(), u.round, u.creator);
        if let Entry::Vacant(entry) = self.unit_store.entry(u.hash()) {
            entry.insert(TerminalUnit::<E::BlockHash, E::Hash>::blank_from_unit(&u));
            self.update_on_store_add(u);
        }
    }

    fn inspect_parents_in_dag(&mut self, u_hash: &E::Hash) {
        let u_parents = self.unit_store.get(&u_hash).unwrap().parents.clone();
        let mut n_parents_in_dag = NodeCount(0);
        for p_hash in u_parents.into_iter().flatten() {
            let p = self.unit_store.get(&p_hash).unwrap();
            if p.status == UnitStatus::InDag {
                n_parents_in_dag += NodeCount(1);
            } else {
                self.add_hash_trigger(&p_hash, u_hash);
            }
        }
        let u = self.unit_store.get_mut(&u_hash).unwrap();
        u.n_miss_par_dag -= n_parents_in_dag;
        if u.n_miss_par_dag == NodeCount(0) {
            self.event_queue
                .push_back(TerminalEvent::ParentsInDag(*u_hash));
        } else {
            u.status = UnitStatus::WaitingParentsInDag;
            // when the last parent is added an appropriate TerminalEvent will be triggered
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
                    if u.verify_control_hash(&self.hashing) {
                        self.inspect_parents_in_dag(&u_hash);
                    } else {
                        u.status = UnitStatus::WrongControlHash;
                        debug!(target: "rush-terminal", "{} wrong control hash", self.node_id);
                        // TODO: should trigger some immediate action here
                    }
                }
                TerminalEvent::BlockAvailable(u_hash) => {
                    let u = self.unit_store.get_mut(&u_hash).unwrap();
                    u.status = UnitStatus::InDag;
                    debug!(target: "rush-terminal", "{} Adding to Dag {:?} r {:?} ix {:?}.", self.node_id, u_hash, u.unit.round, u.unit.creator);
                    self.update_on_dag_add(&u_hash);
                }
                TerminalEvent::ParentsInDag(u_hash) => {
                    let block = self.unit_store.get(&u_hash).unwrap().unit.best_block;
                    let available = (self.check_available)(block);
                    let notify = self.task_notifier.clone();
                    // TODO this is needed for debugging, try to remove the clone later
                    let node_id = self.node_id.clone();
                    let _ = tokio::spawn(async move {
                        if available.await.is_ok() {
                            if let Err(e) = notify.send(Task::BlockAvailable(u_hash)) {
                                debug!(target: "rush-terminal", "{} Error while sending BlockAvailable notification {}", node_id, e);
                            }
                        } else {
                            debug!(target: "rush-terminal", "{} Error while checking block availability for unit {:?}", node_id, u_hash);
                        }
                    });
                }
            }
        }
    }

    fn process_task(&mut self, task: Task<E::Hash>) {
        match task {
            Task::BlockAvailable(u_hash) => self
                .event_queue
                .push_back(TerminalEvent::BlockAvailable(u_hash)),
        }
    }

    pub(crate) fn register_post_insert_hook(
        &mut self,
        hook: SyncClosure<TerminalUnit<E::BlockHash, E::Hash>, ()>,
    ) {
        self.post_insert.push(hook);
    }

    pub(crate) async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(u) = self.new_units_rx.recv() => {
                    self.add_to_store(u);
                },
                Some(task) = self.task_queue.recv() =>{
                    self.process_task(task);
                },
            }
            self.handle_events();
        }
    }
}
