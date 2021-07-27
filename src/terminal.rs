use futures::{channel::oneshot, StreamExt};
use std::collections::{hash_map::Entry, HashMap, VecDeque};

use crate::{
    extender::ExtenderUnit,
    member::{NotificationIn, NotificationOut},
    nodes::{NodeCount, NodeIndex, NodeMap},
    units::{ControlHash, Unit, UnitCoord},
    Hasher, Receiver, Round, Sender,
};
use log::{debug, info, trace, warn};

/// An enum describing the status of a Unit in the Terminal pipeline.
#[derive(Clone, PartialEq)]
pub enum UnitStatus {
    ReconstructingParents,
    WrongControlHash,
    WaitingParentsInDag,
    InDag,
}

/// A Unit struct used in the Terminal. It stores a copy of a unit and apart from that some
/// information on its status, i.e., already reconstructed parents etc.
#[derive(Clone, PartialEq)]
pub struct TerminalUnit<H: Hasher> {
    unit: Unit<H>,
    // This represents the knowledge of what we think the parents of the unit are. It is initialized as all None
    // and hashes of parents are being added at the time we receive a given coord. Once the parents map is complete
    // we test whether the hash of parents agains the control_hash in the unit. If the check fails, we request hashes
    // of parents of this unit via a NotificationOut.
    parents: NodeMap<Option<H::Hash>>,
    n_miss_par_decoded: NodeCount,
    n_miss_par_dag: NodeCount,
    status: UnitStatus,
}

impl<H: Hasher> From<TerminalUnit<H>> for ExtenderUnit<H> {
    fn from(u: TerminalUnit<H>) -> ExtenderUnit<H> {
        ExtenderUnit::new(u.unit.creator(), u.unit.round(), u.unit.hash(), u.parents)
    }
}

impl<H: Hasher> From<TerminalUnit<H>> for Unit<H> {
    fn from(u: TerminalUnit<H>) -> Unit<H> {
        u.unit
    }
}

impl<H: Hasher> TerminalUnit<H> {
    // creates a unit from a Control Hash Unit, that has no parents reconstructed yet
    pub(crate) fn blank_from_unit(unit: &Unit<H>) -> Self {
        let n_members = unit.control_hash().n_members();
        let n_parents = unit.control_hash().n_parents();
        TerminalUnit {
            unit: unit.clone(),
            parents: NodeMap::new_with_len(n_members),
            n_miss_par_decoded: n_parents,
            n_miss_par_dag: n_parents,
            status: UnitStatus::ReconstructingParents,
        }
    }

    pub(crate) fn verify_control_hash(&self) -> bool {
        // this will be called only after all parents have been reconstructed

        self.unit.control_hash().combined_hash == ControlHash::<H>::combine_hashes(&self.parents)
    }
}

pub enum TerminalEvent<H: Hasher> {
    ParentsReconstructed(H::Hash),
    ParentsInDag(H::Hash),
}

type SyncClosure<X, Y> = Box<dyn Fn(X) -> Y + Sync + Send + 'static>;

/// A process whose goal is to receive new units and place them in our local Dag.
/// Importantly, our local Dag is a set of units that are *guaranteed* to be sooner or later
/// received by all honest nodes in the network.
/// The Terminal receives notifications from the ntfct_rx channel endpoint and pushes requests for units
/// and other notifications to the ntfct_tx channel endpoint.
/// The path of a single unit u through the Terminal is as follows:
/// 1) It is added to the unit_store.
/// 2) For each parent coord (r, ix) if we have a unit v with such a coord in store then we "reconstruct" this parent
///    by placing Some(v_hash), where v_hash is the hash of v in parents[ix] in the TerminalUnit object for u.
/// 3) For each parent coord (r, ix) that is missing in the store, we request this unit by sending a
///    suitable NotificationOut. Subsequently we wait for these units, whenever a unit of one of the
///    missing coords is received, the parents field of the TerminalUnit object of u is updated.
/// 4) At the moment when all parents have been reconstructed (which happens right away for round-0 units
///    that have no parents) an appropriate Event is generated and we check whether the reconstructed parents
///    list after hashing agrees with the control_hash in the unit.
///    a) If yes, then the unit u gets status WaitingParentsInDag
///    b) If no, then the unit gets status WrongControlHash and a notification is sent, requesting the list of parent
///       hashes of u. At the moment when a response to this notification is received (which contains correct parent
///       hashes of u), the parents field in the TerminalUnit object of u is updated accordingly, and the unit u
///       gets status WaitingParentsInDag as in a)
/// 5) Now we wait for all the parents of unit u to be in Dag (in order to add it to Dag). Initially, right after u gets
///    gets this status, we mark all the parents that are already in the Dag and set appropriate triggers for when
///    the remaining parents are added to Dag.
/// 6) At the moment when all parents have been added to Dag, the unit itself is added to Dag.
///
/// We also refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/internals.html
/// Section 5.3 for a discussion of this component.

pub(crate) struct Terminal<H: Hasher> {
    node_id: NodeIndex,
    // A channel for receiving notifications (units mainly)
    ntfct_rx: Receiver<NotificationIn<H>>,
    // A channel to push outgoing notifications
    ntfct_tx: Sender<NotificationOut<H>>,
    // A Queue to handle events happening in the Terminal. The reason of this being a queue is because
    // some events trigger other events and because of the Dag structure, these should be handled
    // in a FIFO order (as in BFS) and not recursively (as in DFS).
    event_queue: VecDeque<TerminalEvent<H>>,
    post_insert: Vec<SyncClosure<TerminalUnit<H>, ()>>,
    // Here we store all the units -- the ones in Dag and the ones "hanging".
    unit_store: HashMap<H::Hash, TerminalUnit<H>>,

    // In this Map, for each pair (r, pid) we store the first unit made by pid at round r that we ever received.
    // In case of forks, we still store only the first one -- others are ignored (but stored in store under their hash).
    unit_by_coord: HashMap<(Round, NodeIndex), H::Hash>,
    // This stores, for a pair (r, pid) the list of all units (by hash) that await a unit made by pid at
    // round r, as their parent. Once such a unit arrives, we notify all these children.
    children_coord: HashMap<(Round, NodeIndex), Vec<H::Hash>>,
    // The same as above, but this time we await for a unit (with a particular hash) to be added to the Dag.
    // Once this happens, we notify all the children.
    children_hash: HashMap<H::Hash, Vec<H::Hash>>,
}

impl<H: Hasher> Terminal<H> {
    pub(crate) fn new(
        node_id: NodeIndex,
        ntfct_rx: Receiver<NotificationIn<H>>,
        ntfct_tx: Sender<NotificationOut<H>>,
    ) -> Self {
        Terminal {
            node_id,
            ntfct_rx,
            ntfct_tx,
            event_queue: VecDeque::new(),
            post_insert: Vec::new(),
            unit_store: HashMap::new(),
            unit_by_coord: HashMap::new(),
            children_coord: HashMap::new(),
            children_hash: HashMap::new(),
        }
    }

    // Reconstruct the parent of a unit u (given by hash u_hash) at position pid as p (given by hash p_hash)
    fn reconstruct_parent(&mut self, u_hash: &H::Hash, pid: NodeIndex, p_hash: &H::Hash) {
        let u = self.unit_store.get_mut(u_hash).unwrap();
        // the above unwraps must succeed, should probably add some debug messages here...

        u.parents[pid] = Some(*p_hash);
        u.n_miss_par_decoded -= NodeCount(1);
        if u.n_miss_par_decoded == NodeCount(0) {
            self.event_queue
                .push_back(TerminalEvent::ParentsReconstructed(*u_hash));
        }
    }

    // update u's count (u given by hash u_hash) of parents present in Dag and trigger the event of
    // adding u itself to the Dag
    fn new_parent_in_dag(&mut self, u_hash: &H::Hash) {
        let u = self.unit_store.get_mut(u_hash).unwrap();
        u.n_miss_par_dag -= NodeCount(1);
        if u.n_miss_par_dag == NodeCount(0) {
            self.event_queue
                .push_back(TerminalEvent::ParentsInDag(*u_hash));
        }
    }

    // Adds the unit u (u given by hash u_hash) to the list of units waiting for the coord (round, pid) to be
    // added to store.
    fn add_coord_trigger(&mut self, round: Round, pid: NodeIndex, u_hash: H::Hash) {
        let coord = (round, pid);
        if !self.children_coord.contains_key(&coord) {
            self.children_coord.insert((round, pid), Vec::new());
        }
        let wait_list = self.children_coord.get_mut(&coord).unwrap();
        wait_list.push(u_hash);
    }

    // Adds the unit u (u given by hash u_hash) to the list of units waiting for the unit p (p_hash) to be
    // added to the Dag.
    fn add_hash_trigger(&mut self, p_hash: &H::Hash, u_hash: &H::Hash) {
        if !self.children_hash.contains_key(p_hash) {
            self.children_hash.insert(*p_hash, Vec::new());
        }
        let wait_list = self.children_hash.get_mut(p_hash).unwrap();
        wait_list.push(*u_hash);
    }

    fn update_on_store_add(&mut self, u: Unit<H>) {
        let u_hash = u.hash();
        let (u_round, pid) = (u.round(), u.creator());
        // We place the unit in the coord map only if this is the first variant ever received.
        // This is not crucial for correctness, but helps in clarity.
        if let Entry::Vacant(entry) = self.unit_by_coord.entry((u_round, pid)) {
            entry.insert(u_hash);
        } else {
            debug!(target: "AlephBFT-terminal", "Received a fork at round {:?} creator {:?}", u_round, pid);
        }

        if let Some(children) = self.children_coord.remove(&(u_round, pid)) {
            // We reconstruct the parent for each unit that waits for this coord.
            for v_hash in children {
                self.reconstruct_parent(&v_hash, pid, &u_hash);
            }
        }
        if u_round == 0 {
            self.event_queue
                .push_back(TerminalEvent::ParentsReconstructed(u_hash));
        } else {
            let mut coords_to_request = Vec::new();
            for i in u.control_hash().parents() {
                let coord = (u_round - 1, i);
                let maybe_hash = self.unit_by_coord.get(&coord).cloned();
                match maybe_hash {
                    Some(v_hash) => self.reconstruct_parent(&u_hash, i, &v_hash),
                    None => {
                        self.add_coord_trigger(u_round - 1, i, u_hash);
                        let coord = UnitCoord::new(u.round() - 1, i);
                        coords_to_request.push(coord);
                    }
                }
            }
            if !coords_to_request.is_empty() {
                trace!(target: "AlephBFT-terminal", "{:?} Missing coords {:?}", self.node_id, coords_to_request);
                self.ntfct_tx
                    .unbounded_send(NotificationOut::MissingUnits(coords_to_request))
                    .expect("Channel should be open");
            }
        }
    }

    fn update_on_dag_add(&mut self, u_hash: &H::Hash) {
        let u = self
            .unit_store
            .get(u_hash)
            .expect("Unit to be added to dag must be in store")
            .clone();
        self.post_insert.iter().for_each(|f| f(u.clone()));
        if let Some(children) = self.children_hash.remove(u_hash) {
            for v_hash in children {
                self.new_parent_in_dag(&v_hash);
            }
        }
        let mut parent_hashes = Vec::new();
        for p_hash in u.parents.iter().flatten() {
            parent_hashes.push(*p_hash);
        }

        self.ntfct_tx
            .unbounded_send(NotificationOut::AddedToDag(*u_hash, parent_hashes))
            .expect("Channel should be open");
    }

    // We set the correct parent hashes for unit u.
    fn update_on_wrong_hash_response(&mut self, u_hash: H::Hash, p_hashes: Vec<H::Hash>) {
        let u = self
            .unit_store
            .get_mut(&u_hash)
            .expect("unit with wrong control hash must be in store");
        if u.status != UnitStatus::WrongControlHash {
            trace!(target: "AlephBFT-terminal", "{:?} Received parents response without it being expected for {:?}. Ignoring.", self.node_id, u_hash);
            return;
        }
        for (counter, i) in u.unit.control_hash().parents().enumerate() {
            u.parents[i] = Some(p_hashes[counter]);
        }
        trace!(target: "AlephBFT-terminal", "{:?} Updating parent hashes for wrong control hash unit {:?}", self.node_id, u_hash);
        u.n_miss_par_decoded = NodeCount(0);
        self.inspect_parents_in_dag(&u_hash);
    }

    fn add_to_store(&mut self, u: Unit<H>) {
        trace!(target: "AlephBFT-terminal", "{:?} Adding to store {:?} round {:?} index {:?}", self.node_id, u.hash(), u.round(), u.creator());
        if let Entry::Vacant(entry) = self.unit_store.entry(u.hash()) {
            entry.insert(TerminalUnit::<H>::blank_from_unit(&u));
            self.update_on_store_add(u);
        }
    }

    fn inspect_parents_in_dag(&mut self, u_hash: &H::Hash) {
        let u_parents = self.unit_store.get(u_hash).unwrap().parents.clone();
        let mut n_parents_in_dag = NodeCount(0);
        for p_hash in u_parents.into_iter().flatten() {
            let maybe_p = self.unit_store.get(&p_hash);
            // p might not be even in store because u might be a unit with wrong control hash
            match maybe_p {
                Some(p) if p.status == UnitStatus::InDag => {
                    n_parents_in_dag += NodeCount(1);
                }
                _ => {
                    self.add_hash_trigger(&p_hash, u_hash);
                }
            }
        }
        let u = self.unit_store.get_mut(u_hash).unwrap();
        u.n_miss_par_dag -= n_parents_in_dag;
        trace!(target: "AlephBFT-terminal", "{:?} Inspecting parents for {:?}, missing {:?}", self.node_id, u_hash, u.n_miss_par_dag);
        if u.n_miss_par_dag == NodeCount(0) {
            self.event_queue
                .push_back(TerminalEvent::ParentsInDag(*u_hash));
        } else {
            u.status = UnitStatus::WaitingParentsInDag;
            // when the last parent is added an appropriate TerminalEvent will be triggered
        }
    }

    fn on_wrong_hash_detected(&mut self, u_hash: H::Hash) {
        self.ntfct_tx
            .unbounded_send(NotificationOut::WrongControlHash(u_hash))
            .expect("Channel should be open");
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
                        self.inspect_parents_in_dag(&u_hash);
                    } else {
                        u.status = UnitStatus::WrongControlHash;
                        warn!(target: "AlephBFT-terminal", "{:?} wrong control hash", self.node_id);
                        self.on_wrong_hash_detected(u_hash);
                    }
                }
                TerminalEvent::ParentsInDag(u_hash) => {
                    let u = self.unit_store.get_mut(&u_hash).unwrap();
                    u.status = UnitStatus::InDag;
                    trace!(target: "AlephBFT-terminal", "{:?} Adding to Dag {:?} round {:?} index {:?}.", self.node_id, u_hash, u.unit.round(), u.unit.creator());
                    self.update_on_dag_add(&u_hash);
                }
            }
        }
    }

    pub(crate) fn register_post_insert_hook(&mut self, hook: SyncClosure<TerminalUnit<H>, ()>) {
        self.post_insert.push(hook);
    }

    pub(crate) async fn run(&mut self, mut exit: oneshot::Receiver<()>) {
        loop {
            futures::select! {
                n = self.ntfct_rx.next() => {
                    match n {
                        Some(NotificationIn::NewUnits(units)) => {
                            for u in units {
                                self.add_to_store(u);
                                self.handle_events();
                            }
                        },
                        Some(NotificationIn::UnitParents(u_hash, p_hashes)) => {
                            self.update_on_wrong_hash_response(u_hash, p_hashes);
                            self.handle_events();
                        },
                        _ => {}
                    }
                }
                _ = &mut exit => {
                    info!(target: "AlephBFT-terminal", "{:?} received exit signal.", self.node_id);
                    return
                }
            }
        }
    }
}
