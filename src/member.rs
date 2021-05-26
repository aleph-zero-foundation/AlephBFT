use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, StreamExt,
};
use log::{debug, error};
use rand::Rng;

use crate::{
    bft::{Alert, ForkProof},
    consensus,
    network::{NetworkHub, Recipient},
    signed::{SignatureError, Signed},
    units::{
        ControlHash, FullUnit, PreUnit, SignedUnit, UncheckedSignedUnit, Unit, UnitCoord, UnitStore,
    },
    Data, DataIO, Hasher, KeyBox, Network, NodeCount, NodeIndex, NodeMap, OrderedBatch,
    RequestAuxData, Sender, SessionId, SpawnHandle,
};

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    fmt::Debug,
};
use tokio::time;

const FETCH_INTERVAL: time::Duration = time::Duration::from_secs(4);
const TICK_INTERVAL: time::Duration = time::Duration::from_millis(100);
const INITIAL_MULTICAST_DELAY: time::Duration = time::Duration::from_secs(3);
// we will accept units that are of round <= (round_in_progress + ROUNDS_MARGIN) only
const ROUNDS_MARGIN: usize = 100;
const MAX_UNITS_ALERT: usize = 200;

/// A message concerning units, either about new units or some requests for them.
#[derive(Debug, Encode, Decode, Clone)]
pub(crate) enum UnitMessage<H: Hasher, D: Data, S> {
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

/// A message concerning alerts, either about a new alert or a request for it.
#[derive(Debug, Encode, Decode, Clone)]
pub(crate) enum AlertMessage<H: Hasher, D: Data, S> {
    /// Alert regarding forks,
    ForkAlert(Alert<H, D, S>),
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
    CreatedPreUnit(PreUnit<H>),
    /// Notification that some units are needed but missing. The role of the Member
    /// is to fetch these unit (somehow). Auxiliary data is provided to help handle this request.
    MissingUnits(Vec<UnitCoord>, RequestAuxData),
    /// Notification that Consensus has parents incompatible with the control hash.
    WrongControlHash(H::Hash),
    /// Notification that a new unit has been added to the DAG, list of decoded parents provided
    AddedToDag(H::Hash, Vec<H::Hash>),
}

#[derive(Eq, PartialEq)]
enum Task<H: Hasher> {
    CoordRequest(UnitCoord),
    ParentsRequest(H::Hash),
    // the hash of a unit, and the delay before repeating the multicast
    UnitMulticast(H::Hash, time::Duration),
}

#[derive(Eq, PartialEq)]
struct ScheduledTask<H: Hasher> {
    task: Task<H>,
    scheduled_time: time::Instant,
}

impl<H: Hasher> ScheduledTask<H> {
    fn new(task: Task<H>, scheduled_time: time::Instant) -> Self {
        ScheduledTask {
            task,
            scheduled_time,
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

#[derive(Clone, Debug)]
pub struct Config {
    pub node_id: NodeIndex,
    pub session_id: SessionId,
    pub n_members: NodeCount,
    pub create_lag: time::Duration,
}

pub struct Member<'a, H: Hasher, D: Data, DP: DataIO<D>, KB: KeyBox> {
    config: Config,
    tx_consensus: Option<Sender<NotificationIn<H>>>,
    data_io: DP,
    keybox: &'a KB,
    store: UnitStore<'a, H, D, KB>,
    requests: BinaryHeap<ScheduledTask<H>>,
    threshold: NodeCount,
    n_members: NodeCount,
    outgoing_units: Option<Sender<(UnitMessage<H, D, KB::Signature>, Recipient)>>,
    outgoing_alerts: Option<Sender<(AlertMessage<H, D, KB::Signature>, Recipient)>>,
}

impl<'a, H, D, DP, KB> Member<'a, H, D, DP, KB>
where
    H: Hasher,
    D: Data,
    DP: DataIO<D>,
    KB: KeyBox,
{
    pub fn new(data_io: DP, keybox: &'a KB, config: Config) -> Self {
        let n_members = config.n_members;
        let threshold = (n_members * 2) / 3 + NodeCount(1);
        Member {
            config,
            tx_consensus: None,
            data_io,
            keybox,
            store: UnitStore::new(n_members, threshold),
            requests: BinaryHeap::new(),
            threshold,
            n_members,
            outgoing_units: None,
            outgoing_alerts: None,
        }
    }

    fn send_consensus_notification(&mut self, notification: NotificationIn<H>) {
        if let Err(e) = self
            .tx_consensus
            .as_ref()
            .unwrap()
            .unbounded_send(notification)
        {
            debug!(target: "rush-member", "{:?} Error when sending notification {:?}.", self.index(), e);
        }
    }

    fn on_create(&mut self, u: PreUnit<H>) {
        debug!(target: "rush-member", "{:?} On create notification.", self.index());
        let data = self.data_io.get_data();
        let full_unit = FullUnit::new(u, data, self.config.session_id);
        let hash = full_unit.hash();
        let signed_unit = Signed::sign(full_unit, self.keybox);
        self.store.add_unit(signed_unit, false);
        let curr_time = time::Instant::now();
        let task = ScheduledTask::new(
            Task::UnitMulticast(hash, INITIAL_MULTICAST_DELAY),
            curr_time,
        );
        self.requests.push(task);
    }

    // Pulls tasks from the priority queue (sorted by scheduled time) and sends them to random peers
    // as long as they are scheduled at time <= curr_time
    pub(crate) fn trigger_tasks(&mut self) {
        while let Some(request) = self.requests.peek() {
            let curr_time = time::Instant::now();
            if request.scheduled_time > curr_time {
                break;
            }
            let request = self.requests.pop().expect("The element was peeked");

            match request.task {
                Task::CoordRequest(coord) => {
                    self.schedule_coord_request(coord, curr_time);
                }
                Task::UnitMulticast(hash, interval) => {
                    self.schedule_unit_multicast(hash, interval, curr_time);
                }
                Task::ParentsRequest(u_hash) => {
                    self.schedule_parents_request(u_hash, curr_time);
                }
            }
        }
    }

    fn random_peer(&self) -> NodeIndex {
        rand::thread_rng()
            .gen_range(0..self.n_members.into())
            .into()
    }

    fn index(&self) -> NodeIndex {
        self.keybox.index()
    }

    fn send_unit_message(&mut self, message: UnitMessage<H, D, KB::Signature>, peer_id: NodeIndex) {
        if let Err(e) = self
            .outgoing_units
            .as_ref()
            .unwrap()
            .unbounded_send((message, Recipient::Node(peer_id)))
        {
            debug!(target: "rush-member", "Error when sending units {:?}.", e);
        }
    }

    fn broadcast_units(&mut self, message: UnitMessage<H, D, KB::Signature>) {
        if let Err(e) = self
            .outgoing_units
            .as_ref()
            .unwrap()
            .unbounded_send((message, Recipient::Everyone))
        {
            debug!(target: "rush-member", "Error when broadcasting units {:?}.", e);
        }
    }

    fn broadcast_alert(&mut self, message: AlertMessage<H, D, KB::Signature>) {
        if let Err(e) = self
            .outgoing_alerts
            .as_ref()
            .unwrap()
            .unbounded_send((message, Recipient::Everyone))
        {
            debug!(target: "rush-member", "Error when broadcasting units {:?}.", e);
        }
    }

    fn schedule_parents_request(&mut self, u_hash: H::Hash, curr_time: time::Instant) {
        if self.store.get_parents(u_hash).is_none() {
            let message = UnitMessage::<H, D, KB::Signature>::RequestParents(self.index(), u_hash);
            let peer_id = self.random_peer();
            self.send_unit_message(message, peer_id);
            debug!(target: "rush-member", "{:?} Fetch parents for {:?} sent.", self.index(), u_hash);
            self.requests.push(ScheduledTask::new(
                Task::ParentsRequest(u_hash),
                curr_time + FETCH_INTERVAL,
            ));
        } else {
            debug!(target: "rush-member", "{:?} Request dropped as the parents are in store for {:?}.", self.index(), u_hash);
        }
    }

    fn schedule_coord_request(&mut self, coord: UnitCoord, curr_time: time::Instant) {
        debug!(target: "rush-member", "{:?} Starting request for {:?}", self.index(), coord);
        // If we already have a unit with such a coord in our store then there is no need to request it.
        // It will be sent to consensus soon (or have already been sent).
        if self.store.contains_coord(&coord) {
            debug!(target: "rush-member", "{:?} Request dropped as the unit is in store already {:?}", self.index(), coord);
            return;
        }
        let message = UnitMessage::<H, D, KB::Signature>::RequestCoord(self.index(), coord);
        let peer_id = self.random_peer();
        self.send_unit_message(message, peer_id);
        debug!(target: "rush-member", "{:?} Fetch request for {:?} sent.", self.index(), coord);
        self.requests.push(ScheduledTask::new(
            Task::CoordRequest(coord),
            curr_time + FETCH_INTERVAL,
        ));
    }

    fn schedule_unit_multicast(
        &mut self,
        hash: H::Hash,
        interval: time::Duration,
        curr_time: time::Instant,
    ) {
        let signed_unit = self
            .store
            .unit_by_hash(&hash)
            .cloned()
            .expect("Our units are in store.");
        let message = UnitMessage::<H, D, KB::Signature>::NewUnit(signed_unit.into());
        debug!(target: "rush-member", "{:?} Sending a unit {:?} over network after delay {:?}.", self.index(), hash, interval);
        self.broadcast_units(message);
        // NOTE: we double the delay each time
        self.requests.push(ScheduledTask::new(
            Task::UnitMulticast(hash, interval * 2),
            curr_time + interval,
        ));
    }

    pub(crate) fn on_missing_coords(&mut self, coords: Vec<UnitCoord>) {
        debug!(target: "rush-member", "{:?} Dealing with missing coords notification {:?}.", self.index(), coords);
        let curr_time = time::Instant::now();
        for coord in coords {
            if !self.store.contains_coord(&coord) {
                let task = ScheduledTask::new(Task::CoordRequest(coord), curr_time);
                self.requests.push(task);
            }
        }
        self.trigger_tasks();
    }

    fn on_wrong_control_hash(&mut self, u_hash: H::Hash) {
        debug!(target: "rush-member", "{:?} Dealing with wrong control hash notification {:?}.", self.index(), u_hash);
        if let Some(p_hashes) = self.store.get_parents(u_hash) {
            // We have the parents by some strange reason (someone sent us parents
            // without us requesting them).
            let p_hashes = p_hashes.clone();
            debug!(target: "rush-member", "{:?} We have the parents for {:?} even though we did not request them.", self.index(), u_hash);
            self.send_consensus_notification(NotificationIn::UnitParents(u_hash, p_hashes));
        } else {
            let curr_time = time::Instant::now();
            let task = ScheduledTask::new(Task::ParentsRequest(u_hash), curr_time);
            self.requests.push(task);
            self.trigger_tasks();
        }
    }

    fn on_consensus_notification(&mut self, notification: NotificationOut<H>) {
        match notification {
            NotificationOut::CreatedPreUnit(pu) => {
                self.on_create(pu);
            }
            NotificationOut::MissingUnits(coords, _aux) => {
                self.on_missing_coords(coords);
            }
            NotificationOut::WrongControlHash(h) => {
                self.on_wrong_control_hash(h);
            }
            NotificationOut::AddedToDag(h, p_hashes) => {
                self.store.add_parents(h, p_hashes);
            }
        }
    }

    fn validate_unit_parents(&self, su: &SignedUnit<'a, H, D, KB>) -> bool {
        // NOTE: at this point we cannot validate correctness of the control hash, in principle it could be
        // just a random hash, but we still would not be able to deduce that by looking at the unit only.
        let pre_unit = su.as_signable().as_pre_unit();
        if pre_unit.n_members() != self.config.n_members {
            debug!(target: "rush-member", "{:?} Unit with wrong length of parents map.", self.index());
            return false;
        }
        let round = pre_unit.round();
        let n_parents = pre_unit.n_parents();
        if round == 0 && n_parents > NodeCount(0) {
            debug!(target: "rush-member", "{:?} Unit of round zero with non-zero number of parents.", self.index());
            return false;
        }
        let threshold = self.threshold;
        if round > 0 && n_parents < threshold {
            debug!(target: "rush-member", "{:?} Unit of non-zero round with only {:?} parents while at least {:?} are required.", self.index(), n_parents, threshold);
            return false;
        }
        let control_hash = &pre_unit.control_hash();
        if round > 0 && !control_hash.parents_mask[pre_unit.creator()] {
            debug!(target: "rush-member", "{:?} Unit does not have its creator's previous unit as parent.", self.index());
            return false;
        }
        true
    }

    fn validate_unit(&self, su: &SignedUnit<'a, H, D, KB>) -> bool {
        let full_unit = su.as_signable();
        if full_unit.session_id() != self.config.session_id {
            // NOTE: this implies malicious behavior as the unit's session_id
            // is incompatible with session_id of the message it arrived in.
            debug!(target: "rush-member", "{:?} A unit with incorrect session_id! {:?}", self.index(), full_unit);
            return false;
        }
        if full_unit.round() > self.store.limit_per_node() {
            debug!(target: "rush-member", "{:?} A unit with too high round {}! {:?}", self.index(), full_unit.round(), full_unit);
            return false;
        }
        if full_unit.creator().0 >= self.config.n_members.0 {
            debug!(target: "rush-member", "{:?} A unit with too high creator index {}! {:?}", self.index(), full_unit.creator().0, full_unit);
            return false;
        }
        if !self.validate_unit_parents(su) {
            debug!(target: "rush-member", "{:?} A unit did not pass parents validation. {:?}", self.index(), full_unit);
            return false;
        }
        true
    }

    fn add_unit_to_store_unless_fork(&mut self, su: SignedUnit<'a, H, D, KB>) {
        let full_unit = su.as_signable();
        debug!(target: "rush-member", "{:?} Adding member unit to store {:?}", self.index(), full_unit);
        if self.store.is_forker(full_unit.creator()) {
            debug!(target: "rush-member", "{:?} Ignoring forker's unit {:?}", self.index(), full_unit);
            return;
        }
        if let Some(sv) = self.store.is_new_fork(&su) {
            let creator = full_unit.creator();
            if !self.store.is_forker(creator) {
                // We need to mark the forker if it is not known yet.
                let proof = ForkProof {
                    u1: su.into(),
                    u2: sv.into(),
                };
                self.on_new_forker_detected(creator, proof);
            }
            // We ignore this unit. If it is legit, it will arrive in some alert and we need to wait anyway.
            // There is no point in keeping this unit in any kind of buffer.
            return;
        }
        let u_round = full_unit.round();
        let round_in_progress = self.store.get_round_in_progress();
        if u_round <= round_in_progress + ROUNDS_MARGIN {
            self.store.add_unit(su, false);
        } else {
            debug!(target: "rush-member", "{:?} Unit {:?} ignored because of too high round {} when round in progress is {}.", self.index(), full_unit, u_round, round_in_progress);
        }
    }

    fn move_units_to_consensus(&mut self) {
        let mut units = Vec::new();
        for su in self.store.yield_buffer_units() {
            let unit = su.as_signable().unit();
            units.push(unit);
        }
        if !units.is_empty() {
            self.send_consensus_notification(NotificationIn::NewUnits(units));
        }
    }

    fn on_unit_received(&mut self, su: SignedUnit<'a, H, D, KB>, alert: bool) {
        if alert {
            // The unit has been validated already, we add to store.
            self.store.add_unit(su, true);
        } else if self.validate_unit(&su) {
            self.add_unit_to_store_unless_fork(su);
        }
    }

    fn on_request_coord(&mut self, peer_id: NodeIndex, coord: UnitCoord) {
        debug!(target: "rush-member", "{:?} Received fetch request for coord {:?} from {:?}.", self.index(), coord, peer_id);
        let maybe_su = (self.store.unit_by_coord(coord)).cloned();

        if let Some(su) = maybe_su {
            debug!(target: "rush-member", "{:?} Answering fetch request for coord {:?} from {:?}.", self.index(), coord, peer_id);
            let message = UnitMessage::ResponseCoord(su.into());
            self.send_unit_message(message, peer_id);
        } else {
            debug!(target: "rush-member", "{:?} Not answering fetch request for coord {:?}. Unit not in store.", self.index(), coord);
        }
    }

    fn on_request_parents(&mut self, peer_id: NodeIndex, u_hash: H::Hash) {
        debug!(target: "rush-member", "{:?} Received parents request for hash {:?} from {:?}.", self.index(), u_hash, peer_id);
        let maybe_p_hashes = self.store.get_parents(u_hash);

        if let Some(p_hashes) = maybe_p_hashes {
            let p_hashes = p_hashes.clone();
            debug!(target: "rush-member", "{:?} Answering parents request for hash {:?} from {:?}.", self.index(), u_hash, peer_id);
            let full_units = p_hashes
                .into_iter()
                .map(|hash| self.store.unit_by_hash(&hash).unwrap().clone().into())
                .collect();
            let message = UnitMessage::ResponseParents(u_hash, full_units);
            self.send_unit_message(message, peer_id);
        } else {
            debug!(target: "rush-member", "{:?} Not answering parents request for hash {:?}. Unit not in DAG yet.", self.index(), u_hash);
        }
    }

    fn on_parents_response(&mut self, u_hash: H::Hash, parents: Vec<SignedUnit<'a, H, D, KB>>) {
        let (u_round, u_control_hash, parent_ids) = match self.store.unit_by_hash(&u_hash) {
            Some(su) => {
                let full_unit = su.as_signable();
                let parents: Vec<_> = full_unit.control_hash().parents().collect();
                (
                    full_unit.round(),
                    full_unit.control_hash().combined_hash,
                    parents,
                )
            }
            None => {
                debug!(target: "rush-member", "{:?} We got parents but don't even know the unit. Ignoring.", self.index());
                return;
            }
        };

        if parent_ids.len() != parents.len() {
            debug!(target: "rush-member", "{:?} In received parent response expected {} parents got {} for unit {:?}.", self.index(), parents.len(), parent_ids.len(), u_hash);
            return;
        }

        let mut p_hashes_node_map: NodeMap<Option<H::Hash>> =
            NodeMap::new_with_len(self.config.n_members);
        for (i, su) in parents.into_iter().enumerate() {
            let full_unit = su.as_signable();
            if full_unit.round() + 1 != u_round {
                debug!(target: "rush-member", "{:?} In received parent response received a unit with wrong round.", self.index());
                return;
            }
            if full_unit.creator() != parent_ids[i] {
                debug!(target: "rush-member", "{:?} In received parent response received a unit with wrong creator.", self.index());
                return;
            }
            if !self.validate_unit(&su) {
                debug!(target: "rush-member", "{:?} In received parent response received a unit that does not pass validation.", self.index());
                return;
            }
            let p_hash = full_unit.hash();
            let ix = full_unit.creator();
            p_hashes_node_map[ix] = Some(p_hash);
            // There might be some optimization possible here to not validate twice, but overall
            // this piece of code should be executed extremely rarely.
            self.add_unit_to_store_unless_fork(su);
        }

        if ControlHash::<H>::combine_hashes(&p_hashes_node_map) != u_control_hash {
            debug!(target: "rush-member", "{:?} In received parent response the control hash is incorrect {:?}.", self.index(), p_hashes_node_map);
            return;
        }
        let p_hashes: Vec<H::Hash> = p_hashes_node_map.into_iter().flatten().collect();
        self.store.add_parents(u_hash, p_hashes.clone());
        debug!(target: "rush-member", "{:?} Succesful parents reponse for {:?}.", self.index(), u_hash);
        self.send_consensus_notification(NotificationIn::UnitParents(u_hash, p_hashes));
    }

    fn validate_fork_proof(
        &self,
        forker: NodeIndex,
        proof: &ForkProof<H, D, KB::Signature>,
    ) -> bool {
        let (u1, u2) = {
            let u1 = proof.u1.clone().check(self.keybox);
            let u2 = proof.u2.clone().check(self.keybox);
            match (u1, u2) {
                (Ok(u1), Ok(u2)) => (u1, u2),
                _ => {
                    debug!(target: "rush-member", "{:?} Invalid signatures in a proof.", self.index());
                    return false;
                }
            }
        };
        if !self.validate_unit(&u1) || !self.validate_unit(&u2) {
            debug!(target: "rush-member", "{:?} One of the units in the proof is invalid.", self.index());
            return false;
        }
        let full_unit1 = u1.as_signable();
        let full_unit2 = u2.as_signable();
        if full_unit1.creator() != forker || full_unit2.creator() != forker {
            debug!(target: "rush-member", "{:?} One of the units creators in proof does not match.", self.index());
            return false;
        }
        if full_unit1.round() != full_unit2.round() {
            debug!(target: "rush-member", "{:?} The rounds in proof's units do not match.", self.index());
            return false;
        }
        true
    }

    fn validate_alerted_units(
        &self,
        forker: NodeIndex,
        units: &[SignedUnit<'a, H, D, KB>],
    ) -> bool {
        // Correctness rules:
        // 1) All units must pass unit validation
        // 2) All units must be created by forker
        // 3) All units must come from different rounds
        // 4) There must be <= MAX_UNITS_ALERT of them
        if units.len() > MAX_UNITS_ALERT {
            debug!(target: "rush-member", "{:?} Too many units: {} included in alert.", self.index(), units.len());
            return false;
        }
        let mut rounds: HashSet<usize> = HashSet::new();
        for u in units {
            let full_unit = u.as_signable();
            if full_unit.creator() != forker {
                debug!(target: "rush-member", "{:?} One of the units {:?} has wrong creator.", self.index(), full_unit);
                return false;
            }
            if !self.validate_unit(u) {
                debug!(target: "rush-member", "{:?} One of the units {:?} in alert does not pass validation.", self.index(), full_unit);
                return false;
            }
            if rounds.contains(&full_unit.round()) {
                debug!(target: "rush-member", "{:?} Two or more alerted units have the same round {:?}.", self.index(), full_unit.round());
                return false;
            }
            rounds.insert(full_unit.round());
        }
        true
    }

    fn validate_alert(&self, alert: &Alert<H, D, KB::Signature>) -> bool {
        // The correctness of forker and sender should be checked in RBC, but no harm
        // to have a check here as well for now.
        if alert.forker.0 >= self.config.n_members.0 {
            debug!(target: "rush-member", "{:?} Alert has incorrect forker field {:?}", self.index(), alert.forker);
            return false;
        }
        if alert.sender.0 >= self.config.n_members.0 {
            debug!(target: "rush-member", "{:?} Alert has incorrect sender field {:?}", self.index(), alert.sender);
            return false;
        }
        if !self.validate_fork_proof(alert.forker, &alert.proof) {
            debug!(target: "rush-member", "{:?} Alert has incorrect fork proof.", self.index());
            return false;
        }
        let legit_units: Result<Vec<_>, _> = alert
            .legit_units
            .iter()
            .map(|unchecked| unchecked.clone().check(self.keybox))
            .collect();
        let legit_units = match legit_units {
            Ok(legit_units) => legit_units,
            Err(e) => {
                debug!(target: "rush-member", "{:?} Alert has a badly signed unit: {:?}.", self.index(), e);
                return false;
            }
        };
        if !self.validate_alerted_units(alert.forker, &legit_units[..]) {
            debug!(target: "rush-member", "{:?} Alert has incorrect unit/s.", self.index());
            return false;
        }
        true
    }

    fn form_alert(
        &self,
        forker: NodeIndex,
        proof: ForkProof<H, D, KB::Signature>,
        units: Vec<SignedUnit<'a, H, D, KB>>,
    ) -> Alert<H, D, KB::Signature> {
        Alert {
            sender: self.config.node_id,
            forker,
            proof,
            legit_units: units.into_iter().map(|signed| signed.into()).collect(),
        }
    }

    fn on_new_forker_detected(&mut self, forker: NodeIndex, proof: ForkProof<H, D, KB::Signature>) {
        let mut alerted_units = self.store.mark_forker(forker);
        if alerted_units.len() > MAX_UNITS_ALERT {
            // The ordering is increasing w.r.t. rounds.
            alerted_units.reverse();
            alerted_units.truncate(MAX_UNITS_ALERT);
            alerted_units.reverse();
        }
        let alert = self.form_alert(forker, proof, alerted_units);
        let message = AlertMessage::ForkAlert(alert);
        self.broadcast_alert(message);
    }

    fn on_fork_alert(&mut self, alert: Alert<H, D, KB::Signature>) {
        if self.validate_alert(&alert) {
            let forker = alert.forker;
            if !self.store.is_forker(forker) {
                // We learn about this forker for the first time, need to send our own alert
                self.on_new_forker_detected(forker, alert.proof);
            }
            for unchecked in alert.legit_units {
                let su = unchecked.check(self.keybox).expect("alert is valid; qed.");
                self.on_unit_received(su, true);
            }
        } else {
            debug!(target: "rush-member","{:?} We have received an incorrect alert from {:?} on forker {:?}.", self.index(), alert.sender, alert.forker);
        }
    }

    fn on_unit_message(&mut self, message: UnitMessage<H, D, KB::Signature>) {
        use UnitMessage::*;
        match message {
            NewUnit(unchecked) => {
                debug!(target: "rush-member", "{:?} New unit received {:?}.", self.index(), &unchecked);
                match unchecked.check(self.keybox) {
                    Ok(su) => self.on_unit_received(su, false),
                    Err(unchecked) => {
                        debug!(target: "rush-member", "{:?} Wrong signature received {:?}.", self.index(), &unchecked)
                    }
                }
            }
            RequestCoord(peer_id, coord) => {
                self.on_request_coord(peer_id, coord);
            }
            ResponseCoord(unchecked) => {
                debug!(target: "rush-member", "{:?} Fetch response received {:?}.", self.index(), &unchecked);

                match unchecked.check(self.keybox) {
                    Ok(su) => self.on_unit_received(su, false),
                    Err(unchecked) => {
                        debug!(target: "rush-member", "{:?} Wrong signature received {:?}.", self.index(), &unchecked)
                    }
                }
            }
            RequestParents(peer_id, u_hash) => {
                debug!(target: "rush-member", "{:?} Parents request received {:?}.", self.index(), u_hash);
                self.on_request_parents(peer_id, u_hash);
            }
            ResponseParents(u_hash, parents) => {
                debug!(target: "rush-member", "{:?} Response parents received {:?}.", self.index(), u_hash);
                let parents: Result<Vec<_>, SignatureError<_, _>> = parents
                    .into_iter()
                    .map(|unchecked| unchecked.check(self.keybox))
                    .collect();
                match parents {
                    Ok(parents) => self.on_parents_response(u_hash, parents),
                    Err(err) => {
                        debug!(target: "rush-member", "{:?} Bad signature received {:?}.", self.index(), err)
                    }
                }
            }
        }
    }

    fn on_alert_message(&mut self, message: AlertMessage<H, D, KB::Signature>) {
        use AlertMessage::*;
        match message {
            ForkAlert(alert) => {
                debug!(target: "rush-member", "{:?} Fork alert received {:?}.", self.index(), alert);
                self.on_fork_alert(alert);
            }
        }
    }

    fn on_ordered_batch(&mut self, batch: Vec<H::Hash>) {
        let batch = batch
            .iter()
            .map(|h| {
                self.store
                    .unit_by_hash(h)
                    .expect("Ordered units must be in store")
                    .as_signable()
                    .data()
            })
            .collect::<OrderedBatch<D>>();
        if let Err(e) = self.data_io.send_ordered_batch(batch) {
            debug!(target: "rush-member", "{:?} Error when sending batch {:?}.", self.index(), e);
        }
    }

    pub async fn run_session<N: Network<H, D, KB::Signature> + 'static>(
        mut self,
        network: N,
        spawn_handle: impl SpawnHandle,
        exit: oneshot::Receiver<()>,
    ) {
        let (tx_consensus, consensus_stream) = mpsc::unbounded();
        let (consensus_sink, mut rx_consensus) = mpsc::unbounded();
        let (ordered_batch_tx, mut ordered_batch_rx) = mpsc::unbounded();
        let (consensus_exit, exit_stream) = oneshot::channel();
        let config = self.config.clone();
        let sh = spawn_handle.clone();
        debug!(target: "rush-member", "{:?} Spawning party for a session with config {:?}", self.index(), self.config);
        spawn_handle.spawn("member/consensus", async move {
            consensus::run(
                config,
                consensus_stream,
                consensus_sink,
                ordered_batch_tx,
                sh,
                exit_stream,
            )
            .await
        });
        self.tx_consensus = Some(tx_consensus);
        let (alert_sink, mut incoming_alerts) = mpsc::unbounded();
        let (outgoing_alerts, alert_stream) = mpsc::unbounded();
        let (unit_sink, mut incoming_units) = mpsc::unbounded();
        let (outgoing_units, unit_stream) = mpsc::unbounded();
        let (network_exit, exit_stream) = oneshot::channel();
        spawn_handle.spawn("member/network", async move {
            NetworkHub::new(network, unit_stream, unit_sink, alert_stream, alert_sink)
                .run(exit_stream)
                .await
        });
        self.outgoing_units = Some(outgoing_units);
        self.outgoing_alerts = Some(outgoing_alerts);
        let mut ticker = time::interval(TICK_INTERVAL);
        let mut exit = exit.into_stream();

        debug!(target: "rush-member", "{:?} Start routing messages from consensus to network", self.index());
        loop {
            tokio::select! {
                notification = rx_consensus.next() => match notification {
                        Some(notification) => self.on_consensus_notification(notification),
                        None => {
                            error!(target: "rush-member", "{:?} Consensus notification stream closed.", self.index());
                            break;
                        }
                },

                event = incoming_alerts.next() => match event {
                    Some(event) => self.on_alert_message(event),
                    None => {
                        error!(target: "rush-member", "{:?} Alert message stream closed.", self.index());
                        break;
                    }
                },

                event = incoming_units.next() => match event {
                    Some(event) => self.on_unit_message(event),
                    None => {
                        error!(target: "rush-member", "{:?} Unit message stream closed.", self.index());
                        break;
                    }
                },

                batch = ordered_batch_rx.next() => match batch {
                    Some(batch) => self.on_ordered_batch(batch),
                    None => {
                        error!(target: "rush-member", "{:?} Ordered batch stream closed.", self.index());
                        break;
                    }
                },

                _ = ticker.tick() => self.trigger_tasks(),
                _ = exit.next() => break,
            }
            self.move_units_to_consensus();
        }

        let _ = consensus_exit.send(());
        let _ = network_exit.send(());
    }
}
