use codec::{Decode, Encode};
use futures::{channel::mpsc::unbounded, FutureExt, SinkExt, StreamExt};
use log::{debug, error};
use tokio::{
    sync::{mpsc::unbounded_channel, oneshot},
    time::Duration,
};

use crate::{
    bft::{Alert, ForkProof},
    consensus,
    units::{ControlHash, FullUnit, PreUnit, SignedUnit, Unit, UnitCoord, UnitStore},
    Data, DataIO, Hash, KeyBox, Network, NetworkCommand, NetworkEvent, NodeCount, NodeIdT,
    NodeIndex, NodeMap, OrderedBatch, RequestAuxData, SessionId, SpawnHandle,
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

/// The kind of message that is being sent.
#[derive(Debug, Encode, Decode)]
pub(crate) enum ConsensusMessage<H: Hash, D: Data, Signature: Debug + Clone + Encode + Decode> {
    /// Fo disseminating newly created units.
    NewUnit(SignedUnit<H, D, Signature>),
    /// Request for a unit by its coord.
    RequestCoord(UnitCoord),
    /// Response to a request by coord.
    ResponseCoord(SignedUnit<H, D, Signature>),
    /// Request for the full list of parents of a unit.
    RequestParents(H),
    /// Response to a request for a full list of parents.
    ResponseParents(H, Vec<SignedUnit<H, D, Signature>>),
    /// Alert regarding forks,
    ForkAlert(Alert<H, D, Signature>),
}

/// Type for incoming notifications: Member to Consensus.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NotificationIn<H: Hash> {
    /// A notification carrying a single unit. This might come either from multicast or
    /// from a response to a request. This is of no importance at this layer.
    NewUnits(Vec<Unit<H>>),
    /// Response to a request to decode parents when the control hash is wrong.
    UnitParents(H, Vec<H>),
}

/// Type for outgoing notifications: Consensus to Member.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NotificationOut<H: Hash> {
    /// Notification about a preunit created by this Consensus Node. Member is meant to
    /// disseminate this preunit among other nodes.
    CreatedPreUnit(PreUnit<H>),
    /// Notification that some units are needed but missing. The role of the Member
    /// is to fetch these unit (somehow). Auxiliary data is provided to help handle this request.
    MissingUnits(Vec<UnitCoord>, RequestAuxData),
    /// Notification that Consensus has parents incompatible with the control hash.
    WrongControlHash(H),
    /// Notification that a new unit has been added to the DAG, list of decoded parents provided
    AddedToDag(H, Vec<H>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Task<H: Hash> {
    CoordRequest(UnitCoord),
    ParentsRequest(H),
    // the hash of a unit, and the delay before repeating the multicast
    UnitMulticast(H, time::Duration),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScheduledTask<H: Hash> {
    task: Task<H>,
    scheduled_time: time::Instant,
}

impl<H: Hash> ScheduledTask<H> {
    fn new(task: Task<H>, scheduled_time: time::Instant) -> Self {
        ScheduledTask {
            task,
            scheduled_time,
        }
    }
}

impl<H: Hash> Ord for ScheduledTask<H> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we want earlier times to come first when used in max-heap, hence the below:
        other.scheduled_time.cmp(&self.scheduled_time)
    }
}

impl<H: Hash> PartialOrd for ScheduledTask<H> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub struct Config<NI: NodeIdT> {
    pub node_id: NI,
    pub session_id: SessionId,
    pub n_members: NodeCount,
    pub create_lag: Duration,
}

pub trait HashingT<H>: Fn(&[u8]) -> H + Copy + Send {}

impl<T, H> HashingT<H> for T where T: Fn(&[u8]) -> H + Copy + Send {}

pub struct Member<
    H: Hash,
    D: Data,
    Signature: Debug + Clone + Encode + Decode,
    DP: DataIO<D>,
    KB: KeyBox<Signature>,
    N: Network,
    NI: NodeIdT,
    Hashing: HashingT<H>,
> {
    config: Config<NI>,
    tx_consensus: Option<futures::channel::mpsc::UnboundedSender<NotificationIn<H>>>,
    data_io: DP,
    keybox: KB,
    network: N,
    store: UnitStore<H, D, Signature>,
    requests: BinaryHeap<ScheduledTask<H>>,
    hashing: Hashing,
    threshold: NodeCount,
}

impl<H, D, Signature, DP, KB, N, NI, Hashing> Member<H, D, Signature, DP, KB, N, NI, Hashing>
where
    H: Hash + 'static,
    D: Data,
    Signature: Debug + Clone + Encode + Decode,
    DP: DataIO<D>,
    KB: KeyBox<Signature>,
    N: Network,
    NI: NodeIdT,
    Hashing: HashingT<H> + 'static,
{
    pub fn new(data_io: DP, keybox: KB, network: N, config: Config<NI>, hashing: Hashing) -> Self {
        let n_members = config.n_members;
        let threshold = (n_members * 2) / 3 + NodeCount(1);
        Member {
            config,
            tx_consensus: None,
            data_io,
            keybox,
            network,
            store: UnitStore::new(n_members, threshold, hashing),
            requests: BinaryHeap::new(),
            hashing,
            threshold,
        }
    }

    fn send_consensus_notification(&mut self, notification: NotificationIn<H>) {
        if let Err(e) = self
            .tx_consensus
            .as_ref()
            .unwrap()
            .unbounded_send(notification)
        {
            debug!(target: "rush-member", "Error when sending notification {:?}.", e);
        }
    }

    fn on_create(&mut self, u: PreUnit<H>) {
        debug!(target: "rush-member", "On create notification.");
        let data = self.data_io.get_data();
        let full_unit = FullUnit {
            inner: u,
            data,
            session_id: self.config.session_id,
        };
        // TODO: beware: sign_unit blocks and is quite slow!
        let signed_unit = SignedUnit::sign(&self.keybox, full_unit);
        debug!(target: "rush-member", "On create notification post sign_unit.");
        let hash = signed_unit.hash(&self.hashing);
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

    fn schedule_parents_request(&mut self, u_hash: H, curr_time: time::Instant) {
        if self.store.get_parents(u_hash).is_none() {
            let message = ConsensusMessage::<H, D, Signature>::RequestParents(u_hash);
            let command = NetworkCommand::SendToRandPeer(message.encode());
            self.send_network_command(command);
            debug!(target: "rush-member", "Fetch parents for {:} sent.", u_hash);
            self.requests.push(ScheduledTask::new(
                Task::ParentsRequest(u_hash),
                curr_time + FETCH_INTERVAL,
            ));
        } else {
            debug!(target: "rush-member", "Request dropped as the parents are in store for {:}.", u_hash);
        }
    }

    fn schedule_coord_request(&mut self, coord: UnitCoord, curr_time: time::Instant) {
        debug!(target: "rush-member", "Starting request for {:?}", coord);
        // If we already have a unit with such a coord in our store then there is no need to request it.
        // It will be sent to consensus soon (or have already been sent).
        if self.store.contains_coord(&coord) {
            debug!(target: "rush-member", "Request dropped as the unit is in store already {:?}", coord);
            return;
        }
        let message = ConsensusMessage::<H, D, Signature>::RequestCoord(coord);
        let command = NetworkCommand::SendToRandPeer(message.encode());
        self.send_network_command(command);
        debug!(target: "rush-member", "Fetch request for {:?} sent.", coord);
        self.requests.push(ScheduledTask::new(
            Task::CoordRequest(coord),
            curr_time + FETCH_INTERVAL,
        ));
    }

    fn schedule_unit_multicast(
        &mut self,
        hash: H,
        interval: time::Duration,
        curr_time: time::Instant,
    ) {
        let signed_unit = self
            .store
            .unit_by_hash(&hash)
            .expect("Our units are in store.");
        let message = ConsensusMessage::NewUnit(signed_unit.clone());
        let command = NetworkCommand::SendToAll(message.encode());
        debug!(target: "rush-member", "Sending a unit {} over network after delay {:?}.", hash, interval);
        self.send_network_command(command);
        // NOTE: we double the delay each time
        self.requests.push(ScheduledTask::new(
            Task::UnitMulticast(hash, interval * 2),
            curr_time + interval,
        ));
    }

    pub(crate) fn on_missing_coords(&mut self, coords: Vec<UnitCoord>) {
        debug!(target: "rush-member", "Dealing with missing coords notification {:?}.", coords);
        let curr_time = time::Instant::now();
        for coord in coords {
            if !self.store.contains_coord(&coord) {
                let task = ScheduledTask::new(Task::CoordRequest(coord), curr_time);
                self.requests.push(task);
            }
        }
        self.trigger_tasks();
    }

    fn on_wrong_control_hash(&mut self, u_hash: H) {
        debug!(target: "rush-member", "Dealing with wrong control hash notification {:?}.", u_hash);
        if let Some(p_hashes) = self.store.get_parents(u_hash) {
            // We have the parents by some strange reason (someone sent us parents
            // without us requesting them).
            let p_hashes = p_hashes.clone();
            debug!(target: "rush-member", "We have the parents for {:?} even though we did not request them.", u_hash);
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
                //TODO: this is very RAM-heavy to store, optimizations needed
                self.store.add_parents(h, p_hashes);
            }
        }
    }

    fn validate_unit_parents(&self, su: &SignedUnit<H, D, Signature>) -> bool {
        // NOTE: at this point we cannot validate correctness of the control hash, in principle it could be
        // just a random hash, but we still would not be able to deduce that by looking at the unit only.
        if su.n_members() != self.config.n_members {
            debug!(target: "rush-member", "Unit with wrong length of parents map.");
            return false;
        }
        let round = su.round();
        let n_parents = su.n_parents();
        if round == 0 && n_parents > NodeCount(0) {
            debug!(target: "rush-member", "Unit of round zero with non-zero number of parents.");
            return false;
        }
        let threshold = self.threshold;
        if round > 0 && n_parents < threshold {
            debug!(target: "rush-member", "Unit of non-zero round with only {:?} parents while at least {:?} are required.", n_parents, threshold);
            return false;
        }
        let control_hash = &su.unit.inner.control_hash;
        if round > 0 && !control_hash.parents[su.creator()] {
            debug!(target: "rush-member", "Unit does not have its creator's previous unit as parent.");
            return false;
        }
        true
    }

    fn validate_unit(&self, su: &SignedUnit<H, D, Signature>) -> bool {
        // TODO: make sure we check all that is necessary for unit correctness
        // TODO: consider moving validation logic for units and alerts to another file, note however
        // that access to the authority list is required for validation.
        if su.unit.session_id != self.config.session_id {
            // NOTE: this implies malicious behavior as the unit's session_id
            // is incompatible with session_id of the message it arrived in.
            debug!(target: "rush-member", "A unit with incorrect session_id! {:?}", su);
            return false;
        }
        if !su.verify_signature(&self.keybox) {
            debug!(target: "rush-member", "A unit with incorrect signature! {:?}", su);
            return false;
        }
        if su.round() > self.store.limit_per_node() {
            debug!(target: "rush-member", "A unit with too high round {}! {:?}", su.round(), su);
            return false;
        }
        if su.creator().0 >= self.config.n_members.0 {
            debug!(target: "rush-member", "A unit with too high creator index {}! {:?}", su.creator(), su);
            return false;
        }
        if !self.validate_unit_parents(su) {
            debug!(target: "rush-member", "A unit did not pass parents validation. {:?}", su);
            return false;
        }
        true
    }

    fn add_unit_to_store_unless_fork(&mut self, su: SignedUnit<H, D, Signature>) {
        if let Some(sv) = self.store.is_new_fork(&su) {
            let creator = su.creator();
            if !self.store.is_forker(creator) {
                // We need to mark the forker if it is not known yet.
                let proof = ForkProof { u1: su, u2: sv };
                self.on_new_forker_detected(creator, proof);
            }
            // We ignore this unit. If it is legit, it will arrive in some alert and we need to wait anyway.
            // There is no point in keeping this unit in any kind of buffer.
            return;
        }
        let u_round = su.round();
        let round_in_progress = self.store.get_round_in_progress();
        if u_round <= round_in_progress + ROUNDS_MARGIN {
            self.store.add_unit(su, false);
        } else {
            debug!(target: "rush-member", "Unit {:?} ignored because of too high round {} when round in progress is {}.", su, u_round, round_in_progress);
        }
    }

    fn move_units_to_consensus(&mut self) {
        let mut units = Vec::new();
        for su in self.store.yield_buffer_units() {
            let hash = su.hash(&self.hashing);
            let unit = Unit::new_from_preunit(su.unit.inner.clone(), hash);
            units.push(unit);
        }
        if !units.is_empty() {
            self.send_consensus_notification(NotificationIn::NewUnits(units));
        }
    }

    fn on_unit_received(&mut self, su: SignedUnit<H, D, Signature>, alert: bool) {
        if alert {
            // The unit has been validated already, we add to store.
            self.store.add_unit(su, true);
        } else if self.validate_unit(&su) {
            self.add_unit_to_store_unless_fork(su);
        }
    }

    fn on_request_coord(&mut self, peer_id: Vec<u8>, coord: UnitCoord) {
        debug!(target: "rush-member", "Received fetch request for coord {:?} from {:?}.", coord, peer_id);
        let maybe_su = (self.store.unit_by_coord(coord)).cloned();

        if let Some(su) = maybe_su {
            debug!(target: "rush-member", "Answering fetch request for coord {:?} from {:?}.", coord, peer_id);
            let message = ConsensusMessage::ResponseCoord(su);
            let command = NetworkCommand::SendToPeer(message.encode(), peer_id);
            self.send_network_command(command);
        } else {
            debug!(target: "rush-member", "Not answering fetch request for coord {:?}. Unit not in store.", coord);
        }
    }

    fn send_network_command(&mut self, command: NetworkCommand) {
        if let Err(e) = self.network.send(command) {
            debug!(target: "rush-member", "Failed to send network command {:?}.", e);
        }
    }

    fn on_request_parents(&mut self, peer_id: Vec<u8>, u_hash: H) {
        debug!(target: "rush-member", "Received parents request for hash {:?} from {:?}.", u_hash, peer_id);
        let maybe_p_hashes = self.store.get_parents(u_hash);

        if let Some(p_hashes) = maybe_p_hashes {
            let p_hashes = p_hashes.clone();
            debug!(target: "rush-member", "Answering parents request for hash {:?} from {:?}.", u_hash, peer_id);
            let full_units = p_hashes
                .into_iter()
                .map(|hash| self.store.unit_by_hash(&hash).unwrap().clone())
                .collect();
            let message = ConsensusMessage::ResponseParents(u_hash, full_units).encode();
            let command = NetworkCommand::SendToPeer(message, peer_id);
            self.send_network_command(command);
        } else {
            debug!(target: "rush-member", "Not answering parents request for hash {:?}. Unit not in DAG yet.", u_hash);
        }
    }

    fn on_parents_response(&mut self, u_hash: H, parents: Vec<SignedUnit<H, D, Signature>>) {
        // TODO: we *must* make sure that we have indeed sent such a request before accepting the response.
        let (u_round, u_control_hash, parent_ids) = match self.store.unit_by_hash(&u_hash) {
            Some(u) => (
                u.round(),
                u.unit.inner.control_hash.hash,
                u.unit
                    .inner
                    .control_hash
                    .parents
                    .enumerate()
                    .filter_map(|(i, b)| if *b { Some(i) } else { None })
                    .collect::<Vec<NodeIndex>>(),
            ),
            None => {
                debug!(target: "rush-member", "We got parents but don't even know the unit. Ignoring.");
                return;
            }
        };

        if parent_ids.len() != parents.len() {
            debug!(target: "rush-member", "In received parent response expected {} parents got {} for unit {:?}.", parents.len(), parent_ids.len(), u_hash);
        }

        let mut p_hashes_node_map: NodeMap<Option<H>> =
            NodeMap::new_with_len(self.config.n_members);
        for (i, su) in parents.into_iter().enumerate() {
            if su.round() + 1 != u_round {
                debug!(target: "rush-member", "In received parent response received a unit with wrong round.");
                return;
            }
            if su.creator() != parent_ids[i] {
                debug!(target: "rush-member", "In received parent response received a unit with wrong creator.");
                return;
            }
            if !self.validate_unit(&su) {
                debug!(target: "rush-member", "In received parent response received a unit that does not pass validation.");
                return;
            }
            let p_hash = su.hash(&self.hashing);
            p_hashes_node_map[NodeIndex(i)] = Some(p_hash);
            // There might be some optimization possible here to not validate twice, but overall
            // this piece of code should be executed extremely rarely.
            self.add_unit_to_store_unless_fork(su);
        }

        if ControlHash::combine_hashes(&p_hashes_node_map, &self.hashing) != u_control_hash {
            debug!(target: "rush-member", "In received parent response the control hash is incorrect.");
            return;
        }
        let p_hashes: Vec<H> = p_hashes_node_map.into_iter().flatten().collect();
        self.store.add_parents(u_hash, p_hashes.clone());
        self.send_consensus_notification(NotificationIn::UnitParents(u_hash, p_hashes));
    }

    fn validate_fork_proof(&self, forker: NodeIndex, proof: &ForkProof<H, D, Signature>) -> bool {
        if !self.validate_unit(&proof.u1) || !self.validate_unit(&proof.u2) {
            debug!(target: "rush-member", "One of the units in the proof is invalid.");
            return false;
        }
        if proof.u1.creator() != forker || proof.u2.creator() != forker {
            debug!(target: "rush-member", "One of the units creators in proof does not match.");
            return false;
        }
        if proof.u1.round() != proof.u2.round() {
            debug!(target: "rush-member", "The rounds in proof's units do not match.");
            return false;
        }
        true
    }

    fn validate_alerted_units(
        &self,
        forker: NodeIndex,
        units: &[SignedUnit<H, D, Signature>],
    ) -> bool {
        // Correctness rules:
        // 1) All units must pass unit validation
        // 2) All units must be created by forker
        // 3) All units must come from different rounds
        // 4) There must be <= MAX_UNITS_ALERT of them
        if units.len() > MAX_UNITS_ALERT {
            debug!(target: "rush-member", "Too many units: {} included in alert.", units.len());
            return false;
        }
        let mut rounds: HashSet<usize> = HashSet::new();
        for u in units {
            if u.creator() != forker {
                debug!(target: "rush-member", "One of the units {:?} has wrong creator.", u);
                return false;
            }
            if !self.validate_unit(u) {
                debug!(target: "rush-member", "One of the units {:?} in alert does not pass validation.", u);
                return false;
            }
            if rounds.contains(&u.round()) {
                debug!(target: "rush-member", "Two or more alerted units have the same round {:?}.", u.round());
                return false;
            }
            rounds.insert(u.round());
        }
        true
    }

    fn validate_alert(&self, alert: &Alert<H, D, Signature>) -> bool {
        // The correctness of forker and sender should be checked in RBC, but no harm
        // to have a check here as well for now.
        if alert.forker.0 >= self.config.n_members.0 {
            debug!(target: "rush-member", "Alert has incorrect forker field {:?}", alert.forker);
            return false;
        }
        if alert.sender.0 >= self.config.n_members.0 {
            debug!(target: "rush-member", "Alert has incorrect sender field {:?}", alert.sender);
            return false;
        }
        if !self.validate_fork_proof(alert.forker, &alert.proof) {
            debug!(target: "rush-member", "Alert has incorrect fork proof.");
            return false;
        }
        if !self.validate_alerted_units(alert.forker, &alert.legit_units) {
            debug!(target: "rush-member", "Alert has incorrect unit/s.");
            return false;
        }
        true
    }

    fn form_alert(
        &self,
        forker: NodeIndex,
        proof: ForkProof<H, D, Signature>,
        units: Vec<SignedUnit<H, D, Signature>>,
    ) -> Alert<H, D, Signature> {
        Alert {
            sender: self
                .config
                .node_id
                .index()
                .expect("Consensus is run only by validators"),
            forker,
            proof,
            legit_units: units,
        }
    }

    fn on_new_forker_detected(&mut self, forker: NodeIndex, proof: ForkProof<H, D, Signature>) {
        let mut alerted_units = self.store.mark_forker(forker);
        if alerted_units.len() > MAX_UNITS_ALERT {
            // The ordering is increasing w.r.t. rounds.
            alerted_units.reverse();
            alerted_units.truncate(MAX_UNITS_ALERT);
            alerted_units.reverse();
        }
        let alert = self.form_alert(forker, proof, alerted_units);
        let message = ConsensusMessage::ForkAlert(alert).encode();
        let command = NetworkCommand::ReliableBroadcast(message);
        self.send_network_command(command);
    }

    fn on_fork_alert(&mut self, alert: Alert<H, D, Signature>) {
        if self.validate_alert(&alert) {
            let forker = alert.forker;
            if !self.store.is_forker(forker) {
                // We learn about this forker for the first time, need to send our own alert
                self.on_new_forker_detected(forker, alert.proof);
            }
            for su in alert.legit_units {
                self.on_unit_received(su, true);
            }
        } else {
            debug!(
                "We have received an incorrect alert from {} on forker {}.",
                alert.sender, alert.forker
            );
        }
    }

    fn on_consensus_message(
        &mut self,
        message: ConsensusMessage<H, D, Signature>,
        peer_id: Vec<u8>,
    ) {
        use ConsensusMessage::*;
        match message {
            NewUnit(signed_unit) => {
                debug!(target: "rush-member", "New unit received {:?}.", signed_unit);
                self.on_unit_received(signed_unit, false);
            }
            RequestCoord(coord) => {
                self.on_request_coord(peer_id, coord);
            }
            ResponseCoord(signed_unit) => {
                debug!(target: "rush-member", "Fetch response received {:?}.", signed_unit);
                self.on_unit_received(signed_unit, false);
            }
            RequestParents(u_hash) => {
                debug!(target: "rush-member", "Parents request received {}.", u_hash);
                self.on_request_parents(peer_id, u_hash);
            }
            ResponseParents(u_hash, parents) => {
                debug!(target: "rush-member", "Response parents received {}.", u_hash);
                // TODO: these responses are quite heavy, we should at some point add
                // checks to make sure we are not processing responses to request we did not make.
                // TODO: we need to check if the response (and alert) does not exceed some max message size in network.
                self.on_parents_response(u_hash, parents);
            }
            ForkAlert(alert) => {
                debug!(target: "rush-member", "Fork alert received {:?}.", alert);
                self.on_fork_alert(alert);
            }
        }
    }

    fn on_ordered_batch(&mut self, batch: Vec<H>) {
        let batch = batch
            .iter()
            .map(|h| {
                self.store
                    .unit_by_hash(h)
                    .expect("Ordered units must be in store")
                    .unit
                    .data
                    .clone()
            })
            .collect::<OrderedBatch<D>>();
        if let Err(e) = self.data_io.send_ordered_batch(batch) {
            debug!(target: "rush-member", "Error when sending batch {:?}.", e);
        }
    }

    fn on_network_event(&mut self, event: NetworkEvent) {
        match event {
            NetworkEvent::MessageReceived(message, sender) => {
                match ConsensusMessage::decode(&mut &message[..]) {
                    Ok(message) => {
                        self.on_consensus_message(message, sender);
                    }
                    Err(e) => {
                        debug!(target: "network", "Error decoding message: {}", e);
                    }
                }
            }
        }
    }

    pub async fn run_session(
        mut self,
        spawn_handle: impl SpawnHandle,
        exit: oneshot::Receiver<()>,
    ) {
        let (tx_consensus, consensus_stream) = unbounded();
        let (consensus_sink, mut rx_consensus) = unbounded();
        let (ordered_batch_tx, mut ordered_batch_rx) = unbounded_channel();
        let (consensus_exit, exit_rx) = oneshot::channel();
        let config = self.config.clone();
        let sh = spawn_handle.clone();
        let hashing = self.hashing;
        debug!(target: "rush-member", "Spawning party for a session with config {:?}", self.config);
        spawn_handle.spawn("consensus/root", async move {
            consensus::run(
                config,
                consensus_stream,
                consensus_sink.sink_map_err(|e| e.into()),
                ordered_batch_tx,
                hashing,
                sh,
                exit_rx,
            )
            .await
        });
        self.tx_consensus = Some(tx_consensus);
        let mut ticker = time::interval(TICK_INTERVAL);
        let mut exit = exit.into_stream();

        debug!(target: "rush-member", "Start routing messages from consensus to network");
        loop {
            tokio::select! {
                notification = rx_consensus.next() => match notification {
                        Some(notification) => self.on_consensus_notification(notification),
                        None => {
                            error!(target: "rush-member", "Consensus notification stream closed.");
                            break;
                        }
                },

                event = self.network.next_event() => match event {
                    Some(event) => self.on_network_event(event),
                    None => {
                        error!(target: "rush-member", "Network message stream closed.");
                        break;
                    }
                },

                batch = ordered_batch_rx.recv() => match batch {
                    Some(batch) => self.on_ordered_batch(batch),
                    None => {
                        error!(target: "rush-member", "Consensus notification stream closed.");
                        break;
                    }
                },

                _ = ticker.tick() => self.trigger_tasks(),
                _ = exit.next() => break,
            }
            self.move_units_to_consensus();
        }

        let _ = consensus_exit.send(());
    }
}
