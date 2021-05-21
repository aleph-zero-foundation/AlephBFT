use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, StreamExt,
};
use log::{debug, error};

use crate::{
    bft::{Alert, ForkProof},
    consensus,
    units::{ControlHash, FullUnit, PreUnit, SignedUnit, Unit, UnitCoord, UnitStore},
    Data, DataIO, Hasher, KeyBox, Network, NetworkCommand, NetworkEvent, NodeCount, NodeIndex,
    NodeMap, OrderedBatch, RequestAuxData, Sender, SessionId, SpawnHandle,
};

use crate::{
    signed::{SignatureError, Signed},
    units::UncheckedSignedUnit,
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
pub(crate) enum ConsensusMessage<H: Hasher, D: Data, S> {
    /// Fo disseminating newly created units.
    NewUnit(UncheckedSignedUnit<H, D, S>),
    /// Request for a unit by its coord.
    RequestCoord(UnitCoord),
    /// Response to a request by coord.
    ResponseCoord(UncheckedSignedUnit<H, D, S>),
    /// Request for the full list of parents of a unit.
    RequestParents(H::Hash),
    /// Response to a request for a full list of parents.
    ResponseParents(H::Hash, Vec<UncheckedSignedUnit<H, D, S>>),
    /// Alert regarding forks,
    ForkAlert(Alert<H, D, S>),
}

/// Type for incoming notifications: Member to Consensus.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NotificationIn<H: Hasher> {
    /// A notification carrying a single unit. This might come either from multicast or
    /// from a response to a request. This is of no importance at this layer.
    NewUnits(Vec<Unit<H>>),
    /// Response to a request to decode parents when the control hash is wrong.
    UnitParents(H::Hash, Vec<H::Hash>),
}

/// Type for outgoing notifications: Consensus to Member.
#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum Task<H: Hasher> {
    CoordRequest(UnitCoord),
    ParentsRequest(H::Hash),
    // the hash of a unit, and the delay before repeating the multicast
    UnitMulticast(H::Hash, time::Duration),
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

pub struct Member<'a, H: Hasher, D: Data, DP: DataIO<D>, KB: KeyBox, N: Network> {
    config: Config,
    tx_consensus: Option<Sender<NotificationIn<H>>>,
    data_io: DP,
    keybox: &'a KB,
    network: N,
    store: UnitStore<'a, H, D, KB>,
    requests: BinaryHeap<ScheduledTask<H>>,
    threshold: NodeCount,
}

impl<'a, H, D, DP, KB, N> Member<'a, H, D, DP, KB, N>
where
    H: Hasher,
    D: Data,
    DP: DataIO<D>,
    KB: KeyBox,
    N: Network,
{
    pub fn new(data_io: DP, keybox: &'a KB, network: N, config: Config) -> Self {
        let n_members = config.n_members;
        let threshold = (n_members * 2) / 3 + NodeCount(1);
        Member {
            config,
            tx_consensus: None,
            data_io,
            keybox,
            network,
            store: UnitStore::new(n_members, threshold),
            requests: BinaryHeap::new(),
            threshold,
        }
    }

    fn node_ix(&self) -> NodeIndex {
        self.config.node_id
    }

    fn send_consensus_notification(&mut self, notification: NotificationIn<H>) {
        if let Err(e) = self
            .tx_consensus
            .as_ref()
            .unwrap()
            .unbounded_send(notification)
        {
            debug!(target: "rush-member", "{} Error when sending notification {:?}.", self.node_ix(), e);
        }
    }

    fn on_create(&mut self, u: PreUnit<H>) {
        debug!(target: "rush-member", "{} On create notification.", self.node_ix());
        let data = self.data_io.get_data();
        let full_unit = FullUnit::new(u, data, self.config.session_id);
        let hash = full_unit.hash();
        let signed_unit = Signed::sign(self.keybox, full_unit);
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

    fn schedule_parents_request(&mut self, u_hash: H::Hash, curr_time: time::Instant) {
        if self.store.get_parents(u_hash).is_none() {
            let message = ConsensusMessage::<H, D, KB::Signature>::RequestParents(u_hash);
            let command = NetworkCommand::SendToRandPeer(message.encode());
            self.send_network_command(command);
            debug!(target: "rush-member", "{} Fetch parents for {:?} sent.", self.node_ix(), u_hash);
            self.requests.push(ScheduledTask::new(
                Task::ParentsRequest(u_hash),
                curr_time + FETCH_INTERVAL,
            ));
        } else {
            debug!(target: "rush-member", "{} Request dropped as the parents are in store for {:?}.", self.node_ix(), u_hash);
        }
    }

    fn schedule_coord_request(&mut self, coord: UnitCoord, curr_time: time::Instant) {
        debug!(target: "rush-member", "{} Starting request for {:?}", self.node_ix(), coord);
        // If we already have a unit with such a coord in our store then there is no need to request it.
        // It will be sent to consensus soon (or have already been sent).
        if self.store.contains_coord(&coord) {
            debug!(target: "rush-member", "{} Request dropped as the unit is in store already {:?}", self.node_ix(), coord);
            return;
        }
        let message = ConsensusMessage::<H, D, KB::Signature>::RequestCoord(coord);
        let command = NetworkCommand::SendToRandPeer(message.encode());
        self.send_network_command(command);
        debug!(target: "rush-member", "{} Fetch request for {:?} sent.", self.node_ix(), coord);
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
        let message = ConsensusMessage::<H, D, KB::Signature>::NewUnit(signed_unit.into());
        let command = NetworkCommand::SendToAll(message.encode());
        debug!(target: "rush-member", "{} Sending a unit {:?} over network after delay {:?}.", self.node_ix(), hash, interval);
        self.send_network_command(command);
        // NOTE: we double the delay each time
        self.requests.push(ScheduledTask::new(
            Task::UnitMulticast(hash, interval * 2),
            curr_time + interval,
        ));
    }

    pub(crate) fn on_missing_coords(&mut self, coords: Vec<UnitCoord>) {
        debug!(target: "rush-member", "{} Dealing with missing coords notification {:?}.", self.node_ix(), coords);
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
        debug!(target: "rush-member", "{} Dealing with wrong control hash notification {:?}.", self.node_ix(), u_hash);
        if let Some(p_hashes) = self.store.get_parents(u_hash) {
            // We have the parents by some strange reason (someone sent us parents
            // without us requesting them).
            let p_hashes = p_hashes.clone();
            debug!(target: "rush-member", "{} We have the parents for {:?} even though we did not request them.", self.node_ix(), u_hash);
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
            debug!(target: "rush-member", "{} Unit with wrong length of parents map.", self.node_ix());
            return false;
        }
        let round = pre_unit.round();
        let n_parents = pre_unit.n_parents();
        if round == 0 && n_parents > NodeCount(0) {
            debug!(target: "rush-member", "{} Unit of round zero with non-zero number of parents.", self.node_ix());
            return false;
        }
        let threshold = self.threshold;
        if round > 0 && n_parents < threshold {
            debug!(target: "rush-member", "{} Unit of non-zero round with only {:?} parents while at least {:?} are required.", self.node_ix(), n_parents, threshold);
            return false;
        }
        let control_hash = &pre_unit.control_hash();
        if round > 0 && !control_hash.parents_mask[pre_unit.creator().0] {
            debug!(target: "rush-member", "{} Unit does not have its creator's previous unit as parent.", self.node_ix());
            return false;
        }
        true
    }

    fn validate_unit(&self, su: &SignedUnit<'a, H, D, KB>) -> bool {
        let full_unit = su.as_signable();
        if full_unit.session_id() != self.config.session_id {
            // NOTE: this implies malicious behavior as the unit's session_id
            // is incompatible with session_id of the message it arrived in.
            debug!(target: "rush-member", "{} A unit with incorrect session_id! {:?}", self.node_ix(), full_unit);
            return false;
        }
        if full_unit.round() > self.store.limit_per_node() {
            debug!(target: "rush-member", "{} A unit with too high round {}! {:?}", self.node_ix(), full_unit.round(), full_unit);
            return false;
        }
        if full_unit.creator().0 >= self.config.n_members.0 {
            debug!(target: "rush-member", "{} A unit with too high creator index {}! {:?}", self.node_ix(), full_unit.creator().0, full_unit);
            return false;
        }
        if !self.validate_unit_parents(su) {
            debug!(target: "rush-member", "{} A unit did not pass parents validation. {:?}", self.node_ix(), full_unit);
            return false;
        }
        true
    }

    fn add_unit_to_store_unless_fork(&mut self, su: SignedUnit<'a, H, D, KB>) {
        let full_unit = su.as_signable();
        debug!(target: "rush-member", "{} Adding member unit to store {:?}", self.node_ix(), full_unit);
        if self.store.is_forker(full_unit.creator()) {
            debug!(target: "rush-member", "{} Ignoring forker's unit {:?}", self.node_ix(), full_unit);
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
            debug!(target: "rush-member", "{} Unit {:?} ignored because of too high round {} when round in progress is {}.", self.node_ix(), full_unit, u_round, round_in_progress);
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

    fn on_request_coord(&mut self, peer_id: Vec<u8>, coord: UnitCoord) {
        debug!(target: "rush-member", "{} Received fetch request for coord {:?} from {:?}.", self.node_ix(), coord, peer_id);
        let maybe_su = (self.store.unit_by_coord(coord)).cloned();

        if let Some(su) = maybe_su {
            debug!(target: "rush-member", "{} Answering fetch request for coord {:?} from {:?}.", self.node_ix(), coord, peer_id);
            let message = ConsensusMessage::ResponseCoord(su.into());
            let command = NetworkCommand::SendToPeer(message.encode(), peer_id);
            self.send_network_command(command);
        } else {
            debug!(target: "rush-member", "{} Not answering fetch request for coord {:?}. Unit not in store.", self.node_ix(), coord);
        }
    }

    fn send_network_command(&mut self, command: NetworkCommand) {
        if let Err(e) = self.network.send(command) {
            debug!(target: "rush-member", "{} Failed to send network command {:?}.", self.node_ix(), e);
        }
    }

    fn on_request_parents(&mut self, peer_id: Vec<u8>, u_hash: H::Hash) {
        debug!(target: "rush-member", "{} Received parents request for hash {:?} from {:?}.", self.node_ix(), u_hash, peer_id);
        let maybe_p_hashes = self.store.get_parents(u_hash);

        if let Some(p_hashes) = maybe_p_hashes {
            let p_hashes = p_hashes.clone();
            debug!(target: "rush-member", "{} Answering parents request for hash {:?} from {:?}.", self.node_ix(), u_hash, peer_id);
            let full_units = p_hashes
                .into_iter()
                .map(|hash| self.store.unit_by_hash(&hash).unwrap().clone().into())
                .collect();
            let message = ConsensusMessage::ResponseParents(u_hash, full_units).encode();
            let command = NetworkCommand::SendToPeer(message, peer_id);
            self.send_network_command(command);
        } else {
            debug!(target: "rush-member", "{} Not answering parents request for hash {:?}. Unit not in DAG yet.", self.node_ix(), u_hash);
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
                debug!(target: "rush-member", "{} We got parents but don't even know the unit. Ignoring.", self.node_ix());
                return;
            }
        };

        if parent_ids.len() != parents.len() {
            debug!(target: "rush-member", "{} In received parent response expected {} parents got {} for unit {:?}.", self.node_ix(), parents.len(), parent_ids.len(), u_hash);
            return;
        }

        let mut p_hashes_node_map: NodeMap<Option<H::Hash>> =
            NodeMap::new_with_len(self.config.n_members);
        for (i, su) in parents.into_iter().enumerate() {
            let full_unit = su.as_signable();
            if full_unit.round() + 1 != u_round {
                debug!(target: "rush-member", "{} In received parent response received a unit with wrong round.", self.node_ix());
                return;
            }
            if full_unit.creator() != parent_ids[i] {
                debug!(target: "rush-member", "{} In received parent response received a unit with wrong creator.", self.node_ix());
                return;
            }
            if !self.validate_unit(&su) {
                debug!(target: "rush-member", "{} In received parent response received a unit that does not pass validation.", self.node_ix());
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
            debug!(target: "rush-member", "{} In received parent response the control hash is incorrect {:?}.", self.node_ix(), p_hashes_node_map);
            return;
        }
        let p_hashes: Vec<H::Hash> = p_hashes_node_map.into_iter().flatten().collect();
        self.store.add_parents(u_hash, p_hashes.clone());
        debug!(target: "rush-member", "{} Succesful parents reponse for {:?}.", self.node_ix(), u_hash);
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
                    debug!(target: "rush-member", "{} Invalid signatures in a proof.", self.node_ix());
                    return false;
                }
            }
        };
        if !self.validate_unit(&u1) || !self.validate_unit(&u2) {
            debug!(target: "rush-member", "{} One of the units in the proof is invalid.", self.node_ix());
            return false;
        }
        let full_unit1 = u1.as_signable();
        let full_unit2 = u2.as_signable();
        if full_unit1.creator() != forker || full_unit2.creator() != forker {
            debug!(target: "rush-member", "{} One of the units creators in proof does not match.", self.node_ix());
            return false;
        }
        if full_unit1.round() != full_unit2.round() {
            debug!(target: "rush-member", "{} The rounds in proof's units do not match.", self.node_ix());
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
            debug!(target: "rush-member", "{} Too many units: {} included in alert.", self.node_ix(), units.len());
            return false;
        }
        let mut rounds: HashSet<usize> = HashSet::new();
        for u in units {
            let full_unit = u.as_signable();
            if full_unit.creator() != forker {
                debug!(target: "rush-member", "{} One of the units {:?} has wrong creator.", self.node_ix(), full_unit);
                return false;
            }
            if !self.validate_unit(u) {
                debug!(target: "rush-member", "{} One of the units {:?} in alert does not pass validation.", self.node_ix(), full_unit);
                return false;
            }
            if rounds.contains(&full_unit.round()) {
                debug!(target: "rush-member", "{} Two or more alerted units have the same round {:?}.", self.node_ix(), full_unit.round());
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
            debug!(target: "rush-member", "{} Alert has incorrect forker field {:?}", self.node_ix(), alert.forker);
            return false;
        }
        if alert.sender.0 >= self.config.n_members.0 {
            debug!(target: "rush-member", "{} Alert has incorrect sender field {:?}", self.node_ix(), alert.sender);
            return false;
        }
        if !self.validate_fork_proof(alert.forker, &alert.proof) {
            debug!(target: "rush-member", "{} Alert has incorrect fork proof.", self.node_ix());
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
                debug!(target: "rush-member", "{} Alert has a badly signed unit: {:?}.", self.node_ix(), e);
                return false;
            }
        };
        if !self.validate_alerted_units(alert.forker, &legit_units[..]) {
            debug!(target: "rush-member", "{} Alert has incorrect unit/s.", self.node_ix());
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
        let message = ConsensusMessage::ForkAlert(alert).encode();
        let command = NetworkCommand::ReliableBroadcast(message);
        self.send_network_command(command);
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
            debug!(target: "rush-member","{} We have received an incorrect alert from {} on forker {}.", self.node_ix(), alert.sender, alert.forker);
        }
    }

    fn on_consensus_message(
        &mut self,
        message: ConsensusMessage<H, D, KB::Signature>,
        peer_id: Vec<u8>,
    ) {
        use ConsensusMessage::*;
        match message {
            NewUnit(unchecked) => {
                debug!(target: "rush-member", "{} New unit received {:?}.", self.node_ix(), &unchecked);
                match unchecked.check(self.keybox) {
                    Ok(su) => self.on_unit_received(su, false),
                    Err(unchecked) => {
                        debug!(target: "rush-member", "{} Wrong signature received {:?}.", self.node_ix(), &unchecked)
                    }
                }
            }
            RequestCoord(coord) => {
                self.on_request_coord(peer_id, coord);
            }
            ResponseCoord(unchecked) => {
                debug!(target: "rush-member", "{} Fetch response received {:?}.", self.node_ix(), &unchecked);

                match unchecked.check(self.keybox) {
                    Ok(su) => self.on_unit_received(su, false),
                    Err(unchecked) => {
                        debug!(target: "rush-member", "{} Wrong signature received {:?}.", self.node_ix(), &unchecked)
                    }
                }
            }
            RequestParents(u_hash) => {
                debug!(target: "rush-member", "{} Parents request received {:?}.", self.node_ix(), u_hash);
                self.on_request_parents(peer_id, u_hash);
            }
            ResponseParents(u_hash, parents) => {
                debug!(target: "rush-member", "{} Response parents received {:?}.", self.node_ix(), u_hash);
                let parents: Result<Vec<_>, SignatureError<_, _>> = parents
                    .into_iter()
                    .map(|unchecked| unchecked.check(self.keybox))
                    .collect();
                match parents {
                    Ok(parents) => self.on_parents_response(u_hash, parents),
                    Err(err) => {
                        debug!(target: "rush-member", "{} Bad signature received {:?}.", self.node_ix(), err)
                    }
                }
            }
            ForkAlert(alert) => {
                debug!(target: "rush-member", "{} Fork alert received {:?}.", self.node_ix(), alert);
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
            debug!(target: "rush-member", "{} Error when sending batch {:?}.", self.node_ix(), e);
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
                        debug!(target: "network", "{} Error decoding message: {}", self.node_ix(), e);
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
        let (tx_consensus, consensus_stream) = mpsc::unbounded();
        let (consensus_sink, mut rx_consensus) = mpsc::unbounded();
        let (ordered_batch_tx, mut ordered_batch_rx) = mpsc::unbounded();
        let (consensus_exit, exit_rx) = oneshot::channel();
        let config = self.config.clone();
        let sh = spawn_handle.clone();
        debug!(target: "rush-member", "{} Spawning party for a session with config {:?}", self.node_ix(), self.config);
        spawn_handle.spawn("consensus/root", async move {
            consensus::run(
                config,
                consensus_stream,
                consensus_sink,
                ordered_batch_tx,
                sh,
                exit_rx,
            )
            .await
        });
        self.tx_consensus = Some(tx_consensus);
        let mut ticker = time::interval(TICK_INTERVAL);
        let mut exit = exit.into_stream();

        debug!(target: "rush-member", "{} Start routing messages from consensus to network", self.node_ix());
        loop {
            tokio::select! {
                notification = rx_consensus.next() => match notification {
                        Some(notification) => self.on_consensus_notification(notification),
                        None => {
                            error!(target: "rush-member", "{} Consensus notification stream closed.", self.node_ix());
                            break;
                        }
                },

                event = self.network.next_event() => match event {
                    Some(event) => self.on_network_event(event),
                    None => {
                        error!(target: "rush-member", "{} Network message stream closed.", self.node_ix());
                        break;
                    }
                },

                batch = ordered_batch_rx.next() => match batch {
                    Some(batch) => self.on_ordered_batch(batch),
                    None => {
                        error!(target: "rush-member", "{} Consensus notification stream closed.", self.node_ix());
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
