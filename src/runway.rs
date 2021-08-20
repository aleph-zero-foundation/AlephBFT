use std::collections::HashSet;

use crate::{
    alerts::{Alert, AlertConfig, AlertMessage, Alerter, ForkProof, ForkingNotification},
    consensus,
    member::UnitMessage,
    network::Recipient,
    nodes::NodeMap,
    units::{
        ControlHash, FullUnit, PreUnit, SignedUnit, UncheckedSignedUnit, Unit, UnitCoord, UnitStore,
    },
    utils::into_infinite_stream,
    Config, Data, DataIO, Hasher, Index, MultiKeychain, NodeCount, NodeIndex, OrderedBatch,
    Receiver, Sender, Signature, Signed, SpawnHandle,
};
use futures::{
    channel::{mpsc, oneshot},
    pin_mut, StreamExt,
};
use log::{debug, error, info, trace, warn};

/// Type for incoming notifications: Runway to Consensus.
#[derive(Clone, PartialEq)]
pub(crate) enum NotificationIn<H: Hasher> {
    /// A notification carrying a single unit. This might come either from multicast or
    /// from a response to a request. This is of no importance at this layer.
    NewUnits(Vec<Unit<H>>),
    /// Response to a request to decode parents when the control hash is wrong.
    UnitParents(H::Hash, Vec<H::Hash>),
}

/// Type for outgoing notifications: Consensus to Runway.
#[derive(Debug, PartialEq)]
pub(crate) enum NotificationOut<H: Hasher> {
    /// Notification about a preunit created by this Consensus Node. Member is meant to
    /// disseminate this preunit among other nodes.
    CreatedPreUnit(PreUnit<H>, Vec<H::Hash>),
    /// Notification that some units are needed but missing. The role of the Member
    /// is to fetch these unit (somehow).
    MissingUnits(Vec<UnitCoord>),
    /// Notification that Consensus has parents incompatible with the control hash.
    WrongControlHash(H::Hash),
    /// Notification that a new unit has been added to the DAG, list of decoded parents provided
    AddedToDag(H::Hash, Vec<H::Hash>),
}

pub(crate) enum Request<H: Hasher> {
    RequestCoord(UnitCoord),
    RequestParents(H::Hash),
}

pub(crate) type RequestIn<H> = (Request<H>, NodeIndex);

pub(crate) type RequestOut<H> = (Request<H>, Recipient);

pub(crate) type ResponseOut<H, D, S> = (Response<H, D, S>, NodeIndex);

pub(crate) enum Response<H: Hasher, D: Data, S: Signature> {
    ResponseCoord(UncheckedSignedUnit<H, D, S>),
    ResponseParents(H::Hash, Vec<UncheckedSignedUnit<H, D, S>>),
}

pub(crate) enum RunwayNotification<H: Hasher, D: Data, S: Signature, ROut, RIn> {
    NewUnit(UncheckedSignedUnit<H, D, S>),
    Request(ROut),
    Response(RIn),
}

pub(crate) type RunwayNotificationOut<H, D, S> =
    RunwayNotification<H, D, S, RequestOut<H>, ResponseOut<H, D, S>>;

pub(crate) type RunwayNotificationIn<H, D, S> =
    RunwayNotification<H, D, S, RequestIn<H>, Response<H, D, S>>;

impl<H: Hasher, D: Data, S: Signature> From<UnitMessage<H, D, S>>
    for RunwayNotificationIn<H, D, S>
{
    fn from(message: UnitMessage<H, D, S>) -> Self {
        match message {
            UnitMessage::NewUnit(u) => RunwayNotificationIn::NewUnit(u),
            UnitMessage::RequestCoord(peer_id, coord) => {
                RunwayNotificationIn::Request((Request::RequestCoord(coord), peer_id))
            }
            UnitMessage::RequestParents(peer_id, u_hash) => {
                RunwayNotificationIn::Request((Request::RequestParents(u_hash), peer_id))
            }
            UnitMessage::ResponseCoord(u) => {
                RunwayNotificationIn::Response(Response::ResponseCoord(u))
            }
            UnitMessage::ResponseParents(u_hash, parents) => {
                RunwayNotificationIn::Response(Response::ResponseParents(u_hash, parents))
            }
        }
    }
}
pub(crate) struct Runway<H, D, MK, DP>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
{
    missing_coords: HashSet<UnitCoord>,
    missing_parents: HashSet<H::Hash>,
    config: Config,
    threshold: NodeCount,
    store: UnitStore<H, D, MK>,
    keybox: MK,
    alerts_for_alerter: Sender<Alert<H, D, MK::Signature>>,
    notifications_from_alerter: Receiver<ForkingNotification<H, D, MK::Signature>>,
    unit_messages_from_network: Receiver<RunwayNotificationIn<H, D, MK::Signature>>,
    unit_messages_for_network: Sender<RunwayNotificationOut<H, D, MK::Signature>>,
    resolved_requests: Sender<Request<H>>,
    tx_consensus: Sender<NotificationIn<H>>,
    rx_consensus: Receiver<NotificationOut<H>>,
    ordered_batch_rx: Receiver<Vec<H::Hash>>,
    data_io: DP,
}

struct RunwayConfig<H: Hasher, D: Data, DP: DataIO<D>, MK: MultiKeychain> {
    config: Config,
    keychain: MK,
    data_io: DP,
    alerts_for_alerter: Sender<Alert<H, D, MK::Signature>>,
    notifications_from_alerter: Receiver<ForkingNotification<H, D, MK::Signature>>,
    tx_consensus: Sender<NotificationIn<H>>,
    rx_consensus: Receiver<NotificationOut<H>>,
    unit_messages_from_network: Receiver<RunwayNotificationIn<H, D, MK::Signature>>,
    unit_messages_for_network: Sender<RunwayNotificationOut<H, D, MK::Signature>>,
    ordered_batch_rx: Receiver<Vec<H::Hash>>,
    resolved_requests: Sender<Request<H>>,
}

impl<H, D, MK, DP> Runway<H, D, MK, DP>
where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
{
    fn new(config: RunwayConfig<H, D, DP, MK>) -> Self {
        let n_members = config.config.n_members;
        let threshold = (n_members * 2) / 3 + NodeCount(1);
        let max_round = config.config.max_round;
        let store = UnitStore::new(n_members, max_round);

        Runway {
            config: config.config,
            threshold,
            store,
            keybox: config.keychain,
            missing_coords: HashSet::new(),
            missing_parents: HashSet::new(),
            resolved_requests: config.resolved_requests,
            alerts_for_alerter: config.alerts_for_alerter,
            notifications_from_alerter: config.notifications_from_alerter,
            unit_messages_from_network: config.unit_messages_from_network,
            unit_messages_for_network: config.unit_messages_for_network,
            tx_consensus: config.tx_consensus,
            rx_consensus: config.rx_consensus,
            ordered_batch_rx: config.ordered_batch_rx,
            data_io: config.data_io,
        }
    }

    fn index(&self) -> NodeIndex {
        self.config.node_ix
    }

    fn on_unit_message(&mut self, message: RunwayNotificationIn<H, D, MK::Signature>) {
        match message {
            RunwayNotification::NewUnit(u) => {
                trace!(target: "AlephBFT-runway", "{:?} New unit received {:?}.", self.index(), &u);
                self.on_unit_received(u, false)
            }
            RunwayNotification::Request((request, peer_id)) => match request {
                Request::RequestCoord(coord) => {
                    trace!(target: "AlephBFT-runway", "{:?} Coords request received {:?}.", self.index(), coord);
                    self.on_request_coord(peer_id, coord)
                }
                Request::RequestParents(u_hash) => {
                    trace!(target: "AlephBFT-runway", "{:?} Parents request received {:?}.", self.index(), u_hash);
                    self.on_request_parents(peer_id, u_hash)
                }
            },
            RunwayNotification::Response(res) => match res {
                Response::ResponseCoord(u) => {
                    trace!(target: "AlephBFT-runway", "{:?} Fetch response received {:?}.", self.index(), &u);
                    self.on_unit_received(u, false)
                }
                Response::ResponseParents(u_hash, parents) => {
                    trace!(target: "AlephBFT-runway", "{:?} Response parents received {:?}.", self.index(), u_hash);
                    self.on_parents_response(u_hash, parents)
                }
            },
        }
    }

    fn on_unit_received(&mut self, uu: UncheckedSignedUnit<H, D, MK::Signature>, alert: bool) {
        if let Some(su) = self.validate_unit(uu) {
            if alert {
                // Units from alerts explicitly come from forkers, and we want them anyway.
                let coord = su.as_signable().coord();
                self.resolve_missing_coord(&coord);
                self.store.add_unit(su, true);
            } else {
                self.add_unit_to_store_unless_fork(su);
            }
        }
    }

    fn resolve_missing_coord(&mut self, coord: &UnitCoord) {
        if self.missing_coords.remove(coord) {
            self.resolved_requests
                .unbounded_send(Request::RequestCoord(*coord))
                .expect("resolved_requests channel should be open")
        }
    }

    // TODO: we should return an error and handle it outside
    fn validate_unit(
        &self,
        uu: UncheckedSignedUnit<H, D, MK::Signature>,
    ) -> Option<SignedUnit<H, D, MK>> {
        let su = match uu.check(&self.keybox) {
            Ok(su) => su,
            Err(uu) => {
                warn!(target: "AlephBFT-runway", "{:?} Wrong signature received {:?}.", self.index(), &uu);
                return None;
            }
        };
        let full_unit = su.as_signable();
        if full_unit.session_id() != self.config.session_id {
            // NOTE: this implies malicious behavior as the unit's session_id
            // is incompatible with session_id of the message it arrived in.
            warn!(target: "AlephBFT-runway", "{:?} A unit with incorrect session_id! {:?}", self.index(), full_unit);
            return None;
        }
        if full_unit.round() > self.store.limit_per_node() {
            warn!(target: "AlephBFT-runway", "{:?} A unit with too high round {}! {:?}", self.index(), full_unit.round(), full_unit);
            return None;
        }
        if full_unit.creator().0 >= self.config.n_members.0 {
            warn!(target: "AlephBFT-runway", "{:?} A unit with too high creator index {}! {:?}", self.index(), full_unit.creator().0, full_unit);
            return None;
        }
        if !self.validate_unit_parents(&su) {
            warn!(target: "AlephBFT-runway", "{:?} A unit did not pass parents validation. {:?}", self.index(), full_unit);
            return None;
        }
        Some(su)
    }

    fn add_unit_to_store_unless_fork(&mut self, su: SignedUnit<H, D, MK>) {
        let full_unit = su.as_signable();
        trace!(target: "AlephBFT-member", "{:?} Adding member unit to store {:?}", self.index(), full_unit);
        if self.store.is_forker(full_unit.creator()) {
            trace!(target: "AlephBFT-member", "{:?} Ignoring forker's unit {:?}", self.index(), full_unit);
            return;
        }
        if let Some(sv) = self.store.is_new_fork(full_unit) {
            let creator = full_unit.creator();
            if !self.store.is_forker(creator) {
                // We need to mark the forker if it is not known yet.
                let proof = (su.into(), sv.into());
                self.on_new_forker_detected(creator, proof);
            }
            // We ignore this unit. If it is legit, it will arrive in some alert and we need to wait anyway.
            // There is no point in keeping this unit in any kind of buffer.
            return;
        }

        let coord = su.as_signable().coord();
        self.resolve_missing_coord(&coord);
        self.store.add_unit(su, false);
    }

    fn validate_unit_parents(&self, su: &SignedUnit<H, D, MK>) -> bool {
        // NOTE: at this point we cannot validate correctness of the control hash, in principle it could be
        // just a random hash, but we still would not be able to deduce that by looking at the unit only.
        let pre_unit = su.as_signable().as_pre_unit();
        if pre_unit.n_members() != self.config.n_members {
            warn!(target: "AlephBFT-runway", "{:?} Unit with wrong length of parents map.", self.index());
            return false;
        }
        let round = pre_unit.round();
        let n_parents = pre_unit.n_parents();
        if round == 0 && n_parents > NodeCount(0) {
            warn!(target: "AlephBFT-runway", "{:?} Unit of round zero with non-zero number of parents.", self.index());
            return false;
        }
        let threshold = self.threshold();
        if round > 0 && n_parents < threshold {
            warn!(target: "AlephBFT-runway", "{:?} Unit of non-zero round with only {:?} parents while at least {:?} are required.", self.index(), n_parents, threshold);
            return false;
        }
        let control_hash = &pre_unit.control_hash();
        if round > 0 && !control_hash.parents_mask[pre_unit.creator()] {
            warn!(target: "AlephBFT-runway", "{:?} Unit does not have its creator's previous unit as parent.", self.index());
            return false;
        }
        true
    }

    fn threshold(&self) -> NodeCount {
        self.threshold
    }

    fn on_new_forker_detected(&mut self, forker: NodeIndex, proof: ForkProof<H, D, MK::Signature>) {
        let alerted_units = self.store.mark_forker(forker);
        let alert = self.form_alert(proof, alerted_units);
        self.alerts_for_alerter
            .unbounded_send(alert)
            .expect("Channel to alerter should be open")
    }

    fn form_alert(
        &self,
        proof: ForkProof<H, D, MK::Signature>,
        units: Vec<SignedUnit<H, D, MK>>,
    ) -> Alert<H, D, MK::Signature> {
        Alert::new(
            self.config.node_ix,
            proof,
            units.into_iter().map(|signed| signed.into()).collect(),
        )
    }

    fn on_request_coord(&mut self, peer_id: NodeIndex, coord: UnitCoord) {
        debug!(target: "AlephBFT-runway", "{:?} Received fetch request for coord {:?} from {:?}.", self.index(), coord, peer_id);
        let maybe_su = (self.store.unit_by_coord(coord)).cloned();

        if let Some(su) = maybe_su {
            trace!(target: "AlephBFT-runway", "{:?} Answering fetch request for coord {:?} from {:?}.", self.index(), coord, peer_id);
            self.unit_messages_for_network
                .unbounded_send(RunwayNotification::Response((
                    Response::ResponseCoord(su.into()),
                    peer_id,
                )))
                .expect("unit_messages_for_network channel should be open")
        } else {
            trace!(target: "AlephBFT-runway", "{:?} Not answering fetch request for coord {:?}. Unit not in store.", self.index(), coord);
        }
    }

    fn on_request_parents(&mut self, peer_id: NodeIndex, u_hash: H::Hash) {
        debug!(target: "AlephBFT-runway", "{:?} Received parents request for hash {:?} from {:?}.", self.index(), u_hash, peer_id);

        if let Some(p_hashes) = self.store.get_parents(u_hash) {
            let p_hashes = p_hashes.clone();
            trace!(target: "AlephBFT-runway", "{:?} Answering parents request for hash {:?} from {:?}.", self.index(), u_hash, peer_id);
            let mut full_units = Vec::new();
            for hash in p_hashes.iter() {
                if let Some(fu) = self.store.unit_by_hash(hash) {
                    full_units.push(fu.clone().into());
                } else {
                    debug!(target: "AlephBFT-runway", "{:?} Not answering parents request, one of the parents missing from store.", self.index());
                    //This can happen if we got a parents response from someone, but one of the units was a fork and we dropped it.
                    //Either this parent is legit and we will soon get it in alert or the parent is not legit in which case
                    //the unit u, whose parents are beeing seeked here is not legit either.
                    //In any case, if a node added u to its Dag, then it should never reach this place in code when answering
                    //a parents request (as all the parents must be legit an thus must be in store).
                    return;
                }
            }
            self.unit_messages_for_network
                .unbounded_send(RunwayNotificationOut::Response((
                    Response::ResponseParents(u_hash, full_units),
                    peer_id,
                )))
                .expect("unit_messages_for_network channel should be open")
        } else {
            trace!(target: "AlephBFT-runway", "{:?} Not answering parents request for hash {:?}. Unit not in DAG yet.", self.index(), u_hash);
        }
    }

    fn on_parents_response(
        &mut self,
        u_hash: H::Hash,
        parents: Vec<UncheckedSignedUnit<H, D, MK::Signature>>,
    ) {
        if self.store.get_parents(u_hash).is_some() {
            trace!(target: "AlephBFT-runway", "{:?} We got parents response but already know the parents.", self.index());
            return;
        }
        let (u_round, u_control_hash, parent_ids) = match self.store.unit_by_hash(&u_hash) {
            Some(su) => {
                let full_unit = su.as_signable();
                let parent_ids: Vec<_> = full_unit.control_hash().parents().collect();
                (
                    full_unit.round(),
                    full_unit.control_hash().combined_hash,
                    parent_ids,
                )
            }
            None => {
                trace!(target: "AlephBFT-runway", "{:?} We got parents but don't even know the unit. Ignoring.", self.index());
                return;
            }
        };

        if parent_ids.len() != parents.len() {
            warn!(target: "AlephBFT-runway", "{:?} In received parent response expected {} parents got {} for unit {:?}.", self.index(), parents.len(), parent_ids.len(), u_hash);
            return;
        }

        let mut p_hashes_node_map: NodeMap<Option<H::Hash>> =
            NodeMap::new_with_len(self.config.n_members);
        for (i, uu) in parents.into_iter().enumerate() {
            let su = match self.validate_unit(uu) {
                None => {
                    warn!(target: "AlephBFT-runway", "{:?} In received parent response received a unit that does not pass validation.", self.index());
                    return;
                }
                Some(su) => su,
            };
            let full_unit = su.as_signable();
            if full_unit.round() + 1 != u_round {
                warn!(target: "AlephBFT-runway", "{:?} In received parent response received a unit with wrong round.", self.index());
                return;
            }
            if full_unit.creator() != parent_ids[i] {
                warn!(target: "AlephBFT-runway", "{:?} In received parent response received a unit with wrong creator.", self.index());
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
            warn!(target: "AlephBFT-runway", "{:?} In received parent response the control hash is incorrect {:?}.", self.index(), p_hashes_node_map);
            return;
        }
        let p_hashes: Vec<H::Hash> = p_hashes_node_map.into_iter().flatten().collect();
        self.store.add_parents(u_hash, p_hashes.clone());
        self.resolve_missing_parents(&u_hash);
        trace!(target: "AlephBFT-runway", "{:?} Succesful parents response for {:?}.", self.index(), u_hash);
        self.send_consensus_notification(NotificationIn::UnitParents(u_hash, p_hashes));
    }

    fn resolve_missing_parents(&mut self, u_hash: &H::Hash) {
        if self.missing_parents.remove(u_hash) {
            self.resolved_requests
                .unbounded_send(Request::RequestParents(*u_hash))
                .expect("resolved_requests channel should be open")
        }
    }

    fn send_consensus_notification(&mut self, notification: NotificationIn<H>) {
        self.tx_consensus
            .unbounded_send(notification)
            .expect("Channel to consensus should be open")
    }

    async fn on_create(&mut self, u: PreUnit<H>) {
        debug!(target: "AlephBFT-runway", "{:?} On create notification.", self.index());
        let data = self.data_io.get_data();
        let full_unit = FullUnit::new(u, data, self.config.session_id);
        let hash: <H as Hasher>::Hash = full_unit.hash();
        let signed_unit: Signed<FullUnit<H, D>, MK> = Signed::sign(full_unit, &self.keybox).await;
        self.store.add_unit(signed_unit.clone(), false);

        trace!(target: "AlephBFT-runway", "{:?} Sending a unit {:?}.", self.index(), hash);
        self.unit_messages_for_network
            .unbounded_send(RunwayNotificationOut::NewUnit(signed_unit.into()))
            .expect("network's channel should be open")
    }

    fn on_alert_notification(&mut self, notification: ForkingNotification<H, D, MK::Signature>) {
        use ForkingNotification::*;
        match notification {
            Forker(proof) => {
                let forker = proof.0.index();
                if !self.store.is_forker(forker) {
                    self.on_new_forker_detected(forker, proof);
                }
            }
            Units(units) => {
                for uu in units {
                    self.on_unit_received(uu, true);
                }
            }
        }
    }

    async fn on_consensus_notification(&mut self, notification: NotificationOut<H>) {
        match notification {
            NotificationOut::CreatedPreUnit(pu, _) => {
                self.on_create(pu).await;
            }
            NotificationOut::MissingUnits(coords) => {
                self.on_missing_coords(coords);
            }
            NotificationOut::WrongControlHash(h) => {
                self.on_wrong_control_hash(h);
            }
            NotificationOut::AddedToDag(h, p_hashes) => {
                self.store.add_parents(h, p_hashes);
                self.resolve_missing_parents(&h);
            }
        }
    }

    fn on_missing_coords(&mut self, mut coords: Vec<UnitCoord>) {
        trace!(target: "AlephBFT-runway", "{:?} Dealing with missing coords notification {:?}.", self.index(), coords);
        coords.retain(|coord| !self.store.contains_coord(coord));
        for coord in coords {
            if self.missing_coords.insert(coord) {
                self.unit_messages_for_network
                    .unbounded_send(RunwayNotificationOut::Request((
                        Request::RequestCoord(coord),
                        Recipient::Node(coord.creator()),
                    )))
                    .expect("unit_messages_for_network channel should be open")
            }
        }
    }

    fn on_wrong_control_hash(&mut self, u_hash: H::Hash) {
        trace!(target: "AlephBFT-runway", "{:?} Dealing with wrong control hash notification {:?}.", self.index(), u_hash);
        if let Some(p_hashes) = self.store.get_parents(u_hash) {
            // We have the parents by some strange reason (someone sent us parents
            // without us requesting them).
            let p_hashes = p_hashes.clone();
            trace!(target: "AlephBFT-runway", "{:?} We have the parents for {:?} even though we did not request them.", self.index(), u_hash);
            let notification = NotificationIn::UnitParents(u_hash, p_hashes);
            self.send_consensus_notification(notification);
        } else {
            let peer_id = self
                .store
                .unit_by_hash(&u_hash)
                .map(|u| u.as_signable().creator());
            let recipient = if let Some(peer_id) = peer_id {
                Recipient::Node(peer_id)
            } else {
                Recipient::Everyone
            };
            if self.missing_parents.insert(u_hash) {
                self.unit_messages_for_network
                    .unbounded_send(RunwayNotificationOut::Request((
                        Request::RequestParents(u_hash),
                        recipient,
                    )))
                    .expect("unit_messages_for_network channel should be open")
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
                    .clone()
            })
            .collect::<OrderedBatch<D>>();
        if let Err(e) = self.data_io.send_ordered_batch(batch) {
            error!(target: "AlephBFT-runway", "{:?} Error when sending batch {:?}.", self.index(), e);
        }
    }

    fn move_units_to_consensus(&mut self) {
        let units_to_move = self
            .store
            .yield_buffer_units()
            .into_iter()
            .map(|su| su.as_signable().unit())
            .collect();
        self.send_consensus_notification(NotificationIn::NewUnits(units_to_move))
    }

    pub(crate) async fn run(mut self, mut exit: oneshot::Receiver<()>) {
        info!(target: "AlephBFT-runway", "{:?} Runway starting.", self.index());

        let index = self.index();

        info!(target: "AlephBFT-runway", "{:?} Runway started.", index);
        loop {
            futures::select! {
                notification = self.rx_consensus.next() => match notification {
                        Some(notification) => self.on_consensus_notification(notification).await,
                        None => {
                            error!(target: "AlephBFT-runway", "{:?} Consensus notification stream closed.", index);
                            break;
                        }
                },

                notification = self.notifications_from_alerter.next() => match notification {
                    Some(notification) => {
                        self.on_alert_notification(notification)
                    },
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Alert notification stream closed.", index);
                        break;
                    }
                },

                event = self.unit_messages_from_network.next() => match event {
                    Some(event) => self.on_unit_message(event),
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Unit message stream closed.", index);
                        break;
                    }
                },

                batch = self.ordered_batch_rx.next() => match batch {
                    Some(batch) => self.on_ordered_batch(batch),
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Ordered batch stream closed.", index);
                        break;
                    }
                },

                _ = &mut exit => break,
            };
            self.move_units_to_consensus();
        }

        info!(target: "AlephBFT-runway", "{:?} Run ended.", index);
    }
}

pub(crate) async fn run<H, D, MK, DP, SH>(
    config: Config,
    keychain: MK,
    data_io: DP,
    spawn_handle: SH,
    alert_messages_for_network: Sender<(
        AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
        Recipient,
    )>,
    alert_messages_from_network: Receiver<
        AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
    >,
    unit_messages_from_network: Receiver<RunwayNotificationIn<H, D, MK::Signature>>,
    unit_messages_for_network: Sender<RunwayNotificationOut<H, D, MK::Signature>>,
    resolved_requests: Sender<Request<H>>,
    mut exit: oneshot::Receiver<()>,
) where
    H: Hasher,
    D: Data,
    MK: MultiKeychain,
    DP: DataIO<D>,
    SH: SpawnHandle,
{
    let (tx_consensus, consensus_stream) = mpsc::unbounded();
    let (consensus_sink, rx_consensus) = mpsc::unbounded();
    let (ordered_batch_tx, ordered_batch_rx) = mpsc::unbounded();

    let (alert_notifications_for_units, notifications_from_alerter) = mpsc::unbounded();
    let (alerts_for_alerter, alerts_from_units) = mpsc::unbounded();
    let alert_config = AlertConfig {
        session_id: config.session_id,
        n_members: config.n_members,
    };
    let (alerter_exit, exit_stream) = oneshot::channel();
    let alerter = Alerter::new(
        keychain.clone(),
        alert_messages_for_network,
        alert_messages_from_network,
        alert_notifications_for_units,
        alerts_from_units,
        alert_config,
    );

    let alerter_handle = spawn_handle.spawn_essential("runway/alerter", async move {
        let mut alerter = alerter;
        alerter.run(exit_stream).await;
    });
    let mut alerter_handle = into_infinite_stream(alerter_handle).fuse();

    let (consensus_exit, exit_stream) = oneshot::channel();
    let consensus_config = config.clone();
    let consensus_spawner = spawn_handle.clone();
    let consensus_handle = spawn_handle.spawn_essential("runway/consensus", async move {
        consensus::run(
            consensus_config,
            consensus_stream,
            consensus_sink,
            ordered_batch_tx,
            consensus_spawner,
            exit_stream,
        )
        .await
    });
    let mut consensus_handle = into_infinite_stream(consensus_handle).fuse();

    let index = config.node_ix;

    let runway_config = RunwayConfig {
        config,
        keychain,
        data_io,
        alerts_for_alerter,
        notifications_from_alerter,
        tx_consensus,
        rx_consensus,
        unit_messages_from_network,
        unit_messages_for_network,
        ordered_batch_rx,
        resolved_requests,
    };
    let (runway_exit, exit_stream) = oneshot::channel();
    let runway = Runway::new(runway_config);
    let runway_handle = runway.run(exit_stream);
    let runway_handle = into_infinite_stream(runway_handle).fuse();
    pin_mut!(runway_handle);

    futures::select! {
        _ = runway_handle.next() => {
            debug!(target: "AlephBFT-runway", "{:?} Runway task terminated early.", index);
        },
        _ = alerter_handle.next() => {
            debug!(target: "AlephBFT-runway", "{:?} Alerter task terminated early.", index);
        },
        _ = consensus_handle.next() => {
            debug!(target: "AlephBFT-runway", "{:?} Consensus task terminated early.", index);
        },
        _ = &mut exit => {},
    }

    info!(target: "AlephBFT-runway", "{:?} Ending run.", index);

    if runway_exit.send(()).is_err() {
        debug!(target: "AlephBFT-runway", "{:?} Runway already stopped.", index);
    }
    if alerter_exit.send(()).is_err() {
        debug!(target: "AlephBFT-runway", "{:?} Alerter already stopped.", index);
    }
    if consensus_exit.send(()).is_err() {
        debug!(target: "AlephBFT-runway", "{:?} Consensus already stopped.", index);
    }

    runway_handle.next().await.unwrap();
    alerter_handle.next().await.unwrap();
    consensus_handle.next().await.unwrap();

    info!(target: "AlephBFT-runway", "{:?} Runway ended.", index);
}
