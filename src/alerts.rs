use crate::{
    network::Recipient,
    nodes::NodeCount,
    rmc,
    rmc::{DoublingDelayScheduler, ReliableMulticast},
    signed::{Multisigned, PartialMultisignature, Signable, Signature, Signed, UncheckedSigned},
    units::UncheckedSignedUnit,
    Data, Hasher, Index, MultiKeychain, NodeIndex, Receiver, Sender, SessionId,
};
use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, StreamExt,
};
use log::{debug, error};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    time,
};

pub(crate) type ForkProof<H, D, S> = (UncheckedSignedUnit<H, D, S>, UncheckedSignedUnit<H, D, S>);

#[derive(Clone, Debug, Decode, Encode)]
pub(crate) struct Alert<H: Hasher, D: Data, S: Signature> {
    sender: NodeIndex,
    proof: ForkProof<H, D, S>,
    legit_units: Vec<UncheckedSignedUnit<H, D, S>>,
    #[codec(skip)]
    hash: RefCell<Option<H::Hash>>,
}

impl<H: Hasher, D: Data, S: Signature> Alert<H, D, S> {
    pub fn new(
        sender: NodeIndex,
        proof: ForkProof<H, D, S>,
        legit_units: Vec<UncheckedSignedUnit<H, D, S>>,
    ) -> Alert<H, D, S> {
        Alert {
            sender,
            proof,
            legit_units,
            hash: RefCell::new(None),
        }
    }
    fn hash(&self) -> H::Hash {
        let hash = *self.hash.borrow();
        match hash {
            Some(hash) => hash,
            None => {
                let hash = self.using_encoded(H::hash);
                *self.hash.borrow_mut() = Some(hash);
                hash
            }
        }
    }

    // Simplified forker check, should only be called for alerts that have already been checked to
    // contain valid proofs.
    fn forker(&self) -> NodeIndex {
        self.proof.0.as_signable().creator()
    }
}

impl<H: Hasher, D: Data, S: Signature> Index for Alert<H, D, S> {
    fn index(&self) -> NodeIndex {
        self.sender
    }
}

impl<H: Hasher, D: Data, S: Signature> Signable for Alert<H, D, S> {
    type Hash = H::Hash;
    fn hash(&self) -> Self::Hash {
        self.hash()
    }
}

/// A message concerning alerts.
#[derive(Debug, Encode, Decode, Clone)]
pub(crate) enum AlertMessage<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature> {
    /// Alert regarding forks, signed by the person claiming misconduct.
    ForkAlert(UncheckedSigned<Alert<H, D, S>, S>),
    /// An internal RMC message, together with the id of the sender.
    RmcMessage(NodeIndex, rmc::Message<H::Hash, S, MS>),
    /// A request by a node for a fork alert identified by the given hash.
    AlertRequest(NodeIndex, H::Hash),
}

// Notifications being sent to consensus, so that it can learn about proven forkers and receive
// legitimized units.
pub(crate) enum ForkingNotification<H: Hasher, D: Data, S: Signature> {
    Forker(ForkProof<H, D, S>),
    Units(Vec<UncheckedSignedUnit<H, D, S>>),
}

struct Alerter<'a, H: Hasher, D: Data, MK: MultiKeychain> {
    session_id: SessionId,
    keychain: &'a MK,
    messages_for_network: Sender<(
        AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
        Recipient,
    )>,
    messages_from_network: Receiver<AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>>,
    notifications_for_units: Sender<ForkingNotification<H, D, MK::Signature>>,
    alerts_from_units: Receiver<Alert<H, D, MK::Signature>>,
    max_units_per_alert: usize,
    known_forkers: HashMap<NodeIndex, ForkProof<H, D, MK::Signature>>,
    known_alerts: HashMap<H::Hash, Signed<'a, Alert<H, D, MK::Signature>, MK>>,
    known_rmcs: HashMap<(NodeIndex, NodeIndex), H::Hash>,
    rmc: ReliableMulticast<'a, H::Hash, MK>,
    messages_from_rmc: Receiver<(
        NodeIndex,
        rmc::Message<H::Hash, MK::Signature, MK::PartialMultisignature>,
    )>,
    messages_for_rmc: Sender<rmc::Message<H::Hash, MK::Signature, MK::PartialMultisignature>>,
}

pub(crate) struct AlertConfig {
    pub max_units_per_alert: usize,
    pub n_members: NodeCount,
    pub session_id: SessionId,
}

impl<'a, H: Hasher, D: Data, MK: MultiKeychain> Alerter<'a, H, D, MK> {
    fn new(
        keychain: &'a MK,
        messages_for_network: Sender<(
            AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
            Recipient,
        )>,
        messages_from_network: Receiver<
            AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
        >,
        notifications_for_units: Sender<ForkingNotification<H, D, MK::Signature>>,
        alerts_from_units: Receiver<Alert<H, D, MK::Signature>>,
        config: AlertConfig,
    ) -> Self {
        let (messages_for_rmc, messages_from_us) = mpsc::unbounded();
        let (messages_for_us, messages_from_rmc) = mpsc::unbounded();
        Self {
            session_id: config.session_id,
            keychain,
            messages_for_network,
            messages_from_network,
            notifications_for_units,
            alerts_from_units,
            max_units_per_alert: config.max_units_per_alert,
            known_forkers: HashMap::new(),
            known_alerts: HashMap::new(),
            known_rmcs: HashMap::new(),
            rmc: ReliableMulticast::new(
                messages_from_us,
                messages_for_us,
                keychain,
                config.n_members,
                DoublingDelayScheduler::new(time::Duration::from_millis(500)),
            ),
            messages_from_rmc,
            messages_for_rmc,
        }
    }

    fn index(&self) -> NodeIndex {
        self.keychain.index()
    }

    fn is_forker(&self, forker: NodeIndex) -> bool {
        self.known_forkers.contains_key(&forker)
    }

    fn on_new_forker_detected(&mut self, forker: NodeIndex, proof: ForkProof<H, D, MK::Signature>) {
        use ForkingNotification::Forker;
        self.known_forkers.insert(forker, proof.clone());
        self.notifications_for_units
            .unbounded_send(Forker(proof))
            .expect("Channel should be open")
    }

    // Correctness rules:
    // 1) All units must be created by forker
    // 2) All units must come from different rounds
    // 3) There must be fewer of them than the maximum defined in the configuration.
    // Note that these units will have to be validated before being used in the consensus.
    // This is alright, if someone uses their alert to commit to incorrect units it's their own
    // problem.
    fn correct_commitment(
        &self,
        forker: NodeIndex,
        units: &[UncheckedSignedUnit<H, D, MK::Signature>],
    ) -> bool {
        if units.len() > self.max_units_per_alert {
            debug!(target: "AlephBFT-alerter", "{:?} Too many units: {} included in alert.", self.index(), units.len());
            return false;
        }
        let mut rounds: HashSet<usize> = HashSet::new();
        for u in units {
            let u = match u.clone().check(self.keychain) {
                Ok(u) => u,
                Err(_) => {
                    debug!(target: "AlephBFT-alerter", "{:?} One of the units is incorrectly signed.", self.index());
                    return false;
                }
            };
            let full_unit = u.as_signable();
            if full_unit.creator() != forker {
                debug!(target: "AlephBFT-alerter", "{:?} One of the units {:?} has wrong creator.", self.index(), full_unit);
                return false;
            }
            if rounds.contains(&full_unit.round()) {
                debug!(target: "AlephBFT-alerter", "{:?} Two or more alerted units have the same round {:?}.", self.index(), full_unit.round());
                return false;
            }
            rounds.insert(full_unit.round());
        }
        true
    }

    fn who_is_forking(&self, proof: &ForkProof<H, D, MK::Signature>) -> Option<NodeIndex> {
        let (u1, u2) = proof;
        let (u1, u2) = {
            let u1 = u1.clone().check(self.keychain);
            let u2 = u2.clone().check(self.keychain);
            match (u1, u2) {
                (Ok(u1), Ok(u2)) => (u1, u2),
                _ => {
                    debug!(target: "AlephBFT-alerter", "{:?} Invalid signatures in a proof.", self.index());
                    return None;
                }
            }
        };
        let full_unit1 = u1.as_signable();
        let full_unit2 = u2.as_signable();
        if full_unit1.session_id() != self.session_id || full_unit2.session_id() != self.session_id
        {
            debug!(target: "AlephBFT-alerter", "{:?} Alert from different session.", self.index());
            return None;
        }
        if full_unit1 == full_unit2 {
            debug!(target: "AlephBFT-alerter", "{:?} Two copies of the same unit do not constitute a fork.", self.index());
            return None;
        }
        if full_unit1.creator() != full_unit2.creator() {
            debug!(target: "AlephBFT-alerter", "{:?} One of the units creators in proof does not match.", self.index());
            return None;
        }
        if full_unit1.round() != full_unit2.round() {
            debug!(target: "AlephBFT-alerter", "{:?} The rounds in proof's units do not match.", self.index());
            return None;
        }
        Some(full_unit1.creator())
    }

    fn rmc_alert(&mut self, forker: NodeIndex, alert: Signed<'a, Alert<H, D, MK::Signature>, MK>) {
        let hash = alert.as_signable().hash();
        self.known_rmcs
            .insert((alert.as_signable().sender, forker), hash);
        self.known_alerts.insert(hash, alert);
        self.rmc.start_rmc(hash);
    }

    fn on_own_alert(&mut self, alert: Alert<H, D, MK::Signature>) {
        let forker = alert.forker();
        self.known_forkers.insert(forker, alert.proof.clone());
        let alert = Signed::sign(alert, self.keychain);
        self.messages_for_network
            .unbounded_send((
                AlertMessage::ForkAlert(alert.clone().into()),
                Recipient::Everyone,
            ))
            .expect("Channel should be open");
        self.rmc_alert(forker, alert);
    }

    fn on_network_alert(
        &mut self,
        alert: UncheckedSigned<Alert<H, D, MK::Signature>, MK::Signature>,
    ) {
        let alert = match alert.check(self.keychain) {
            Ok(alert) => alert,
            Err(e) => {
                debug!(target: "AlephBFT-alerter","{:?} We have received an incorrectly signed alert: {:?}.", self.index(), e);
                return;
            }
        };
        let contents = alert.as_signable();
        if let Some(forker) = self.who_is_forking(&contents.proof) {
            if self.known_rmcs.contains_key(&(contents.sender, forker)) {
                debug!(target: "AlephBFT-alerter","{:?} We already know about an alert by {:?} about {:?}.", self.index(), alert.as_signable().sender, forker);
                self.known_alerts.insert(contents.hash(), alert);
                return;
            }
            if !self.is_forker(forker) {
                // We learn about this forker for the first time, need to send our own alert
                self.on_new_forker_detected(forker, contents.proof.clone());
            }
            self.rmc_alert(forker, alert);
        } else {
            debug!(target: "AlephBFT-alerter","{:?} We have received an incorrect forking proof from {:?}.", self.index(), alert.as_signable().sender);
        }
    }

    fn send_alert_to(&mut self, hash: H::Hash, node: NodeIndex) {
        let alert = match self.known_alerts.get(&hash) {
            Some(alert) => alert.clone(),
            None => {
                debug!(target: "AlephBFT-alerter", "{:?} Received request for unknown alert.", self.index());
                return;
            }
        };
        self.messages_for_network
            .unbounded_send((AlertMessage::ForkAlert(alert.into()), Recipient::Node(node)))
            .expect("Channel should be open")
    }

    fn on_rmc_message(
        &mut self,
        sender: NodeIndex,
        message: rmc::Message<H::Hash, MK::Signature, MK::PartialMultisignature>,
    ) {
        let hash = message.hash();
        if let Some(alert) = self.known_alerts.get(hash) {
            let alert_id = (alert.as_signable().sender, alert.as_signable().forker());
            if self.known_rmcs.get(&alert_id) == Some(hash) || message.is_complete() {
                self.messages_for_rmc
                    .unbounded_send(message)
                    .expect("Channel should be open")
            }
        } else {
            self.messages_for_network
                .unbounded_send((
                    AlertMessage::AlertRequest(self.index(), *hash),
                    Recipient::Node(sender),
                ))
                .expect("Channel should be open")
        }
    }

    fn on_message(
        &mut self,
        message: AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
    ) {
        use AlertMessage::*;
        match message {
            ForkAlert(alert) => {
                debug!(target: "AlephBFT-alerter", "{:?} Fork alert received {:?}.", self.index(), alert);
                self.on_network_alert(alert);
            }
            RmcMessage(sender, message) => self.on_rmc_message(sender, message),
            AlertRequest(node, hash) => self.send_alert_to(hash, node),
        }
    }

    fn alert_confirmed(&mut self, multisigned: Multisigned<'a, H::Hash, MK>) {
        let alert = match self.known_alerts.get(multisigned.as_signable()) {
            Some(alert) => alert.as_signable(),
            None => {
                error!(target: "AlephBFT-alerter", "{:?} Completed an RMC for an unknown alert.", self.index());
                return;
            }
        };
        let forker = alert.proof.0.as_signable().creator();
        self.known_rmcs.insert((alert.sender, forker), alert.hash());
        if !self.correct_commitment(forker, &alert.legit_units) {
            debug!(target: "AlephBFT-alerter","{:?} We have received an incorrect unit commitment from {:?}.", self.index(), alert.sender);
            return;
        }
        self.notifications_for_units
            .unbounded_send(ForkingNotification::Units(alert.legit_units.clone()))
            .expect("Channel should be open")
    }

    fn rmc_message_to_network(
        &mut self,
        message: (
            NodeIndex,
            rmc::Message<H::Hash, MK::Signature, MK::PartialMultisignature>,
        ),
    ) {
        let (node, message) = message;
        self.messages_for_network
            .unbounded_send((
                AlertMessage::RmcMessage(self.index(), message),
                Recipient::Node(node),
            ))
            .expect("Channel should be open")
    }

    async fn run(&mut self, mut exit: oneshot::Receiver<()>) {
        loop {
            futures::select! {
                message = self.messages_from_network.next() => match message {
                    Some(message) => self.on_message(message),
                    None => {
                        error!(target: "AlephBFT-alerter", "{:?} Message stream closed.", self.index());
                        break;
                    }
                },
                alert = self.alerts_from_units.next() => match alert {
                    Some(alert) => self.on_own_alert(alert),
                    None => {
                        error!(target: "AlephBFT-alerter", "{:?} Alert stream closed.", self.index());
                        break;
                    }
                },
                message = self.messages_from_rmc.next() => match message {
                    Some(message) => self.rmc_message_to_network(message),
                    None => {
                        error!(target: "AlephBFT-alerter", "{:?} RMC message stream closed.", self.index());
                        break;
                    }
                },
                multisigned = self.rmc.next_multisigned_hash().fuse() => self.alert_confirmed(multisigned),
                _ = &mut exit => break,
            }
        }
    }
}

pub(crate) async fn run<H: Hasher, D: Data, MK: MultiKeychain>(
    keychain: MK,
    messages_for_network: Sender<(
        AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
        Recipient,
    )>,
    messages_from_network: Receiver<AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>>,
    notifications_for_units: Sender<ForkingNotification<H, D, MK::Signature>>,
    alerts_from_units: Receiver<Alert<H, D, MK::Signature>>,
    config: AlertConfig,
    exit: oneshot::Receiver<()>,
) {
    Alerter::new(
        &keychain,
        messages_for_network,
        messages_from_network,
        notifications_for_units,
        alerts_from_units,
        config,
    )
    .run(exit)
    .await
}
