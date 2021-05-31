use crate::{
    network::Recipient, units::UncheckedSignedUnit, Data, Hasher, KeyBox, NodeIndex, Receiver,
    Sender,
};
use codec::{Decode, Encode};
use futures::{channel::oneshot, FutureExt, StreamExt};
use log::{debug, error};
use std::collections::{HashMap, HashSet};

pub(crate) type ForkProof<H, D, S> = (UncheckedSignedUnit<H, D, S>, UncheckedSignedUnit<H, D, S>);

#[derive(Clone, Debug, Decode, Encode)]
pub(crate) struct Alert<H: Hasher, D: Data, S> {
    sender: NodeIndex,
    proof: ForkProof<H, D, S>,
    legit_units: Vec<UncheckedSignedUnit<H, D, S>>,
}

impl<H: Hasher, D: Data, S> Alert<H, D, S> {
    pub fn new(
        sender: NodeIndex,
        proof: ForkProof<H, D, S>,
        legit_units: Vec<UncheckedSignedUnit<H, D, S>>,
    ) -> Alert<H, D, S> {
        Alert {
            sender,
            proof,
            legit_units,
        }
    }
}

/// A message concerning alerts.
#[derive(Debug, Encode, Decode, Clone)]
pub(crate) enum AlertMessage<H: Hasher, D: Data, S> {
    /// Alert regarding forks,
    ForkAlert(Alert<H, D, S>),
}

// Notifications being sent to consensus, so that it can learn about proven forkers and receive
// legitimized units.
pub(crate) enum ForkingNotification<H: Hasher, D: Data, S> {
    Forker(ForkProof<H, D, S>),
    Units(Vec<UncheckedSignedUnit<H, D, S>>),
}

pub(crate) struct Alerter<H: Hasher, D: Data, KB: KeyBox> {
    keychain: KB,
    messages_for_network: Sender<(AlertMessage<H, D, KB::Signature>, Recipient)>,
    messages_from_network: Receiver<AlertMessage<H, D, KB::Signature>>,
    notifications_for_units: Sender<ForkingNotification<H, D, KB::Signature>>,
    alerts_from_units: Receiver<Alert<H, D, KB::Signature>>,
    max_units_per_alert: usize,
    known_forkers: HashMap<NodeIndex, ForkProof<H, D, KB::Signature>>,
}

impl<H: Hasher, D: Data, KB: KeyBox> Alerter<H, D, KB> {
    pub fn new(
        keychain: KB,
        messages_for_network: Sender<(AlertMessage<H, D, KB::Signature>, Recipient)>,
        messages_from_network: Receiver<AlertMessage<H, D, KB::Signature>>,
        notifications_for_units: Sender<ForkingNotification<H, D, KB::Signature>>,
        alerts_from_units: Receiver<Alert<H, D, KB::Signature>>,
        max_units_per_alert: usize,
    ) -> Self {
        Self {
            keychain,
            messages_for_network,
            messages_from_network,
            notifications_for_units,
            alerts_from_units,
            max_units_per_alert,
            known_forkers: HashMap::new(),
        }
    }

    fn index(&self) -> NodeIndex {
        self.keychain.index()
    }

    fn is_forker(&self, forker: NodeIndex) -> bool {
        self.known_forkers.contains_key(&forker)
    }

    fn on_new_forker_detected(&mut self, forker: NodeIndex, proof: ForkProof<H, D, KB::Signature>) {
        use ForkingNotification::Forker;
        self.known_forkers.insert(forker, proof.clone());
        if let Err(e) = self.notifications_for_units.unbounded_send(Forker(proof)) {
            error!(target: "alerter", "{:?} Failed to send for proof to units: {:?}.", self.index(), e);
        }
    }

    // Legitimacy rules:
    // 1) All units must be created by forker
    // 2) All units must come from different rounds
    // 3) There must be fewer of them than the maximum defined in the configuration.
    // Note that these units will have to be validated before being used in the consensus.
    // This is alright, if someone uses their alert to commit to incorrect units it's their own
    // problem.
    fn units_are_legit(
        &self,
        forker: NodeIndex,
        units: &[UncheckedSignedUnit<H, D, KB::Signature>],
    ) -> bool {
        if units.len() > self.max_units_per_alert {
            debug!(target: "alerter", "{:?} Too many units: {} included in alert.", self.index(), units.len());
            return false;
        }
        let mut rounds: HashSet<usize> = HashSet::new();
        for u in units {
            let u = match u.clone().check(&self.keychain) {
                Ok(u) => u,
                Err(_) => {
                    debug!(target: "alerter", "{:?} One of the units is incorrectly signed.", self.index());
                    return false;
                }
            };
            let full_unit = u.as_signable();
            if full_unit.creator() != forker {
                debug!(target: "alerter", "{:?} One of the units {:?} has wrong creator.", self.index(), full_unit);
                return false;
            }
            if rounds.contains(&full_unit.round()) {
                debug!(target: "alerter", "{:?} Two or more alerted units have the same round {:?}.", self.index(), full_unit.round());
                return false;
            }
            rounds.insert(full_unit.round());
        }
        true
    }

    fn who_is_forking(&self, proof: &ForkProof<H, D, KB::Signature>) -> Option<NodeIndex> {
        let (u1, u2) = proof;
        let (u1, u2) = {
            let u1 = u1.clone().check(&self.keychain);
            let u2 = u2.clone().check(&self.keychain);
            match (u1, u2) {
                (Ok(u1), Ok(u2)) => (u1, u2),
                _ => {
                    debug!(target: "alerter", "{:?} Invalid signatures in a proof.", self.index());
                    return None;
                }
            }
        };
        let full_unit1 = u1.as_signable();
        let full_unit2 = u2.as_signable();
        if full_unit1 == full_unit2 {
            debug!(target: "alerter", "{:?} Two copies of the same unit do not constitute a fork.", self.index());
            return None;
        }
        if full_unit1.creator() != full_unit2.creator() {
            debug!(target: "alerter", "{:?} One of the units creators in proof does not match.", self.index());
            return None;
        }
        if full_unit1.round() != full_unit2.round() {
            debug!(target: "alerter", "{:?} The rounds in proof's units do not match.", self.index());
            return None;
        }
        Some(full_unit1.creator())
    }

    fn on_own_alert(&mut self, alert: Alert<H, D, KB::Signature>) {
        if let Err(e) = self
            .messages_for_network
            .unbounded_send((AlertMessage::ForkAlert(alert), Recipient::Everyone))
        {
            debug!(target: "alerter", "Error when broadcasting units {:?}.", e);
        }
    }

    fn on_network_alert(&mut self, alert: Alert<H, D, KB::Signature>) {
        if let Some(forker) = self.who_is_forking(&alert.proof) {
            if !self.is_forker(forker) {
                // We learn about this forker for the first time, need to send our own alert
                self.on_new_forker_detected(forker, alert.proof);
            }
            if !self.units_are_legit(forker, &alert.legit_units) {
                debug!(target: "alerter","{:?} We have received an incorrect unit commitment from {:?} for forker {:?}.", self.index(), alert.sender, forker);
                return;
            }
            if let Err(e) = self
                .notifications_for_units
                .unbounded_send(ForkingNotification::Units(alert.legit_units))
            {
                debug!(target: "alerter","{:?} Failed to send forking notification to units: {:?}.", self.index(), e);
            }
        } else {
            debug!(target: "alerter","{:?} We have received an incorrect forking proof from {:?}.", self.index(), alert.sender);
        }
    }

    fn on_message(&mut self, message: AlertMessage<H, D, KB::Signature>) {
        use AlertMessage::*;
        match message {
            ForkAlert(alert) => {
                debug!(target: "alerter", "{:?} Fork alert received {:?}.", self.index(), alert);
                self.on_network_alert(alert);
            }
        }
    }

    pub async fn run(&mut self, exit: oneshot::Receiver<()>) {
        let mut exit = exit.into_stream();
        loop {
            tokio::select! {
                message = self.messages_from_network.next() => match message {
                    Some(message) => self.on_message(message),
                    None => {
                        error!(target: "alerter", "{:?} Message stream closed.", self.index());
                        break;
                    }
                },
                alert = self.alerts_from_units.next() => match alert {
                    Some(alert) => self.on_own_alert(alert),
                    None => {
                        error!(target: "alerter", "{:?} Alert stream closed.", self.index());
                        break;
                    }
                },
                _ = exit.next() => break,
            }
        }
    }
}
