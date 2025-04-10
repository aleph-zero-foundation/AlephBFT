use crate::{
    alerts::{Alert, AlertMessage, ForkProof, ForkingNotification},
    units::Unit,
    Data, Hasher, Keychain, MultiKeychain, Multisigned, NodeIndex, PartialMultisignature,
    Recipient, SessionId, Signature, Signed, UncheckedSigned,
};
use aleph_bft_rmc::Message as RmcMessage;
use aleph_bft_types::Round;
use codec::{Decode, Encode};
use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
};

#[derive(Debug, PartialEq)]
pub enum Error {
    // commitment validity errors
    IncorrectlySignedUnit(NodeIndex),
    SameRound(Round, NodeIndex),
    WrongCreator(NodeIndex),
    // fork validity errors
    DifferentRounds(NodeIndex),
    SingleUnit(NodeIndex),
    WrongSession(NodeIndex),
    // other errors
    IncorrectlySignedAlert,
    RepeatedAlert(NodeIndex, NodeIndex),
    UnknownAlertRequest,
    UnknownAlertRMC,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IncorrectlySignedUnit(sender) => write!(f, "Incorrect commitment from {:?}: Some unit is incorrectly signed", sender),
            Error::SameRound(round, sender) => write!(f, "Incorrect commitment from {:?}: Two or more alerted units have the same round {:?}", sender, round),
            Error::WrongCreator(sender) => write!(f, "Incorrect commitment from {:?}: Some unit has a wrong creator", sender),
            Error::DifferentRounds(sender) => write!(f, "Incorrect fork alert from {:?}: Forking units come from different rounds", sender),
            Error::SingleUnit(sender) => write!(f, "Incorrect fork alert from {:?}: Two copies of a single unit do not constitute a fork", sender),
            Error::WrongSession(sender) => write!(f, "Incorrect fork alert from {:?}: Wrong session", sender),
            Error::IncorrectlySignedAlert => write!(f, "Received an incorrectly signed alert"),
            Error::RepeatedAlert(forker, sender) => write!(f, "We already know about an alert by {:?} about {:?}", sender, forker),
            Error::UnknownAlertRequest => write!(f, "Received a request for an unknown alert"),
            Error::UnknownAlertRMC => write!(f, "Completed an RMC for an unknown alert"),
        }
    }
}

type KnownAlerts<H, D, MK> =
    HashMap<<H as Hasher>::Hash, Signed<Alert<H, D, <MK as Keychain>::Signature>, MK>>;

pub type OnOwnAlertResponse<H, D, MK> = (
    AlertMessage<H, D, <MK as Keychain>::Signature, <MK as MultiKeychain>::PartialMultisignature>,
    Recipient,
    <H as Hasher>::Hash,
);

pub type OnNetworkAlertResponse<H, D, MK> = (
    Option<ForkingNotification<H, D, <MK as Keychain>::Signature>>,
    <H as Hasher>::Hash,
);

type OnAlertRequestResponse<H, D, MK> = (
    UncheckedSigned<Alert<H, D, <MK as Keychain>::Signature>, <MK as Keychain>::Signature>,
    Recipient,
);

#[derive(Clone, Eq, PartialEq, Hash, Debug, Decode, Encode)]
pub enum RmcResponse<H: Hasher, S: Signature, MS: PartialMultisignature> {
    RmcMessage(RmcMessage<H::Hash, S, MS>),
    AlertRequest(H::Hash, Recipient),
    Noop,
}

/// The component responsible for fork alerts in AlephBFT. We refer to the documentation
/// https://cardinal-cryptography.github.io/AlephBFT/how_alephbft_does_it.html Section 2.5 and
/// https://cardinal-cryptography.github.io/AlephBFT/reliable_broadcast.html and to the Aleph
/// paper https://arxiv.org/abs/1908.05156 Appendix A1 for a discussion.
pub struct Handler<H: Hasher, D: Data, MK: MultiKeychain> {
    session_id: SessionId,
    keychain: MK,
    known_forkers: HashMap<NodeIndex, ForkProof<H, D, MK::Signature>>,
    known_alerts: KnownAlerts<H, D, MK>,
    known_rmcs: HashMap<(NodeIndex, NodeIndex), H::Hash>,
}

impl<H: Hasher, D: Data, MK: MultiKeychain> Handler<H, D, MK> {
    pub fn new(keychain: MK, session_id: SessionId) -> Self {
        Self {
            session_id,
            keychain,
            known_forkers: HashMap::new(),
            known_alerts: HashMap::new(),
            known_rmcs: HashMap::new(),
        }
    }

    fn is_forker(&self, forker: NodeIndex) -> bool {
        self.known_forkers.contains_key(&forker)
    }

    fn on_new_forker_detected(&mut self, forker: NodeIndex, proof: ForkProof<H, D, MK::Signature>) {
        self.known_forkers.insert(forker, proof);
    }

    // Correctness rules:
    // 1) All units must be created by forker
    // 2) All units must come from different rounds
    // 3) There must be fewer of them than the maximum defined in the configuration.
    // Note that these units will have to be validated before being used in the consensus.
    // This is alright, if someone uses their alert to commit to incorrect units it's their own
    // problem.
    fn verify_commitment(&self, alert: &Alert<H, D, MK::Signature>) -> Result<(), Error> {
        let mut rounds = HashSet::new();
        for u in &alert.legit_units {
            let u = match u.clone().check(&self.keychain) {
                Ok(u) => u,
                Err(_) => return Err(Error::IncorrectlySignedUnit(alert.sender)),
            };
            let full_unit = u.as_signable();
            if full_unit.creator() != alert.forker() {
                return Err(Error::WrongCreator(alert.sender));
            }
            if rounds.contains(&full_unit.round()) {
                return Err(Error::SameRound(full_unit.round(), alert.sender));
            }
            rounds.insert(full_unit.round());
        }
        Ok(())
    }

    fn verify_fork(&self, alert: &Alert<H, D, MK::Signature>) -> Result<(), Error> {
        let (u1, u2) = &alert.proof;
        let (u1, u2) = {
            let u1 = u1.clone().check(&self.keychain);
            let u2 = u2.clone().check(&self.keychain);
            match (u1, u2) {
                (Ok(u1), Ok(u2)) => (u1, u2),
                _ => return Err(Error::IncorrectlySignedUnit(alert.sender)),
            }
        };
        let full_unit1 = u1.as_signable();
        let full_unit2 = u2.as_signable();
        if full_unit1.session_id() != self.session_id || full_unit2.session_id() != self.session_id
        {
            return Err(Error::WrongSession(alert.sender));
        }
        if full_unit1 == full_unit2 {
            return Err(Error::SingleUnit(alert.sender));
        }
        if full_unit1.creator() != full_unit2.creator() {
            return Err(Error::WrongCreator(alert.sender));
        }
        if full_unit1.round() != full_unit2.round() {
            return Err(Error::DifferentRounds(alert.sender));
        }
        Ok(())
    }

    /// Registers the RMC but does not actually send it; the returned hash must be passed to `start_rmc()` separately
    fn rmc_alert(
        &mut self,
        forker: NodeIndex,
        alert: Signed<Alert<H, D, MK::Signature>, MK>,
    ) -> H::Hash {
        let hash = alert.as_signable().hash();
        self.known_rmcs
            .insert((alert.as_signable().sender, forker), hash);
        self.known_alerts.insert(hash, alert);
        hash
    }

    /// Registers RMCs and messages but does not actually send them; make sure the returned values are forwarded to IO
    pub fn on_own_alert(
        &mut self,
        alert: Alert<H, D, MK::Signature>,
    ) -> OnOwnAlertResponse<H, D, MK> {
        let forker = alert.forker();
        self.known_forkers.insert(forker, alert.proof.clone());
        let alert = Signed::sign(alert, &self.keychain);
        let hash = self.rmc_alert(forker, alert.clone());
        (
            AlertMessage::ForkAlert(alert.into_unchecked()),
            Recipient::Everyone,
            hash,
        )
    }

    /// May return a `ForkingNotification`, which should be propagated
    pub fn on_network_alert(
        &mut self,
        alert: UncheckedSigned<Alert<H, D, MK::Signature>, MK::Signature>,
    ) -> Result<OnNetworkAlertResponse<H, D, MK>, Error> {
        let alert = match alert.check(&self.keychain) {
            Ok(alert) => alert,
            Err(_) => {
                return Err(Error::IncorrectlySignedAlert);
            }
        };
        let contents = alert.as_signable();
        self.verify_fork(contents)?;
        let forker = contents.forker();
        let sender = alert.as_signable().sender;
        if self.known_rmcs.contains_key(&(contents.sender, forker)) {
            self.known_alerts.insert(contents.hash(), alert);
            return Err(Error::RepeatedAlert(sender, forker));
        }
        let maybe_notification = if self.is_forker(forker) {
            None
        } else {
            // We learn about this forker for the first time, need to send our own alert
            self.on_new_forker_detected(forker, contents.proof.clone());
            Some(ForkingNotification::Forker(contents.proof.clone()))
        };
        let hash_for_rmc = self.rmc_alert(forker, alert);

        // A response to a valid fork alert.
        // It should be handled by starting RMC on the contained hash
        // and sending the contained notification (if present) to consensus.
        Ok((maybe_notification, hash_for_rmc))
    }

    // returns AlerterResponse::{AlertRequest, RmcMessage} or None (no error, can't fail)
    pub fn on_rmc_message(
        &self,
        sender: NodeIndex,
        message: RmcMessage<H::Hash, MK::Signature, MK::PartialMultisignature>,
    ) -> RmcResponse<H, MK::Signature, MK::PartialMultisignature> {
        let hash = message.hash();
        if let Some(alert) = self.known_alerts.get(hash) {
            let alert_id = (alert.as_signable().sender, alert.as_signable().forker());
            if self.known_rmcs.get(&alert_id) == Some(hash) || message.is_complete() {
                // An internal RMC message, with the sender being the local node.
                // It should be handled by sending the message.
                RmcResponse::RmcMessage(message)
            } else {
                // Should be handled by doing nothing.
                RmcResponse::Noop
            }
        } else {
            // A request for a fork alert from another node.
            // It should be handled by sending the request via the network to the contained recipient.
            RmcResponse::AlertRequest(*hash, Recipient::Node(sender))
        }
    }

    pub fn on_alert_request(
        &self,
        node: NodeIndex,
        hash: H::Hash,
    ) -> Result<OnAlertRequestResponse<H, D, MK>, Error> {
        match self.known_alerts.get(&hash) {
            Some(alert) => {
                // A copy of a fork alert.
                // It should be handled by sending the contained `Alert` via the network to the contained recipient.
                Ok((alert.clone().into_unchecked(), Recipient::Node(node)))
            }
            None => Err(Error::UnknownAlertRequest),
        }
    }

    /// May return a `ForkingNotification`, which should be propagated
    pub fn alert_confirmed(
        &mut self,
        multisigned: Multisigned<H::Hash, MK>,
    ) -> Result<ForkingNotification<H, D, MK::Signature>, Error> {
        let alert = match self.known_alerts.get(multisigned.as_signable()) {
            Some(alert) => alert.as_signable(),
            None => return Err(Error::UnknownAlertRMC),
        };
        let forker = alert.proof.0.as_signable().creator();
        self.known_rmcs.insert((alert.sender, forker), alert.hash());
        self.verify_commitment(alert)?;
        Ok(ForkingNotification::Units(alert.legit_units.clone()))
    }
}

#[cfg(test)]
mod tests {
    use crate::units::ControlHash;
    use crate::{
        alerts::{
            handler::{Error, Handler, RmcResponse},
            Alert, AlertMessage, ForkProof, ForkingNotification,
        },
        units::{FullUnit, PreUnit},
        PartiallyMultisigned, Recipient, Round,
    };
    use aleph_bft_mock::{Data, Hasher64, Keychain, Signature};
    use aleph_bft_rmc::Message;
    use aleph_bft_types::{NodeCount, NodeIndex, NodeMap, Signable, Signed};

    type TestForkProof = ForkProof<Hasher64, Data, Signature>;

    fn full_unit(
        n_members: NodeCount,
        node_id: NodeIndex,
        round: Round,
        variant: Option<u32>,
    ) -> FullUnit<Hasher64, Data> {
        FullUnit::new(
            PreUnit::new(
                node_id,
                round,
                ControlHash::new(&NodeMap::with_size(n_members)),
            ),
            variant,
            0,
        )
    }

    /// Fabricates proof of a fork by a particular node, given its private key.
    fn make_fork_proof(
        node_id: NodeIndex,
        keychain: &Keychain,
        round: Round,
        n_members: NodeCount,
    ) -> TestForkProof {
        let unit_0 = full_unit(n_members, node_id, round, Some(0));
        let unit_1 = full_unit(n_members, node_id, round, Some(1));
        let signed_unit_0 = Signed::sign(unit_0, keychain).into_unchecked();
        let signed_unit_1 = Signed::sign(unit_1, keychain).into_unchecked();
        (signed_unit_0, signed_unit_1)
    }

    #[test]
    fn distributes_alert_from_units() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(0);
        let forker_index = NodeIndex(6);
        let own_keychain = Keychain::new(n_members, own_index);
        let forker_keychain = Keychain::new(n_members, forker_index);
        let mut this = Handler::new(own_keychain, 0);
        let fork_proof = make_fork_proof(forker_index, &forker_keychain, 0, n_members);
        let alert = Alert::new(own_index, fork_proof, vec![]);
        let signed_alert = Signed::sign(alert.clone(), &this.keychain).into_unchecked();
        let alert_hash = Signable::hash(&alert);
        assert_eq!(
            this.on_own_alert(alert),
            (
                AlertMessage::ForkAlert(signed_alert),
                Recipient::Everyone,
                alert_hash,
            ),
        );
    }

    #[test]
    fn reacts_to_correctly_incoming_alert() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(1);
        let forker_index = NodeIndex(6);
        let own_keychain = Keychain::new(n_members, own_index);
        let forker_keychain = Keychain::new(n_members, forker_index);
        let mut this = Handler::new(own_keychain, 0);
        let fork_proof = make_fork_proof(forker_index, &forker_keychain, 0, n_members);
        let alert = Alert::new(own_index, fork_proof.clone(), vec![]);
        let alert_hash = Signable::hash(&alert);
        let signed_alert = Signed::sign(alert, &this.keychain).into_unchecked();
        assert_eq!(
            this.on_network_alert(signed_alert),
            Ok((Some(ForkingNotification::Forker(fork_proof)), alert_hash)),
        );
    }

    #[test]
    fn asks_about_unknown_alert() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(0);
        let alerter_index = NodeIndex(1);
        let forker_index = NodeIndex(6);
        let own_keychain = Keychain::new(n_members, own_index);
        let alerter_keychain = Keychain::new(n_members, alerter_index);
        let forker_keychain = Keychain::new(n_members, forker_index);
        let this: Handler<Hasher64, Data, _> = Handler::new(own_keychain, 0);
        let fork_proof = make_fork_proof(forker_index, &forker_keychain, 0, n_members);
        let alert = Alert::new(alerter_index, fork_proof, vec![]);
        let alert_hash = Signable::hash(&alert);
        let signed_alert_hash =
            Signed::sign_with_index(alert_hash, &alerter_keychain).into_unchecked();
        let response = this.on_rmc_message(alerter_index, Message::SignedHash(signed_alert_hash));
        assert_eq!(
            response,
            RmcResponse::AlertRequest(alert_hash, Recipient::Node(alerter_index),),
        );
    }

    #[test]
    fn ignores_wrong_alert() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(0);
        let forker_index = NodeIndex(6);
        let own_keychain = Keychain::new(n_members, own_index);
        let forker_keychain = Keychain::new(n_members, forker_index);
        let mut this = Handler::new(own_keychain, 0);
        let valid_unit = Signed::sign(
            full_unit(n_members, forker_index, 0, Some(0)),
            &forker_keychain,
        )
        .into_unchecked();
        let wrong_fork_proof = (valid_unit.clone(), valid_unit);
        let wrong_alert = Alert::new(own_index, wrong_fork_proof, vec![]);
        let signed_wrong_alert = Signed::sign(wrong_alert, &own_keychain).into_unchecked();
        assert_eq!(
            this.on_network_alert(signed_wrong_alert),
            Err(Error::SingleUnit(own_index)),
        );
    }

    #[test]
    fn responds_to_alert_queries() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(0);
        let forker_index = NodeIndex(6);
        let own_keychain = Keychain::new(n_members, own_index);
        let forker_keychain = Keychain::new(n_members, forker_index);
        let mut this = Handler::new(own_keychain, 0);
        let alert = Alert::new(
            own_index,
            make_fork_proof(forker_index, &forker_keychain, 0, n_members),
            vec![],
        );
        let alert_hash = Signable::hash(&alert);
        let signed_alert = Signed::sign(alert, &own_keychain).into_unchecked();
        this.on_network_alert(signed_alert.clone()).unwrap();
        for i in 1..n_members.0 {
            let node_id = NodeIndex(i);
            assert_eq!(
                this.on_alert_request(node_id, alert_hash),
                Ok((signed_alert.clone(), Recipient::Node(node_id),)),
            );
        }
    }

    #[test]
    fn notifies_only_about_multisigned_alert() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(0);
        let other_honest_node = NodeIndex(1);
        let double_committer = NodeIndex(5);
        let forker_index = NodeIndex(6);
        let keychains: Vec<_> = (0..n_members.0)
            .map(|i| Keychain::new(n_members, NodeIndex(i)))
            .collect();
        let mut this = Handler::new(keychains[own_index.0], 0);
        let fork_proof = make_fork_proof(forker_index, &keychains[forker_index.0], 0, n_members);
        let empty_alert = Alert::new(double_committer, fork_proof.clone(), vec![]);
        let empty_alert_hash = Signable::hash(&empty_alert);
        let signed_empty_alert =
            Signed::sign(empty_alert, &keychains[double_committer.0]).into_unchecked();
        let signed_empty_alert_hash =
            Signed::sign_with_index(empty_alert_hash, &keychains[double_committer.0])
                .into_unchecked();
        let multisigned_empty_alert_hash = signed_empty_alert_hash
            .check(&keychains[double_committer.0])
            .expect("the signature is correct")
            .into_partially_multisigned(&keychains[double_committer.0]);
        assert_eq!(
            this.on_network_alert(signed_empty_alert),
            Ok((
                Some(ForkingNotification::Forker(fork_proof.clone())),
                empty_alert_hash,
            )),
        );
        let message = Message::MultisignedHash(multisigned_empty_alert_hash.into_unchecked());
        assert_eq!(
            this.on_rmc_message(other_honest_node, message.clone()),
            RmcResponse::RmcMessage(message),
        );
        let forker_unit = fork_proof.0.clone();
        let nonempty_alert = Alert::new(double_committer, fork_proof, vec![forker_unit]);
        let nonempty_alert_hash = Signable::hash(&nonempty_alert);
        let signed_nonempty_alert =
            Signed::sign(nonempty_alert, &keychains[double_committer.0]).into_unchecked();
        let signed_nonempty_alert_hash =
            Signed::sign_with_index(nonempty_alert_hash, &keychains[double_committer.0])
                .into_unchecked();
        let mut multisigned_nonempty_alert_hash = signed_nonempty_alert_hash
            .check(&keychains[double_committer.0])
            .expect("the signature is correct")
            .into_partially_multisigned(&keychains[double_committer.0]);
        for i in 1..n_members.0 - 2 {
            let node_id = NodeIndex(i);
            let signed_nonempty_alert_hash =
                Signed::sign_with_index(nonempty_alert_hash, &keychains[node_id.0])
                    .into_unchecked();
            multisigned_nonempty_alert_hash = multisigned_nonempty_alert_hash.add_signature(
                signed_nonempty_alert_hash
                    .check(&keychains[double_committer.0])
                    .expect("the signature is correct"),
                &keychains[double_committer.0],
            );
        }
        let message = Message::MultisignedHash(multisigned_nonempty_alert_hash.into_unchecked());
        assert_eq!(
            this.on_network_alert(signed_nonempty_alert),
            Err(Error::RepeatedAlert(double_committer, forker_index)),
        );
        assert_eq!(
            this.on_rmc_message(other_honest_node, message.clone()),
            RmcResponse::RmcMessage(message),
        );
    }

    #[test]
    fn ignores_insufficiently_multisigned_alert() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(0);
        let other_honest_node = NodeIndex(1);
        let double_committer = NodeIndex(5);
        let forker_index = NodeIndex(6);
        let keychains: Vec<_> = (0..n_members.0)
            .map(|i| Keychain::new(n_members, NodeIndex(i)))
            .collect();
        let mut this = Handler::new(keychains[own_index.0], 0);
        let fork_proof = make_fork_proof(forker_index, &keychains[forker_index.0], 0, n_members);
        let empty_alert = Alert::new(double_committer, fork_proof.clone(), vec![]);
        let empty_alert_hash = Signable::hash(&empty_alert);
        let signed_empty_alert =
            Signed::sign(empty_alert, &keychains[double_committer.0]).into_unchecked();
        assert_eq!(
            this.on_network_alert(signed_empty_alert),
            Ok((
                Some(ForkingNotification::Forker(fork_proof.clone())),
                empty_alert_hash,
            )),
        );
        let forker_unit = fork_proof.0.clone();
        let nonempty_alert = Alert::new(double_committer, fork_proof, vec![forker_unit]);
        let nonempty_alert_hash = Signable::hash(&nonempty_alert);
        let signed_nonempty_alert =
            Signed::sign(nonempty_alert, &keychains[double_committer.0]).into_unchecked();
        let signed_nonempty_alert_hash =
            Signed::sign_with_index(nonempty_alert_hash, &keychains[double_committer.0])
                .into_unchecked();
        let mut multisigned_nonempty_alert_hash = signed_nonempty_alert_hash
            .check(&keychains[double_committer.0])
            .expect("the signature is correct")
            .into_partially_multisigned(&keychains[double_committer.0]);
        for i in 1..3 {
            let node_id = NodeIndex(i);
            let signed_nonempty_alert_hash =
                Signed::sign_with_index(nonempty_alert_hash, &keychains[node_id.0])
                    .into_unchecked();
            multisigned_nonempty_alert_hash = multisigned_nonempty_alert_hash.add_signature(
                signed_nonempty_alert_hash
                    .check(&keychains[double_committer.0])
                    .expect("the signature is correct"),
                &keychains[double_committer.0],
            );
        }
        let message = Message::MultisignedHash(multisigned_nonempty_alert_hash.into_unchecked());
        assert_eq!(
            this.on_network_alert(signed_nonempty_alert),
            Err(Error::RepeatedAlert(double_committer, forker_index)),
        );
        assert_eq!(
            this.on_rmc_message(other_honest_node, message.clone()),
            RmcResponse::RmcMessage(message),
        );
    }

    #[test]
    fn verify_fork_ok() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(0);
        let forker_index = NodeIndex(6);
        let own_keychain = Keychain::new(n_members, own_index);
        let forker_keychain = Keychain::new(n_members, forker_index);
        let this = Handler::new(own_keychain, 0);
        let fork_proof = make_fork_proof(forker_index, &forker_keychain, 0, n_members);
        let alert = Alert::new(own_index, fork_proof, vec![]);
        assert_eq!(this.verify_fork(&alert), Ok(()));
    }

    #[test]
    fn verify_fork_wrong_session() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(0);
        let forker_index = NodeIndex(6);
        let own_keychain = Keychain::new(n_members, own_index);
        let forker_keychain = Keychain::new(n_members, forker_index);
        let this = Handler::new(own_keychain, 1);
        let fork_proof = make_fork_proof(forker_index, &forker_keychain, 0, n_members);
        let alert = Alert::new(own_index, fork_proof, vec![]);
        assert_eq!(
            this.verify_fork(&alert),
            Err(Error::WrongSession(own_index))
        );
    }

    #[test]
    fn verify_fork_different_creators() {
        let n_members = NodeCount(7);
        let keychains: Vec<_> = (0..n_members.0)
            .map(|i| Keychain::new(n_members, NodeIndex(i)))
            .collect();
        let this = Handler::new(keychains[0], 0);
        let fork_proof = {
            let unit_0 = full_unit(n_members, NodeIndex(6), 0, Some(0));
            let unit_1 = full_unit(n_members, NodeIndex(5), 0, Some(0));
            let signed_unit_0 = Signed::sign(unit_0, &keychains[6]).into_unchecked();
            let signed_unit_1 = Signed::sign(unit_1, &keychains[5]).into_unchecked();
            (signed_unit_0, signed_unit_1)
        };
        let sender = NodeIndex(0);
        let alert = Alert::new(sender, fork_proof, vec![]);
        assert_eq!(this.verify_fork(&alert), Err(Error::WrongCreator(sender)));
    }

    #[test]
    fn verify_fork_different_rounds() {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(0);
        let forker_index = NodeIndex(6);
        let own_keychain = Keychain::new(n_members, own_index);
        let forker_keychain = Keychain::new(n_members, forker_index);
        let this = Handler::new(own_keychain, 0);
        let fork_proof = {
            let unit_0 = full_unit(n_members, forker_index, 0, Some(0));
            let unit_1 = full_unit(n_members, forker_index, 1, Some(0));
            let signed_unit_0 = Signed::sign(unit_0, &forker_keychain).into_unchecked();
            let signed_unit_1 = Signed::sign(unit_1, &forker_keychain).into_unchecked();
            (signed_unit_0, signed_unit_1)
        };
        let alert = Alert::new(own_index, fork_proof, vec![]);
        assert_eq!(
            this.verify_fork(&alert),
            Err(Error::DifferentRounds(own_index))
        );
    }

    #[test]
    fn alert_confirmed_out_of_the_blue() {
        alert_confirmed(false, true);
    }

    #[test]
    fn alert_confirmed_bad_commitment() {
        alert_confirmed(true, false);
    }

    #[test]
    fn alert_confirmed_correct() {
        alert_confirmed(true, true);
    }

    fn alert_confirmed(make_known: bool, good_commitment: bool) {
        let n_members = NodeCount(7);
        let own_index = NodeIndex(1);
        let forker_index = NodeIndex(6);
        let keychains: Vec<_> = (0..n_members.0)
            .map(|i| Keychain::new(n_members, NodeIndex(i)))
            .collect();
        let mut this = Handler::new(keychains[own_index.0], 0);
        let fork_proof = if good_commitment {
            make_fork_proof(forker_index, &keychains[forker_index.0], 0, n_members)
        } else {
            let unit_0 = full_unit(n_members, forker_index, 0, Some(0));
            let unit_1 = full_unit(n_members, forker_index, 1, Some(1));
            let signed_unit_0 = Signed::sign(unit_0, &keychains[forker_index.0]).into_unchecked();
            let signed_unit_1 = Signed::sign(unit_1, &keychains[forker_index.0]).into_unchecked();
            (signed_unit_0, signed_unit_1)
        };
        let alert = Alert::new(own_index, fork_proof, vec![]);
        let alert_hash = Signable::hash(&alert);
        let signed_alert = Signed::sign(alert, &keychains[own_index.0]).into_unchecked();
        if make_known {
            let _ = this.on_network_alert(signed_alert);
        }
        let signed_alert_hash =
            Signed::sign_with_index(alert_hash, &keychains[own_index.0]).into_unchecked();
        let mut multisigned_alert_hash = signed_alert_hash
            .check(&keychains[forker_index.0])
            .expect("the signature is correct")
            .into_partially_multisigned(&keychains[own_index.0]);
        for i in 1..n_members.0 - 1 {
            let node_id = NodeIndex(i);
            let signed_alert_hash =
                Signed::sign_with_index(alert_hash, &keychains[node_id.0]).into_unchecked();
            multisigned_alert_hash = multisigned_alert_hash.add_signature(
                signed_alert_hash
                    .check(&keychains[forker_index.0])
                    .expect("the signature is correct"),
                &keychains[forker_index.0],
            );
        }
        assert!(multisigned_alert_hash.is_complete());
        let multisigned_alert_hash = match multisigned_alert_hash {
            PartiallyMultisigned::Complete { multisigned } => multisigned,
            PartiallyMultisigned::Incomplete { .. } => unreachable!(),
        };
        let expected = match (make_known, good_commitment) {
            (true, true) => Ok(ForkingNotification::Units(vec![])),
            (true, false) => Err(Error::UnknownAlertRMC),
            (false, true) => Err(Error::UnknownAlertRMC),
            (false, false) => Err(Error::UnknownAlertRMC),
        };
        assert_eq!(this.alert_confirmed(multisigned_alert_hash), expected);
    }
}
