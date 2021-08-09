use crate::{
    alerts::{run, Alert, AlertConfig, AlertMessage, ForkProof, ForkingNotification},
    network::Recipient,
    nodes::{NodeCount, NodeIndex},
    rmc::Message as RmcMessage,
    signed::KeyBox as _,
    testing::mock::{Data, Hasher64, KeyBox, PartialMultisignature, Signature},
    units::{ControlHash, FullUnit, PreUnit, UnitCoord},
    Index, Indexed, NodeMap, Round, Signable, Signed, UncheckedSigned,
};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, StreamExt,
};
use futures_timer::Delay;
use log::trace;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    time::Duration,
};

type TestMessage = AlertMessage<Hasher64, Data, Signature, PartialMultisignature>;
type TestAlert = Alert<Hasher64, Data, Signature>;
type TestNotification = ForkingNotification<Hasher64, Data, Signature>;
type TestForkProof = ForkProof<Hasher64, Data, Signature>;
type TestFullUnit = FullUnit<Hasher64, Data>;

enum Input {
    Incoming(TestMessage),
    Alert(TestAlert),
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum Output {
    Outgoing(TestMessage, Recipient),
    Notification(TestNotification),
}

struct Segment {
    inputs: Vec<Input>,
    expected: HashMap<Output, usize>,
    unexpected: HashSet<Output>,
}

impl Segment {
    fn new() -> Self {
        Segment {
            inputs: Vec::new(),
            expected: HashMap::new(),
            unexpected: HashSet::new(),
        }
    }

    fn check_output(&mut self, output: Output) {
        if self.unexpected.contains(&output) {
            panic!("Unexpected {:?} emitted by alerter.", output);
        }
        match self.expected.get_mut(&output) {
            Some(count) => *count -= 1,
            None => trace!("Possibly unnecessary {:?} emitted by alerter.", output),
        }
        if self.expected.get(&output) == Some(&0) {
            self.expected.remove(&output);
        }
    }
}

struct TestCase {
    keychains: Vec<KeyBox>,
    segments: Vec<Segment>,
}

impl TestCase {
    fn new(n_members: NodeCount) -> Self {
        let mut keychains = Vec::new();
        for i in 0..n_members.0 {
            keychains.push(KeyBox::new(n_members, NodeIndex(i)))
        }
        Self {
            keychains,
            segments: vec![Segment::new()],
        }
    }

    fn keychain(&self, node: NodeIndex) -> &KeyBox {
        &self.keychains[node.0]
    }

    async fn unchecked_signed<T: Signable + Index>(
        &self,
        to_sign: T,
        signer: NodeIndex,
    ) -> UncheckedSigned<T, Signature> {
        Signed::sign(to_sign, self.keychain(signer)).await.into()
    }

    async fn indexed_unchecked_signed<T: Signable>(
        &self,
        to_sign: T,
        signer: NodeIndex,
    ) -> UncheckedSigned<Indexed<T>, Signature> {
        Signed::sign_with_index(to_sign, self.keychain(signer))
            .await
            .into()
    }

    fn full_unit(&self, forker: NodeIndex, round: Round, variant: u32) -> TestFullUnit {
        FullUnit::new(
            PreUnit::new(
                forker,
                round,
                ControlHash::new(&NodeMap::new_with_len(
                    self.keychain(NodeIndex(0)).node_count(),
                )),
            ),
            Data::new(UnitCoord::new(round, forker), variant),
            0,
        )
    }

    async fn unchecked_signed_unit(
        &self,
        creator: NodeIndex,
        round: Round,
        variant: u32,
    ) -> UncheckedSigned<TestFullUnit, Signature> {
        self.unchecked_signed(self.full_unit(creator, round, variant), creator)
            .await
    }

    async fn fork_proof(&self, forker: NodeIndex, round: Round) -> TestForkProof {
        let u0 = self.unchecked_signed_unit(forker, round, 0).await;
        let u1 = self.unchecked_signed_unit(forker, round, 1).await;
        (u0, u1)
    }

    fn alert_with_commitment(
        &self,
        sender: NodeIndex,
        proof: TestForkProof,
        commitment: Vec<UncheckedSigned<TestFullUnit, Signature>>,
    ) -> TestAlert {
        Alert::new(sender, proof, commitment)
    }

    fn alert(&self, sender: NodeIndex, proof: TestForkProof) -> TestAlert {
        self.alert_with_commitment(sender, proof, Vec::new())
    }

    fn incoming_message(&mut self, message: TestMessage) -> &mut Self {
        self.segments
            .last_mut()
            .expect("there is a segment")
            .inputs
            .push(Input::Incoming(message));
        self
    }

    fn incoming_alert(&mut self, alert: TestAlert) -> &mut Self {
        self.segments
            .last_mut()
            .expect("there is a segment")
            .inputs
            .push(Input::Alert(alert));
        self
    }

    fn outgoing_message(&mut self, message: TestMessage, recipient: Recipient) -> &mut Self {
        *self
            .segments
            .last_mut()
            .expect("there is a segment")
            .expected
            .entry(Output::Outgoing(message, recipient))
            .or_insert(0) += 1;
        self
    }

    fn outgoing_notification(&mut self, notification: TestNotification) -> &mut Self {
        *self
            .segments
            .last_mut()
            .expect("there is a segment")
            .expected
            .entry(Output::Notification(notification))
            .or_insert(0) += 1;
        self
    }

    fn unexpected_message(&mut self, message: TestMessage, recipient: Recipient) -> &mut Self {
        self.segments
            .last_mut()
            .expect("there is a segment")
            .unexpected
            .insert(Output::Outgoing(message, recipient));
        self
    }

    fn unexpected_notification(&mut self, notification: TestNotification) -> &mut Self {
        self.segments
            .last_mut()
            .expect("there is a segment")
            .unexpected
            .insert(Output::Notification(notification));
        self
    }

    fn wait(&mut self) -> &mut Self {
        self.segments.push(Segment::new());
        self
    }

    async fn test(self, keychain: KeyBox) {
        let (messages_for_network, mut messages_from_alerter) = mpsc::unbounded();
        let (messages_for_alerter, messages_from_network) = mpsc::unbounded();
        let (notifications_for_units, mut notifications_from_alerter) = mpsc::unbounded();
        let (alerts_for_alerter, alerts_from_units) = mpsc::unbounded();
        let (exit_alerter, exit) = oneshot::channel();
        let n_members = keychain.node_count();
        tokio::spawn(run(
            keychain,
            messages_for_network,
            messages_from_network,
            notifications_for_units,
            alerts_from_units,
            AlertConfig {
                n_members,
                session_id: 0,
            },
            exit,
        ));

        use Input::*;
        use Output::*;

        for mut segment in self.segments {
            for i in &segment.inputs {
                match i {
                    Incoming(message) => messages_for_alerter
                        .unbounded_send(message.clone())
                        .expect("the message channel works"),
                    Alert(alert) => alerts_for_alerter
                        .unbounded_send(alert.clone())
                        .expect("the alert channel works"),
                }
            }
            while !segment.expected.is_empty() {
                tokio::select! {
                    message = messages_from_alerter.next() => match message {
                        Some((message, recipient)) => segment.check_output(Outgoing(message, recipient)),
                        None => panic!("Message stream unexpectedly closed."),
                    },
                    notification = notifications_from_alerter.next() => match notification {
                        Some(notification) => segment.check_output(Output::Notification(notification)),
                        None => panic!("Notification stream unexpectedly closed."),
                    }
                }
                trace!("Remaining items in this segment: {:?}.", segment.expected);
            }
        }
        exit_alerter
            .send(())
            .expect("exit channel shouldn't be closed");
    }

    async fn run(self, run_as: NodeIndex) {
        let keychain = self.keychain(run_as).clone();
        let mut timeout = Delay::new(Duration::from_millis(500)).fuse();
        futures::select! {
            _ = self.test(keychain).fuse() => {},
            _ = timeout => {
                panic!("Alerter took too long to emit expected items.");
            },
        }
    }
}

#[tokio::test]
async fn distributes_alert_from_units() {
    let n_members = NodeCount(7);
    let own_index = NodeIndex(0);
    let forker = NodeIndex(6);
    let mut test_case = TestCase::new(n_members);
    let alert = test_case.alert(own_index, test_case.fork_proof(forker, 0).await);
    let signed_alert = test_case.unchecked_signed(alert.clone(), own_index).await;
    test_case
        .incoming_alert(alert.clone())
        .outgoing_message(AlertMessage::ForkAlert(signed_alert), Recipient::Everyone);
    test_case.run(own_index).await;
}

#[tokio::test]
async fn reacts_to_correctly_incoming_alert() {
    let n_members = NodeCount(7);
    let own_index = NodeIndex(0);
    let alerter_index = NodeIndex(1);
    let forker = NodeIndex(6);
    let mut test_case = TestCase::new(n_members);
    let fork_proof = test_case.fork_proof(forker, 0).await;
    let alert = test_case.alert(alerter_index, fork_proof.clone());
    let signed_alert_hash = test_case
        .indexed_unchecked_signed(Signable::hash(&alert), own_index)
        .await;
    let signed_alert = test_case
        .unchecked_signed(alert.clone(), alerter_index)
        .await;
    test_case
        .incoming_message(AlertMessage::ForkAlert(signed_alert))
        .outgoing_notification(ForkingNotification::Forker(fork_proof));
    test_case.outgoing_message(
        AlertMessage::RmcMessage(own_index, RmcMessage::SignedHash(signed_alert_hash.clone())),
        Recipient::Everyone,
    );
    test_case.run(own_index).await;
}

#[tokio::test]
async fn notifies_about_finished_alert() {
    let n_members = NodeCount(7);
    let own_index = NodeIndex(0);
    let alerter_index = NodeIndex(1);
    let forker = NodeIndex(6);
    let mut test_case = TestCase::new(n_members);
    let fork_proof = test_case.fork_proof(forker, 0).await;
    let alert = test_case.alert(alerter_index, fork_proof.clone());
    let alert_hash = Signable::hash(&alert);
    let signed_alert = test_case
        .unchecked_signed(alert.clone(), alerter_index)
        .await;
    test_case
        .incoming_message(AlertMessage::ForkAlert(signed_alert))
        .outgoing_notification(ForkingNotification::Forker(fork_proof))
        .wait();
    for i in 1..n_members.0 - 1 {
        let node_id = NodeIndex(i);
        let signed_alert_hash = test_case
            .indexed_unchecked_signed(alert_hash, node_id)
            .await;
        test_case.incoming_message(AlertMessage::RmcMessage(
            node_id,
            RmcMessage::SignedHash(signed_alert_hash),
        ));
    }
    test_case.outgoing_notification(ForkingNotification::Units(Vec::new()));
    test_case.run(own_index).await;
}

#[tokio::test]
async fn asks_about_unknown_alert() {
    let n_members = NodeCount(7);
    let own_index = NodeIndex(0);
    let alerter_index = NodeIndex(1);
    let forker = NodeIndex(6);
    let mut test_case = TestCase::new(n_members);
    let fork_proof = test_case.fork_proof(forker, 0).await;
    let alert = test_case.alert(alerter_index, fork_proof.clone());
    let alert_hash = Signable::hash(&alert);
    let signed_alert_hash = test_case
        .indexed_unchecked_signed(alert_hash, alerter_index)
        .await;
    test_case
        .incoming_message(AlertMessage::RmcMessage(
            alerter_index,
            RmcMessage::SignedHash(signed_alert_hash),
        ))
        .outgoing_message(
            AlertMessage::AlertRequest(own_index, alert_hash),
            Recipient::Node(alerter_index),
        );
    test_case.run(own_index).await;
}

#[tokio::test]
async fn ignores_wrong_alert() {
    let n_members = NodeCount(7);
    let own_index = NodeIndex(0);
    let alerter_index = NodeIndex(1);
    let forker = NodeIndex(6);
    let mut test_case = TestCase::new(n_members);
    let valid_unit = test_case.unchecked_signed_unit(alerter_index, 0, 0).await;
    let wrong_fork_proof = (valid_unit.clone(), valid_unit);
    let wrong_alert = test_case.alert(forker, wrong_fork_proof.clone());
    let signed_wrong_alert = test_case
        .unchecked_signed(wrong_alert.clone(), forker)
        .await;
    let signed_wrong_alert_hash = test_case
        .indexed_unchecked_signed(Signable::hash(&wrong_alert), own_index)
        .await;
    test_case
        .incoming_message(AlertMessage::ForkAlert(signed_wrong_alert))
        .unexpected_notification(ForkingNotification::Forker(wrong_fork_proof));
    for i in 1..n_members.0 {
        test_case.unexpected_message(
            AlertMessage::RmcMessage(
                own_index,
                RmcMessage::SignedHash(signed_wrong_alert_hash.clone()),
            ),
            Recipient::Node(NodeIndex(i)),
        );
    }
    // We also make a proper alert to actually have something to wait for.
    let fork_proof = test_case.fork_proof(forker, 0).await;
    let alert = test_case.alert(alerter_index, fork_proof.clone());
    let signed_alert = test_case
        .unchecked_signed(alert.clone(), alerter_index)
        .await;
    test_case
        .incoming_message(AlertMessage::ForkAlert(signed_alert))
        .outgoing_notification(ForkingNotification::Forker(fork_proof));
    test_case.run(own_index).await;
}

#[tokio::test]
async fn responds_to_alert_queries() {
    let n_members = NodeCount(7);
    let own_index = NodeIndex(0);
    let querier = NodeIndex(1);
    let forker = NodeIndex(6);
    let mut test_case = TestCase::new(n_members);
    let alert = test_case.alert(own_index, test_case.fork_proof(forker, 0).await);
    let alert_hash = Signable::hash(&alert);
    let signed_alert = test_case.unchecked_signed(alert.clone(), own_index).await;
    let signed_alert_hash = test_case
        .indexed_unchecked_signed(alert_hash, own_index)
        .await;
    test_case
        .incoming_alert(alert.clone())
        .outgoing_message(
            AlertMessage::ForkAlert(signed_alert.clone()),
            Recipient::Everyone,
        )
        .outgoing_message(
            AlertMessage::RmcMessage(own_index, RmcMessage::SignedHash(signed_alert_hash.clone())),
            Recipient::Everyone,
        )
        .wait()
        .incoming_message(AlertMessage::AlertRequest(querier, alert_hash))
        .outgoing_message(
            AlertMessage::ForkAlert(signed_alert.clone()),
            Recipient::Node(querier),
        );
    for i in 1..n_members.0 {
        let node_id = NodeIndex(i);
        test_case
            .incoming_message(AlertMessage::AlertRequest(node_id, alert_hash))
            .outgoing_message(
                AlertMessage::ForkAlert(signed_alert.clone()),
                Recipient::Node(node_id),
            );
    }
    test_case.run(own_index).await;
}

#[tokio::test]
async fn notifies_only_about_multisigned_alert() {
    let n_members = NodeCount(7);
    let own_index = NodeIndex(0);
    let other_honest_node = NodeIndex(1);
    let double_committer = NodeIndex(5);
    let forker = NodeIndex(6);
    let mut test_case = TestCase::new(n_members);
    let fork_proof = test_case.fork_proof(forker, 0).await;
    let empty_alert = test_case.alert(double_committer, fork_proof.clone());
    let empty_alert_hash = Signable::hash(&empty_alert);
    let signed_empty_alert = test_case
        .unchecked_signed(empty_alert.clone(), double_committer)
        .await;
    let signed_empty_alert_hash = test_case
        .indexed_unchecked_signed(empty_alert_hash, double_committer)
        .await;
    test_case
        .incoming_message(AlertMessage::ForkAlert(signed_empty_alert))
        .incoming_message(AlertMessage::RmcMessage(
            double_committer,
            RmcMessage::SignedHash(signed_empty_alert_hash),
        ))
        .outgoing_notification(ForkingNotification::Forker(fork_proof.clone()))
        .unexpected_notification(ForkingNotification::Units(Vec::new()))
        .wait();
    let forker_unit = fork_proof.0.clone();
    let nonempty_alert = test_case.alert_with_commitment(
        double_committer,
        fork_proof.clone(),
        vec![forker_unit.clone()],
    );
    let nonempty_alert_hash = Signable::hash(&nonempty_alert);
    let signed_nonempty_alert = test_case
        .unchecked_signed(nonempty_alert.clone(), double_committer)
        .await;
    let signed_nonempty_alert_hash = test_case
        .indexed_unchecked_signed(nonempty_alert_hash, double_committer)
        .await;
    let keychain = test_case.keychain(double_committer);
    let mut multisigned_nonempty_alert_hash = signed_nonempty_alert_hash
        .check(keychain)
        .expect("the signature is correct")
        .into_partially_multisigned(keychain);
    for i in 1..n_members.0 - 2 {
        let node_id = NodeIndex(i);
        let signed_nonempty_alert_hash = test_case
            .indexed_unchecked_signed(nonempty_alert_hash, node_id)
            .await;
        multisigned_nonempty_alert_hash = multisigned_nonempty_alert_hash.add_signature(
            signed_nonempty_alert_hash
                .check(keychain)
                .expect("the signature is correct"),
            keychain,
        );
    }
    let unchecked_multisigned_nonempty_alert_hash =
        multisigned_nonempty_alert_hash.into_unchecked();
    test_case
        .incoming_message(AlertMessage::ForkAlert(signed_nonempty_alert))
        .incoming_message(AlertMessage::RmcMessage(
            other_honest_node,
            RmcMessage::MultisignedHash(unchecked_multisigned_nonempty_alert_hash),
        ))
        .outgoing_notification(ForkingNotification::Units(vec![forker_unit]))
        .unexpected_notification(ForkingNotification::Units(Vec::new()));
    test_case.run(own_index).await;
}
