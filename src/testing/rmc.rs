use crate::{
    nodes::NodeCount,
    rmc::*,
    signed::*,
    testing::signed::{TestMultiKeychain, TestPartialMultisignature, TestSignature},
    NodeIndex,
};
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    future::{self, BoxFuture, FutureExt},
    stream::{self, Stream},
    StreamExt,
};
use rand::Rng;
use std::{collections::HashMap, pin::Pin, time::Duration};

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Ord, PartialOrd)]
struct Hash {
    byte: u8,
}

impl Signable for Hash {
    type Hash = [u8; 1];

    fn hash(&self) -> Self::Hash {
        [self.byte]
    }
}

type TestMessage = Message<Hash, TestSignature, TestPartialMultisignature>;

struct TestNetwork {
    outgoing_rx: Pin<Box<dyn Stream<Item = (NodeIndex, TestMessage)>>>,
    incoming_txs: Vec<UnboundedSender<TestMessage>>,
    message_filter: Box<dyn FnMut(NodeIndex, TestMessage) -> bool>,
}

impl TestNetwork {
    fn new(
        node_count: NodeCount,
        message_filter: impl FnMut(NodeIndex, TestMessage) -> bool + 'static,
    ) -> (
        Self,
        Vec<(
            UnboundedReceiver<TestMessage>,
            UnboundedSender<(NodeIndex, TestMessage)>,
        )>,
    ) {
        let all_nodes: Vec<_> = (0..node_count.0).map(NodeIndex).collect();
        let (incomng_txs, incoming_rxs): (Vec<_>, Vec<_>) =
            all_nodes.iter().map(|_| unbounded::<TestMessage>()).unzip();
        let (outgoing_txs, outgoing_rxs): (Vec<_>, Vec<_>) = {
            all_nodes
                .iter()
                .map(|_| {
                    let (tx, rx) = unbounded::<(NodeIndex, TestMessage)>();
                    (tx, rx)
                })
                .unzip()
        };
        let network = TestNetwork {
            outgoing_rx: Box::pin(stream::select_all(outgoing_rxs)),
            incoming_txs: incomng_txs,
            message_filter: Box::new(message_filter),
        };

        let channels = incoming_rxs.into_iter().zip(outgoing_txs).collect();
        (network, channels)
    }

    fn broadcast_message(&mut self, msg: TestMessage) {
        for tx in &mut self.incoming_txs {
            tx.unbounded_send(msg.clone())
                .expect("Channel should be open");
        }
    }
}

impl TestNetwork {
    async fn run(&mut self) {
        while let Some((recipient, message)) = self.outgoing_rx.next().await {
            if (self.message_filter)(recipient, message.clone()) {
                self.incoming_txs[recipient.0]
                    .unbounded_send(message)
                    .expect("Channel should be open");
            }
        }
    }
}

fn prepare_keychains(node_count: NodeCount) -> Vec<TestMultiKeychain> {
    (0..node_count.0)
        .map(|i| TestMultiKeychain::new(node_count, i.into()))
        .collect()
}

struct TestData<'a> {
    network: TestNetwork,
    rmcs: Vec<ReliableMulticast<'a, Hash, TestMultiKeychain>>,
}

impl<'a> TestData<'a> {
    fn new(
        node_count: NodeCount,
        keychains: &'a [TestMultiKeychain],
        message_filter: impl FnMut(NodeIndex, TestMessage) -> bool + 'static,
    ) -> Self {
        let (network, channels) = TestNetwork::new(node_count, message_filter);
        let mut rmcs = Vec::new();
        for (i, (rx, tx)) in channels.into_iter().enumerate() {
            let rmc = ReliableMulticast::new(
                rx,
                tx,
                &keychains[i],
                node_count,
                DoublingDelayScheduler::new(Duration::from_millis(1)),
            );
            rmcs.push(rmc);
        }
        TestData { network, rmcs }
    }

    async fn collect_multisigned_hashes(
        mut self,
        count: usize,
    ) -> HashMap<NodeIndex, Vec<Multisigned<'a, Hash, TestMultiKeychain>>> {
        let mut hashes = HashMap::new();

        for _ in 0..count {
            // covert each RMC into a future returning an optional unchecked multisigned hash.
            let rmc_futures: Vec<BoxFuture<Multisigned<'a, Hash, TestMultiKeychain>>> = self
                .rmcs
                .iter_mut()
                .map(|rmc| rmc.next_multisigned_hash().boxed())
                .collect();
            tokio::select! {
                (unchecked, i, _) = future::select_all(rmc_futures) => {
                    hashes.entry(i.into()).or_insert_with(Vec::new).push(unchecked);
                }
                _ = self.network.run() => {
                    panic!("network ended unexpectedly");
                }
            }
        }
        hashes
    }
}

/// Create 10 honest nodes and let each of them start rmc for the same hash.
#[tokio::test]
async fn simple_scenario() {
    let node_count = NodeCount(10);
    let keychains = prepare_keychains(node_count);
    let mut data = TestData::new(node_count, &keychains, |_, _| true);

    let hash = Hash { byte: 56 };
    for i in 0..node_count.0 {
        data.rmcs[i].start_rmc(hash);
    }

    let hashes = data.collect_multisigned_hashes(node_count.0).await;
    assert_eq!(hashes.len(), node_count.0);
    for i in 0..node_count.0 {
        let multisignatures = &hashes[&i.into()];
        assert_eq!(multisignatures.len(), 1);
        assert_eq!(multisignatures[0].as_signable(), &hash);
    }
}

/// Each message is delivered with 20% probability
#[tokio::test]
async fn faulty_network() {
    let node_count = NodeCount(10);
    let keychains = prepare_keychains(node_count);
    let mut rng = rand::thread_rng();
    let mut data = TestData::new(node_count, &keychains, move |_, _| rng.gen_range(0..5) == 0);

    let hash = Hash { byte: 56 };
    for i in 0..node_count.0 {
        data.rmcs[i].start_rmc(hash);
    }

    let hashes = data.collect_multisigned_hashes(node_count.0).await;
    assert_eq!(hashes.len(), node_count.0);
    for i in 0..node_count.0 {
        let multisignatures = &hashes[&i.into()];
        assert_eq!(multisignatures.len(), 1);
        assert_eq!(multisignatures[0].as_signable(), &hash);
    }
}

/// Only 7 nodes start rmc and one of the nodes which didn't start rmc
/// is delivered only messages with complete multisignatures
#[tokio::test]
async fn node_hearing_only_multisignatures() {
    let node_count = NodeCount(10);
    let keychains = prepare_keychains(node_count);
    let mut data = TestData::new(node_count, &keychains, move |node_ix, message| {
        !matches!((node_ix.0, message), (0, Message::SignedHash(_)))
    });

    let threshold = (2 * node_count.0 + 1) / 3;
    let hash = Hash { byte: 56 };
    for i in 0..threshold {
        data.rmcs[i].start_rmc(hash);
    }

    let hashes = data.collect_multisigned_hashes(node_count.0).await;
    assert_eq!(hashes.len(), node_count.0);
    for i in 0..node_count.0 {
        let multisignatures = &hashes[&i.into()];
        assert_eq!(multisignatures.len(), 1);
        assert_eq!(multisignatures[0].as_signable(), &hash);
    }
}

fn bad_signature() -> TestSignature {
    TestSignature {
        msg: Vec::new(),
        index: 111.into(),
    }
}

fn bad_multisignature() -> TestPartialMultisignature {
    TestPartialMultisignature {
        msg: Vec::new(),
        signers: Default::default(),
    }
}

/// 7 honest nodes and 3 dishonest nodes which emit bad signatures and multisignatures
#[tokio::test]
async fn bad_signatures_and_multisignatures_are_ignored() {
    let node_count = NodeCount(10);
    let keychains = prepare_keychains(node_count);
    let mut data = TestData::new(node_count, &keychains, |_, _| true);

    let bad_hash = Hash { byte: 65 };
    let bad_msg = TestMessage::SignedHash(UncheckedSigned::new(
        Indexed::new(bad_hash, 0.into()),
        bad_signature(),
    ));
    data.network.broadcast_message(bad_msg);
    let bad_msg =
        TestMessage::MultisignedHash(UncheckedSigned::new(bad_hash, bad_multisignature()));
    data.network.broadcast_message(bad_msg);

    let hash = Hash { byte: 56 };
    for i in 0..node_count.0 {
        data.rmcs[i].start_rmc(hash);
    }

    let hashes = data.collect_multisigned_hashes(node_count.0).await;
    assert_eq!(hashes.len(), node_count.0);
    for i in 0..node_count.0 {
        let multisignatures = &hashes[&i.into()];
        assert_eq!(multisignatures.len(), 1);
        assert_eq!(multisignatures[0].as_signable(), &hash);
    }
}
