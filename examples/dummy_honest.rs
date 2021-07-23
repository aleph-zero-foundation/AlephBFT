use aleph_bft::{NodeCount, NodeIndex, OrderedBatch, Recipient, TaskHandle};
use async_trait::async_trait;
use codec::{Decode, Encode};
use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Future, FutureExt, StreamExt,
};
use log::{debug, info, warn};
use parking_lot::Mutex;
use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    error::Error,
    hash::Hasher as StdHasher,
    sync::Arc,
    time::Duration,
};

use libp2p::{
    development_transport,
    floodsub::{self, Floodsub, FloodsubEvent},
    identity,
    mdns::{Mdns, MdnsConfig, MdnsEvent},
    swarm::{NetworkBehaviourEventProcess, SwarmBuilder},
    NetworkBehaviour, PeerId, Swarm,
};

const USAGE_MSG: &str = "Missing arg. Usage
    cargo run --example honest my_id n_members n_finalized

    my_id -- our index
    n_members -- size of the committee
    n_finalized -- number of data to be finalized";

fn parse_arg(n: usize) -> usize {
    if let Some(int) = std::env::args().nth(n) {
        match int.parse::<usize>() {
            Ok(int) => int,
            Err(err) => {
                panic!("Failed to parse arg {:?}", err);
            }
        }
    } else {
        panic!("{}", USAGE_MSG);
    }
}

#[tokio::main]
async fn main() {
    env_logger::builder()
        .filter_module("dummy_honest", log::LevelFilter::Info)
        .init();

    let my_id = parse_arg(1);
    let n_members = parse_arg(2);
    let n_finalized = parse_arg(3);

    info!(target: "dummy-honest", "Getting network up.");
    let (network, mut manager) = Network::new().await.unwrap();
    let (close_network, exit) = oneshot::channel();
    tokio::spawn(async move { manager.run(exit).await });

    let (data_io, mut to_finalize) = DataIO::new();

    let (close_member, exit) = oneshot::channel();
    tokio::spawn(async move {
        let keybox = KeyBox {
            count: n_members,
            index: my_id.into(),
        };
        let config = aleph_bft::default_config(n_members.into(), my_id.into(), 0);
        let member = aleph_bft::Member::new(data_io, &keybox, config, Spawner {});

        member.run_session(network, exit).await
    });

    let mut finalized = HashSet::new();
    while let Some(batch) = to_finalize.next().await {
        for data in batch {
            if !finalized.contains(&data) {
                finalized.insert(data);
            }
        }
        debug!(target: "dummy-honest", "Got new batch. Finalized = {:?}", finalized.len());
        if finalized.len() == n_finalized {
            break;
        }
    }
    close_member.send(()).expect("should send");
    close_network.send(()).expect("should send");
}

// This is not cryptographically secure, mocked only for demonstration purposes
#[derive(PartialEq, Eq, Clone, Debug)]
struct Hasher64;

impl aleph_bft::Hasher for Hasher64 {
    type Hash = [u8; 8];

    fn hash(x: &[u8]) -> Self::Hash {
        let mut hasher = DefaultHasher::new();
        hasher.write(x);
        hasher.finish().to_ne_bytes()
    }
}

type Data = u64;
struct DataIO {
    next_data: Arc<Mutex<u64>>,
    finalized_tx: UnboundedSender<OrderedBatch<Data>>,
}

impl aleph_bft::DataIO<Data> for DataIO {
    type Error = ();
    fn get_data(&self) -> Data {
        let mut data = self.next_data.lock();
        *data += 1;

        *data
    }
    fn send_ordered_batch(&mut self, data: OrderedBatch<Data>) -> Result<(), Self::Error> {
        self.finalized_tx.unbounded_send(data).map_err(|_| ())
    }
}

impl DataIO {
    fn new() -> (Self, UnboundedReceiver<OrderedBatch<Data>>) {
        let (finalized_tx, finalized_rx) = mpsc::unbounded();
        (
            DataIO {
                next_data: Arc::new(Mutex::new(0)),
                finalized_tx,
            },
            finalized_rx,
        )
    }
}

// This is not cryptographically secure, mocked only for demonstration purposes
#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode)]
struct Signature;

// This is not cryptographically secure, mocked only for demonstration purposes
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct PartialMultisignature {
    signed_by: Vec<NodeIndex>,
}

impl aleph_bft::PartialMultisignature for PartialMultisignature {
    type Signature = Signature;
    fn add_signature(self, _: &Self::Signature, index: NodeIndex) -> Self {
        let Self { mut signed_by } = self;
        for id in &signed_by {
            if *id == index {
                return Self { signed_by };
            }
        }
        signed_by.push(index);
        Self { signed_by }
    }
}

// This is not cryptographically secure, mocked only for demonstration purposes
#[derive(Clone)]
struct KeyBox {
    count: usize,
    index: NodeIndex,
}

#[async_trait]
impl aleph_bft::KeyBox for KeyBox {
    type Signature = Signature;

    fn node_count(&self) -> NodeCount {
        self.count.into()
    }

    async fn sign(&self, _msg: &[u8]) -> Self::Signature {
        Signature {}
    }
    fn verify(&self, _msg: &[u8], _sgn: &Self::Signature, _index: NodeIndex) -> bool {
        true
    }
}

impl aleph_bft::MultiKeychain for KeyBox {
    type PartialMultisignature = PartialMultisignature;
    fn from_signature(&self, _: &Self::Signature, index: NodeIndex) -> Self::PartialMultisignature {
        let signed_by = vec![index];
        PartialMultisignature { signed_by }
    }
    fn is_complete(&self, _: &[u8], partial: &Self::PartialMultisignature) -> bool {
        (self.count * 2) / 3 < partial.signed_by.len()
    }
}

impl aleph_bft::Index for KeyBox {
    fn index(&self) -> NodeIndex {
        self.index
    }
}

#[derive(Clone)]
struct Spawner;

impl aleph_bft::SpawnHandle for Spawner {
    fn spawn(&self, _: &str, task: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(task);
    }
    fn spawn_essential(
        &self,
        _: &str,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> TaskHandle {
        Box::pin(async move { tokio::spawn(task).await.map_err(|_| ()) })
    }
}

const ALEPH_PROTOCOL_NAME: &str = "aleph";

type NetworkData = aleph_bft::NetworkData<Hasher64, Data, Signature, PartialMultisignature>;

#[derive(NetworkBehaviour)]
struct Behaviour {
    mdns: Mdns,
    floodsub: Floodsub,
    #[behaviour(ignore)]
    peers: Vec<PeerId>,
    #[behaviour(ignore)]
    msg_tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl NetworkBehaviourEventProcess<MdnsEvent> for Behaviour {
    fn inject_event(&mut self, event: MdnsEvent) {
        if let MdnsEvent::Discovered(list) = event {
            for (peer, _) in list {
                if self.peers.iter().any(|p| *p == peer) {
                    continue;
                }
                self.peers.push(peer);
                self.floodsub.add_node_to_partial_view(peer);
            }
        }
    }
}

impl NetworkBehaviourEventProcess<FloodsubEvent> for Behaviour {
    fn inject_event(&mut self, message: FloodsubEvent) {
        if let FloodsubEvent::Message(message) = message {
            self.msg_tx
                .unbounded_send(message.data)
                .expect("should succeed");
        }
    }
}

struct Network {
    outgoing_tx: mpsc::UnboundedSender<Vec<u8>>,
    msg_rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

#[async_trait::async_trait]
impl aleph_bft::Network<Hasher64, Data, Signature, PartialMultisignature> for Network {
    fn send(&self, data: NetworkData, _recipient: Recipient) {
        if let Err(e) = self.outgoing_tx.unbounded_send(data.encode()) {
            warn!(target: "dummy-honest", "Failed network send: {:?}", e)
        }
    }
    async fn next_event(&mut self) -> Option<NetworkData> {
        self.msg_rx.next().await.map(|msg| {
            NetworkData::decode(&mut &msg[..]).expect("honest network data should decode")
        })
    }
}

struct NetworkManager {
    swarm: Swarm<Behaviour>,
    outgoing_rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl Network {
    async fn new() -> Result<(Self, NetworkManager), Box<dyn Error>> {
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        info!(target: "dummy-honest", "Local peer id: {:?}", local_peer_id);

        let transport = development_transport(local_key).await?;

        let topic = floodsub::Topic::new(ALEPH_PROTOCOL_NAME);

        let (msg_tx, msg_rx) = mpsc::unbounded();
        let mut swarm = {
            let mdns_config = MdnsConfig {
                ttl: Duration::from_secs(6 * 60),
                query_interval: Duration::from_millis(100),
            };
            let mdns = Mdns::new(mdns_config).await?;
            let mut behaviour = Behaviour {
                floodsub: Floodsub::new(local_peer_id),
                mdns,
                peers: vec![],
                msg_tx: msg_tx.clone(),
            };
            behaviour.floodsub.subscribe(topic.clone());
            SwarmBuilder::new(transport, behaviour, local_peer_id)
                .executor(Box::new(|fut| {
                    tokio::spawn(fut);
                }))
                .build()
        };

        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

        let (outgoing_tx, outgoing_rx) = mpsc::unbounded();
        let network = Network {
            outgoing_tx,
            msg_rx,
        };
        let network_manager = NetworkManager { swarm, outgoing_rx };

        Ok((network, network_manager))
    }
}

impl NetworkManager {
    async fn run(&mut self, exit: oneshot::Receiver<()>) {
        let mut exit = exit.into_stream();
        loop {
            tokio::select! {
                maybe_pm = self.outgoing_rx.next() => {
                    if let Some(message) = maybe_pm {
                        let floodsub = &mut self.swarm.behaviour_mut().floodsub;
                        let topic = floodsub::Topic::new(ALEPH_PROTOCOL_NAME);
                        floodsub.publish(topic, message);
                    }
                }
                event = self.swarm.next() => {
                    // called only to poll inner future
                    panic!("Unexpected event: {:?}", event);
                }
                _ = exit.next() => break,
            }
        }
    }
}
