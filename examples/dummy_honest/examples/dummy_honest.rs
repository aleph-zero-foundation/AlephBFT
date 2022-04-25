use aleph_bft::{run_session, Recipient};
use aleph_bft_mock::{
    Data, DataProvider, FinalizationHandler, Hasher64, Keychain, Loader, PartialMultisignature,
    Saver, Signature, Spawner,
};
use chrono::Local;
use clap::Parser;
use codec::{Decode, Encode};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, StreamExt,
};
use libp2p::{
    development_transport,
    floodsub::{self, Floodsub, FloodsubEvent},
    identity,
    mdns::{Mdns, MdnsConfig, MdnsEvent},
    swarm::{NetworkBehaviourEventProcess, SwarmBuilder},
    NetworkBehaviour, PeerId, Swarm,
};
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use std::{error::Error, io::Write, sync::Arc, time::Duration};

const ALEPH_PROTOCOL_NAME: &str = "aleph";

/// Dummy honest node example.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Our index
    #[clap(long)]
    my_id: usize,

    /// Size of the committee
    #[clap(long)]
    n_members: usize,

    /// Number of data to be finalized
    #[clap(long)]
    n_finalized: usize,
}

#[tokio::main]
async fn main() {
    env_logger::builder()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} {}: {}",
                record.level(),
                Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Debug)
        .init();

    let args = Args::parse();

    info!(target: "dummy-honest", "Getting network up.");
    let (network, mut manager) = Network::new().await.unwrap();
    let (close_network, exit) = oneshot::channel();
    let network_handle = tokio::spawn(async move { manager.run(exit).await });

    let data_provider = DataProvider::new();
    let (finalization_handler, mut finalized_rx) = FinalizationHandler::new();

    let backup_loader = Loader::new(vec![]);
    let backup_saver = Saver::new(Arc::new(Mutex::new(vec![])));
    let local_io = aleph_bft::LocalIO::new(
        data_provider,
        finalization_handler,
        backup_saver,
        backup_loader,
    );

    let (close_member, exit) = oneshot::channel();
    let member_handle = tokio::spawn(async move {
        let keychain = Keychain::new(args.n_members.into(), args.my_id.into());
        let config = aleph_bft::default_config(args.n_members.into(), args.my_id.into(), 0);
        run_session(config, local_io, network, keychain, Spawner {}, exit).await
    });

    for i in 0..args.n_finalized {
        match finalized_rx.next().await {
            Some(_) => debug!(target: "dummy-honest", "Got new batch. Finalized: {:?}", i+1),
            None => {
                error!(target: "dummy-honest", "Finalization stream finished too soon. Got {:?} batches, wanted {:?} batches", i+1, args.n_finalized);
                panic!("Finalization stream finished too soon.");
            }
        }
    }
    close_member.send(()).expect("should send");
    member_handle.await.unwrap();
    close_network.send(()).expect("should send");
    network_handle.await.unwrap();
}

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

struct NetworkManager {
    swarm: Swarm<Behaviour>,
    outgoing_rx: mpsc::UnboundedReceiver<Vec<u8>>,
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
                    event.unwrap();
                }
                _ = exit.next() => break,
            }
        }
    }
}

type NetworkData = aleph_bft::NetworkData<Hasher64, Data, Signature, PartialMultisignature>;

struct Network {
    outgoing_tx: mpsc::UnboundedSender<Vec<u8>>,
    msg_rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

#[async_trait::async_trait]
impl aleph_bft::Network<NetworkData> for Network {
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
