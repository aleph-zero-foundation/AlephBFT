use crate::chain::Block;
use aleph_bft::{NodeIndex, Recipient, TaskHandle};
use codec::{Decode, Encode};
use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    prelude::*,
    Future, FutureExt, StreamExt,
};

use log::{debug, info, warn};

use std::{collections::HashMap, error::Error, io, iter, time::Duration};

use libp2p::{
    core::upgrade,
    identity,
    mdns::{Mdns, MdnsConfig, MdnsEvent},
    mplex, noise,
    request_response::{
        ProtocolSupport, RequestResponse, RequestResponseCodec, RequestResponseConfig,
        RequestResponseEvent, RequestResponseMessage,
    },
    swarm::{NetworkBehaviourEventProcess, SwarmBuilder},
    tcp::TokioTcpConfig,
    NetworkBehaviour, PeerId, Swarm, Transport,
};

use crate::{
    chain::Data,
    crypto::{Hasher256, PartialMultisignature, Signature},
};

#[derive(Clone)]
pub(crate) struct Spawner;

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

const ALEPH_PROTOCOL_NAME: &str = "/alephbft/test/1";

pub(crate) type NetworkData =
    aleph_bft::NetworkData<Hasher256, Data, Signature, PartialMultisignature>;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Encode, Decode)]
enum Message {
    Auth(NodeIndex),
    Consensus(NetworkData),
    Block(Block),
}

/// Implements the libp2p [`RequestResponseCodec`] trait.
/// GenericCodec is a suitably adjusted version of the GenericCodec implemented in sc-network in substrate.
/// Defines how streams of bytes are turned into requests and responses and vice-versa.
#[derive(Debug, Clone)]
struct GenericCodec {}

type Request = Vec<u8>;
// The Response type is dummy -- we use RequestResponse just to send regular messages (requests).
type Response = ();

#[async_trait::async_trait]
impl RequestResponseCodec for GenericCodec {
    type Protocol = Vec<u8>;
    type Request = Request;
    type Response = Response;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        mut io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let length = unsigned_varint::aio::read_usize(&mut io)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        let mut buffer = vec![0; length];
        io.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        mut _io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        Ok(())
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let mut buffer = unsigned_varint::encode::usize_buffer();
        io.write_all(unsigned_varint::encode::usize(req.len(), &mut buffer))
            .await?;

        io.write_all(&req).await?;

        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
        _res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        Ok(())
    }
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    mdns: Mdns,
    rq_rp: RequestResponse<GenericCodec>,
    #[behaviour(ignore)]
    peers: Vec<PeerId>,
    #[behaviour(ignore)]
    peer_by_index: HashMap<NodeIndex, PeerId>,
    #[behaviour(ignore)]
    consensus_tx: mpsc::UnboundedSender<NetworkData>,
    #[behaviour(ignore)]
    block_tx: mpsc::UnboundedSender<Block>,
    #[behaviour(ignore)]
    node_ix: NodeIndex,
}

impl Behaviour {
    fn send_consensus_message(&mut self, message: NetworkData, recipient: Recipient) {
        let message = Message::Consensus(message).encode();
        use Recipient::*;
        match recipient {
            Node(node_ix) => {
                if let Some(peer_id) = self.peer_by_index.get(&node_ix) {
                    self.rq_rp.send_request(peer_id, message);
                } else {
                    debug!(target: "Blockchain-network", "No peer_id known for node {:?}.", node_ix);
                }
            }
            Everyone => {
                for peer_id in self.peers.iter() {
                    self.rq_rp.send_request(peer_id, message.clone());
                }
            }
        }
    }

    fn send_block_message(&mut self, block: Block) {
        debug!(target: "Blockchain-network", "Sending block message num {:?}.", block.num);
        let message = Message::Block(block).encode();
        for peer_id in self.peers.iter() {
            self.rq_rp.send_request(peer_id, message.clone());
        }
    }
}

impl NetworkBehaviourEventProcess<MdnsEvent> for Behaviour {
    fn inject_event(&mut self, event: MdnsEvent) {
        if let MdnsEvent::Discovered(list) = event {
            let auth_message = Message::Auth(self.node_ix).encode();
            for (peer, _) in list {
                if self.peers.iter().any(|p| *p == peer) {
                    continue;
                }
                self.peers.push(peer);
                self.rq_rp.send_request(&peer, auth_message.clone());
            }
        }
    }
}

impl NetworkBehaviourEventProcess<RequestResponseEvent<Request, Response>> for Behaviour {
    fn inject_event(&mut self, event: RequestResponseEvent<Request, Response>) {
        if let RequestResponseEvent::Message {
            peer: peer_id,
            message,
        } = event
        {
            match message {
                RequestResponseMessage::Request {
                    request_id: _,
                    request,
                    channel: _,
                } => {
                    let message = Message::decode(&mut &request[..])
                        .expect("honest network data should decode");
                    match message {
                        Message::Consensus(msg) => {
                            self.consensus_tx
                                .unbounded_send(msg)
                                .expect("Network must listen");
                        }
                        Message::Auth(node_ix) => {
                            debug!(target: "Blockchain-network", "Authenticated peer: {:?} {:?}", node_ix, peer_id);
                            self.peer_by_index.insert(node_ix, peer_id);
                        }
                        Message::Block(block) => {
                            debug!(target: "Blockchain-network", "Received block num {:?}", block.num);
                            self.block_tx
                                .unbounded_send(block)
                                .expect("Blockchain process must listen");
                        }
                    }
                    // We do not send back a response to a request. We treat them simply as one-way messages.
                }
                RequestResponseMessage::Response { .. } => {
                    //We ignore the response, as it is dummy anyway.
                }
            }
        }
    }
}

pub(crate) struct Network {
    msg_to_manager_tx: mpsc::UnboundedSender<(NetworkData, Recipient)>,
    msg_from_manager_rx: mpsc::UnboundedReceiver<NetworkData>,
}

#[async_trait::async_trait]
impl aleph_bft::Network<Hasher256, Data, Signature, PartialMultisignature> for Network {
    fn send(&self, data: NetworkData, recipient: Recipient) {
        if let Err(e) = self.msg_to_manager_tx.unbounded_send((data, recipient)) {
            warn!(target: "Blockchain-network", "Failed network send: {:?}", e);
        }
    }
    async fn next_event(&mut self) -> Option<NetworkData> {
        self.msg_from_manager_rx.next().await
    }
}

pub(crate) struct NetworkManager {
    swarm: Swarm<Behaviour>,
    consensus_rx: UnboundedReceiver<(NetworkData, Recipient)>,
    block_rx: UnboundedReceiver<Block>,
}

impl Network {
    pub(crate) async fn new(
        node_ix: NodeIndex,
    ) -> Result<
        (
            Self,
            NetworkManager,
            UnboundedSender<Block>,
            UnboundedReceiver<Block>,
            UnboundedSender<NetworkData>,
            UnboundedReceiver<NetworkData>,
        ),
        Box<dyn Error>,
    > {
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        info!(target: "Blockchain-network", "Local peer id: {:?}", local_peer_id);

        // Create a keypair for authenticated encryption of the transport.
        let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&local_key)
            .expect("Signing libp2p-noise static DH keypair failed.");

        // Create a tokio-based TCP transport use noise for authenticated
        // encryption and Mplex for multiplexing of substreams on a TCP stream.
        let transport = TokioTcpConfig::new()
            .nodelay(true)
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
            .multiplex(mplex::MplexConfig::new())
            .boxed();

        let (msg_to_manager_tx, msg_to_manager_rx) = mpsc::unbounded();
        let (msg_for_store, msg_from_manager) = mpsc::unbounded();
        let (msg_for_network, msg_from_store) = mpsc::unbounded();
        let (block_to_data_io_tx, block_to_data_io_rx) = mpsc::unbounded();
        let (block_from_data_io_tx, block_from_data_io_rx) = mpsc::unbounded();
        let mut swarm = {
            let mut rr_cfg = RequestResponseConfig::default();
            rr_cfg.set_connection_keep_alive(Duration::from_secs(10));
            rr_cfg.set_request_timeout(Duration::from_secs(4));
            let protocol_support = ProtocolSupport::Full;
            let mdns_config = MdnsConfig {
                ttl: Duration::from_secs(6 * 60),
                query_interval: Duration::from_millis(100),
            };
            let rq_rp = RequestResponse::new(
                GenericCodec {},
                iter::once((ALEPH_PROTOCOL_NAME.as_bytes().to_vec(), protocol_support)),
                rr_cfg,
            );

            let mdns = Mdns::new(mdns_config).await?;
            let behaviour = Behaviour {
                rq_rp,
                mdns,
                peers: vec![],
                peer_by_index: HashMap::new(),
                consensus_tx: msg_for_store,
                block_tx: block_to_data_io_tx,
                node_ix,
            };
            SwarmBuilder::new(transport, behaviour, local_peer_id)
                .executor(Box::new(|fut| {
                    tokio::spawn(fut);
                }))
                .build()
        };

        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

        let network = Network {
            msg_to_manager_tx,
            msg_from_manager_rx: msg_from_store,
        };

        let network_manager = NetworkManager {
            swarm,
            consensus_rx: msg_to_manager_rx,
            block_rx: block_from_data_io_rx,
        };

        Ok((
            network,
            network_manager,
            block_from_data_io_tx,
            block_to_data_io_rx,
            msg_for_network,
            msg_from_manager,
        ))
    }
}

impl NetworkManager {
    pub(crate) async fn run(&mut self, mut exit: oneshot::Receiver<()>) {
        loop {
            futures::select! {
                maybe_msg = self.consensus_rx.next() => {
                    if let Some((consensus_msg, recipient)) = maybe_msg {
                        let handle = &mut self.swarm.behaviour_mut();
                        handle.send_consensus_message(consensus_msg, recipient);
                    }
                }
                maybe_block = self.block_rx.next() => {
                    if let Some(block) = maybe_block {
                        let handle = &mut self.swarm.behaviour_mut();
                        handle.send_block_message(block);
                    }
                }
                event = self.swarm.next().fuse() => {
                    // called only to poll inner future
                    panic!("Unexpected event: {:?}", event);
                }
               _ = &mut exit  => break,
            }
        }
    }
}
