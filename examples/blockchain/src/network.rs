use crate::{Block, Data};
use aleph_bft::{NodeIndex, Recipient, Terminator};
use aleph_bft_mock::{Hasher64, PartialMultisignature, Signature};
use codec::{Decode, Encode};
use futures::{
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    FutureExt, StreamExt,
};
use log::{debug, error, warn};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{Debug, Formatter},
    io::Write,
    net::{SocketAddr, SocketAddrV4, TcpStream},
    str::FromStr,
};
use tokio::{io::AsyncReadExt, net::TcpListener};

pub type NetworkData = aleph_bft::NetworkData<Hasher64, Data, Signature, PartialMultisignature>;

#[derive(Clone, Eq, PartialEq, Debug, Decode, Encode)]
pub struct Address {
    octets: [u8; 4],
    port: u16,
}

impl From<SocketAddrV4> for Address {
    fn from(addr: SocketAddrV4) -> Self {
        Self {
            octets: addr.ip().octets(),
            port: addr.port(),
        }
    }
}

impl From<SocketAddr> for Address {
    fn from(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(addr) => addr.into(),
            SocketAddr::V6(_) => panic!(),
        }
    }
}

impl FromStr for Address {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.parse::<SocketAddr>().unwrap().into())
    }
}

impl Address {
    pub async fn new_bind(ip_addr: String) -> (TcpListener, Self) {
        let listener = TcpListener::bind(ip_addr.parse::<SocketAddr>().unwrap())
            .await
            .unwrap();
        let ip_addr = listener.local_addr().unwrap().to_string();
        (listener, Self::from_str(&ip_addr).unwrap())
    }

    pub fn connect(&self) -> std::io::Result<TcpStream> {
        TcpStream::connect(SocketAddr::from((self.octets, self.port)))
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Decode, Encode, Debug)]
enum Message {
    DNSHello(NodeIndex, Address),
    DNSRequest(NodeIndex, Address),
    DNSResponse(Vec<(NodeIndex, Address)>),
    Consensus(NetworkData),
    Block(Block),
}

pub struct Network {
    msg_to_manager_tx: mpsc::UnboundedSender<(NetworkData, Recipient)>,
    msg_from_manager_rx: mpsc::UnboundedReceiver<NetworkData>,
}

#[async_trait::async_trait]
impl aleph_bft::Network<NetworkData> for Network {
    fn send(&self, data: NetworkData, recipient: Recipient) {
        if let Err(e) = self.msg_to_manager_tx.unbounded_send((data, recipient)) {
            warn!(target: "Blockchain-network", "Failed network send: {:?}", e);
        }
    }
    async fn next_event(&mut self) -> Option<NetworkData> {
        self.msg_from_manager_rx.next().await
    }
}

pub struct NetworkManager {
    id: NodeIndex,
    address: Address,
    addresses: HashMap<NodeIndex, Address>,
    bootnodes: HashSet<NodeIndex>,
    n_nodes: usize,
    listener: TcpListener,
    consensus_tx: UnboundedSender<NetworkData>,
    consensus_rx: UnboundedReceiver<(NetworkData, Recipient)>,
    block_tx: UnboundedSender<Block>,
    block_rx: UnboundedReceiver<Block>,
}

impl Debug for NetworkManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkManager")
            .field("id", &self.id)
            .field("address", &self.address)
            .field("address count", &self.addresses.len())
            .field("bootnode count", &self.bootnodes.len())
            .field("node count", &self.n_nodes)
            .finish()
    }
}

impl NetworkManager {
    pub async fn new(
        id: NodeIndex,
        ip_addr: String,
        n_nodes: usize,
        bootnodes: HashMap<NodeIndex, Address>,
    ) -> Result<
        (
            Self,
            Network,
            UnboundedSender<Block>,
            UnboundedReceiver<Block>,
            UnboundedSender<NetworkData>,
            UnboundedReceiver<NetworkData>,
        ),
        Box<dyn Error>,
    > {
        let mut addresses = bootnodes.clone();
        let (listener, address) = Address::new_bind(ip_addr).await;
        addresses.insert(id, address.clone());

        let (msg_to_manager_tx, msg_to_manager_rx) = mpsc::unbounded();
        let (msg_for_store, msg_from_manager) = mpsc::unbounded();
        let (msg_for_network, msg_from_store) = mpsc::unbounded();
        let (block_to_data_io_tx, block_to_data_io_rx) = mpsc::unbounded();
        let (block_from_data_io_tx, block_from_data_io_rx) = mpsc::unbounded();

        let network = Network {
            msg_to_manager_tx,
            msg_from_manager_rx: msg_from_store,
        };

        let network_manager = NetworkManager {
            id,
            address,
            addresses,
            bootnodes: bootnodes.into_keys().collect(),
            n_nodes,
            listener,
            consensus_tx: msg_for_store,
            consensus_rx: msg_to_manager_rx,
            block_tx: block_to_data_io_tx,
            block_rx: block_from_data_io_rx,
        };

        Ok((
            network_manager,
            network,
            block_from_data_io_tx,
            block_to_data_io_rx,
            msg_for_network,
            msg_from_manager,
        ))
    }

    fn reset_dns(&mut self, n: &NodeIndex) {
        if !self.bootnodes.contains(n) {
            error!("Resetting address of node {}", n.0);
            self.addresses.remove(n);
        }
    }

    fn send(&mut self, message: Message, recipient: Recipient) {
        let mut to_reset = vec![];
        match recipient {
            Recipient::Node(n) => {
                if let Some(addr) = self.addresses.get(&n) {
                    if let Err(e) = self.try_send(&message, addr) {
                        error!("Failed to send message {:?} to {:?}: {}", message, addr, e);
                        to_reset.push(n)
                    }
                }
            }
            Recipient::Everyone => {
                let my_id = self.id;
                for (n, addr) in self.addresses.iter().filter(|(n, _)| n != &&my_id) {
                    if let Err(e) = self.try_send(&message, addr) {
                        error!("Failed to send message {:?} to {:?}: {}", message, addr, e);
                        to_reset.push(*n)
                    }
                }
            }
        }
        for n in to_reset {
            self.reset_dns(&n);
        }
    }

    fn try_send(&self, message: &Message, address: &Address) -> std::io::Result<()> {
        debug!("Trying to send message {:?} to {:?}", message, address);
        address.connect()?.write_all(&message.encode())
    }

    fn dns_response(&mut self, id: NodeIndex, address: Address) {
        self.addresses.insert(id, address.clone());
        self.try_send(
            &Message::DNSResponse(self.addresses.clone().into_iter().collect()),
            &address,
        )
        .unwrap_or(());
    }

    pub async fn run(&mut self, mut terminator: Terminator) {
        let mut dns_interval = tokio::time::interval(std::time::Duration::from_millis(1000));
        let mut dns_hello_interval = tokio::time::interval(std::time::Duration::from_millis(5000));
        loop {
            let mut buffer = Vec::new();
            tokio::select! {

                event = self.listener.accept() => match event {
                    Ok((mut socket, _addr)) => {
                        match socket.read_to_end(&mut buffer).await {
                            Ok(_) => {
                                let message = Message::decode(&mut &buffer[..]);
                                debug!("Received message: {:?}", message);
                                match message {
                                    Ok(Message::Consensus(data)) => self.consensus_tx.unbounded_send(data).expect("Network must listen"),
                                    Ok(Message::Block(block)) => {
                                        debug!(target: "Blockchain-network", "Received block num {:?}", block.num);
                                        self.block_tx
                                            .unbounded_send(block)
                                            .expect("Blockchain process must listen");
                                    },
                                    Ok(Message::DNSHello(id, address)) => { self.addresses.insert(id, address); },
                                    Ok(Message::DNSRequest(id, address)) => self.dns_response(id, address),
                                    Ok(Message::DNSResponse(addresses)) => self.addresses.extend(addresses.into_iter()),
                                    Err(_) => (),
                                };
                            },
                            Err(_) => {
                                error!("Could not decode incoming data");
                            },
                        }
                    },
                    Err(e) => {
                        error!("Couldn't accept connection: {:?}", e);
                    },
                },

                _ = dns_interval.tick() => {
                    if self.addresses.len() < self.n_nodes {
                        self.send(Message::DNSRequest(self.id, self.address.clone()), Recipient::Everyone);
                        debug!("Requesting IP addresses");
                    }
                },

                _ = dns_hello_interval.tick() => {
                    self.send(Message::DNSHello(self.id, self.address.clone()), Recipient::Everyone);
                    debug!("Sending Hello!");
                },

                Some((consensus_msg, recipient)) = self.consensus_rx.next() => {
                    self.send(Message::Consensus(consensus_msg), recipient);
                }

                Some(block) = self.block_rx.next() => {
                    debug!(target: "Blockchain-network", "Sending block message num {:?}.", block.num);
                    self.send(Message::Block(block), Recipient::Everyone);
                }

               _ = terminator.get_exit().fuse()  => {
                    terminator.terminate_sync().await;
                    break;
               },
            }
        }
    }
}
