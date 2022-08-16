use crate::Data;
use aleph_bft::{NodeIndex, Recipient};
use aleph_bft_mock::{Hasher64, PartialMultisignature, Signature};
use codec::{Decode, Encode};
use log::error;
use std::net::SocketAddr;
use tokio::{
    io,
    net::UdpSocket,
    time::{sleep, Duration},
};

const MAX_UDP_DATAGRAM_BYTES: usize = 65536;

pub type NetworkData = aleph_bft::NetworkData<Hasher64, Data, Signature, PartialMultisignature>;

#[derive(Debug)]
pub struct Network {
    my_id: usize,
    addresses: Vec<SocketAddr>,
    socket: UdpSocket,
    /// Buffer for incoming data.
    ///
    /// It's allocated on the heap, because otherwise it overflows the stack when used inside a future.
    buffer: Box<[u8; MAX_UDP_DATAGRAM_BYTES]>,
}

impl Network {
    pub async fn new(
        my_id: NodeIndex,
        ports: &[usize],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let my_id = my_id.0;
        assert!(my_id < ports.len());

        let addresses = ports
            .iter()
            .map(|p| format!("127.0.0.1:{}", p).parse::<SocketAddr>())
            .collect::<Result<Vec<_>, _>>()?;

        let socket = Self::bind_socket(addresses[my_id]).await;
        Ok(Network {
            my_id,
            addresses,
            socket,
            buffer: Box::new([0; MAX_UDP_DATAGRAM_BYTES]),
        })
    }

    async fn bind_socket(address: SocketAddr) -> UdpSocket {
        loop {
            match UdpSocket::bind(address).await {
                Ok(socket) => {
                    return socket;
                }
                Err(e) => {
                    error!("{}", e);
                    error!("Waiting 10 seconds before the next attempt...");
                    sleep(Duration::from_secs(10)).await;
                }
            };
        }
    }

    fn send_to_peer(&self, data: NetworkData, recipient: usize) {
        if let Err(e) = self.try_send_to_peer(data, recipient) {
            error!("Sending failed, recipient: {:?}, error: {:?}", recipient, e);
        }
    }

    fn try_send_to_peer(&self, data: NetworkData, recipient: usize) -> io::Result<()> {
        let encoded = data.encode();
        assert!(encoded.len() <= MAX_UDP_DATAGRAM_BYTES);

        self.socket
            .try_send_to(&encoded, self.addresses[recipient])?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl aleph_bft::Network<NetworkData> for Network {
    fn send(&self, data: NetworkData, recipient: Recipient) {
        match recipient {
            Recipient::Everyone => {
                for r in 0..self.addresses.len() {
                    if r != self.my_id {
                        self.send_to_peer(data.clone(), r);
                    }
                }
            }
            Recipient::Node(r) => {
                if r.0 < self.addresses.len() {
                    self.send_to_peer(data, r.0);
                } else {
                    error!("Recipient unknown: {}", r.0);
                }
            }
        }
    }

    async fn next_event(&mut self) -> Option<NetworkData> {
        match self.socket.recv_from(self.buffer.as_mut()).await {
            Ok((_len, _addr)) => NetworkData::decode(&mut &self.buffer[..]).ok(),
            Err(e) => {
                error!("Couldn't receive datagram: {:?}", e);
                None
            }
        }
    }
}
