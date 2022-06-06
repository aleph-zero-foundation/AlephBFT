use aleph_bft::Recipient;
use aleph_bft_mock::{Data, Hasher64, PartialMultisignature, Signature};
use codec::{Decode, Encode};
use log::error;
use std::{io::Write, net::SocketAddr};
use tokio::{
    io::{self, AsyncReadExt},
    net::TcpListener,
};

pub type NetworkData = aleph_bft::NetworkData<Hasher64, Data, Signature, PartialMultisignature>;

pub struct Network {
    my_id: usize,
    addresses: Vec<SocketAddr>,
    listener: TcpListener,
}

impl Network {
    pub async fn new(my_id: usize, ports: &[usize]) -> Result<Self, Box<dyn std::error::Error>> {
        assert!(my_id < ports.len());
        let addresses = ports
            .iter()
            .map(|p| format!("127.0.0.1:{}", p).parse::<SocketAddr>())
            .collect::<Result<Vec<_>, _>>()?;
        let listener = TcpListener::bind(addresses[my_id]).await?;
        Ok(Network {
            my_id,
            addresses,
            listener,
        })
    }

    fn send_to_peer(&self, data: NetworkData, recipient: usize) {
        if self.try_send_to_peer(data, recipient).is_err() {
            error!("Sending failed, recipient: {:?}", recipient);
        }
    }

    fn try_send_to_peer(&self, data: NetworkData, recipient: usize) -> io::Result<()> {
        let mut stream = std::net::TcpStream::connect(self.addresses[recipient])?;
        stream.write_all(&data.encode())?;
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
        let mut buffer = Vec::new();
        match self.listener.accept().await {
            Ok((mut socket, _addr)) => match socket.read_to_end(&mut buffer).await {
                Ok(_) => NetworkData::decode(&mut &buffer[..]).ok(),
                Err(_) => {
                    error!("Could not decode incoming data");
                    None
                }
            },
            Err(e) => {
                error!("Couldn't accept connection: {:?}", e);
                None
            }
        }
    }
}
