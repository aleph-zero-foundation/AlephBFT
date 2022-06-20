use crate::Data;
use aleph_bft::{NodeIndex, Recipient};
use aleph_bft_mock::{Hasher64, PartialMultisignature, Signature};
use codec::{Decode, Encode};
use log::error;
use std::{io::Write, net::SocketAddr};
use tokio::{
    io::{self, AsyncReadExt},
    net::TcpListener,
    time::{sleep, Duration},
};

pub type NetworkData = aleph_bft::NetworkData<Hasher64, Data, Signature, PartialMultisignature>;

pub struct Network {
    my_id: usize,
    addresses: Vec<SocketAddr>,
    listener: TcpListener,
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
        let listener;
        loop {
            match TcpListener::bind(addresses[my_id]).await {
                Ok(l) => {
                    listener = l;
                    break;
                }
                Err(e) => {
                    error!("{}", e);
                    error!("Waiting 10 seconds before the next attempt...");
                    sleep(Duration::from_secs(10)).await;
                }
            };
        }
        Ok(Network {
            my_id,
            addresses,
            listener,
        })
    }

    fn send_to_peer(&self, data: NetworkData, recipient: usize) {
        if let Err(e) = self.try_send_to_peer(data, recipient) {
            error!("Sending failed, recipient: {:?}, error: {:?}", recipient, e);
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
