use crate::{
    alerts::AlertMessage, member::UnitMessage, nodes::NodeIndex, Data, Hasher, Receiver, Sender,
};
use codec::{Decode, Encode};
use futures::{channel::oneshot, FutureExt, StreamExt};
use log::error;
use std::fmt::Debug;

/// Network represents an interface for sending and receiving NetworkData.
/// We only assume that every send has a nonzero probability of succeeding,
/// and a nonerror result might still correspond to an unsuccessful transfer.
#[async_trait::async_trait]
pub trait Network<H: Hasher, D: Data, S: Encode + Decode>: Send {
    type Error: Debug;
    fn send(&self, data: NetworkData<H, D, S>, node: NodeIndex) -> Result<(), Self::Error>;
    fn broadcast(&self, data: NetworkData<H, D, S>) -> Result<(), Self::Error>;
    async fn next_event(&mut self) -> Option<NetworkData<H, D, S>>;
}

#[derive(Encode, Decode, Clone)]
pub(crate) enum NetworkDataInner<H: Hasher, D: Data, S: Encode + Decode> {
    Units(UnitMessage<H, D, S>),
    Alert(AlertMessage<H, D, S>),
}

/// NetworkData is the opaque format for all data that a committee member needs
/// to send to other nodes to perform the protocol.
#[derive(Clone)]
pub struct NetworkData<H: Hasher, D: Data, S: Encode + Decode>(
    pub(crate) NetworkDataInner<H, D, S>,
);

impl<H: Hasher, D: Data, S: Encode + Decode> Encode for NetworkData<H, D, S> {
    fn size_hint(&self) -> usize {
        self.0.size_hint()
    }

    fn encode_to<T: codec::Output + ?Sized>(&self, dest: &mut T) {
        self.0.encode_to(dest)
    }

    fn encode(&self) -> Vec<u8> {
        self.0.encode()
    }

    fn using_encoded<R, F: FnOnce(&[u8]) -> R>(&self, f: F) -> R {
        self.0.using_encoded(f)
    }
}

impl<H: Hasher, D: Data, S: Encode + Decode> Decode for NetworkData<H, D, S> {
    fn decode<I: codec::Input>(input: &mut I) -> Result<Self, codec::Error> {
        Ok(Self(NetworkDataInner::decode(input)?))
    }
}

pub(crate) enum Recipient {
    Everyone,
    Node(NodeIndex),
}

pub(crate) struct NetworkHub<H: Hasher, D: Data, S: Encode + Decode, N: Network<H, D, S>> {
    network: N,
    units_to_send: Receiver<(UnitMessage<H, D, S>, Recipient)>,
    units_received: Sender<UnitMessage<H, D, S>>,
    alerts_to_send: Receiver<(AlertMessage<H, D, S>, Recipient)>,
    alerts_received: Sender<AlertMessage<H, D, S>>,
}

impl<H: Hasher, D: Data, S: Encode + Decode, N: Network<H, D, S>> NetworkHub<H, D, S, N> {
    pub fn new(
        network: N,
        units_to_send: Receiver<(UnitMessage<H, D, S>, Recipient)>,
        units_received: Sender<UnitMessage<H, D, S>>,
        alerts_to_send: Receiver<(AlertMessage<H, D, S>, Recipient)>,
        alerts_received: Sender<AlertMessage<H, D, S>>,
    ) -> Self {
        NetworkHub {
            network,
            units_to_send,
            units_received,
            alerts_to_send,
            alerts_received,
        }
    }

    fn send(&self, data: NetworkData<H, D, S>, recipient: Recipient) {
        use Recipient::*;
        match recipient {
            Everyone => {
                if let Err(error) = self.network.broadcast(data) {
                    error!(target: "network-hub", "Broadcast error: {:?}", error);
                }
            }
            Node(node_id) => {
                if let Err(error) = self.network.send(data, node_id) {
                    error!(target: "network-hub", "Send to {:?} error: {:?}", node_id, error);
                }
            }
        }
    }

    fn handle_incoming(&self, network_data: NetworkData<H, D, S>) {
        let NetworkData(network_data) = network_data;
        use NetworkDataInner::*;
        match network_data {
            Units(unit_message) => {
                if let Err(error) = self.units_received.unbounded_send(unit_message) {
                    error!(target: "network-hub", "Unit message push error: {:?}", error)
                }
            }
            Alert(alert_message) => {
                if let Err(error) = self.alerts_received.unbounded_send(alert_message) {
                    error!(target: "network-hub", "Alert message push error: {:?}", error)
                }
            }
        }
    }

    pub async fn run(&mut self, exit: oneshot::Receiver<()>) {
        let mut exit = exit.into_stream();
        loop {
            use NetworkDataInner::*;
            tokio::select! {
                unit_message = self.units_to_send.next() => match unit_message {
                    Some((unit_message, recipient)) => self.send(NetworkData(Units(unit_message)), recipient),
                    None => {
                        error!(target: "network-hub", "Outgoing units stream closed.");
                        break;
                    }
                },
                alert_message = self.alerts_to_send.next() => match alert_message {
                    Some((alert_message, recipient)) => self.send(NetworkData(Alert(alert_message)), recipient),
                    None => {
                        error!(target: "network-hub", "Outgoing alerts stream closed.");
                        break;
                    }
                },
                incoming_message = self.network.next_event() => match incoming_message {
                    Some(incoming_message) => self.handle_incoming(incoming_message),
                    None => {
                        error!(target: "network-hub", "Network stopped working.");
                        break;
                    }
                },
                _ = exit.next() => break,
            }
        }
    }
}
