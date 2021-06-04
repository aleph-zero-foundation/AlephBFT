use crate::{
    alerts::AlertMessage,
    member::UnitMessage,
    nodes::NodeIndex,
    signed::{PartialMultisignature, Signature},
    Data, Hasher, Receiver, Sender,
};
use codec::{Decode, Encode};
use futures::{channel::oneshot, FutureExt, StreamExt};
use log::error;
use std::fmt::Debug;

/// Network represents an interface for sending and receiving NetworkData.
/// We only assume that every send has a nonzero probability of succeeding,
/// and a nonerror result might still correspond to an unsuccessful transfer.
#[async_trait::async_trait]
pub trait Network<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature>: Send {
    type Error: Debug;
    fn send(&self, data: NetworkData<H, D, S, MS>, node: NodeIndex) -> Result<(), Self::Error>;
    fn broadcast(&self, data: NetworkData<H, D, S, MS>) -> Result<(), Self::Error>;
    async fn next_event(&mut self) -> Option<NetworkData<H, D, S, MS>>;
}

#[derive(Encode, Decode, Clone)]
pub(crate) enum NetworkDataInner<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature> {
    Units(UnitMessage<H, D, S>),
    Alert(AlertMessage<H, D, S, MS>),
}

/// NetworkData is the opaque format for all data that a committee member needs
/// to send to other nodes to perform the protocol.
#[derive(Clone)]
pub struct NetworkData<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature>(
    pub(crate) NetworkDataInner<H, D, S, MS>,
);

impl<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature> Encode
    for NetworkData<H, D, S, MS>
{
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

impl<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature> Decode
    for NetworkData<H, D, S, MS>
{
    fn decode<I: codec::Input>(input: &mut I) -> Result<Self, codec::Error> {
        Ok(Self(NetworkDataInner::decode(input)?))
    }
}

pub(crate) enum Recipient {
    Everyone,
    Node(NodeIndex),
}

pub(crate) struct NetworkHub<
    H: Hasher,
    D: Data,
    S: Signature,
    MS: PartialMultisignature,
    N: Network<H, D, S, MS>,
> {
    network: N,
    units_to_send: Receiver<(UnitMessage<H, D, S>, Recipient)>,
    units_received: Sender<UnitMessage<H, D, S>>,
    alerts_to_send: Receiver<(AlertMessage<H, D, S, MS>, Recipient)>,
    alerts_received: Sender<AlertMessage<H, D, S, MS>>,
}

impl<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature, N: Network<H, D, S, MS>>
    NetworkHub<H, D, S, MS, N>
{
    pub fn new(
        network: N,
        units_to_send: Receiver<(UnitMessage<H, D, S>, Recipient)>,
        units_received: Sender<UnitMessage<H, D, S>>,
        alerts_to_send: Receiver<(AlertMessage<H, D, S, MS>, Recipient)>,
        alerts_received: Sender<AlertMessage<H, D, S, MS>>,
    ) -> Self {
        NetworkHub {
            network,
            units_to_send,
            units_received,
            alerts_to_send,
            alerts_received,
        }
    }

    fn send(&self, data: NetworkData<H, D, S, MS>, recipient: Recipient) {
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

    fn handle_incoming(&self, network_data: NetworkData<H, D, S, MS>) {
        let NetworkData(network_data) = network_data;
        use NetworkDataInner::*;
        match network_data {
            Units(unit_message) => self
                .units_received
                .unbounded_send(unit_message)
                .expect("Channel should be open"),
            Alert(alert_message) => self
                .alerts_received
                .unbounded_send(alert_message)
                .expect("Channel should be open"),
        }
    }

    pub async fn run(&mut self, mut exit: oneshot::Receiver<()>) {
        loop {
            use NetworkDataInner::*;
            futures::select! {
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
                incoming_message = self.network.next_event().fuse() => match incoming_message {
                    Some(incoming_message) => self.handle_incoming(incoming_message),
                    None => {
                        error!(target: "network-hub", "Network stopped working.");
                        break;
                    }
                },
                _ = &mut exit => break,
            }
        }
    }
}
