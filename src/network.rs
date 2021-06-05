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
///
/// Note on Rate Control: it is assumed that Network implements a rate control mechanism guaranteeing
/// that no node is allowed to spam messages without limits. We do not specify details yet, but in
/// future releases we plan to publish recommended upper bounds for the amounts of bandwidth and
/// number of messages allowed per node per a unit of time. These bounds must be carefully crafted
/// based upon the number of nodes N and the configured delays between subsequent Dag rounds, so
/// that at the same time spammers are cut off but honest nodes are able function correctly within
/// these bounds.
///
/// Note on Network Reliability: it is not assumed that each message that AlephBFT orders to send
/// reaches its intended recipient, there are some built-in reliability mechanisms within AlephBFT
/// that will automatically detect certain failures and resend messages as needed. Clearly, the less
/// reliable the network is, the worse the performarmence of AlephBFT will be (generally slower to
/// produce output). Also, not surprisingly if the percentage of dropped messages is too high
/// AlephBFT might stop making progress, but from what we observe in tests, this happens only when
/// the reliability is extremely bad, i.e., drops below 50% (which means there is some significant
/// issue with the network).
#[async_trait::async_trait]
pub trait Network<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature>: Send {
    type Error: Debug;
    /// Send a message to a single node.
    fn send(&self, data: NetworkData<H, D, S, MS>, node: NodeIndex) -> Result<(), Self::Error>;
    /// Send a message to all nodes.
    fn broadcast(&self, data: NetworkData<H, D, S, MS>) -> Result<(), Self::Error>;
    /// Receive a message from the network.
    async fn next_event(&mut self) -> Option<NetworkData<H, D, S, MS>>;
}

#[derive(Encode, Decode, Clone)]
pub(crate) enum NetworkDataInner<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature> {
    Units(UnitMessage<H, D, S>),
    Alert(AlertMessage<H, D, S, MS>),
}

/// NetworkData is the opaque format for all data that a committee member needs to send to other nodes.
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
                    error!(target: "AlephBFT-etwork-hub", "Broadcast error: {:?}", error);
                }
            }
            Node(node_id) => {
                if let Err(error) = self.network.send(data, node_id) {
                    error!(target: "AlephBFT-etwork-hub", "Send to {:?} error: {:?}", node_id, error);
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
                        error!(target: "AlephBFT-etwork-hub", "Outgoing units stream closed.");
                        break;
                    }
                },
                alert_message = self.alerts_to_send.next() => match alert_message {
                    Some((alert_message, recipient)) => self.send(NetworkData(Alert(alert_message)), recipient),
                    None => {
                        error!(target: "AlephBFT-etwork-hub", "Outgoing alerts stream closed.");
                        break;
                    }
                },
                incoming_message = self.network.next_event().fuse() => match incoming_message {
                    Some(incoming_message) => self.handle_incoming(incoming_message),
                    None => {
                        error!(target: "AlephBFT-etwork-hub", "Network stopped working.");
                        break;
                    }
                },
                _ = &mut exit => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nodes::BoolNodeMap,
        testing::mock::{self, Data, Hasher64, PartialMultisignature, Signature},
        units::{ControlHash, FullUnit, PreUnit, UncheckedSignedUnit, UnitCoord},
        Round, Signable, UncheckedSigned,
    };

    fn test_unchecked_unit(
        creator: NodeIndex,
        round: Round,
        variant: u32,
    ) -> UncheckedSignedUnit<Hasher64, Data, Signature> {
        let control_hash = ControlHash {
            parents_mask: BoolNodeMap::with_capacity(7.into()),
            combined_hash: 0.using_encoded(Hasher64::hash),
        };
        let pu = PreUnit::new(creator, round, control_hash);
        let data = Data::new(UnitCoord::new(7, 13.into()), variant);
        UncheckedSigned::new(FullUnit::new(pu, data, 0), Signature {})
    }

    #[test]
    fn decoding_network_data_units_new_unit() {
        use NetworkDataInner::Units;
        use UnitMessage::NewUnit;

        let uu = test_unchecked_unit(5.into(), 43, 1729);
        let nd = NetworkData::<Hasher64, Data, Signature, PartialMultisignature>(Units(NewUnit(
            uu.clone(),
        )));
        let decoded = mock::NetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in dencode/decode for Units(NewUnit)");
        if let Units(NewUnit(decoded_unchecked)) = decoded.unwrap().0 {
            assert!(
                uu.as_signable() == decoded_unchecked.as_signable(),
                "decoded should equel encodee"
            );
        }
    }

    #[test]
    fn decoding_network_data_units_request_coord() {
        use NetworkDataInner::Units;
        use UnitMessage::RequestCoord;

        let ni = 7.into();
        let uc = UnitCoord::new(3, 13.into());
        let nd = NetworkData::<Hasher64, Data, Signature, PartialMultisignature>(Units(
            RequestCoord(ni, uc),
        ));
        let decoded = mock::NetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in dencode/decode for Units(NewUnit)");
        if let Units(RequestCoord(dni, duc)) = decoded.unwrap().0 {
            assert!(ni == dni && uc == duc, "decoded should equel encodee");
        }
    }

    #[test]
    fn decoding_network_data_units_response_coord() {
        use NetworkDataInner::Units;
        use UnitMessage::ResponseCoord;

        let uu = test_unchecked_unit(5.into(), 43, 1729);
        let nd = NetworkData::<Hasher64, Data, Signature, PartialMultisignature>(Units(
            ResponseCoord(uu.clone()),
        ));
        let decoded = mock::NetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in dencode/decode for Units(NewUnit)");
        if let Units(ResponseCoord(decoded_unchecked)) = decoded.unwrap().0 {
            assert!(
                uu.as_signable() == decoded_unchecked.as_signable(),
                "decoded should equel encodee"
            );
        }
    }

    #[test]
    fn decoding_network_data_units_request_parents() {
        use NetworkDataInner::Units;
        use UnitMessage::RequestParents;

        let ni = 7.into();
        let h = 43.using_encoded(Hasher64::hash);
        let nd = NetworkData::<Hasher64, Data, Signature, PartialMultisignature>(Units(
            RequestParents(ni, h),
        ));
        let decoded = mock::NetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in dencode/decode for Units(NewUnit)");
        if let Units(RequestParents(dni, dh)) = decoded.unwrap().0 {
            assert!(ni == dni && h == dh, "decoded should equel encodee");
        }
    }

    #[test]
    fn decoding_network_data_units_response_parents() {
        use NetworkDataInner::Units;
        use UnitMessage::ResponseParents;

        let h = 43.using_encoded(Hasher64::hash);
        let p1 = test_unchecked_unit(5.into(), 43, 1729);
        let p2 = test_unchecked_unit(13.into(), 43, 1729);
        let p3 = test_unchecked_unit(17.into(), 43, 1729);
        let parents = vec![p1, p2, p3];

        let nd = NetworkData::<Hasher64, Data, Signature, PartialMultisignature>(Units(
            ResponseParents(h, parents.clone()),
        ));
        let decoded = mock::NetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in dencode/decode for Units(NewUnit)");
        if let Units(ResponseParents(dh, dparents)) = decoded.unwrap().0 {
            assert!(h == dh, "decoded should equel encodee");
            assert!(
                parents.len() == dparents.len(),
                "decoded should equel encodee"
            );
            for (p, dp) in parents.iter().zip(dparents.iter()) {
                assert!(
                    p.as_signable() == dp.as_signable(),
                    "decoded should equel encodee"
                );
            }
        }
    }

    #[test]
    fn decoding_network_data_alert_fork_alert() {
        use AlertMessage::ForkAlert;
        use NetworkDataInner::Alert;

        let forker = 9.into();
        let f1 = test_unchecked_unit(forker, 10, 0);
        let f2 = test_unchecked_unit(forker, 10, 1);
        let lu1 = test_unchecked_unit(forker, 11, 0);
        let lu2 = test_unchecked_unit(forker, 12, 0);
        let alert = crate::alerts::Alert::new(7.into(), (f1, f2), vec![lu1, lu2]);

        let nd = NetworkData::<Hasher64, Data, Signature, PartialMultisignature>(Alert(ForkAlert(
            UncheckedSigned::new(alert.clone(), Signature {}),
        )));
        let decoded = mock::NetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in dencode/decode for Units(NewUnit)");
        if let Alert(ForkAlert(unchecked_alert)) = decoded.unwrap().0 {
            assert!(
                alert.hash() == unchecked_alert.as_signable().hash(),
                "decoded should equel encodee"
            )
        }
    }
}
