use crate::{
    alerts::AlertMessage,
    network::{NetworkData, NetworkDataInner, UnitMessage},
    Data, Hasher, Network, PartialMultisignature, Receiver, Recipient, Sender, Signature,
    Terminator,
};
use futures::{FutureExt, StreamExt};
use log::{debug, error, warn};

pub struct Hub<
    H: Hasher,
    D: Data,
    S: Signature,
    MS: PartialMultisignature,
    N: Network<NetworkData<H, D, S, MS>>,
> {
    network: N,
    units_to_send: Receiver<(UnitMessage<H, D, S>, Recipient)>,
    units_received: Sender<UnitMessage<H, D, S>>,
    alerts_to_send: Receiver<(AlertMessage<H, D, S, MS>, Recipient)>,
    alerts_received: Sender<AlertMessage<H, D, S, MS>>,
}

impl<
        H: Hasher,
        D: Data,
        S: Signature,
        MS: PartialMultisignature,
        N: Network<NetworkData<H, D, S, MS>>,
    > Hub<H, D, S, MS, N>
{
    pub fn new(
        network: N,
        units_to_send: Receiver<(UnitMessage<H, D, S>, Recipient)>,
        units_received: Sender<UnitMessage<H, D, S>>,
        alerts_to_send: Receiver<(AlertMessage<H, D, S, MS>, Recipient)>,
        alerts_received: Sender<AlertMessage<H, D, S, MS>>,
    ) -> Self {
        Hub {
            network,
            units_to_send,
            units_received,
            alerts_to_send,
            alerts_received,
        }
    }

    fn send(&self, data: NetworkData<H, D, S, MS>, recipient: Recipient) {
        self.network.send(data, recipient);
    }

    fn handle_incoming(&self, network_data: NetworkData<H, D, S, MS>) {
        let NetworkData(network_data) = network_data;
        use NetworkDataInner::*;
        match network_data {
            Units(unit_message) => {
                if let Err(e) = self.units_received.unbounded_send(unit_message) {
                    warn!(target: "AlephBFT-network-hub", "Error when sending units to consensus {:?}", e);
                }
            }

            Alert(alert_message) => {
                if let Err(e) = self.alerts_received.unbounded_send(alert_message) {
                    warn!(target: "AlephBFT-network-hub", "Error when sending alerts to consensus {:?}", e);
                }
            }
        }
    }

    pub async fn run(mut self, mut terminator: Terminator) {
        loop {
            use NetworkDataInner::*;
            futures::select! {
                unit_message = self.units_to_send.next() => match unit_message {
                    Some((unit_message, recipient)) => self.send(NetworkData(Units(unit_message)), recipient),
                    None => {
                        error!(target: "AlephBFT-network-hub", "Outgoing units stream closed.");
                        break;
                    }
                },
                alert_message = self.alerts_to_send.next() => match alert_message {
                    Some((alert_message, recipient)) => self.send(NetworkData(Alert(alert_message)), recipient),
                    None => {
                        error!(target: "AlephBFT-network-hub", "Outgoing alerts stream closed.");
                        break;
                    }
                },
                incoming_message = self.network.next_event().fuse() => match incoming_message {
                    Some(incoming_message) => self.handle_incoming(incoming_message),
                    None => {
                        error!(target: "AlephBFT-network-hub", "Network stopped working.");
                        break;
                    }
                },
                _ = terminator.get_exit().fuse() => {
                    terminator.terminate_sync().await;
                    break;
                }
            }
        }

        debug!(target: "AlephBFT-network-hub", "Network ended.");
    }
}
