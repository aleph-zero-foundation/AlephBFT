use crate::{Environment, Message, Receiver, Sender, Unit};
use futures::{SinkExt, StreamExt};
use log::{debug, error};
use tokio::sync::mpsc;

/// A process responsible for managing input and output messages.
pub(crate) struct Syncer<E: Environment> {
    /// The id of the Node
    node_id: E::NodeId,
    /// Outgoing messages.
    messages_tx: E::Out,
    /// Incoming messages.
    messages_rx: E::In,
    /// A channel for sending units to the [Terminal].
    units_tx: Sender<Unit<E::BlockHash, E::Hash>>,
    /// A channel for receiving messages from the outside world.
    requests_rx: Receiver<Message<E::BlockHash, E::Hash>>,
}

impl<E: Environment> Syncer<E> {
    pub(crate) fn new(
        node_id: E::NodeId,
        messages_tx: E::Out,
        messages_rx: E::In,
    ) -> (
        Self,
        Sender<Message<E::BlockHash, E::Hash>>,
        Receiver<Unit<E::BlockHash, E::Hash>>,
        Sender<Unit<E::BlockHash, E::Hash>>,
    ) {
        let (units_tx, units_rx) = mpsc::unbounded_channel();

        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        (
            Syncer {
                node_id,
                messages_tx,
                messages_rx,
                units_tx: units_tx.clone(),
                requests_rx,
            },
            requests_tx,
            units_rx,
            units_tx,
        )
    }
    pub(crate) async fn sync(&mut self) {
        loop {
            tokio::select! {
                Some(m) = self.requests_rx.recv() => {
                    let send_result = self.messages_tx.send(m).await;
                    if let Err(e) = send_result {
                        error!(target: "rush-syncer", "{} Unable to send a message: {:?}.", self.node_id, e);
                    }
                }
                Some(m) = self.messages_rx.next() => {
                    match m {
                        Message::Multicast(u) => {
                            debug!(target: "rush-syncer", "{} Received a unit {} via Multicast.", self.node_id, u.hash());
                            let send_result = self.units_tx.send(u);
                            if let Err(e) =send_result {
                                error!(target: "rush-syncer", "{} Unable to send a unit from Multicast to Terminal: {:?}.", self.node_id, e);
                            }

                        }
                        Message::FetchResponse(units, _) => {
                            debug!(target: "rush-syncer", "{} Received {} units cia FetchResponse.", self.node_id, units.len());
                            units
                            .into_iter()
                            .for_each(|u| {
                                let send_result = self.units_tx.send(u);
                                if let Err(e) = send_result {
                                    error!(target: "rush-syncer", "{} Unable to send a unit from Fetch to Terminal: {:?}.", self.node_id, e);
                                }
                            })
                        },
                        _ => {
                            debug!(target: "rush-syncer", "{} Unsupported Message type: {:?}.", self.node_id, m);
                        }
                    }
                }
            }
        }
    }
}
