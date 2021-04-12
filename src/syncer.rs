use crate::{HashT, NodeIdT, NotificationIn, NotificationOut, Receiver, Sender, Unit};
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use log::{debug, error};
use tokio::sync::{mpsc, oneshot};

/// A process responsible for managing input and output notifications.
pub(crate) struct Syncer<H: HashT, NI: NodeIdT> {
    /// The id of the Node
    node_id: NI,
    /// Outgoing notifications.
    ntfct_rx: Box<dyn Stream<Item = NotificationIn<H>> + Send + Unpin>,
    /// Incoming notifications.
    ntfct_tx: Box<dyn Sink<NotificationOut<H>, Error = Box<dyn std::error::Error>> + Send + Unpin>,
    /// A channel for sending units to the [Terminal].
    units_tx: Sender<Unit<H>>,
    /// A channel for receiving notifications to be sent out to the outside world.
    requests_rx: Receiver<NotificationOut<H>>,
}

impl<H: HashT, NI: NodeIdT> Syncer<H, NI> {
    pub(crate) fn new(
        node_id: NI,
        ntfct_tx: impl Sink<NotificationOut<H>, Error = Box<dyn std::error::Error>>
            + Send
            + Unpin
            + 'static,
        ntfct_rx: impl Stream<Item = NotificationIn<H>> + Send + Unpin + 'static,
    ) -> (Self, Sender<NotificationOut<H>>, Receiver<Unit<H>>) {
        let (units_tx, units_rx) = mpsc::unbounded_channel();

        let (requests_tx, requests_rx) = mpsc::unbounded_channel();
        (
            Syncer {
                node_id,
                ntfct_tx: Box::new(ntfct_tx),
                ntfct_rx: Box::new(ntfct_rx),
                units_tx,
                requests_rx,
            },
            requests_tx,
            units_rx,
        )
    }
    pub(crate) async fn sync(&mut self, exit: oneshot::Receiver<()>) {
        let mut exit = exit.into_stream();
        loop {
            tokio::select! {
                Some(m) = self.requests_rx.recv() => {
                    if let Err(e) = self.ntfct_tx.send(m).await {
                        error!(target: "rush-syncer", "{:?} Unable to send a message: {:?}.", self.node_id, e);
                    }
                }
                Some(m) = self.ntfct_rx.next() => {
                    match m {
                        NotificationIn::NewUnits(units) => {
                            for u in units {
                                debug!(target: "rush-syncer", "{} Received a unit {} from Environment.", self.node_id, u.hash());
                                let send_result = self.units_tx.send(u);
                                if let Err(e) =send_result {
                                    error!(target: "rush-syncer", "{:?} Unable to send a unit to Terminal: {:?}.", self.node_id, e);
                                }
                            }
                        }
                    }
                }
                _ = exit.next() => {
                    debug!(target: "rush-syncer", "{} received exit signal.", self.node_id);
                    break
                }
            }
        }
    }
}
