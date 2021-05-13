use crate::{
    member::{NotificationIn, NotificationOut},
    Hasher, NodeIdT, Receiver, Sender,
};
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use log::{debug, error};
use tokio::sync::{mpsc, oneshot};

/// A process responsible for managing input and output notifications.
pub(crate) struct Syncer<H: Hasher, NI: NodeIdT> {
    /// The id of the Node
    node_id: NI,
    /// Endpoint for receiving notifications from Environment.
    ntfct_env_rx: Box<dyn Stream<Item = NotificationIn<H>> + Send + Unpin>,
    /// Endpoint for sending notifications to Environment.
    ntfct_env_tx:
        Box<dyn Sink<NotificationOut<H>, Error = Box<dyn std::error::Error>> + Send + Unpin>,
    /// Endpoint for routing notifications to the [Terminal].
    ntfct_term_tx: Sender<NotificationIn<H>>,
    /// Endpoint for receiving notifications from components within Consensus (Creator and Terminal).
    ntfct_common_rx: Receiver<NotificationOut<H>>,
}

impl<H: Hasher, NI: NodeIdT> Syncer<H, NI> {
    pub(crate) fn new(
        node_id: NI,
        ntfct_env_tx: impl Sink<NotificationOut<H>, Error = Box<dyn std::error::Error>>
            + Send
            + Unpin
            + 'static,
        ntfct_env_rx: impl Stream<Item = NotificationIn<H>> + Send + Unpin + 'static,
    ) -> (
        Self,
        Sender<NotificationOut<H>>,
        Receiver<NotificationIn<H>>,
    ) {
        let (ntfct_term_tx, ntfct_term_rx) = mpsc::unbounded_channel();

        let (ntfct_common_tx, ntfct_common_rx) = mpsc::unbounded_channel();
        (
            Syncer {
                node_id,
                ntfct_env_tx: Box::new(ntfct_env_tx),
                ntfct_env_rx: Box::new(ntfct_env_rx),
                ntfct_term_tx,
                ntfct_common_rx,
            },
            ntfct_common_tx,
            ntfct_term_rx,
        )
    }
    pub(crate) async fn sync(&mut self, exit: oneshot::Receiver<()>) {
        let mut exit = exit.into_stream();
        loop {
            tokio::select! {
                Some(m) = self.ntfct_common_rx.recv() => {
                    if let Err(e) = self.ntfct_env_tx.send(m).await {
                        error!(target: "rush-syncer", "{:?} Unable to send a message: {:?}.", self.node_id, e);
                    }
                }
                Some(m) = self.ntfct_env_rx.next() => {
                    debug!(target: "rush-syncer", "{} Received a notification {:?} from Environment.", self.node_id, m);
                    let send_result = self.ntfct_term_tx.send(m);


                                if let Err(e) =send_result {
                                    error!(target: "rush-syncer", "{:?} Unable to send a notification to Terminal: {:?}.", self.node_id, e);
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
