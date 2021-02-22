use crate::{Environment, Message, Receiver, Sender, Unit};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;

pub(crate) struct Syncer<E: Environment> {
    // outgoing messages
    messages_tx: E::Out,
    // incoming messages
    messages_rx: E::In,
    // channel for sending units to the terminal
    units_tx: Sender<Unit<E::BlockHash, E::Hash>>,
    // channel for receiving messages to the outside world
    requests_rx: Receiver<Message<E::BlockHash, E::Hash>>,
}

impl<E: Environment> Syncer<E> {
    pub(crate) fn new(
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
                    let _ = self.messages_tx.send(m).await;
                }
                Some(m) = self.messages_rx.next() => {
                    match m {
                        Message::Multicast(u) => if self.units_tx.send(u).is_err() {},
                        Message::FetchResponse(units, _) => units
                            .into_iter()
                            .for_each(|u| if self.units_tx.send(u).is_err() {}),
                        _ => {}
                    }
                }
            }
        }
    }
}
