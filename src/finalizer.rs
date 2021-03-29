use futures::{FutureExt, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::{Environment, Receiver, Sender};
use log::debug;

/// A process that receives batches of BlockHashes from the [Extender] (from the batch_rx channel
/// endopoint) and finalizes blocks based on this input. Finalizer takes advantage of the access to the
/// `extends_finalized` oracle in order to decide which block to finalize from a batch. Important: this
/// oracle should output `true` only for *strict extension* i.e. `extends_finalized(h)` should be
/// `false` in case h is the most recently finalized block.
pub(crate) struct Finalizer<E: Environment> {
    node_id: E::NodeId,
    batch_rx: Receiver<Vec<E::BlockHash>>,
    finalize: Box<dyn Fn(E::BlockHash) + Sync + Send + 'static>,
    extends_finalized: Box<dyn Fn(E::BlockHash) -> bool + Sync + Send + 'static>,
}

impl<E: Environment> Finalizer<E> {
    pub(crate) fn new(
        node_id: E::NodeId,
        finalize: Box<dyn Fn(E::BlockHash) + Send + Sync + 'static>,
        extends_finalized: Box<dyn Fn(E::BlockHash) -> bool + Sync + Send + 'static>,
    ) -> (Self, Sender<Vec<E::BlockHash>>) {
        let (batch_tx, batch_rx) = mpsc::unbounded_channel();
        (
            Finalizer {
                node_id,
                batch_rx,
                finalize,
                extends_finalized,
            },
            batch_tx,
        )
    }
    pub(crate) async fn finalize(&mut self, exit: oneshot::Receiver<()>) {
        let mut exit = exit.into_stream();
        loop {
            tokio::select! {
                Some(batch) = self.batch_rx.recv() =>{
                    for h in batch {
                        if (self.extends_finalized)(h) {
                            (self.finalize)(h);
                            debug!(target: "rush-finalizer", "{} Finalized block hash {}.", self.node_id, h);
                        }
                    }
                }
                _ = exit.next() => {
                    debug!(target: "rush-extender", "{} received exit signal.", self.node_id);
                    break
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::environment::{self, BlockHash, Network, NodeId};
    use parking_lot::Mutex;
    use std::sync::Arc;

    #[tokio::test(max_threads = 3)]
    async fn finalize_blocks() {
        let net = Network::new();
        let (env, mut finalized_blocks_rx) = environment::Environment::new(NodeId(0), net);
        env.import_block(1.into(), 0.into());
        env.import_block(2.into(), 1.into());

        let env = Arc::new(Mutex::new(env));

        let e = env.clone();
        let env_finalize = Box::new(move |h| e.lock().finalize_block(h));
        let e = env.clone();
        let env_extends_finalized = Box::new(move |h| e.lock().check_extends_finalized(h));

        let (mut finalizer, batch_tx) = Finalizer::<environment::Environment>::new(
            0.into(),
            env_finalize,
            env_extends_finalized,
        );
        let (exit_tx, exit_rx) = oneshot::channel();
        let finalizer = tokio::spawn(async move { finalizer.finalize(exit_rx).await });

        let _ = batch_tx.send(vec![BlockHash(1)]);
        let _ = batch_tx.send(vec![BlockHash(1), BlockHash(2)]);
        let block_1 = finalized_blocks_rx.recv().await.unwrap();
        let block_2 = finalized_blocks_rx.recv().await.unwrap();

        assert_eq!(block_1.hash(), BlockHash(1));
        assert_eq!(block_2.hash(), BlockHash(2));
        assert_eq!(
            env.lock().get_log_finalize_calls(),
            vec![BlockHash(1), BlockHash(2)]
        );

        let _ = exit_tx.send(());
        let _ = finalizer.await;
    }

    #[tokio::test(max_threads = 3)]
    async fn ignore_block_not_extending_finalized() {
        let net = Network::new();
        let (env, mut finalized_blocks_rx) = environment::Environment::new(NodeId(0), net);

        // Block tree constructed below:
        //     5
        //     |
        //     3   4
        //      \ /
        //       2
        //       |
        //       1
        //       |
        //       0
        env.import_block(1.into(), 0.into());
        env.import_block(2.into(), 1.into());
        env.import_block(3.into(), 2.into());
        env.import_block(4.into(), 2.into());
        env.import_block(5.into(), 3.into());

        let env = Arc::new(Mutex::new(env));

        let e = env.clone();
        let env_finalize = Box::new(move |h| e.lock().finalize_block(h));
        let e = env.clone();
        let env_extends_finalized = Box::new(move |h| e.lock().check_extends_finalized(h));

        let (mut finalizer, batch_tx) = Finalizer::<environment::Environment>::new(
            0.into(),
            env_finalize,
            env_extends_finalized,
        );
        let (exit_tx, exit_rx) = oneshot::channel();
        let finalizer = tokio::spawn(async move { finalizer.finalize(exit_rx).await });

        let _ = batch_tx.send(vec![
            BlockHash(1),
            BlockHash(1),
            BlockHash(2),
            BlockHash(3),
            BlockHash(4),
        ]);
        let _ = batch_tx.send(vec![BlockHash(4), BlockHash(4), BlockHash(4), BlockHash(5)]);

        let block_1 = finalized_blocks_rx.recv().await.unwrap();
        let block_2 = finalized_blocks_rx.recv().await.unwrap();
        let block_3 = finalized_blocks_rx.recv().await.unwrap();
        let block_4 = finalized_blocks_rx.recv().await.unwrap();
        assert_eq!(block_1.hash(), BlockHash(1));
        assert_eq!(block_2.hash(), BlockHash(2));
        assert_eq!(block_3.hash(), BlockHash(3));
        assert_eq!(block_4.hash(), BlockHash(5));
        assert_eq!(
            env.lock().get_log_finalize_calls(),
            vec![BlockHash(1), BlockHash(2), BlockHash(3), BlockHash(5)]
        );

        let _ = exit_tx.send(());
        let _ = finalizer.await;
    }
}
