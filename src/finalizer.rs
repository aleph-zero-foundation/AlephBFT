use tokio::sync::mpsc;

use crate::{
    skeleton::{Receiver, Sender},
    traits::Environment,
};

pub(crate) struct Finalizer<E: Environment> {
    batch_rx: Receiver<Vec<E::BlockHash>>,
    finalize: Box<dyn Fn(E::BlockHash) + Sync + Send + 'static>,
    extends_finalized: Box<dyn Fn(E::BlockHash) -> bool + Sync + Send + 'static>,
}

impl<E: Environment> Finalizer<E> {
    pub(crate) fn new(
        finalize: Box<dyn Fn(E::BlockHash) + Send + Sync + 'static>,
        extends_finalized: Box<dyn Fn(E::BlockHash) -> bool + Sync + Send + 'static>,
    ) -> (Self, Sender<Vec<E::BlockHash>>) {
        let (batch_tx, batch_rx) = mpsc::unbounded_channel();
        (
            Finalizer {
                batch_rx,
                finalize,
                extends_finalized,
            },
            batch_tx,
        )
    }
    pub(crate) async fn finalize(&mut self) {
        loop {
            if let Some(batch) = self.batch_rx.recv().await {
                for h in batch {
                    if (self.extends_finalized)(h) {
                        (self.finalize)(h);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::environment::{self, BlockHash, Network};
    use parking_lot::Mutex;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn finalize_blocks() {
        let net = Network::new(1);
        let (env, mut finalized_blocks_rx) = environment::Environment::new(net);
        env.import_block(1.into(), 0.into());
        env.import_block(2.into(), 1.into());

        let env = Arc::new(Mutex::new(env));

        let e = env.clone();
        let env_finalize = Box::new(move |h| e.lock().finalize_block(h));
        let e = env.clone();
        let env_extends_finalized = Box::new(move |h| e.lock().check_extends_finalized(h));

        let (mut finalizer, batch_tx) =
            Finalizer::<environment::Environment>::new(env_finalize, env_extends_finalized);
        let _ = tokio::spawn(async move { finalizer.finalize().await });

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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn ignore_block_not_extending_finalized() {
        let net = Network::new(1);
        let (env, mut finalized_blocks_rx) = environment::Environment::new(net);

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

        let (mut finalizer, batch_tx) =
            Finalizer::<environment::Environment>::new(env_finalize, env_extends_finalized);
        let _ = tokio::spawn(async move { finalizer.finalize().await });

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
    }
}
