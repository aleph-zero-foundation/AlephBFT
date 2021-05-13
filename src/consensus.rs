use futures::{Sink, Stream};
use log::{debug, error};
use tokio::sync::{mpsc, oneshot};

use crate::{
    creator::Creator,
    extender::Extender,
    member::{Config, NotificationIn, NotificationOut},
    syncer::Syncer,
    terminal::Terminal,
    Hasher, NodeIdT, OrderedBatch, Sender, SpawnHandle,
};

pub(crate) async fn run<H: Hasher + 'static, NI: NodeIdT>(
    conf: Config<NI>,
    ntfct_env_rx: impl Stream<Item = NotificationIn<H>> + Send + Unpin + 'static,
    ntfct_env_tx: impl Sink<NotificationOut<H>, Error = Box<dyn std::error::Error>>
        + Send
        + Unpin
        + 'static,
    ordered_batch_tx: Sender<OrderedBatch<H::Hash>>,
    spawn_handle: impl SpawnHandle,
    exit: oneshot::Receiver<()>,
) {
    debug!(target: "rush-root", "{} Starting all services...", conf.node_id);

    let n_members = conf.n_members;

    let (electors_tx, electors_rx) = mpsc::unbounded_channel();
    let mut extender = Extender::<H, NI>::new(
        conf.node_id.clone(),
        n_members,
        electors_rx,
        ordered_batch_tx,
    );
    let (extender_exit, exit_rx) = oneshot::channel();
    spawn_handle.spawn("consensus/extender", async move {
        extender.extend(exit_rx).await
    });

    let (mut syncer, ntfct_common_tx, ntfct_term_rx) =
        Syncer::new(conf.node_id.clone(), ntfct_env_tx, ntfct_env_rx);
    let (syncer_exit, exit_rx) = oneshot::channel();
    spawn_handle.spawn(
        "consensus/syncer",
        async move { syncer.sync(exit_rx).await },
    );

    let (parents_tx, parents_rx) = mpsc::unbounded_channel();
    let new_units_tx = ntfct_common_tx.clone();
    let mut creator = Creator::new(conf.clone(), parents_rx, new_units_tx);

    let (creator_exit, exit_rx) = oneshot::channel();
    spawn_handle.spawn(
        "consensus/creator",
        async move { creator.create(exit_rx).await },
    );

    let mut terminal = Terminal::new(conf.node_id.clone(), ntfct_term_rx, ntfct_common_tx);

    // send a new parent candidate to the creator
    terminal.register_post_insert_hook(Box::new(move |u| {
        let send_result = parents_tx.send(u.into());
        if let Err(e) = send_result {
            error!(target:"rush-terminal", "Unable to send a unit to Creator: {:?}.", e);
        }
    }));
    // try to extend the partial order after adding a unit to the dag
    terminal.register_post_insert_hook(Box::new(move |u| {
        let send_result = electors_tx.send(u.into());
        if let Err(e) = send_result {
            error!(target:"rush-terminal", "Unable to send a unit to Extender: {:?}.", e);
        }
    }));

    let (terminal_exit, exit_rx) = oneshot::channel();
    spawn_handle.spawn(
        "consensus/terminal",
        async move { terminal.run(exit_rx).await },
    );
    debug!(target: "rush-root", "{} All services started.", conf.node_id);

    let _ = exit.await;
    // we stop no matter if received Ok or Err
    let _ = creator_exit.send(());
    let _ = terminal_exit.send(());
    let _ = extender_exit.send(());
    let _ = syncer_exit.send(());

    debug!(target: "rush-root", "{} All services stopped.", conf.node_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        testing::mock::{Hasher64, HonestHub, NodeId},
        units::{PreUnit, Unit},
        NodeIndex,
    };
    use futures::{channel, sink::SinkExt, stream::StreamExt, Future};
    use parking_lot::Mutex;
    use std::sync::Arc;
    use tokio::time::Duration;

    fn init_log() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::max())
            .is_test(true)
            .try_init();
    }

    #[derive(Default, Clone)]
    struct Spawner {
        handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    }

    impl SpawnHandle for Spawner {
        fn spawn(&self, _name: &str, task: impl Future<Output = ()> + Send + 'static) {
            self.handles.lock().push(tokio::spawn(task))
        }
    }

    impl Spawner {
        async fn wait(&self) {
            for h in self.handles.lock().iter_mut() {
                let _ = h.await;
            }
        }
    }

    #[tokio::test(max_threads = 1)]
    async fn agree_on_first_batch() {
        init_log();
        let n_members: usize = 16;
        let mut hub = HonestHub::new(n_members);

        let mut exits = vec![];
        let mut batch_rxs = vec![];
        let spawner = Spawner::default();

        for node_ix in 0..n_members {
            let (tx, rx) = hub.connect(NodeIndex(node_ix));
            let tx = tx.sink_map_err(|e| e.into());
            let conf = Config::<NodeId> {
                node_id: node_ix.into(),
                session_id: 0u64,
                n_members: n_members.into(),
                create_lag: Duration::from_millis(10),
            };
            let (exit_tx, exit_rx) = oneshot::channel();
            exits.push(exit_tx);
            let (batch_tx, batch_rx) = mpsc::unbounded_channel();
            batch_rxs.push(batch_rx);
            spawner.spawn(
                "consensus",
                run(conf, rx, tx, batch_tx, spawner.clone(), exit_rx),
            );
        }

        spawner.spawn("hub", hub);

        let mut batches = vec![];
        for mut rx in batch_rxs.drain(..) {
            let batch = rx.recv().await.unwrap();
            assert!(!batch.is_empty());
            batches.push(batch);
        }

        for node_ix in 1..n_members {
            assert_eq!(batches[0], batches[node_ix]);
        }

        exits.into_iter().for_each(|tx| {
            let _ = tx.send(());
        });
        spawner.wait().await;
    }

    #[tokio::test(max_threads = 1)]
    async fn catches_wrong_control_hash() {
        init_log();
        let n_nodes = 4;
        let spawner = Spawner::default();
        let node_ix = 0;
        let (mut tx_in, rx_in) = channel::mpsc::unbounded();
        let (tx_out, mut rx_out) = channel::mpsc::unbounded();
        let tx_out = tx_out.sink_map_err(|e| e.into());

        let conf = Config::<NodeId> {
            node_id: node_ix.into(),
            session_id: 0u64,
            n_members: n_nodes.into(),
            create_lag: Duration::from_millis(10),
        };
        let (exit_tx, exit_rx) = oneshot::channel();
        let (batch_tx, _batch_rx) = mpsc::unbounded_channel();

        spawner.spawn(
            "consensus",
            run(conf, rx_in, tx_out, batch_tx, spawner.clone(), exit_rx),
        );
        let mut bad_pu =
            PreUnit::<Hasher64>::new_from_parents(1.into(), 0, (vec![None; n_nodes]).into());
        let bad_control_hash: <Hasher64 as Hasher>::Hash = 1111111;
        assert!(
            bad_control_hash != bad_pu.control_hash.hash,
            "Bad control hash cannot be the correct one."
        );
        bad_pu.control_hash.hash = bad_control_hash;
        let bad_hash: <Hasher64 as Hasher>::Hash = 1234567;
        let bad_unit = Unit::new_from_preunit(bad_pu, bad_hash);
        let _ = tx_in.send(NotificationIn::NewUnits(vec![bad_unit])).await;
        loop {
            let notification = rx_out.next().await.unwrap();
            debug!("notification {:?}", notification);
            if let NotificationOut::WrongControlHash(h) = notification {
                assert_eq!(h, bad_hash, "Expected notification for our bad unit.");
                break;
            }
        }

        let _ = exit_tx.send(());
        spawner.wait().await;
    }
}
