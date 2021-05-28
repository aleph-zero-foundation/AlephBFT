use futures::channel::{mpsc, oneshot};
use log::{debug, error};

use crate::{
    config::Config,
    creator::Creator,
    extender::Extender,
    member::{NotificationIn, NotificationOut},
    terminal::Terminal,
    Hasher, OrderedBatch, Receiver, Sender, SpawnHandle,
};

pub(crate) async fn run<H: Hasher + 'static>(
    conf: Config,
    incoming_notifications: Receiver<NotificationIn<H>>,
    outgoing_notifications: Sender<NotificationOut<H>>,
    ordered_batch_tx: Sender<OrderedBatch<H::Hash>>,
    spawn_handle: impl SpawnHandle,
    exit: oneshot::Receiver<()>,
) {
    debug!(target: "rush-root", "{:?} Starting all services...", conf.node_ix);

    let n_members = conf.n_members;

    let (electors_tx, electors_rx) = mpsc::unbounded();
    let mut extender = Extender::<H>::new(conf.node_ix, n_members, electors_rx, ordered_batch_tx);
    let (extender_exit, exit_rx) = oneshot::channel();
    spawn_handle.spawn("consensus/extender", async move {
        extender.extend(exit_rx).await
    });

    let (parents_tx, parents_rx) = mpsc::unbounded();
    let new_units_tx = outgoing_notifications.clone();
    let mut creator = Creator::new(conf.clone(), parents_rx, new_units_tx);

    let (creator_exit, exit_rx) = oneshot::channel();
    spawn_handle.spawn(
        "consensus/creator",
        async move { creator.create(exit_rx).await },
    );

    let mut terminal = Terminal::new(conf.node_ix, incoming_notifications, outgoing_notifications);

    // send a new parent candidate to the creator
    terminal.register_post_insert_hook(Box::new(move |u| {
        let send_result = parents_tx.unbounded_send(u.into());
        if let Err(e) = send_result {
            error!(target:"rush-terminal", "Unable to send a unit to Creator: {:?}.", e);
        }
    }));
    // try to extend the partial order after adding a unit to the dag
    terminal.register_post_insert_hook(Box::new(move |u| {
        let send_result = electors_tx.unbounded_send(u.into());
        if let Err(e) = send_result {
            error!(target:"rush-terminal", "Unable to send a unit to Extender: {:?}.", e);
        }
    }));

    let (terminal_exit, exit_rx) = oneshot::channel();
    spawn_handle.spawn(
        "consensus/terminal",
        async move { terminal.run(exit_rx).await },
    );
    debug!(target: "rush-root", "{:?} All services started.", conf.node_ix);

    let _ = exit.await;
    // we stop no matter if received Ok or Err
    let _ = creator_exit.send(());
    let _ = terminal_exit.send(());
    let _ = extender_exit.send(());

    debug!(target: "rush-root", "{:?} All services stopped.", conf.node_ix);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        testing::mock::{gen_config, Hasher64, HonestHub, Spawner},
        units::{ControlHash, PreUnit, Unit},
        NodeIndex,
    };
    use futures::{channel::mpsc, sink::SinkExt, stream::StreamExt};

    fn init_log() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::max())
            .is_test(true)
            .try_init();
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
            let conf = gen_config(NodeIndex(node_ix), n_members.into());
            let (exit_tx, exit_rx) = oneshot::channel();
            exits.push(exit_tx);
            let (batch_tx, batch_rx) = mpsc::unbounded();
            batch_rxs.push(batch_rx);
            spawner.spawn(
                "consensus",
                run(conf, rx, tx, batch_tx, spawner.clone(), exit_rx),
            );
        }

        spawner.spawn("hub", hub);

        let mut batches = vec![];
        for mut rx in batch_rxs.drain(..) {
            let batch = rx.next().await.unwrap();
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
        let (mut tx_in, rx_in) = mpsc::unbounded();
        let (tx_out, mut rx_out) = mpsc::unbounded();

        let conf = gen_config(NodeIndex(node_ix), n_nodes.into());
        let (exit_tx, exit_rx) = oneshot::channel();
        let (batch_tx, _batch_rx) = mpsc::unbounded();

        spawner.spawn(
            "consensus",
            run(conf, rx_in, tx_out, batch_tx, spawner.clone(), exit_rx),
        );
        let control_hash = ControlHash::new(&(vec![None; n_nodes]).into());
        let bad_pu = PreUnit::<Hasher64>::new(1.into(), 0, control_hash);
        let bad_control_hash: <Hasher64 as Hasher>::Hash = [0, 1, 0, 1, 0, 1, 0, 1];
        assert!(
            bad_control_hash != bad_pu.control_hash().combined_hash,
            "Bad control hash cannot be the correct one."
        );
        let mut control_hash = bad_pu.control_hash().clone();
        control_hash.combined_hash = bad_control_hash;
        let bad_pu = PreUnit::new(bad_pu.creator(), bad_pu.round(), control_hash);
        let bad_hash: <Hasher64 as Hasher>::Hash = [0, 1, 0, 1, 0, 1, 0, 1];
        let bad_unit = Unit::new(bad_pu, bad_hash);
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
