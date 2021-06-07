use futures::channel::{mpsc, oneshot};
use log::debug;

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
    debug!(target: "AlephBFT", "{:?} Starting all services...", conf.node_ix);

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
        if let Err(e) = parents_tx.unbounded_send(u.into()) {
            debug!(target: "AlephBFT", "channel to creator is closed {:?}", e);
        }
    }));
    // try to extend the partial order after adding a unit to the dag
    terminal.register_post_insert_hook(Box::new(move |u| {
        if let Err(e) = electors_tx.unbounded_send(u.into()) {
            debug!(target: "AlephBFT", "channel to extender is closed {:?}", e);
        }
    }));

    let (terminal_exit, exit_rx) = oneshot::channel();
    spawn_handle.spawn(
        "consensus/terminal",
        async move { terminal.run(exit_rx).await },
    );
    debug!(target: "AlephBFT", "{:?} All services started.", conf.node_ix);

    let _ = exit.await;
    // we stop no matter if received Ok or Err
    let _ = terminal_exit.send(());
    let _ = creator_exit.send(());
    let _ = extender_exit.send(());

    debug!(target: "AlephBFT", "{:?} All services stopped.", conf.node_ix);
}
