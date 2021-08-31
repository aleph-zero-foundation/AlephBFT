use futures::{
    channel::{mpsc, oneshot},
    FutureExt,
};
use log::{debug, info};

use crate::{
    config::Config,
    creator::Creator,
    extender::Extender,
    runway::{NotificationIn, NotificationOut},
    terminal::Terminal,
    Hasher, OrderedBatch, Receiver, Sender, SpawnHandle,
};

pub(crate) async fn run<H: Hasher + 'static>(
    conf: Config,
    incoming_notifications: Receiver<NotificationIn<H>>,
    outgoing_notifications: Sender<NotificationOut<H>>,
    ordered_batch_tx: Sender<OrderedBatch<H::Hash>>,
    spawn_handle: impl SpawnHandle,
    mut exit: oneshot::Receiver<()>,
) {
    info!(target: "AlephBFT", "{:?} Starting all services...", conf.node_ix);

    let n_members = conf.n_members;

    let (electors_tx, electors_rx) = mpsc::unbounded();
    let mut extender = Extender::<H>::new(conf.node_ix, n_members, electors_rx, ordered_batch_tx);
    let (extender_exit, exit_rx) = oneshot::channel();
    let mut extender_handle = spawn_handle
        .spawn_essential("consensus/extender", async move {
            extender.extend(exit_rx).await
        })
        .fuse();

    let (parents_tx, parents_rx) = mpsc::unbounded();
    let new_units_tx = outgoing_notifications.clone();
    let mut creator = Creator::new(conf.clone(), parents_rx, new_units_tx);

    let (creator_exit, exit_rx) = oneshot::channel();
    let mut creator_handle = spawn_handle
        .spawn_essential(
            "consensus/creator",
            async move { creator.create(exit_rx).await },
        )
        .fuse();

    let mut terminal = Terminal::new(conf.node_ix, incoming_notifications, outgoing_notifications);

    // send a new parent candidate to the creator
    terminal.register_post_insert_hook(Box::new(move |u| {
        parents_tx
            .unbounded_send(u.into())
            .expect("Channel to creator should be open.");
    }));
    // try to extend the partial order after adding a unit to the dag
    terminal.register_post_insert_hook(Box::new(move |u| {
        electors_tx
            .unbounded_send(u.into())
            .expect("Channel to extender should be open.")
    }));

    let (terminal_exit, exit_rx) = oneshot::channel();
    let mut terminal_handle = spawn_handle
        .spawn_essential(
            "consensus/terminal",
            async move { terminal.run(exit_rx).await },
        )
        .fuse();
    info!(target: "AlephBFT", "{:?} All services started.", conf.node_ix);

    let mut terminal_exited = false;
    let mut creator_exited = false;
    let mut extender_exited = false;
    futures::select! {
        _ = exit => {},
        _ = terminal_handle => {
            terminal_exited = true;
            debug!(target: "AlephBFT-consensus", "{:?} terminal task terminated early.", conf.node_ix);
        },
        _ = creator_handle => {
            creator_exited = true;
            debug!(target: "AlephBFT-consensus", "{:?} creator task terminated early.", conf.node_ix);
        },
        _ = extender_handle => {
            extender_exited = true;
            debug!(target: "AlephBFT-consensus", "{:?} extender task terminated early.", conf.node_ix);
        }
    }

    // we stop no matter if received Ok or Err
    let _ = terminal_exit.send(());
    if !terminal_exited {
        terminal_handle.await.unwrap();
    }
    let _ = creator_exit.send(());
    if !creator_exited {
        creator_handle.await.unwrap();
    }
    let _ = extender_exit.send(());
    if !extender_exited {
        extender_handle.await.unwrap();
    }

    info!(target: "AlephBFT", "{:?} All services stopped.", conf.node_ix);
}
