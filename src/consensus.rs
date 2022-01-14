use futures::{
    channel::{mpsc, oneshot},
    future::FusedFuture,
    FutureExt,
};
use log::{debug, info, warn};

use crate::{
    config::Config,
    creation,
    extender::Extender,
    runway::{NotificationIn, NotificationOut},
    terminal::Terminal,
    Hasher, OrderedBatch, Receiver, Round, Sender, SpawnHandle,
};

pub(crate) async fn run<H: Hasher + 'static>(
    conf: Config,
    incoming_notifications: Receiver<NotificationIn<H>>,
    outgoing_notifications: Sender<NotificationOut<H>>,
    ordered_batch_tx: Sender<OrderedBatch<H::Hash>>,
    spawn_handle: impl SpawnHandle,
    starting_round: oneshot::Receiver<Round>,
    mut exit: oneshot::Receiver<()>,
) {
    info!(target: "AlephBFT", "{:?} Starting all services...", conf.node_ix);

    let n_members = conf.n_members;
    let index = conf.node_ix;

    let (electors_tx, electors_rx) = mpsc::unbounded();
    let mut extender = Extender::<H>::new(index, n_members, electors_rx, ordered_batch_tx);
    let (extender_exit, exit_rx) = oneshot::channel();
    let mut extender_handle = spawn_handle
        .spawn_essential("consensus/extender", async move {
            extender.extend(exit_rx).await
        })
        .fuse();

    let (parents_for_creator, parents_from_terminal) = mpsc::unbounded();

    let (creator_exit, exit_rx) = oneshot::channel();
    let io = creation::IO {
        outgoing_units: outgoing_notifications.clone(),
        incoming_parents: parents_from_terminal,
    };
    let mut creator_handle = spawn_handle
        .spawn_essential("consensus/creation", async move {
            creation::run(conf.clone().into(), io, starting_round, exit_rx).await;
        })
        .fuse();

    let mut terminal = Terminal::new(index, incoming_notifications, outgoing_notifications);

    // send a new parent candidate to the creator
    terminal.register_post_insert_hook(Box::new(move |u| {
        parents_for_creator
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
    info!(target: "AlephBFT", "{:?} All services started.", index);

    futures::select! {
        _ = exit => {},
        _ = terminal_handle => {
            debug!(target: "AlephBFT-consensus", "{:?} terminal task terminated early.", index);
        },
        _ = creator_handle => {
            debug!(target: "AlephBFT-consensus", "{:?} creator task terminated early.", index);
        },
        _ = extender_handle => {
            debug!(target: "AlephBFT-consensus", "{:?} extender task terminated early.", index);
        }
    }
    info!(target: "AlephBFT", "{:?} All services stopping.", index);

    // we stop no matter if received Ok or Err
    if terminal_exit.send(()).is_err() {
        debug!(target: "AlephBFT-consensus", "{:?} terminal already stopped.", index);
    }
    if !terminal_handle.is_terminated() {
        if let Err(()) = terminal_handle.await {
            warn!(target: "AlephBFT-consensus", "{:?} Terminal finished with an error", index);
        }
        debug!(target: "AlephBFT-consensus", "{:?} terminal stopped.", index);
    }

    if creator_exit.send(()).is_err() {
        debug!(target: "AlephBFT-consensus", "{:?} creator already stopped.", index);
    }
    if !creator_handle.is_terminated() {
        if let Err(()) = creator_handle.await {
            warn!(target: "AlephBFT-consensus", "{:?} Creator finished with an error", index);
        }
        debug!(target: "AlephBFT-consensus", "{:?} creator stopped.", index);
    }

    if extender_exit.send(()).is_err() {
        debug!(target: "AlephBFT-consensus", "{:?} extender already stopped.", index);
    }
    if !extender_handle.is_terminated() {
        if let Err(()) = extender_handle.await {
            warn!(target: "AlephBFT-consensus", "{:?} Extender finished with an error", index);
        }
        debug!(target: "AlephBFT-consensus", "{:?} extender stopped.", index);
    }

    info!(target: "AlephBFT", "{:?} All services stopped.", index);
}
