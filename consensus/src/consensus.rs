use futures::{
    channel::{mpsc, oneshot},
    future::FusedFuture,
    FutureExt,
};
use log::{debug, warn};

use crate::{
    config::Config,
    creation,
    extender::Extender,
    runway::{NotificationIn, NotificationOut},
    terminal::Terminal,
    Hasher, Receiver, Round, Sender, SpawnHandle, Terminator,
};

pub(crate) async fn run<H: Hasher + 'static>(
    conf: Config,
    incoming_notifications: Receiver<NotificationIn<H>>,
    outgoing_notifications: Sender<NotificationOut<H>>,
    ordered_batch_tx: Sender<Vec<H::Hash>>,
    spawn_handle: impl SpawnHandle,
    starting_round: oneshot::Receiver<Option<Round>>,
    mut terminator: Terminator,
) {
    debug!(target: "AlephBFT", "{:?} Starting all services...", conf.node_ix);

    let n_members = conf.n_members;
    let index = conf.node_ix;

    let (electors_tx, electors_rx) = mpsc::unbounded();
    let mut extender = Extender::<H>::new(index, n_members, electors_rx, ordered_batch_tx);
    let extender_terminator = terminator.add_offspring_connection("AlephBFT-extender");
    let mut extender_handle = spawn_handle
        .spawn_essential("consensus/extender", async move {
            extender.extend(extender_terminator).await
        })
        .fuse();

    let (parents_for_creator, parents_from_terminal) = mpsc::unbounded();

    let creator_terminator = terminator.add_offspring_connection("creator");
    let io = creation::IO {
        outgoing_units: outgoing_notifications.clone(),
        incoming_parents: parents_from_terminal,
    };
    let mut creator_handle = spawn_handle
        .spawn_essential("consensus/creation", async move {
            creation::run(conf.clone().into(), io, starting_round, creator_terminator).await;
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

    let terminal_terminator = terminator.add_offspring_connection("terminal");
    let mut terminal_handle = spawn_handle
        .spawn_essential("consensus/terminal", async move {
            terminal.run(terminal_terminator).await
        })
        .fuse();
    debug!(target: "AlephBFT", "{:?} All services started.", index);

    futures::select! {
        _ = terminator.get_exit() => {},
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
    debug!(target: "AlephBFT", "{:?} All services stopping.", index);

    // we stop no matter if received Ok or Err
    terminator.terminate_sync().await;

    if !terminal_handle.is_terminated() {
        if let Err(()) = terminal_handle.await {
            warn!(target: "AlephBFT-consensus", "{:?} Terminal finished with an error", index);
        }
        debug!(target: "AlephBFT-consensus", "{:?} terminal stopped.", index);
    }

    if !creator_handle.is_terminated() {
        if let Err(()) = creator_handle.await {
            warn!(target: "AlephBFT-consensus", "{:?} Creator finished with an error", index);
        }
        debug!(target: "AlephBFT-consensus", "{:?} creator stopped.", index);
    }

    if !extender_handle.is_terminated() {
        if let Err(()) = extender_handle.await {
            warn!(target: "AlephBFT-consensus", "{:?} Extender finished with an error", index);
        }
        debug!(target: "AlephBFT-consensus", "{:?} extender stopped.", index);
    }

    debug!(target: "AlephBFT", "{:?} All services stopped.", index);
}
