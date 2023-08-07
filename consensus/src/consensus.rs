use futures::{
    channel::{mpsc, oneshot},
    future::pending,
    FutureExt,
};
use log::{debug, error, warn};

use crate::{
    config::Config,
    creation,
    extender::Extender,
    handle_task_termination,
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
    debug!(target: "AlephBFT", "{:?} Starting all services...", conf.node_ix());

    let n_members = conf.n_members();
    let index = conf.node_ix();

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
    let creator_handle = spawn_handle
        .spawn_essential(
            "consensus/creation",
            creation::run(conf.into(), io, starting_round, creator_terminator),
        )
        .shared();
    let creator_handle_for_panic = creator_handle.clone();
    let creator_panic_handle = async move {
        if creator_handle_for_panic.await.is_err() {
            return;
        }
        pending().await
    };

    let mut terminal = Terminal::new(index, incoming_notifications, outgoing_notifications);

    // send a new parent candidate to the creator
    let mut parents_for_creator = Some(parents_for_creator);
    terminal.register_post_insert_hook(Box::new(move |u| {
        if let Some(parents_for_creator_tx) = &parents_for_creator {
            if parents_for_creator_tx.unbounded_send(u.into()).is_err() {
                warn!(target: "AlephBFT", "Channel to creator was closed.");
                parents_for_creator = None;
            }
        }
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
        _ = terminator.get_exit().fuse() => {},
        _ = terminal_handle => {
            debug!(target: "AlephBFT-consensus", "{:?} terminal task terminated early.", index);
        },
        _ = creator_panic_handle.fuse() => {
            error!(target: "AlephBFT-consensus", "{:?} creator task terminated early with its task being dropped.", index);
        },
        _ = extender_handle => {
            debug!(target: "AlephBFT-consensus", "{:?} extender task terminated early.", index);
        }
    }
    debug!(target: "AlephBFT", "{:?} All services stopping.", index);

    // we stop no matter if received Ok or Err
    terminator.terminate_sync().await;

    handle_task_termination(terminal_handle, "AlephBFT-consensus", "Terminal", index).await;
    handle_task_termination(creator_handle, "AlephBFT-consensus", "Creator", index).await;
    handle_task_termination(extender_handle, "AlephBFT-consensus", "Extender", index).await;

    debug!(target: "AlephBFT", "{:?} All services stopped.", index);
}
