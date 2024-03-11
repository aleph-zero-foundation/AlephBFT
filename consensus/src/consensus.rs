use futures::{
    channel::{mpsc, oneshot},
    future::pending,
    FutureExt,
};
use log::{debug, error};

use crate::{
    config::Config,
    creation,
    extension::Service as Extender,
    handle_task_termination,
    reconstruction::Service as ReconstructionService,
    runway::{NotificationIn, NotificationOut},
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

    let index = conf.node_ix();

    let (electors_tx, electors_rx) = mpsc::unbounded();
    let extender = Extender::<H>::new(index, electors_rx, ordered_batch_tx);
    let extender_terminator = terminator.add_offspring_connection("AlephBFT-extender");
    let mut extender_handle = spawn_handle
        .spawn_essential("consensus/extender", async move {
            extender.run(extender_terminator).await
        })
        .fuse();

    let (parents_for_creator, parents_from_dag) = mpsc::unbounded();

    let creator_terminator = terminator.add_offspring_connection("creator");
    let io = creation::IO {
        outgoing_units: outgoing_notifications.clone(),
        incoming_parents: parents_from_dag,
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

    let reconstruction = ReconstructionService::new(
        incoming_notifications,
        outgoing_notifications,
        parents_for_creator,
        electors_tx,
    );

    let reconstruction_terminator = terminator.add_offspring_connection("reconstruction");
    let mut reconstruction_handle = spawn_handle
        .spawn_essential("consensus/reconstruction", async move {
            reconstruction.run(reconstruction_terminator).await
        })
        .fuse();
    debug!(target: "AlephBFT", "{:?} All services started.", index);

    futures::select! {
        _ = terminator.get_exit().fuse() => {},
        _ = reconstruction_handle => {
            debug!(target: "AlephBFT-consensus", "{:?} unit reconstruction task terminated early.", index);
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

    handle_task_termination(
        reconstruction_handle,
        "AlephBFT-consensus",
        "Reconstruction",
        index,
    )
    .await;
    handle_task_termination(creator_handle, "AlephBFT-consensus", "Creator", index).await;
    handle_task_termination(extender_handle, "AlephBFT-consensus", "Extender", index).await;

    debug!(target: "AlephBFT", "{:?} All services stopped.", index);
}
