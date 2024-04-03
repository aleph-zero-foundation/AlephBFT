use futures::{
    channel::{mpsc, oneshot},
    future::pending,
    FutureExt,
};
use log::{debug, error};

use crate::{
    config::Config,
    creation, handle_task_termination,
    reconstruction::{ReconstructedUnit, Request, Service as ReconstructionService},
    runway::ExplicitParents,
    units::SignedUnit,
    Data, DataProvider, Hasher, MultiKeychain, Receiver, Round, Sender, SpawnHandle, Terminator,
};

pub struct IO<H: Hasher, D: Data, MK: MultiKeychain, DP: DataProvider<D>> {
    pub units_from_runway: Receiver<SignedUnit<H, D, MK>>,
    pub parents_from_runway: Receiver<ExplicitParents<H>>,
    pub units_for_runway: Sender<ReconstructedUnit<SignedUnit<H, D, MK>>>,
    pub requests_for_runway: Sender<Request<H>>,
    pub new_units_for_runway: Sender<SignedUnit<H, D, MK>>,
    pub data_provider: DP,
    pub starting_round: oneshot::Receiver<Option<Round>>,
}

pub async fn run<H: Hasher, D: Data, MK: MultiKeychain, DP: DataProvider<D>>(
    conf: Config,
    io: IO<H, D, MK, DP>,
    keychain: MK,
    spawn_handle: impl SpawnHandle,
    mut terminator: Terminator,
) {
    debug!(target: "AlephBFT", "{:?} Starting all services...", conf.node_ix());
    let IO {
        units_from_runway,
        parents_from_runway,
        units_for_runway,
        requests_for_runway,
        new_units_for_runway,
        data_provider,
        starting_round,
    } = io;

    let index = conf.node_ix();

    let (parents_for_creator, parents_from_dag) = mpsc::unbounded();

    let creator_terminator = terminator.add_offspring_connection("creator");
    let io = creation::IO {
        outgoing_units: new_units_for_runway,
        incoming_parents: parents_from_dag,
        data_provider,
    };
    let creator_handle = spawn_handle
        .spawn_essential(
            "consensus/creation",
            creation::run(conf, io, keychain, starting_round, creator_terminator),
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
        units_from_runway,
        parents_from_runway,
        requests_for_runway,
        units_for_runway,
        parents_for_creator,
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

    debug!(target: "AlephBFT", "{:?} All services stopped.", index);
}
