use crate::{
    alerts::{Handler as AlertHandler, Service as AlertService, IO as AlertIO},
    backup::{BackupLoader, BackupSaver},
    collection::initial_unit_collection,
    consensus::{
        handler::Consensus,
        service::{Service, IO as ConsensusIO},
    },
    creation, handle_task_termination,
    interface::LocalIO,
    network::{Hub as NetworkHub, NetworkData},
    units::Validator,
    Config, DataProvider, MultiKeychain, Network, SpawnHandle, Terminator, UnitFinalizationHandler,
};
use futures::{
    channel::{mpsc, oneshot},
    future::pending,
    pin_mut, AsyncRead, AsyncWrite, FutureExt,
};
use log::{debug, error, info};

mod handler;
mod service;

const LOG_TARGET: &str = "AlephBFT-consensus";

/// Starts the consensus algorithm as an async task. It stops establishing consensus for new data items after
/// reaching the threshold specified in [`Config::max_round`] or upon receiving a stop signal from `exit`.
/// For a detailed description of the consensus implemented by `run_session` see
/// [docs for devs](https://cardinal-cryptography.github.io/AlephBFT/index.html)
/// or the [original paper](https://arxiv.org/abs/1908.05156).
pub async fn run_session<
    DP: DataProvider,
    UFH: UnitFinalizationHandler<Data = DP::Output>,
    US: AsyncWrite + Send + Sync + 'static,
    UL: AsyncRead + Send + Sync + 'static,
    N: Network<NetworkData<UFH::Hasher, DP::Output, MK::Signature, MK::PartialMultisignature>>,
    SH: SpawnHandle,
    MK: MultiKeychain,
>(
    config: Config,
    local_io: LocalIO<DP, UFH, US, UL>,
    network: N,
    keychain: MK,
    spawn_handle: SH,
    mut terminator: Terminator,
) {
    info!(target: LOG_TARGET, "Starting a new session.");
    let index = keychain.index();
    let session_id = config.session_id();
    let (data_provider, finalization_handler, unit_saver, unit_loader) = local_io.into_components();

    info!(target: LOG_TARGET, "Loading units from backup.");
    let (loaded_units, loaded_starting_round) =
        match BackupLoader::new(unit_loader, index, session_id)
            .load_backup()
            .await
        {
            Ok(result) => result,
            Err(e) => {
                error!(target: LOG_TARGET, "Error loading units from backup: {}.", e);
                return;
            }
        };
    info!(
        target: LOG_TARGET,
        "Loaded {:?} units from backup. Able to continue from round: {:?}.",
        loaded_units.len(),
        loaded_starting_round,
    );

    let (alert_messages_for_alerter, alert_messages_from_network) = mpsc::unbounded();
    let (alert_messages_for_network, alert_messages_from_alerter) = mpsc::unbounded();
    let (unit_messages_for_service, unit_messages_from_network) = mpsc::unbounded();
    let (unit_messages_for_network, unit_messages_from_service) = mpsc::unbounded();

    debug!(target: LOG_TARGET, "Spawning network.");
    let network_terminator = terminator.add_offspring_connection("network");
    let network_handle = spawn_handle
        .spawn_essential("consensus/network", async move {
            NetworkHub::new(
                network,
                unit_messages_from_service,
                unit_messages_for_service,
                alert_messages_from_alerter,
                alert_messages_for_alerter,
            )
            .run(network_terminator)
            .await
        })
        .fuse();
    pin_mut!(network_handle);
    debug!(target: LOG_TARGET, "Network spawned.");

    let (new_units_for_service, new_units_from_creator) = mpsc::unbounded();
    let (parents_for_creator, parents_from_service) = mpsc::unbounded();
    let (starting_round_for_creator, starting_round_from_collection) = oneshot::channel();

    debug!(target: LOG_TARGET, "Spawning creator.");
    let creator_terminator = terminator.add_offspring_connection("creator");
    let creator_config = config.clone();
    let creator_keychain = keychain.clone();
    let creator_handle = spawn_handle
        .spawn_essential("consensus/creator", async move {
            creation::run(
                creator_config,
                creation::IO {
                    outgoing_units: new_units_for_service,
                    incoming_parents: parents_from_service,
                    data_provider,
                },
                creator_keychain,
                starting_round_from_collection,
                creator_terminator,
            )
            .await
        })
        .shared();
    let creator_handle_for_panic = creator_handle.clone();
    let creator_panic_handle = async move {
        if creator_handle_for_panic.await.is_err() {
            return;
        }
        pending().await
    }
    .fuse();
    pin_mut!(creator_panic_handle);
    let creator_handle = creator_handle.fuse();
    debug!(target: LOG_TARGET, "Creator spawned.");

    let (backup_units_for_saver, backup_units_from_service) = mpsc::unbounded();
    let (backup_units_for_service, backup_units_from_saver) = mpsc::unbounded();

    debug!(target: LOG_TARGET, "Spawning backup saver.");
    let backup_saver_terminator = terminator.add_offspring_connection("backup-saver");
    let backup_saver_handle = spawn_handle.spawn_essential("consensus/backup_saver", {
        let mut backup_saver = BackupSaver::new(
            backup_units_from_service,
            backup_units_for_service,
            unit_saver,
        );
        async move {
            backup_saver.run(backup_saver_terminator).await;
        }
    });
    let mut backup_saver_handle = backup_saver_handle.fuse();
    debug!(target: LOG_TARGET, "Backup saver spawned.");

    let (alert_notifications_for_units, notifications_from_alerter) = mpsc::unbounded();
    let (alerts_for_alerter, alerts_from_units) = mpsc::unbounded();

    debug!(target: LOG_TARGET, "Spawning alerter.");
    let alerter_terminator = terminator.add_offspring_connection("alerter");
    let alerter_keychain = keychain.clone();
    let alerter_handler = AlertHandler::new(alerter_keychain.clone(), session_id);
    let mut alerter_service = AlertService::new(
        alerter_keychain,
        AlertIO {
            messages_for_network: alert_messages_for_network,
            messages_from_network: alert_messages_from_network,
            notifications_for_units: alert_notifications_for_units,
            alerts_from_units,
        },
        alerter_handler,
    );
    let mut alerter_handle = spawn_handle
        .spawn_essential("consensus/alerter", async move {
            alerter_service.run(alerter_terminator).await;
        })
        .fuse();
    debug!(target: LOG_TARGET, "Alerter spawned.");

    let validator = Validator::new(session_id, keychain.clone(), config.max_round());
    let (responses_for_collection, responses_from_service) = mpsc::unbounded();

    debug!(target: LOG_TARGET, "Spawning initial unit collection.");
    let starting_round_handle = match initial_unit_collection(
        &keychain,
        &validator,
        unit_messages_for_network.clone(),
        starting_round_for_creator,
        loaded_starting_round,
        responses_from_service,
        config.delay_config().newest_request_delay.clone(),
    ) {
        Ok(handle) => handle.fuse(),
        Err(_) => return,
    };
    pin_mut!(starting_round_handle);
    debug!(target: LOG_TARGET, "Initial unit collection spawned.");

    debug!(target: LOG_TARGET, "Spawning consensus service.");
    let consensus = Consensus::new(
        keychain.clone(),
        validator.clone(),
        finalization_handler,
        config.delay_config().clone(),
    );
    let service_handle = spawn_handle
        .spawn_essential("consensus/service", {
            let consensus_io = ConsensusIO {
                backup_units_for_saver,
                backup_units_from_saver,
                alerts_for_alerter,
                notifications_from_alerter,
                unit_messages_for_network,
                unit_messages_from_network,
                responses_for_collection,
                parents_for_creator,
                new_units_from_creator,
            };
            let service_terminator = terminator.add_offspring_connection("service");
            let service = Service::new(consensus, consensus_io);

            async move { service.run(loaded_units, service_terminator).await }
        })
        .fuse();
    pin_mut!(service_handle);
    debug!(target: LOG_TARGET, "Consensus service spawned.");

    loop {
        futures::select! {
            _ = starting_round_handle => {
                debug!(target: LOG_TARGET, "Starting round task terminated.");
            },
            _ = network_handle => {
                error!(target: LOG_TARGET, "Network hub terminated early.");
                break;
            },
            _ = creator_panic_handle => {
                error!(target: LOG_TARGET, "Creator task terminated early.");
                break;
            },
            _ = backup_saver_handle => {
                error!(target: LOG_TARGET, "Backup saving task terminated early.");
                break;
            },
            _ = alerter_handle => {
                error!(target: LOG_TARGET, "Alerter task terminated early.");
                break;
            },
            _ = service_handle => {
                error!(target: LOG_TARGET, "Consensus service terminated early.");
                break;
            },
            _ = terminator.get_exit().fuse() => {
                debug!(target: LOG_TARGET, "Exit channel was called.");
                break;
            },
        }
    }

    debug!(target: LOG_TARGET, "Run ending.");

    terminator.terminate_sync().await;

    handle_task_termination(network_handle, LOG_TARGET, "Network").await;
    handle_task_termination(creator_handle, LOG_TARGET, "Creator").await;
    handle_task_termination(backup_saver_handle, LOG_TARGET, "Backup saver").await;
    handle_task_termination(alerter_handle, LOG_TARGET, "Alerter").await;
    handle_task_termination(service_handle, LOG_TARGET, "Consensus service").await;

    info!(target: LOG_TARGET, "Session ended.");
}
