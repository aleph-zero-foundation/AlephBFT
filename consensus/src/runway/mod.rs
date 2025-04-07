use crate::{
    alerts::{Alert, ForkingNotification, NetworkMessage},
    collection::{Collection, NewestUnitResponse, IO as CollectionIO},
    config::{DelayConfig, DelaySchedule},
    creation,
    dag::{Dag, DagResult, DagStatus, DagUnit, Request as ReconstructionRequest},
    dissemination::{Addressed, DisseminationMessage, Responder, TaskManager, TaskManagerStatus},
    extension::Ordering,
    handle_task_termination,
    units::{SignedUnit, UncheckedSignedUnit, Unit, UnitStore, UnitStoreStatus, Validator},
    Config, Data, DataProvider, Hasher, Index, Keychain, MultiKeychain, NodeIndex, Receiver,
    Recipient, Round, Sender, SpawnHandle, Terminator, UncheckedSigned, UnitFinalizationHandler,
};
use futures::{
    channel::{mpsc, oneshot},
    future::pending,
    pin_mut, AsyncRead, AsyncWrite, Future, FutureExt, StreamExt,
};
use futures_timer::Delay;
use log::{debug, error, info, trace, warn};
use std::{
    cmp::max,
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
    time::Duration,
};

use crate::backup::{BackupLoader, BackupSaver};

type CollectionResponse<H, D, MK> = UncheckedSigned<
    NewestUnitResponse<H, D, <MK as Keychain>::Signature>,
    <MK as Keychain>::Signature,
>;

type AddressedDisseminationMessage<FH, MK> = Addressed<
    DisseminationMessage<
        <FH as UnitFinalizationHandler>::Hasher,
        <FH as UnitFinalizationHandler>::Data,
        <MK as Keychain>::Signature,
    >,
>;

struct Runway<FH, MK>
where
    FH: UnitFinalizationHandler,
    MK: MultiKeychain,
{
    own_id: NodeIndex,
    store: UnitStore<DagUnit<FH::Hasher, FH::Data, MK>>,
    dag: Dag<FH::Hasher, FH::Data, MK>,
    ordering: Ordering<MK, FH>,
    responder: Responder<FH::Hasher, FH::Data, MK>,
    task_manager: TaskManager<FH::Hasher>,
    alerts_for_alerter: Sender<Alert<FH::Hasher, FH::Data, MK::Signature>>,
    notifications_from_alerter: Receiver<ForkingNotification<FH::Hasher, FH::Data, MK::Signature>>,
    unit_messages_from_network: Receiver<DisseminationMessage<FH::Hasher, FH::Data, MK::Signature>>,
    unit_messages_for_network: Sender<AddressedDisseminationMessage<FH, MK>>,
    responses_for_collection: Sender<CollectionResponse<FH::Hasher, FH::Data, MK>>,
    parents_for_creator: Sender<DagUnit<FH::Hasher, FH::Data, MK>>,
    backup_units_for_saver: Sender<DagUnit<FH::Hasher, FH::Data, MK>>,
    backup_units_from_saver: Receiver<DagUnit<FH::Hasher, FH::Data, MK>>,
    new_units_from_creation: Receiver<SignedUnit<FH::Hasher, FH::Data, MK>>,
    exiting: bool,
}

struct RunwayStatus<H: Hasher> {
    task_manager_status: TaskManagerStatus<H>,
    dag_status: DagStatus,
    store_status: UnitStoreStatus,
}

impl<H: Hasher> RunwayStatus<H> {
    fn short_report(&self) -> String {
        let rounds_behind = max(self.dag_status.top_round(), self.store_status.top_round())
            - self.store_status.top_round();
        match rounds_behind {
            (0..=2) => "healthy".to_string(),
            (3..) => format!("behind by {rounds_behind} rounds"),
        }
    }
}

impl<H: Hasher> Display for RunwayStatus<H> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Runway status report: {}", self.short_report())?;
        write!(f, ";task manager: {}", self.task_manager_status)?;
        write!(f, ";reconstructed DAG: {}", self.store_status)?;
        write!(f, ";additional information: {}", self.dag_status)?;
        write!(f, ".")?;
        Ok(())
    }
}

struct RunwayConfig<UFH: UnitFinalizationHandler, MK: MultiKeychain> {
    delay_config: DelayConfig,
    finalization_handler: UFH,
    backup_units_for_saver: Sender<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    backup_units_from_saver: Receiver<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    alerts_for_alerter: Sender<Alert<UFH::Hasher, UFH::Data, MK::Signature>>,
    notifications_from_alerter:
        Receiver<ForkingNotification<UFH::Hasher, UFH::Data, MK::Signature>>,
    unit_messages_from_network:
        Receiver<DisseminationMessage<UFH::Hasher, UFH::Data, MK::Signature>>,
    unit_messages_for_network: Sender<AddressedDisseminationMessage<UFH, MK>>,
    responses_for_collection: Sender<CollectionResponse<UFH::Hasher, UFH::Data, MK>>,
    parents_for_creator: Sender<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    new_units_from_creation: Receiver<SignedUnit<UFH::Hasher, UFH::Data, MK>>,
}

type BackupUnits<UFH, MK> = Vec<
    UncheckedSignedUnit<
        <UFH as UnitFinalizationHandler>::Hasher,
        <UFH as UnitFinalizationHandler>::Data,
        <MK as Keychain>::Signature,
    >,
>;

impl<UFH, MK> Runway<UFH, MK>
where
    UFH: UnitFinalizationHandler,
    MK: MultiKeychain,
{
    fn new(config: RunwayConfig<UFH, MK>, keychain: MK, validator: Validator<MK>) -> Self {
        let n_members = keychain.node_count();
        let own_id = keychain.index();
        let RunwayConfig {
            delay_config,
            finalization_handler,
            backup_units_for_saver,
            backup_units_from_saver,
            alerts_for_alerter,
            notifications_from_alerter,
            unit_messages_from_network,
            unit_messages_for_network,
            responses_for_collection,
            parents_for_creator,
            new_units_from_creation,
        } = config;
        let store = UnitStore::new(n_members);
        let dag = Dag::new(validator);
        let ordering = Ordering::new(finalization_handler);
        let task_manager = TaskManager::new(own_id, n_members, delay_config);

        Runway {
            own_id,
            store,
            dag,
            ordering,
            task_manager,
            responder: Responder::new(keychain),
            alerts_for_alerter,
            notifications_from_alerter,
            unit_messages_from_network,
            unit_messages_for_network,
            parents_for_creator,
            backup_units_for_saver,
            backup_units_from_saver,
            responses_for_collection,
            new_units_from_creation,
            exiting: false,
        }
    }

    fn index(&self) -> NodeIndex {
        self.own_id
    }

    fn handle_dag_result(&mut self, result: DagResult<UFH::Hasher, UFH::Data, MK>) {
        let DagResult {
            units,
            requests,
            alerts,
        } = result;
        for unit in units {
            self.on_unit_reconstructed(unit);
        }
        for request in requests {
            self.on_reconstruction_request(request);
        }
        for alert in alerts {
            if self.alerts_for_alerter.unbounded_send(alert).is_err() {
                warn!(target: "AlephBFT-runway", "{:?} Channel to alerter should be open", self.index());
                self.exiting = true;
            }
        }
    }

    fn on_unit_received(
        &mut self,
        unit: UncheckedSignedUnit<UFH::Hasher, UFH::Data, MK::Signature>,
    ) {
        let result = self.dag.add_unit(unit, &self.store);
        self.handle_dag_result(result);
    }

    fn on_unit_message(
        &mut self,
        message: DisseminationMessage<UFH::Hasher, UFH::Data, MK::Signature>,
    ) {
        use DisseminationMessage::*;
        match message {
            Unit(u) => {
                trace!(target: "AlephBFT-runway", "{:?} New unit received {:?}.", self.index(), &u);
                self.on_unit_received(u)
            }

            Request(node_id, request) => {
                match self.responder.handle_request(request, &self.store) {
                    Ok(response) => self.send_message_for_network(
                        Addressed::addressed_to(response, node_id).into(),
                    ),
                    Err(err) => {
                        trace!(target: "AlephBFT-runway", "Not answering request from node {:?}: {}.", node_id, err)
                    }
                }
            }

            ParentsResponse(u_hash, parents) => {
                trace!(target: "AlephBFT-runway", "{:?} Response parents received {:?}.", self.index(), u_hash);
                self.on_parents_response(u_hash, parents)
            }
            NewestUnitRequest(node_id, salt) => {
                let response =
                    self.responder
                        .handle_newest_unit_request(node_id, salt, &self.store);
                self.send_message_for_network(Addressed::addressed_to(response, node_id).into())
            }
            NewestUnitResponse(response) => {
                trace!(target: "AlephBFT-runway", "{:?} Response newest unit received from {:?}.", self.index(), response.index());
                let res = self.responses_for_collection.unbounded_send(response);
                if res.is_err() {
                    debug!(target: "AlephBFT-runway", "{:?} Could not send response to collection ({:?}).", self.index(), res)
                }
            }
        }
    }

    fn on_parents_response(
        &mut self,
        u_hash: <UFH::Hasher as Hasher>::Hash,
        parents: Vec<UncheckedSignedUnit<UFH::Hasher, UFH::Data, MK::Signature>>,
    ) {
        if self.store.unit(&u_hash).is_some() {
            trace!(target: "AlephBFT-runway", "{:?} We got parents response but already imported the unit.", self.index());
            return;
        }
        let result = self.dag.add_parents(u_hash, parents, &self.store);
        self.handle_dag_result(result);
    }

    fn on_forking_notification(
        &mut self,
        notification: ForkingNotification<UFH::Hasher, UFH::Data, MK::Signature>,
    ) {
        let result = self
            .dag
            .process_forking_notification(notification, &self.store);
        self.handle_dag_result(result);
    }

    fn on_reconstruction_request(&mut self, request: ReconstructionRequest<UFH::Hasher>) {
        self.task_manager.add_request(request);
        for message in self
            .task_manager
            .trigger_tasks(&self.store, self.dag.processing_units())
        {
            self.send_message_for_network(message);
        }
    }

    fn trigger_tasks(&mut self) {
        for message in self
            .task_manager
            .trigger_tasks(&self.store, self.dag.processing_units())
        {
            self.send_message_for_network(message);
        }
    }

    fn on_unit_reconstructed(&mut self, unit: DagUnit<UFH::Hasher, UFH::Data, MK>) {
        let unit_hash = unit.hash();
        trace!(target: "AlephBFT-runway", "Unit {:?} {} reconstructed.", unit_hash, unit.coord());
        if self.backup_units_for_saver.unbounded_send(unit).is_err() {
            error!(target: "AlephBFT-runway", "{:?} A unit couldn't be sent to backup: {:?}.", self.index(), unit_hash);
        }
    }

    fn on_unit_backup_saved(&mut self, unit: DagUnit<UFH::Hasher, UFH::Data, MK>) {
        let unit_hash = unit.hash();
        self.store.insert(unit.clone());
        self.dag.finished_processing(&unit_hash);
        if let Some(message) = self.task_manager.add_unit(&unit) {
            self.send_message_for_network(message);
        }
        if self
            .parents_for_creator
            .unbounded_send(unit.clone())
            .is_err()
        {
            warn!(target: "AlephBFT-runway", "Creator channel should be open.");
            self.exiting = true;
        }
        self.ordering.add_unit(unit.clone());
    }

    fn send_message_for_network(&mut self, notification: AddressedDisseminationMessage<UFH, MK>) {
        if self
            .unit_messages_for_network
            .unbounded_send(notification)
            .is_err()
        {
            warn!(target: "AlephBFT-runway", "{:?} unit_messages_for_network channel should be open", self.index());
            self.exiting = true;
        }
    }

    fn status(&self) -> RunwayStatus<UFH::Hasher> {
        RunwayStatus {
            task_manager_status: self.task_manager.status(),
            dag_status: self.dag.status(),
            store_status: self.store.status(),
        }
    }

    fn status_report(&self) {
        info!(target: "AlephBFT-runway", "{}", self.status());
    }

    async fn run(
        mut self,
        data_from_backup: oneshot::Receiver<BackupUnits<UFH, MK>>,
        mut terminator: Terminator,
    ) {
        let index = self.index();
        let data_from_backup = data_from_backup.fuse();
        pin_mut!(data_from_backup);

        let status_ticker_delay = Duration::from_secs(10);
        let mut status_ticker = Delay::new(status_ticker_delay).fuse();
        let mut task_ticker = Delay::new(self.task_manager.next_tick()).fuse();

        match data_from_backup.await {
            Ok(units) => {
                for unit in units {
                    self.on_unit_received(unit);
                }
            }
            Err(e) => {
                error!(target: "AlephBFT-runway", "{:?} Units message from backup channel closed: {:?}", index, e);
                return;
            }
        }

        debug!(target: "AlephBFT-runway", "{:?} Runway started.", index);
        loop {
            futures::select! {
                signed_unit = self.new_units_from_creation.next() => match signed_unit {
                    Some(signed_unit) => self.on_unit_received(signed_unit.into()),
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Creation stream closed.", index);
                        break;
                    }
                },

                notification = self.notifications_from_alerter.next() => match notification {
                    Some(notification) => {
                        trace!(target: "AlephBFT-runway", "Received alerter notification: {:?}.", notification);
                        self.on_forking_notification(notification);
                    },
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Alert notification stream closed.", index);
                        break;
                    }
                },

                event = self.unit_messages_from_network.next() => match event {
                    Some(event) => self.on_unit_message(event),
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Unit message stream closed.", index);
                        break;
                    }
                },

                message = self.backup_units_from_saver.next() => match message {
                    Some(unit) => self.on_unit_backup_saved(unit),
                    None => {
                        error!(target: "AlephBFT-runway", "{:?} Saved units receiver closed.", index);
                    }
                },

                _ = &mut task_ticker => {
                    self.trigger_tasks();
                    task_ticker = Delay::new(self.task_manager.next_tick()).fuse();
                },

                _ = &mut status_ticker => {
                    self.status_report();
                    status_ticker = Delay::new(status_ticker_delay).fuse();
                },

                _ = terminator.get_exit().fuse() => {
                    debug!(target: "AlephBFT-runway", "{:?} received exit signal", index);
                    self.exiting = true;
                }
            }

            if self.exiting {
                debug!(target: "AlephBFT-runway", "{:?} Runway decided to exit.", index);
                terminator.terminate_sync().await;
                break;
            }
        }

        debug!(target: "AlephBFT-runway", "{:?} Run ended.", index);
    }
}

pub(crate) struct NetworkIO<H: Hasher, D: Data, MK: MultiKeychain> {
    pub(crate) alert_messages_for_network: Sender<(NetworkMessage<H, D, MK>, Recipient)>,
    pub(crate) alert_messages_from_network: Receiver<NetworkMessage<H, D, MK>>,
    pub(crate) unit_messages_for_network:
        Sender<Addressed<DisseminationMessage<H, D, MK::Signature>>>,
    pub(crate) unit_messages_from_network: Receiver<DisseminationMessage<H, D, MK::Signature>>,
}

#[cfg(feature = "initial_unit_collection")]
fn initial_unit_collection<'a, H: Hasher, D: Data, MK: MultiKeychain>(
    keychain: &'a MK,
    validator: &'a Validator<MK>,
    messages_for_network: Sender<Addressed<DisseminationMessage<H, D, MK::Signature>>>,
    unit_collection_sender: oneshot::Sender<Round>,
    responses_from_runway: Receiver<CollectionResponse<H, D, MK>>,
    request_delay: DelaySchedule,
) -> Result<impl Future<Output = ()> + 'a, ()> {
    let collection = Collection::new(keychain, validator);

    let collection = CollectionIO::new(
        unit_collection_sender,
        responses_from_runway,
        messages_for_network,
        collection,
        request_delay,
    );
    Ok(collection.run())
}

#[cfg(not(feature = "initial_unit_collection"))]
fn trivial_start(
    starting_round_sender: oneshot::Sender<Round>,
) -> Result<impl Future<Output = ()>, ()> {
    if let Err(e) = starting_round_sender.send(0) {
        error!(target: "AlephBFT-runway", "Unable to send the starting round: {}", e);
        return Err(());
    }
    Ok(async {})
}

pub struct RunwayIO<
    MK: MultiKeychain,
    W: AsyncWrite + Send + Sync + 'static,
    R: AsyncRead + Send + Sync + 'static,
    DP: DataProvider,
    UFH: UnitFinalizationHandler,
> {
    pub data_provider: DP,
    pub finalization_handler: UFH,
    pub backup_write: W,
    pub backup_read: R,
    _phantom: PhantomData<MK::Signature>,
}

impl<
        MK: MultiKeychain,
        W: AsyncWrite + Send + Sync + 'static,
        R: AsyncRead + Send + Sync + 'static,
        DP: DataProvider,
        UFH: UnitFinalizationHandler,
    > RunwayIO<MK, W, R, DP, UFH>
{
    pub fn new(
        data_provider: DP,
        finalization_handler: UFH,
        backup_write: W,
        backup_read: R,
    ) -> Self {
        RunwayIO {
            data_provider,
            finalization_handler,
            backup_write,
            backup_read,
            _phantom: PhantomData,
        }
    }
}

pub(crate) async fn run<US, UL, MK, DP, UFH, SH>(
    config: Config,
    runway_io: RunwayIO<MK, US, UL, DP, UFH>,
    keychain: MK,
    spawn_handle: SH,
    network_io: NetworkIO<UFH::Hasher, DP::Output, MK>,
    mut terminator: Terminator,
) where
    US: AsyncWrite + Send + Sync + 'static,
    UL: AsyncRead + Send + Sync + 'static,
    DP: DataProvider,
    UFH: UnitFinalizationHandler<Data = DP::Output>,
    MK: MultiKeychain,
    SH: SpawnHandle,
{
    let RunwayIO {
        data_provider,
        finalization_handler,
        backup_write,
        backup_read,
        _phantom: _,
    } = runway_io;

    let (new_units_for_runway, new_units_from_creation) = mpsc::unbounded();

    let (parents_for_creator, parents_from_runway) = mpsc::unbounded();
    let creation_terminator = terminator.add_offspring_connection("creator");
    let creation_config = config.clone();
    let (starting_round_sender, starting_round) = oneshot::channel();

    let creation_keychain = keychain.clone();
    let creation_handle = spawn_handle
        .spawn_essential("runway/creation", async move {
            creation::run(
                creation_config,
                creation::IO {
                    outgoing_units: new_units_for_runway,
                    incoming_parents: parents_from_runway,
                    data_provider,
                },
                creation_keychain,
                starting_round,
                creation_terminator,
            )
            .await
        })
        .shared();
    let creator_handle_for_panic = creation_handle.clone();
    let creator_panic_handle = async move {
        if creator_handle_for_panic.await.is_err() {
            return;
        }
        pending().await
    }
    .fuse();
    pin_mut!(creator_panic_handle);
    let creation_handle = creation_handle.fuse();

    let (backup_units_for_saver, backup_units_from_runway) = mpsc::unbounded();
    let (backup_units_for_runway, backup_units_from_saver) = mpsc::unbounded();

    let backup_saver_terminator = terminator.add_offspring_connection("AlephBFT-backup-saver");
    let backup_saver_handle = spawn_handle.spawn_essential("runway/backup_saver", {
        let mut backup_saver = BackupSaver::new(
            backup_units_from_runway,
            backup_units_for_runway,
            backup_write,
        );
        async move {
            backup_saver.run(backup_saver_terminator).await;
        }
    });
    let mut backup_saver_handle = backup_saver_handle.fuse();

    let (alert_notifications_for_units, notifications_from_alerter) = mpsc::unbounded();
    let (alerts_for_alerter, alerts_from_units) = mpsc::unbounded();

    let alerter_terminator = terminator.add_offspring_connection("AlephBFT-alerter");
    let alerter_keychain = keychain.clone();
    let alert_messages_for_network = network_io.alert_messages_for_network;
    let alert_messages_from_network = network_io.alert_messages_from_network;
    let alerter_handler =
        crate::alerts::Handler::new(alerter_keychain.clone(), config.session_id());

    let mut alerter_service = crate::alerts::Service::new(
        alerter_keychain,
        crate::alerts::IO {
            messages_for_network: alert_messages_for_network,
            messages_from_network: alert_messages_from_network,
            notifications_for_units: alert_notifications_for_units,
            alerts_from_units,
        },
        alerter_handler,
    );

    let mut alerter_handle = spawn_handle
        .spawn_essential("runway/alerter", async move {
            alerter_service.run(alerter_terminator).await;
        })
        .fuse();

    let index = keychain.index();
    let validator = Validator::new(config.session_id(), keychain.clone(), config.max_round());
    let (responses_for_collection, responses_from_runway) = mpsc::unbounded();
    let (unit_collections_sender, unit_collection_result) = oneshot::channel();
    let (loaded_data_tx, loaded_data_rx) = oneshot::channel();
    let session_id = config.session_id();

    let backup_loading_handle = spawn_handle
        .spawn_essential("runway/loading", {
            let mut backup_loader = BackupLoader::new(backup_read, index, session_id);
            async move {
                backup_loader
                    .run(
                        loaded_data_tx,
                        starting_round_sender,
                        unit_collection_result,
                    )
                    .await
            }
        })
        .fuse();
    pin_mut!(backup_loading_handle);

    #[cfg(feature = "initial_unit_collection")]
    let starting_round_handle = match initial_unit_collection(
        &keychain,
        &validator,
        network_io.unit_messages_for_network.clone(),
        unit_collections_sender,
        responses_from_runway,
        config.delay_config().newest_request_delay.clone(),
    ) {
        Ok(handle) => handle.fuse(),
        Err(_) => return,
    };
    #[cfg(not(feature = "initial_unit_collection"))]
    let starting_round_handle = match trivial_start(unit_collections_sender) {
        Ok(handle) => handle.fuse(),
        Err(_) => return,
    };
    pin_mut!(starting_round_handle);

    let runway_handle = spawn_handle
        .spawn_essential("runway", {
            let runway_config = RunwayConfig {
                delay_config: config.delay_config().clone(),
                finalization_handler,
                backup_units_for_saver,
                backup_units_from_saver,
                alerts_for_alerter,
                notifications_from_alerter,
                unit_messages_from_network: network_io.unit_messages_from_network,
                unit_messages_for_network: network_io.unit_messages_for_network,
                parents_for_creator,
                responses_for_collection,
                new_units_from_creation,
            };
            let runway_terminator = terminator.add_offspring_connection("AlephBFT-runway");
            let validator = validator.clone();
            let keychain = keychain.clone();
            let runway = Runway::new(runway_config, keychain, validator);

            async move { runway.run(loaded_data_rx, runway_terminator).await }
        })
        .fuse();
    pin_mut!(runway_handle);

    loop {
        futures::select! {
            _ = runway_handle => {
                debug!(target: "AlephBFT-runway", "{:?} Runway task terminated early.", index);
                break;
            },
            _ = alerter_handle => {
                debug!(target: "AlephBFT-runway", "{:?} Alerter task terminated early.", index);
                break;
            },
            _ = creator_panic_handle => {
                debug!(target: "AlephBFT-runway", "{:?} creator task terminated early with its task being dropped.", index);
                break;
            },
            _ = backup_saver_handle => {
                debug!(target: "AlephBFT-runway", "{:?} Backup saving task terminated early.", index);
                break;
            },
            _ = starting_round_handle => {
                debug!(target: "AlephBFT-runway", "{:?} Starting round task terminated.", index);
            },
            _ = backup_loading_handle => {
                debug!(target: "AlephBFT-runway", "{:?} Backup loading task terminated.", index);
            },
            _ = terminator.get_exit().fuse() => {
                break;
            }
        }
    }

    debug!(target: "AlephBFT-runway", "{:?} Ending run.", index);
    terminator.terminate_sync().await;

    handle_task_termination(creation_handle, "AlephBFT-runway", "Creator", index).await;
    handle_task_termination(alerter_handle, "AlephBFT-runway", "Alerter", index).await;
    handle_task_termination(runway_handle, "AlephBFT-runway", "Runway", index).await;
    handle_task_termination(backup_saver_handle, "AlephBFT-runway", "BackupSaver", index).await;

    debug!(target: "AlephBFT-runway", "{:?} Runway ended.", index);
}
