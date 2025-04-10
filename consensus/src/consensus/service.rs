use crate::{
    alerts::{Alert, ForkingNotification},
    collection::CollectionResponse,
    config::DelayConfig,
    consensus::LOG_TARGET,
    dag::{Dag, DagResult, DagStatus, DagUnit, Request as ReconstructionRequest},
    dissemination::{Addressed, DisseminationMessage, Responder, TaskManager, TaskManagerStatus},
    extension::Ordering,
    network::{UnitMessage, UnitMessageTo},
    units::{SignedUnit, UncheckedSignedUnit, Unit, UnitStore, UnitStoreStatus, Validator},
    Hasher, Index, MultiKeychain, Receiver, Sender, Terminator, UnitFinalizationHandler,
};
use futures::{FutureExt, StreamExt};
use futures_timer::Delay;
use log::{debug, error, info, trace, warn};
use std::{
    cmp::max,
    fmt::{Display, Formatter, Result as FmtResult},
    time::Duration,
};

pub struct Service<UFH, MK>
where
    UFH: UnitFinalizationHandler,
    MK: MultiKeychain,
{
    store: UnitStore<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    dag: Dag<UFH::Hasher, UFH::Data, MK>,
    ordering: Ordering<MK, UFH>,
    responder: Responder<UFH::Hasher, UFH::Data, MK>,
    task_manager: TaskManager<UFH::Hasher>,
    alerts_for_alerter: Sender<Alert<UFH::Hasher, UFH::Data, MK::Signature>>,
    notifications_from_alerter:
        Receiver<ForkingNotification<UFH::Hasher, UFH::Data, MK::Signature>>,
    unit_messages_for_network: Sender<UnitMessageTo<UFH::Hasher, UFH::Data, MK::Signature>>,
    unit_messages_from_network: Receiver<UnitMessage<UFH::Hasher, UFH::Data, MK::Signature>>,
    responses_for_collection: Sender<CollectionResponse<UFH::Hasher, UFH::Data, MK>>,
    parents_for_creator: Sender<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    backup_units_for_saver: Sender<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    backup_units_from_saver: Receiver<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    new_units_from_creator: Receiver<SignedUnit<UFH::Hasher, UFH::Data, MK>>,
    exiting: bool,
}

struct Status<H: Hasher> {
    task_manager_status: TaskManagerStatus<H>,
    dag_status: DagStatus,
    store_status: UnitStoreStatus,
}

impl<H: Hasher> Status<H> {
    fn short_report(&self) -> String {
        let rounds_behind = max(self.dag_status.top_round(), self.store_status.top_round())
            - self.store_status.top_round();
        match rounds_behind {
            (0..=2) => "healthy".to_string(),
            (3..) => format!("behind by {rounds_behind} rounds"),
        }
    }
}

impl<H: Hasher> Display for Status<H> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", self.short_report())?;
        write!(f, ";task manager: {}", self.task_manager_status)?;
        write!(f, ";reconstructed DAG: {}", self.store_status)?;
        write!(f, ";additional information: {}", self.dag_status)?;
        write!(f, ".")?;
        Ok(())
    }
}

pub struct Config<UFH: UnitFinalizationHandler, MK: MultiKeychain> {
    pub delay_config: DelayConfig,
    pub finalization_handler: UFH,
    pub backup_units_for_saver: Sender<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    pub backup_units_from_saver: Receiver<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    pub alerts_for_alerter: Sender<Alert<UFH::Hasher, UFH::Data, MK::Signature>>,
    pub notifications_from_alerter:
        Receiver<ForkingNotification<UFH::Hasher, UFH::Data, MK::Signature>>,
    pub unit_messages_for_network: Sender<UnitMessageTo<UFH::Hasher, UFH::Data, MK::Signature>>,
    pub unit_messages_from_network: Receiver<UnitMessage<UFH::Hasher, UFH::Data, MK::Signature>>,
    pub responses_for_collection: Sender<CollectionResponse<UFH::Hasher, UFH::Data, MK>>,
    pub parents_for_creator: Sender<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    pub new_units_from_creator: Receiver<SignedUnit<UFH::Hasher, UFH::Data, MK>>,
}

impl<UFH, MK> Service<UFH, MK>
where
    UFH: UnitFinalizationHandler,
    MK: MultiKeychain,
{
    pub fn new(config: Config<UFH, MK>, keychain: MK, validator: Validator<MK>) -> Self {
        let n_members = keychain.node_count();
        let own_id = keychain.index();
        let Config {
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
            new_units_from_creator,
        } = config;
        let store = UnitStore::new(n_members);
        let dag = Dag::new(validator);
        let ordering = Ordering::new(finalization_handler);
        let task_manager = TaskManager::new(own_id, n_members, delay_config);

        Service {
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
            new_units_from_creator,
            exiting: false,
        }
    }

    fn crucial_channel_closed(&mut self, channel_name: &str) {
        warn!(target: LOG_TARGET, "{} channel unexpectedly closed, exiting.", channel_name);
        self.exiting = true;
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
                self.crucial_channel_closed("Alerter");
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
                trace!(target: LOG_TARGET, "New unit received {:?}.", &u);
                self.on_unit_received(u)
            }

            Request(node_id, request) => {
                trace!(target: LOG_TARGET, "Request {:?} received from {:?}.", request, node_id);
                match self.responder.handle_request(request, &self.store) {
                    Ok(response) => self.send_message_for_network(
                        Addressed::addressed_to(response, node_id).into(),
                    ),
                    Err(err) => {
                        debug!(target: LOG_TARGET, "Not answering request from node {:?}: {}.", node_id, err)
                    }
                }
            }

            ParentsResponse(u_hash, parents) => {
                trace!(target: LOG_TARGET, "Response parents received for unit {:?}.", u_hash);
                self.on_parents_response(u_hash, parents)
            }
            NewestUnitRequest(node_id, salt) => {
                trace!(target: LOG_TARGET, "Newest unit request received from {:?}.", node_id);
                let response =
                    self.responder
                        .handle_newest_unit_request(node_id, salt, &self.store);
                self.send_message_for_network(Addressed::addressed_to(response, node_id).into())
            }
            NewestUnitResponse(response) => {
                trace!(target: LOG_TARGET, "Response newest unit received from {:?}.", response.index());
                if self
                    .responses_for_collection
                    .unbounded_send(response)
                    .is_err()
                {
                    debug!(target: LOG_TARGET, "Initial unit collection channel closed, dropping response.")
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
            trace!(target: LOG_TARGET, "We got parents response but already imported the unit.");
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
        self.trigger_tasks();
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
        trace!(target: LOG_TARGET, "Unit {:?} {} reconstructed.", unit.hash(), unit.coord());
        if self.backup_units_for_saver.unbounded_send(unit).is_err() {
            self.crucial_channel_closed("Backup");
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
            self.crucial_channel_closed("Creator");
        }
        self.ordering.add_unit(unit.clone());
    }

    fn send_message_for_network(
        &mut self,
        notification: Addressed<DisseminationMessage<UFH::Hasher, UFH::Data, MK::Signature>>,
    ) {
        for recipient in notification.recipients() {
            if self
                .unit_messages_for_network
                .unbounded_send((notification.message().clone().into(), recipient.clone()))
                .is_err()
            {
                self.crucial_channel_closed("Network");
            }
        }
    }

    fn status(&self) -> Status<UFH::Hasher> {
        Status {
            task_manager_status: self.task_manager.status(),
            dag_status: self.dag.status(),
            store_status: self.store.status(),
        }
    }

    fn status_report(&self) {
        info!(target: LOG_TARGET, "Consensus status report: {}", self.status());
    }

    pub async fn run(
        mut self,
        data_from_backup: Vec<UncheckedSignedUnit<UFH::Hasher, UFH::Data, MK::Signature>>,
        mut terminator: Terminator,
    ) {
        let status_ticker_delay = Duration::from_secs(10);
        let mut status_ticker = Delay::new(status_ticker_delay).fuse();
        let mut task_ticker = Delay::new(self.task_manager.next_tick()).fuse();

        for unit in data_from_backup {
            self.on_unit_received(unit);
        }

        debug!(target: LOG_TARGET, "Consensus started.");
        loop {
            futures::select! {
                signed_unit = self.new_units_from_creator.next() => match signed_unit {
                    Some(signed_unit) => self.on_unit_received(signed_unit.into()),
                    None => {
                        error!(target: LOG_TARGET, "Creation stream closed.");
                        break;
                    }
                },

                notification = self.notifications_from_alerter.next() => match notification {
                    Some(notification) => {
                        trace!(target: LOG_TARGET, "Received alerter notification: {:?}.", notification);
                        self.on_forking_notification(notification);
                    },
                    None => {
                        error!(target: LOG_TARGET, "Alert notification stream closed.");
                        break;
                    }
                },

                event = self.unit_messages_from_network.next() => match event {
                    Some(event) => self.on_unit_message(event.into()),
                    None => {
                        error!(target: LOG_TARGET, "Unit message stream closed.");
                        break;
                    }
                },

                message = self.backup_units_from_saver.next() => match message {
                    Some(unit) => self.on_unit_backup_saved(unit),
                    None => {
                        error!(target: LOG_TARGET, "Saved units receiver closed.");
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
                    debug!(target: LOG_TARGET, "Consensus received exit signal.");
                    self.exiting = true;
                }
            }

            if self.exiting {
                debug!(target: LOG_TARGET, "Consensus decided to exit.");
                terminator.terminate_sync().await;
                break;
            }
        }

        debug!(target: LOG_TARGET, "Consensus run ended.");
    }
}
