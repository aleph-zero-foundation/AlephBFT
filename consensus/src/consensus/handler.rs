use crate::{
    alerts::{Alert, ForkingNotification},
    collection::Salt,
    consensus::LOG_TARGET,
    dag::{Dag, DagResult, DagStatus, DagUnit, Request as ReconstructionRequest},
    dissemination::{Addressed, DisseminationMessage, Responder, TaskManager, TaskManagerStatus},
    extension::Ordering,
    units::{UncheckedSignedUnit, Unit, UnitStore, UnitStoreStatus, Validator},
    Data, DelayConfig, Hasher, MultiKeychain, NodeIndex, UnitFinalizationHandler,
};
use log::{debug, trace};
use std::{
    cmp::max,
    fmt::{Display, Formatter, Result as FmtResult},
    time::Duration,
};

/// The main logic of the consensus, minus all the asynchronous components.
pub struct Consensus<UFH, MK>
where
    UFH: UnitFinalizationHandler,
    MK: MultiKeychain,
{
    store: UnitStore<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    dag: Dag<UFH::Hasher, UFH::Data, MK>,
    responder: Responder<UFH::Hasher, UFH::Data, MK>,
    ordering: Ordering<MK, UFH>,
    task_manager: TaskManager<UFH::Hasher>,
}

/// The status of the consensus, for logging purposes.
pub struct Status<H: Hasher> {
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
        write!(f, ";reconstructed DAG: {}", self.store_status)?;
        write!(f, ";additional information: {}", self.dag_status)?;
        write!(f, ";task manager: {}", self.task_manager_status)?;
        Ok(())
    }
}

type AddressedDisseminationMessage<H, D, MK> = Addressed<DisseminationMessage<H, D, MK>>;

/// The result of some operation within the consensus, requiring either other components should get
/// informed about it, or messages should be sent to the network.
pub struct ConsensusResult<H: Hasher, D: Data, MK: MultiKeychain> {
    /// Units that should be sent for backup saving.
    pub units: Vec<DagUnit<H, D, MK>>,
    /// Alerts that should be sent to the alerting component.
    pub alerts: Vec<Alert<H, D, MK::Signature>>,
    /// Messages that should be sent to other committee members.
    pub messages: Vec<AddressedDisseminationMessage<H, D, MK::Signature>>,
}

impl<H: Hasher, D: Data, MK: MultiKeychain> ConsensusResult<H, D, MK> {
    fn noop() -> Self {
        ConsensusResult {
            units: Vec::new(),
            alerts: Vec::new(),
            messages: Vec::new(),
        }
    }
}

impl<UFH, MK> Consensus<UFH, MK>
where
    UFH: UnitFinalizationHandler,
    MK: MultiKeychain,
{
    /// Create a new Consensus.
    pub fn new(
        keychain: MK,
        validator: Validator<MK>,
        finalization_handler: UFH,
        delay_config: DelayConfig,
    ) -> Self {
        let n_members = keychain.node_count();
        let index = keychain.index();
        Consensus {
            store: UnitStore::new(n_members),
            dag: Dag::new(validator),
            responder: Responder::new(keychain),
            ordering: Ordering::new(finalization_handler),
            task_manager: TaskManager::new(index, n_members, delay_config),
        }
    }

    fn handle_dag_result(
        &mut self,
        result: DagResult<UFH::Hasher, UFH::Data, MK>,
    ) -> ConsensusResult<UFH::Hasher, UFH::Data, MK> {
        let DagResult {
            units,
            alerts,
            requests,
        } = result;
        for request in requests {
            self.task_manager.add_request(request);
        }
        let messages = self.trigger_tasks();
        ConsensusResult {
            units,
            alerts,
            messages,
        }
    }

    /// Process a unit received (usually) from the network.
    pub fn process_incoming_unit(
        &mut self,
        unit: UncheckedSignedUnit<UFH::Hasher, UFH::Data, MK::Signature>,
    ) -> ConsensusResult<UFH::Hasher, UFH::Data, MK> {
        let result = self.dag.add_unit(unit, &self.store);
        self.handle_dag_result(result)
    }

    /// Process a request received from the network.
    pub fn process_request(
        &mut self,
        request: ReconstructionRequest<UFH::Hasher>,
        node_id: NodeIndex,
    ) -> Option<AddressedDisseminationMessage<UFH::Hasher, UFH::Data, MK::Signature>> {
        match self.responder.handle_request(request, &self.store) {
            Ok(response) => Some(Addressed::addressed_to(response.into(), node_id)),
            Err(err) => {
                debug!(target: LOG_TARGET, "Not answering request from node {:?}: {}.", node_id, err);
                None
            }
        }
    }

    /// Process a parents response.
    pub fn process_parents(
        &mut self,
        u_hash: <UFH::Hasher as Hasher>::Hash,
        parents: Vec<UncheckedSignedUnit<UFH::Hasher, UFH::Data, MK::Signature>>,
    ) -> ConsensusResult<UFH::Hasher, UFH::Data, MK> {
        if self.store.unit(&u_hash).is_some() {
            trace!(target: LOG_TARGET, "We got parents response but already imported the unit.");
            return ConsensusResult::noop();
        }
        let result = self.dag.add_parents(u_hash, parents, &self.store);
        self.handle_dag_result(result)
    }

    /// Process a newest unit request.
    pub fn process_newest_unit_request(
        &mut self,
        salt: Salt,
        node_id: NodeIndex,
    ) -> AddressedDisseminationMessage<UFH::Hasher, UFH::Data, MK::Signature> {
        Addressed::addressed_to(
            self.responder
                .handle_newest_unit_request(node_id, salt, &self.store)
                .into(),
            node_id,
        )
    }

    /// Process a forking notification.
    pub fn process_forking_notification(
        &mut self,
        notification: ForkingNotification<UFH::Hasher, UFH::Data, MK::Signature>,
    ) -> ConsensusResult<UFH::Hasher, UFH::Data, MK> {
        let result = self
            .dag
            .process_forking_notification(notification, &self.store);
        self.handle_dag_result(result)
    }

    /// What to do once a unit has been securely backed up on disk.
    pub fn on_unit_backup_saved(
        &mut self,
        unit: DagUnit<UFH::Hasher, UFH::Data, MK>,
    ) -> Option<AddressedDisseminationMessage<UFH::Hasher, UFH::Data, MK::Signature>> {
        let unit_hash = unit.hash();
        self.store.insert(unit.clone());
        self.dag.finished_processing(&unit_hash);
        self.ordering.add_unit(unit.clone());
        self.task_manager.add_unit(&unit)
    }

    /// When should `trigger_tasks` be called next.
    pub fn next_tick(&self) -> Duration {
        self.task_manager.next_tick()
    }

    /// Trigger all the ready tasks and get all the messages that should be sent now.
    pub fn trigger_tasks(
        &mut self,
    ) -> Vec<AddressedDisseminationMessage<UFH::Hasher, UFH::Data, MK::Signature>> {
        self.task_manager
            .trigger_tasks(&self.store, self.dag.processing_units())
    }

    /// The status of the consensus handler, for logging purposes.
    pub fn status(&self) -> Status<UFH::Hasher> {
        Status {
            dag_status: self.dag.status(),
            store_status: self.store.status(),
            task_manager_status: self.task_manager.status(),
        }
    }
}
