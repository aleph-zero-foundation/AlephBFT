use crate::{
    alerts::{Alert, ForkingNotification},
    collection::CollectionResponse,
    consensus::{
        handler::{Consensus, ConsensusResult},
        LOG_TARGET,
    },
    dag::DagUnit,
    dissemination::{Addressed, DisseminationMessage},
    network::{UnitMessage, UnitMessageTo},
    units::{SignedUnit, UncheckedSignedUnit, Unit},
    Data, Hasher, Index, MultiKeychain, Receiver, Sender, Terminator, UnitFinalizationHandler,
};
use futures::{FutureExt, StreamExt};
use futures_timer::Delay;
use log::{debug, error, info, trace, warn};
use std::time::Duration;

pub struct Service<UFH, MK>
where
    UFH: UnitFinalizationHandler,
    MK: MultiKeychain,
{
    handler: Consensus<UFH, MK>,
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

pub struct IO<H: Hasher, D: Data, MK: MultiKeychain> {
    pub backup_units_for_saver: Sender<DagUnit<H, D, MK>>,
    pub backup_units_from_saver: Receiver<DagUnit<H, D, MK>>,
    pub alerts_for_alerter: Sender<Alert<H, D, MK::Signature>>,
    pub notifications_from_alerter: Receiver<ForkingNotification<H, D, MK::Signature>>,
    pub unit_messages_for_network: Sender<UnitMessageTo<H, D, MK::Signature>>,
    pub unit_messages_from_network: Receiver<UnitMessage<H, D, MK::Signature>>,
    pub responses_for_collection: Sender<CollectionResponse<H, D, MK>>,
    pub parents_for_creator: Sender<DagUnit<H, D, MK>>,
    pub new_units_from_creator: Receiver<SignedUnit<H, D, MK>>,
}

impl<UFH, MK> Service<UFH, MK>
where
    UFH: UnitFinalizationHandler,
    MK: MultiKeychain,
{
    pub fn new(handler: Consensus<UFH, MK>, io: IO<UFH::Hasher, UFH::Data, MK>) -> Self {
        let IO {
            backup_units_for_saver,
            backup_units_from_saver,
            alerts_for_alerter,
            notifications_from_alerter,
            unit_messages_from_network,
            unit_messages_for_network,
            responses_for_collection,
            parents_for_creator,
            new_units_from_creator,
        } = io;

        Service {
            handler,
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

    fn handle_result(&mut self, result: ConsensusResult<UFH::Hasher, UFH::Data, MK>) {
        let ConsensusResult {
            units,
            alerts,
            messages,
        } = result;
        for unit in units {
            self.on_unit_reconstructed(unit);
        }
        for alert in alerts {
            if self.alerts_for_alerter.unbounded_send(alert).is_err() {
                self.crucial_channel_closed("Alerter");
            }
        }
        for message in messages {
            self.send_message_for_network(message.into())
        }
    }

    fn on_unit_received(
        &mut self,
        unit: UncheckedSignedUnit<UFH::Hasher, UFH::Data, MK::Signature>,
    ) {
        let result = self.handler.process_incoming_unit(unit);
        self.handle_result(result);
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
                if let Some(message) = self.handler.process_request(request, node_id) {
                    self.send_message_for_network(message.into());
                }
            }
            ParentsResponse(u_hash, parents) => {
                trace!(target: LOG_TARGET, "Response parents received for unit {:?}.", u_hash);
                let result = self.handler.process_parents(u_hash, parents);
                self.handle_result(result);
            }
            NewestUnitRequest(node_id, salt) => {
                trace!(target: LOG_TARGET, "Newest unit request received from {:?}.", node_id);
                let message = self.handler.process_newest_unit_request(salt, node_id);
                self.send_message_for_network(message.into())
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

    fn on_forking_notification(
        &mut self,
        notification: ForkingNotification<UFH::Hasher, UFH::Data, MK::Signature>,
    ) {
        let result = self.handler.process_forking_notification(notification);
        self.handle_result(result);
    }

    fn trigger_tasks(&mut self) {
        for message in self.handler.trigger_tasks() {
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
        if self
            .parents_for_creator
            .unbounded_send(unit.clone())
            .is_err()
        {
            self.crucial_channel_closed("Creator");
        }
        if let Some(message) = self.handler.on_unit_backup_saved(unit) {
            self.send_message_for_network(message);
        }
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

    fn status_report(&self) {
        info!(target: LOG_TARGET, "Consensus status report: {}.", self.handler.status());
    }

    pub async fn run(
        mut self,
        data_from_backup: Vec<UncheckedSignedUnit<UFH::Hasher, UFH::Data, MK::Signature>>,
        mut terminator: Terminator,
    ) {
        let status_ticker_delay = Duration::from_secs(10);
        let mut status_ticker = Delay::new(status_ticker_delay).fuse();
        let mut task_ticker = Delay::new(self.handler.next_tick()).fuse();

        for unit in data_from_backup {
            self.on_unit_received(unit);
        }

        debug!(target: LOG_TARGET, "Consensus started.");
        loop {
            futures::select! {
                signed_unit = self.new_units_from_creator.next() => match signed_unit {
                    Some(signed_unit) => {
                        self.on_unit_received(signed_unit.into())
                    },
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
                    task_ticker = Delay::new(self.handler.next_tick()).fuse();
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
