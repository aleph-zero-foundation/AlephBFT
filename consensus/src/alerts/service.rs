use crate::{
    alerts::{
        handler::Handler, Alert, AlertMessage, AlerterResponse, ForkingNotification, NetworkMessage,
    },
    Data, Hasher, MultiKeychain, Multisigned, NodeCount, NodeIndex, Receiver, Recipient, Sender,
    Terminator,
};
use aleph_bft_rmc::{DoublingDelayScheduler, Message as RmcMessage, ReliableMulticast};
use futures::{channel::mpsc, FutureExt, StreamExt};
use log::{debug, error, warn};
use std::time;

const LOG_TARGET: &str = "AlephBFT-alerter";

pub struct Service<H: Hasher, D: Data, MK: MultiKeychain> {
    messages_for_network: Sender<(NetworkMessage<H, D, MK>, Recipient)>,
    messages_from_network: Receiver<NetworkMessage<H, D, MK>>,
    notifications_for_units: Sender<ForkingNotification<H, D, MK::Signature>>,
    alerts_from_units: Receiver<Alert<H, D, MK::Signature>>,
    rmc: ReliableMulticast<H::Hash, MK>,
    messages_for_rmc: Sender<RmcMessage<H::Hash, MK::Signature, MK::PartialMultisignature>>,
    messages_from_rmc: Receiver<RmcMessage<H::Hash, MK::Signature, MK::PartialMultisignature>>,
    node_index: NodeIndex,
    exiting: bool,
}

impl<H: Hasher, D: Data, MK: MultiKeychain> Service<H, D, MK> {
    pub fn new(
        keychain: MK,
        messages_for_network: Sender<(NetworkMessage<H, D, MK>, Recipient)>,
        messages_from_network: Receiver<NetworkMessage<H, D, MK>>,
        notifications_for_units: Sender<ForkingNotification<H, D, MK::Signature>>,
        alerts_from_units: Receiver<Alert<H, D, MK::Signature>>,
        n_members: NodeCount,
    ) -> Service<H, D, MK> {
        let (messages_for_rmc, messages_from_us) = mpsc::unbounded();
        let (messages_for_us, messages_from_rmc) = mpsc::unbounded();

        let rmc = ReliableMulticast::new(
            messages_from_us,
            messages_for_us,
            keychain.clone(),
            n_members,
            DoublingDelayScheduler::new(time::Duration::from_millis(500)),
        );

        Service {
            messages_for_network,
            messages_from_network,
            notifications_for_units,
            alerts_from_units,
            rmc,
            messages_for_rmc,
            messages_from_rmc,
            node_index: keychain.index(),
            exiting: false,
        }
    }

    fn rmc_message_to_network(
        &mut self,
        message: RmcMessage<H::Hash, MK::Signature, MK::PartialMultisignature>,
    ) {
        self.send_message_for_network(
            AlertMessage::RmcMessage(self.node_index, message),
            Recipient::Everyone,
        );
    }

    fn send_notification_for_units(
        &mut self,
        notification: ForkingNotification<H, D, MK::Signature>,
    ) {
        if self
            .notifications_for_units
            .unbounded_send(notification)
            .is_err()
        {
            warn!(
                target: LOG_TARGET,
                "{:?} Channel with forking notifications should be open", self.node_index
            );
            self.exiting = true;
        }
    }

    fn send_message_for_network(
        &mut self,
        message: AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
        recipient: Recipient,
    ) {
        if self
            .messages_for_network
            .unbounded_send((message, recipient))
            .is_err()
        {
            warn!(
                target: LOG_TARGET,
                "{:?} Channel with notifications for network should be open", self.node_index
            );
            self.exiting = true;
        }
    }

    fn handle_message_from_network(
        &mut self,
        handler: &mut Handler<H, D, MK>,
        message: AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
    ) {
        match handler.on_message(message) {
            Ok(Some(AlerterResponse::ForkAlert(alert, recipient))) => {
                self.send_message_for_network(AlertMessage::ForkAlert(alert), recipient);
            }
            Ok(Some(AlerterResponse::AlertRequest(hash, peer))) => {
                let message = AlertMessage::AlertRequest(self.node_index, hash);
                self.send_message_for_network(message, peer);
            }
            Ok(Some(AlerterResponse::RmcMessage(message))) => {
                if self.messages_for_rmc.unbounded_send(message).is_err() {
                    warn!(
                        target: LOG_TARGET,
                        "{:?} Channel with messages for rmc should be open", self.node_index
                    );
                    self.exiting = true;
                }
            }
            Ok(Some(AlerterResponse::ForkResponse(maybe_notification, hash))) => {
                self.rmc.start_rmc(hash);
                if let Some(notification) = maybe_notification {
                    self.send_notification_for_units(notification);
                }
            }
            Ok(None) => {}
            Err(error) => debug!(target: LOG_TARGET, "{}", error),
        }
    }

    fn handle_alert_from_runway(
        &mut self,
        handler: &mut Handler<H, D, MK>,
        alert: Alert<H, D, MK::Signature>,
    ) {
        let (message, recipient, hash) = handler.on_own_alert(alert);
        self.send_message_for_network(message, recipient);
        self.rmc.start_rmc(hash);
    }

    fn handle_message_from_rmc(
        &mut self,
        message: RmcMessage<H::Hash, MK::Signature, MK::PartialMultisignature>,
    ) {
        self.rmc_message_to_network(message)
    }

    fn handle_multisigned(
        &mut self,
        handler: &mut Handler<H, D, MK>,
        multisigned: Multisigned<H::Hash, MK>,
    ) {
        match handler.alert_confirmed(multisigned) {
            Ok(notification) => self.send_notification_for_units(notification),
            Err(error) => warn!(target: LOG_TARGET, "{}", error),
        }
    }

    pub async fn run(&mut self, mut handler: Handler<H, D, MK>, mut terminator: Terminator) {
        loop {
            futures::select! {
                message = self.messages_from_network.next() => match message {
                    Some(message) => self.handle_message_from_network(&mut handler, message),
                    None => {
                        error!(target: LOG_TARGET, "{:?} Message stream closed.", self.node_index);
                        break;
                    }
                },
                alert = self.alerts_from_units.next() => match alert {
                    Some(alert) => self.handle_alert_from_runway(&mut handler, alert),
                    None => {
                        error!(target: LOG_TARGET, "{:?} Alert stream closed.", self.node_index);
                        break;
                    }
                },
                message = self.messages_from_rmc.next() => match message {
                    Some(message) => self.handle_message_from_rmc(message),
                    None => {
                        error!(target: LOG_TARGET, "{:?} RMC message stream closed.", self.node_index);
                        break;
                    }
                },
                multisigned = self.rmc.next_multisigned_hash().fuse() => self.handle_multisigned(&mut handler, multisigned),
                _ = terminator.get_exit().fuse() => {
                    debug!(target: LOG_TARGET, "{:?} received exit signal", self.node_index);
                    self.exiting = true;
                },
            }
            if self.exiting {
                debug!(
                    target: LOG_TARGET,
                    "{:?} Alerter decided to exit.", self.node_index
                );
                terminator.terminate_sync().await;
                break;
            }
        }
    }
}
