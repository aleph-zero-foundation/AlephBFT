use crate::{
    alerts::{
        handler::{Handler, RmcResponse},
        Alert, AlertMessage, ForkingNotification, NetworkMessage,
    },
    Data, Hasher, MultiKeychain, Multisigned, NodeIndex, Receiver, Recipient, Sender, Terminator,
};
use aleph_bft_rmc::{DoublingDelayScheduler, Message as RmcMessage};
use futures::{FutureExt, StreamExt};
use log::{debug, error, warn};
use std::time::Duration;

const LOG_TARGET: &str = "AlephBFT-alerter";
type RmcService<H, MK, S, M> =
    aleph_bft_rmc::Service<H, MK, DoublingDelayScheduler<RmcMessage<H, S, M>>>;

pub struct Service<H: Hasher, D: Data, MK: MultiKeychain> {
    messages_for_network: Sender<(NetworkMessage<H, D, MK>, Recipient)>,
    messages_from_network: Receiver<NetworkMessage<H, D, MK>>,
    notifications_for_units: Sender<ForkingNotification<H, D, MK::Signature>>,
    alerts_from_units: Receiver<Alert<H, D, MK::Signature>>,
    node_index: NodeIndex,
    exiting: bool,
    handler: Handler<H, D, MK>,
    rmc_service: RmcService<H::Hash, MK, MK::Signature, MK::PartialMultisignature>,
}

pub struct IO<H: Hasher, D: Data, MK: MultiKeychain> {
    pub messages_for_network: Sender<(NetworkMessage<H, D, MK>, Recipient)>,
    pub messages_from_network: Receiver<NetworkMessage<H, D, MK>>,
    pub notifications_for_units: Sender<ForkingNotification<H, D, MK::Signature>>,
    pub alerts_from_units: Receiver<Alert<H, D, MK::Signature>>,
}

impl<H: Hasher, D: Data, MK: MultiKeychain> Service<H, D, MK> {
    pub fn new(keychain: MK, io: IO<H, D, MK>, handler: Handler<H, D, MK>) -> Service<H, D, MK> {
        let IO {
            messages_for_network,
            messages_from_network,
            notifications_for_units,
            alerts_from_units,
        } = io;

        let node_index = keychain.index();
        let rmc_handler = aleph_bft_rmc::Handler::new(keychain);
        let rmc_service = aleph_bft_rmc::Service::new(
            DoublingDelayScheduler::new(Duration::from_millis(500)),
            rmc_handler,
        );

        Service {
            messages_for_network,
            messages_from_network,
            notifications_for_units,
            alerts_from_units,
            node_index,
            exiting: false,
            handler,
            rmc_service,
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
                "Channel with forking notifications should be open"
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
                "Channel with notifications for network should be open"
            );
            self.exiting = true;
        }
    }

    fn handle_message_from_network(
        &mut self,
        message: AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
    ) {
        match message {
            AlertMessage::ForkAlert(alert) => match self.handler.on_network_alert(alert.clone()) {
                Ok((maybe_notification, hash)) => {
                    if let Some(multisigned) = self.rmc_service.start_rmc(hash) {
                        self.handle_multisigned(multisigned);
                    }
                    if let Some(notification) = maybe_notification {
                        self.send_notification_for_units(notification);
                    }
                }
                Err(error) => debug!(target: LOG_TARGET, "{}", error),
            },
            AlertMessage::RmcMessage(sender, message) => {
                match self.handler.on_rmc_message(sender, message) {
                    RmcResponse::RmcMessage(message) => {
                        if let Some(multisigned) = self.rmc_service.process_message(message) {
                            self.handle_multisigned(multisigned);
                        }
                    }
                    RmcResponse::AlertRequest(hash, recipient) => {
                        let message = AlertMessage::AlertRequest(self.node_index, hash);
                        self.send_message_for_network(message, recipient);
                    }
                    RmcResponse::Noop => {}
                }
            }
            AlertMessage::AlertRequest(node, hash) => {
                match self.handler.on_alert_request(node, hash) {
                    Ok((alert, recipient)) => {
                        self.send_message_for_network(AlertMessage::ForkAlert(alert), recipient);
                    }
                    Err(error) => debug!(target: LOG_TARGET, "{}", error),
                }
            }
        }
    }

    fn handle_alert_from_runway(&mut self, alert: Alert<H, D, MK::Signature>) {
        let (message, recipient, hash) = self.handler.on_own_alert(alert.clone());
        self.send_message_for_network(message, recipient);
        if let Some(multisigned) = self.rmc_service.start_rmc(hash) {
            self.handle_multisigned(multisigned);
        }
    }

    fn handle_multisigned(&mut self, multisigned: Multisigned<H::Hash, MK>) {
        match self.handler.alert_confirmed(multisigned.clone()) {
            Ok(notification) => {
                self.send_notification_for_units(notification);
            }
            Err(error) => warn!(target: LOG_TARGET, "{}", error),
        }
    }

    pub async fn run(&mut self, mut terminator: Terminator) {
        loop {
            futures::select! {
                message = self.messages_from_network.next() => match message {
                    Some(message) => self.handle_message_from_network(message),
                    None => {
                        error!(target: LOG_TARGET, "Message stream closed.");
                        break;
                    }
                },
                alert = self.alerts_from_units.next() => match alert {
                    Some(alert) => self.handle_alert_from_runway(alert),
                    None => {
                        error!(target: LOG_TARGET, "Alert stream closed.");
                        break;
                    }
                },
                message = self.rmc_service.next_message().fuse() => {
                    self.rmc_message_to_network(message);
                },
                _ = terminator.get_exit().fuse() => {
                    debug!(target: LOG_TARGET, "Received exit signal.");
                    self.exiting = true;
                },
            }
            if self.exiting {
                debug!(target: LOG_TARGET, "Alerter decided to exit.");
                terminator.terminate_sync().await;
                break;
            }
        }
    }
}
