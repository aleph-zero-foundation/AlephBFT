use super::*;

pub struct IO<'a, H: Hasher, D: Data, MK: MultiKeychain> {
    pub messages_for_network: Sender<(NetworkMessage<H, D, MK>, Recipient)>,
    pub messages_from_network: Receiver<NetworkMessage<H, D, MK>>,
    pub notifications_for_units: Sender<ForkingNotification<H, D, MK::Signature>>,
    pub alerts_from_units: Receiver<Alert<H, D, MK::Signature>>,
    pub rmc: ReliableMulticast<'a, H::Hash, MK>,
    pub messages_from_rmc: Receiver<RmcMessage<H::Hash, MK::Signature, MK::PartialMultisignature>>,
    pub messages_for_rmc: Sender<RmcMessage<H::Hash, MK::Signature, MK::PartialMultisignature>>,
    pub alerter_index: NodeIndex,
}

impl<'a, H: Hasher, D: Data, MK: MultiKeychain> IO<'a, H, D, MK> {
    pub fn rmc_message_to_network(
        &mut self,
        message: RmcMessage<H::Hash, MK::Signature, MK::PartialMultisignature>,
        exiting: &mut bool,
    ) {
        self.send_message_for_network(
            AlertMessage::RmcMessage(self.alerter_index, message),
            Recipient::Everyone,
            exiting,
        );
    }

    pub fn send_notification_for_units(
        &mut self,
        notification: ForkingNotification<H, D, MK::Signature>,
        exiting: &mut bool,
    ) {
        if self
            .notifications_for_units
            .unbounded_send(notification)
            .is_err()
        {
            warn!(target: "AlephBFT-alerter", "{:?} Channel with forking notifications should be open", self.alerter_index);
            *exiting = true;
        }
    }

    pub fn send_message_for_network(
        &mut self,
        message: AlertMessage<H, D, MK::Signature, MK::PartialMultisignature>,
        recipient: Recipient,
        exiting: &mut bool,
    ) {
        if self
            .messages_for_network
            .unbounded_send((message, recipient))
            .is_err()
        {
            warn!(target: "AlephBFT-alerter", "{:?} Channel with notifications for network should be open", self.alerter_index);
            *exiting = true;
        }
    }
}
