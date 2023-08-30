use crate::{
    units::UncheckedSignedUnit, Data, Hasher, MultiKeychain, Receiver, Sender, Terminator,
};

use crate::alerts::AlertData;
use codec::Encode;
use futures::{FutureExt, StreamExt};

use crate::backup::BackupItem;
use log::{debug, error};
use std::io::Write;

const LOG_TARGET: &str = "AlephBFT-backup-saver";

/// Component responsible for saving units and alert data into backup.
/// It waits for items to appear on its receivers, and writes them to backup.
/// It announces a successful write through an appropriate response sender.
pub struct BackupSaver<H: Hasher, D: Data, MK: MultiKeychain, W: Write> {
    units_from_runway: Receiver<UncheckedSignedUnit<H, D, MK::Signature>>,
    data_from_alerter: Receiver<AlertData<H, D, MK>>,
    responses_for_runway: Sender<UncheckedSignedUnit<H, D, MK::Signature>>,
    responses_for_alerter: Sender<AlertData<H, D, MK>>,
    backup: W,
}

impl<H: Hasher, D: Data, MK: MultiKeychain, W: Write> BackupSaver<H, D, MK, W> {
    pub fn new(
        units_from_runway: Receiver<UncheckedSignedUnit<H, D, MK::Signature>>,
        data_from_alerter: Receiver<AlertData<H, D, MK>>,
        responses_for_runway: Sender<UncheckedSignedUnit<H, D, MK::Signature>>,
        responses_for_alerter: Sender<AlertData<H, D, MK>>,
        backup: W,
    ) -> BackupSaver<H, D, MK, W> {
        BackupSaver {
            units_from_runway,
            data_from_alerter,
            responses_for_runway,
            responses_for_alerter,
            backup,
        }
    }

    pub fn save_item(&mut self, item: BackupItem<H, D, MK>) -> Result<(), std::io::Error> {
        self.backup.write_all(&item.encode())?;
        self.backup.flush()?;
        Ok(())
    }

    pub async fn run(&mut self, mut terminator: Terminator) {
        let mut terminator_exit = false;
        loop {
            futures::select! {
                unit = self.units_from_runway.next() => {
                    let unit = match unit {
                        Some(unit) => unit,
                        None => {
                            error!(target: LOG_TARGET, "receiver of units to save closed early");
                            break;
                        },
                    };
                    let item = BackupItem::Unit(unit.clone());
                    if let Err(e) = self.save_item(item) {
                        error!(target: LOG_TARGET, "couldn't save item to backup: {:?}", e);
                        break;
                    }
                    if self.responses_for_runway.unbounded_send(unit).is_err() {
                        error!(target: LOG_TARGET, "couldn't respond with saved unit to runway");
                        break;
                    }
                },
                data = self.data_from_alerter.next() => {
                    let data = match data {
                        Some(data) => data,
                        None => {
                            error!(target: LOG_TARGET, "receiver of alert data to save closed early");
                            break;
                        },
                    };
                    let item = BackupItem::AlertData(data.clone());
                    if let Err(e) = self.save_item(item) {
                        error!(target: LOG_TARGET, "couldn't save item to backup: {:?}", e);
                        break;
                    }
                    if self.responses_for_alerter.unbounded_send(data).is_err() {
                        error!(target: LOG_TARGET, "couldn't respond with saved alert data to runway");
                        break;
                    }
                }
                _ = terminator.get_exit().fuse() => {
                    debug!(target: LOG_TARGET, "backup saver received exit signal.");
                    terminator_exit = true;
                }
            }

            if terminator_exit {
                debug!(target: LOG_TARGET, "backup saver decided to exit.");
                terminator.terminate_sync().await;
                break;
            }
        }
    }
}
