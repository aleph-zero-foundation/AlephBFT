use std::io::Write;

use crate::{units::UncheckedSignedUnit, Data, Hasher, Receiver, Sender, Signature, Terminator};
use codec::Encode;
use futures::{FutureExt, StreamExt};
use log::{debug, error};

const LOG_TARGET: &str = "AlephBFT-backup-saver";

/// Component responsible for saving units into backup.
/// It waits for items to appear on its receivers, and writes them to backup.
/// It announces a successful write through an appropriate response sender.
pub struct BackupSaver<H: Hasher, D: Data, S: Signature, W: Write> {
    units_from_runway: Receiver<UncheckedSignedUnit<H, D, S>>,
    responses_for_runway: Sender<UncheckedSignedUnit<H, D, S>>,
    backup: W,
}

impl<H: Hasher, D: Data, S: Signature, W: Write> BackupSaver<H, D, S, W> {
    pub fn new(
        units_from_runway: Receiver<UncheckedSignedUnit<H, D, S>>,
        responses_for_runway: Sender<UncheckedSignedUnit<H, D, S>>,
        backup: W,
    ) -> BackupSaver<H, D, S, W> {
        BackupSaver {
            units_from_runway,
            responses_for_runway,
            backup,
        }
    }

    pub fn save_item(&mut self, item: &UncheckedSignedUnit<H, D, S>) -> Result<(), std::io::Error> {
        self.backup.write_all(&item.encode())?;
        self.backup.flush()?;
        Ok(())
    }

    pub async fn run(&mut self, mut terminator: Terminator) {
        let mut terminator_exit = false;
        loop {
            futures::select! {
                unit = self.units_from_runway.next() => {
                    let item = match unit {
                        Some(unit) => unit,
                        None => {
                            error!(target: LOG_TARGET, "receiver of units to save closed early");
                            break;
                        },
                    };
                    if let Err(e) = self.save_item(&item) {
                        error!(target: LOG_TARGET, "couldn't save item to backup: {:?}", e);
                        break;
                    }
                    if self.responses_for_runway.unbounded_send(item).is_err() {
                        error!(target: LOG_TARGET, "couldn't respond with saved unit to runway");
                        break;
                    }
                },
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

#[cfg(test)]
mod tests {
    use futures::{
        channel::{mpsc, oneshot},
        StreamExt,
    };

    use aleph_bft_mock::{Data, Hasher64, Keychain, Saver, Signature};

    use crate::{
        backup::BackupSaver,
        units::{creator_set, preunit_to_unchecked_signed_unit, UncheckedSignedUnit},
        NodeCount, NodeIndex, Terminator,
    };

    type TestBackupSaver = BackupSaver<Hasher64, Data, Signature, Saver>;
    type TestUnit = UncheckedSignedUnit<Hasher64, Data, Signature>;
    struct PrepareSaverResponse<F: futures::Future> {
        task: F,
        units_for_saver: mpsc::UnboundedSender<TestUnit>,
        units_from_saver: mpsc::UnboundedReceiver<TestUnit>,
        exit_tx: oneshot::Sender<()>,
    }

    fn prepare_saver() -> PrepareSaverResponse<impl futures::Future> {
        let (units_for_saver, units_from_runway) = mpsc::unbounded();
        let (units_for_runway, units_from_saver) = mpsc::unbounded();
        let (exit_tx, exit_rx) = oneshot::channel();
        let backup = Saver::new();

        let task = {
            let mut saver: TestBackupSaver =
                BackupSaver::new(units_from_runway, units_for_runway, backup);

            async move {
                saver.run(Terminator::create_root(exit_rx, "saver")).await;
            }
        };

        PrepareSaverResponse {
            task,
            units_for_saver,
            units_from_saver,
            exit_tx,
        }
    }

    #[tokio::test]
    async fn test_proper_relative_responses_ordering() {
        let PrepareSaverResponse {
            task,
            units_for_saver,
            mut units_from_saver,
            exit_tx,
        } = prepare_saver();

        let handle = tokio::spawn(async {
            task.await;
        });

        let creators = creator_set(NodeCount(5));
        let keychains: Vec<_> = (0..5)
            .map(|id| Keychain::new(NodeCount(5), NodeIndex(id)))
            .collect();
        let units: Vec<TestUnit> = (0..5)
            .map(|k| {
                preunit_to_unchecked_signed_unit(
                    creators[k].create_unit(0).unwrap().0,
                    0,
                    &keychains[k],
                )
            })
            .collect();

        for u in units.iter() {
            units_for_saver.unbounded_send(u.clone()).unwrap();
        }

        for u in units {
            let u_backup = units_from_saver.next().await.unwrap();
            assert_eq!(u, u_backup);
        }

        exit_tx.send(()).unwrap();
        handle.await.unwrap();
    }
}
