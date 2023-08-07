use crate::{
    units::{UncheckedSignedUnit, UnitCoord},
    Data, Hasher, NodeIndex, Receiver, Round, Sender, SessionId, Signature, Terminator,
};
use codec::{Decode, Encode, Error as CodecError};
use futures::{channel::oneshot, FutureExt, StreamExt};
use log::{debug, error, info, warn};
use std::{
    collections::HashSet,
    fmt,
    io::{Read, Write},
    marker::PhantomData,
};

/// Backup load error. Could be either caused by io error from Reader, or by decoding.
#[derive(Debug)]
pub enum LoaderError {
    IO(std::io::Error),
    Codec(CodecError),
    InconsistentData(UnitCoord),
    WrongSession(UnitCoord, SessionId, SessionId),
}

impl fmt::Display for LoaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoaderError::IO(err) => {
                write!(
                    f,
                    "Received IO error while reading from backup source: {}",
                    err
                )
            }

            LoaderError::Codec(err) => {
                write!(f, "Received Codec error while decoding backup: {}", err)
            }

            LoaderError::InconsistentData(coord) => {
                write!(
                    f,
                    "Inconsistent backup data. Unit from round {:?} of creator {:?} is missing a parent in backup.",
                    coord.round(), coord.creator()
                )
            }

            LoaderError::WrongSession(coord, expected_session, actual_session) => {
                write!(
                    f,
                    "Unit from round {:?} of creator {:?} has a wrong session id in backup. Expected: {:?} got: {:?}",
                    coord.round(), coord.creator(), expected_session, actual_session
                )
            }
        }
    }
}

impl From<std::io::Error> for LoaderError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl From<CodecError> for LoaderError {
    fn from(err: CodecError) -> Self {
        Self::Codec(err)
    }
}

/// Abstraction over Unit backup saving mechanism
pub struct UnitSaver<W: Write, H: Hasher, D: Data, S: Signature> {
    inner: W,
    _phantom: PhantomData<(H, D, S)>,
}

/// Abstraction over Unit backup loading mechanism
pub struct UnitLoader<R: Read, H: Hasher, D: Data, S: Signature> {
    inner: R,
    _phantom: PhantomData<(H, D, S)>,
}

impl<W: Write, H: Hasher, D: Data, S: Signature> UnitSaver<W, H, D, S> {
    pub fn new(write: W) -> Self {
        Self {
            inner: write,
            _phantom: PhantomData,
        }
    }

    pub fn save(&mut self, unit: UncheckedSignedUnit<H, D, S>) -> Result<(), std::io::Error> {
        self.inner.write_all(&unit.encode())?;
        self.inner.flush()?;
        Ok(())
    }
}

impl<R: Read, H: Hasher, D: Data, S: Signature> UnitLoader<R, H, D, S> {
    pub fn new(read: R) -> Self {
        Self {
            inner: read,
            _phantom: PhantomData,
        }
    }

    fn load(mut self) -> Result<Vec<UncheckedSignedUnit<H, D, S>>, LoaderError> {
        let mut buf = Vec::new();
        self.inner.read_to_end(&mut buf)?;
        let input = &mut &buf[..];
        let mut result = Vec::new();
        while !input.is_empty() {
            result.push(<UncheckedSignedUnit<H, D, S>>::decode(input)?);
        }
        Ok(result)
    }
}

fn load_backup<H: Hasher, D: Data, S: Signature, R: Read>(
    unit_loader: UnitLoader<R, H, D, S>,
    session_id: SessionId,
) -> Result<Vec<UncheckedSignedUnit<H, D, S>>, LoaderError> {
    let loaded_units = unit_loader.load()?;
    let mut already_loaded_coords = HashSet::new();

    for unit in loaded_units.iter() {
        let full_unit = unit.as_signable();
        let coord = full_unit.coord();

        if full_unit.session_id() != session_id {
            return Err(LoaderError::WrongSession(
                coord,
                session_id,
                full_unit.session_id(),
            ));
        }

        let parent_ids = &full_unit.as_pre_unit().control_hash().parents_mask;

        // Sanity check: verify that all unit's parents appeared in backup before it.
        for parent_id in parent_ids.elements() {
            let parent = UnitCoord::new(coord.round() - 1, parent_id);
            if !already_loaded_coords.contains(&parent) {
                return Err(LoaderError::InconsistentData(coord));
            }
        }

        already_loaded_coords.insert(coord);
    }

    Ok(loaded_units)
}

fn on_shutdown(starting_round_tx: oneshot::Sender<Option<Round>>) {
    if starting_round_tx.send(None).is_err() {
        warn!(target: "AlephBFT-unit-backup", "Could not send `None` starting round.");
    }
}

/// Loads Unit data from `unit_loader` and awaits on response from unit collection.
/// It sends all loaded units by `loaded_unit_tx`.
/// If loaded Units are compatible with the unit collection result (meaning the highest unit is from at least
/// round from unit collection + 1) it sends `Some(starting_round)` by
/// `starting_round_tx`. If Units are not compatible it sends `None` by `starting_round_tx`
pub async fn run_loading_mechanism<'a, H: Hasher, D: Data, S: Signature, R: Read>(
    unit_loader: UnitLoader<R, H, D, S>,
    index: NodeIndex,
    session_id: SessionId,
    loaded_unit_tx: oneshot::Sender<Vec<UncheckedSignedUnit<H, D, S>>>,
    starting_round_tx: oneshot::Sender<Option<Round>>,
    next_round_collection_rx: oneshot::Receiver<Round>,
) {
    let units = match load_backup(unit_loader, session_id) {
        Ok(units) => units,
        Err(e) => {
            error!(target: "AlephBFT-unit-backup", "unable to load unit backup: {}", e);
            on_shutdown(starting_round_tx);
            return;
        }
    };

    let next_round_backup: Round = units
        .iter()
        .filter(|u| u.as_signable().creator() == index)
        .map(|u| u.as_signable().round())
        .max()
        .map(|round| round + 1)
        .unwrap_or(0);

    info!(
        target: "AlephBFT-unit-backup", "Loaded {:?} units from backup. Able to continue from round: {:?}.",
        units.len(),
        next_round_backup
    );

    if let Err(e) = loaded_unit_tx.send(units) {
        error!(target: "AlephBFT-unit-backup", "Could not send loaded units: {:?}", e);
        on_shutdown(starting_round_tx);
        return;
    }

    let next_round_collection = match next_round_collection_rx.await {
        Ok(round) => round,
        Err(e) => {
            error!(target: "AlephBFT-unit-backup", "Unable to receive response from unit collection: {}", e);
            on_shutdown(starting_round_tx);
            return;
        }
    };

    info!(target: "AlephBFT-unit-backup", "Next round inferred from collection: {:?}", next_round_collection);

    if next_round_backup < next_round_collection {
        // Our newest unit doesn't appear in the backup. This indicates a serious issue, for example
        // a different node running with the same pair of keys. It's safer not to continue.
        error!(
            target: "AlephBFT-unit-backup", "Backup state behind unit collection state. Next round inferred from: collection: {:?}, backup: {:?}",
            next_round_collection,
            next_round_backup,
        );
        on_shutdown(starting_round_tx);
        return;
    };

    if next_round_collection < next_round_backup {
        // Our newest unit didn't reach any peer, but it resides in our backup. One possible reason
        // is that our node was taken down after saving the unit, but before broadcasting it.
        warn!(
            target: "AlephBFT-unit-backup", "Backup state ahead of than unit collection state. Next round inferred from: collection: {:?}, backup: {:?}",
            next_round_backup,
            next_round_collection
        );
    }

    if let Err(e) = starting_round_tx.send(Some(next_round_backup)) {
        error!(target: "AlephBFT-unit-backup", "Could not send starting round: {:?}", e);
    }
}

/// A task responsible for saving units into backup.
/// It waits for units to appear in `backup_units_from_runway`, and writes them to backup.
/// It announces a successful write through `backup_units_for_runway`.
pub async fn run_saving_mechanism<'a, H: Hasher, D: Data, S: Signature, W: Write>(
    mut unit_saver: UnitSaver<W, H, D, S>,
    mut backup_units_from_runway: Receiver<UncheckedSignedUnit<H, D, S>>,
    backup_units_for_runway: Sender<UncheckedSignedUnit<H, D, S>>,
    mut terminator: Terminator,
) {
    let mut terminator_exit = false;
    loop {
        futures::select! {
            unit_to_save = backup_units_from_runway.next() => {
                let unit_to_save = match unit_to_save {
                    Some(unit) => unit,
                    None => {
                        error!(target: "AlephBFT-backup-saver", "Receiver of units to save closed early.");
                        break;
                    },
                };

                if let Err(e) = unit_saver.save(unit_to_save.clone()) {
                    error!(target: "AlephBFT-backup-saver", "Couldn't save unit to backup: {:?}", e);
                    break;
                }
                if backup_units_for_runway.unbounded_send(unit_to_save).is_err() {
                    error!(target: "AlephBFT-backup-saver", "Couldn't respond with saved unit.");
                    break;
                }
            },
            _ = terminator.get_exit().fuse() => {
                debug!(target: "AlephBFT-backup-saver", "Backup saver received exit signal.");
                terminator_exit = true;
            }
        }

        if terminator_exit {
            debug!(target: "AlephBFT-backup-saver", "Backup saver decided to exit.");
            terminator.terminate_sync().await;
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{run_loading_mechanism, UnitLoader};
    use crate::{
        units::{
            create_units, creator_set, preunit_to_unchecked_signed_unit, preunit_to_unit,
            UncheckedSignedUnit as GenericUncheckedSignedUnit,
        },
        NodeCount, NodeIndex, Round, SessionId,
    };
    use aleph_bft_mock::{Data, Hasher64, Keychain, Loader, Signature};
    use codec::Encode;
    use futures::channel::oneshot;

    type UncheckedSignedUnit = GenericUncheckedSignedUnit<Hasher64, Data, Signature>;

    const SESSION_ID: SessionId = 43;
    const NODE_ID: NodeIndex = NodeIndex(0);
    const N_MEMBERS: NodeCount = NodeCount(4);

    fn produce_units(rounds: usize, session_id: SessionId) -> Vec<Vec<UncheckedSignedUnit>> {
        let mut creators = creator_set(N_MEMBERS);
        let keychains: Vec<_> = (0..N_MEMBERS.0)
            .map(|id| Keychain::new(N_MEMBERS, NodeIndex(id)))
            .collect();

        let mut units_per_round = Vec::with_capacity(rounds);

        for round in 0..rounds {
            let pre_units = create_units(creators.iter(), round as Round);

            let units: Vec<_> = pre_units
                .iter()
                .map(|(pre_unit, _)| preunit_to_unit(pre_unit.clone(), session_id))
                .collect();
            for creator in creators.iter_mut() {
                creator.add_units(&units);
            }

            let mut unchecked_signed_units = Vec::with_capacity(pre_units.len());
            for ((pre_unit, _), keychain) in pre_units.into_iter().zip(keychains.iter()) {
                unchecked_signed_units.push(preunit_to_unchecked_signed_unit(
                    pre_unit, session_id, keychain,
                ))
            }

            units_per_round.push(unchecked_signed_units);
        }

        // units_per_round[i][j] is the unit produced in round i by creator j
        units_per_round
    }

    fn units_of_creator(
        units: Vec<Vec<UncheckedSignedUnit>>,
        creator: NodeIndex,
    ) -> Vec<UncheckedSignedUnit> {
        units
            .into_iter()
            .map(|units_per_round| units_per_round[creator.0].clone())
            .collect()
    }

    fn encode_all(units: Vec<UncheckedSignedUnit>) -> Vec<Vec<u8>> {
        units.iter().map(|u| u.encode()).collect()
    }

    fn prepare_test(
        encoded_units: Vec<u8>,
    ) -> (
        impl futures::Future,
        oneshot::Receiver<Vec<UncheckedSignedUnit>>,
        oneshot::Sender<Round>,
        oneshot::Receiver<Option<Round>>,
    ) {
        let unit_loader = UnitLoader::new(Loader::new(encoded_units));
        let (loaded_unit_tx, loaded_unit_rx) = oneshot::channel();
        let (starting_round_tx, starting_round_rx) = oneshot::channel();
        let (highest_response_tx, highest_response_rx) = oneshot::channel();

        (
            run_loading_mechanism(
                unit_loader,
                NODE_ID,
                SESSION_ID,
                loaded_unit_tx,
                starting_round_tx,
                highest_response_rx,
            ),
            loaded_unit_rx,
            highest_response_tx,
            starting_round_rx,
        )
    }

    #[tokio::test]
    async fn nothing_loaded_nothing_collected_succeeds() {
        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(Vec::new());

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(Some(0)));
        assert_eq!(loaded_unit_rx.await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn something_loaded_nothing_collected_succeeds() {
        let units: Vec<_> = produce_units(5, SESSION_ID).into_iter().flatten().collect();
        let encoded_units = encode_all(units.clone()).into_iter().flatten().collect();

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units);

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(Some(5)));
        assert_eq!(loaded_unit_rx.await, Ok(units));
    }

    #[tokio::test]
    async fn something_loaded_something_collected_succeeds() {
        let units: Vec<_> = produce_units(5, SESSION_ID).into_iter().flatten().collect();
        let encoded_units = encode_all(units.clone()).into_iter().flatten().collect();

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units);

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(5).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(Some(5)));
        assert_eq!(loaded_unit_rx.await, Ok(units));
    }

    #[tokio::test]
    async fn nothing_loaded_something_collected_fails() {
        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(Vec::new());

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(1).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert_eq!(loaded_unit_rx.await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn loaded_smaller_then_collected_fails() {
        let units: Vec<_> = produce_units(3, SESSION_ID).into_iter().flatten().collect();
        let encoded_units = encode_all(units.clone()).into_iter().flatten().collect();

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units);

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(4).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert_eq!(loaded_unit_rx.await, Ok(units));
    }

    #[tokio::test]
    async fn dropped_collection_fails() {
        let units: Vec<_> = produce_units(3, SESSION_ID).into_iter().flatten().collect();
        let encoded_units = encode_all(units.clone()).into_iter().flatten().collect();

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units);

        let handle = tokio::spawn(async {
            task.await;
        });

        drop(highest_response_tx);

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert_eq!(loaded_unit_rx.await, Ok(units));
    }

    #[tokio::test]
    async fn backup_with_corrupted_encoding_fails() {
        let units = produce_units(5, SESSION_ID).into_iter().flatten().collect();
        let mut unit_encodings = encode_all(units);
        let unit2_encoding_len = unit_encodings[2].len();
        unit_encodings[2].resize(unit2_encoding_len - 1, 0); // remove the last byte
        let encoded_units = unit_encodings.into_iter().flatten().collect();

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units);
        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert!(loaded_unit_rx.await.is_err());
    }

    #[tokio::test]
    async fn backup_with_missing_parent_fails() {
        let mut units: Vec<_> = produce_units(5, SESSION_ID).into_iter().flatten().collect();
        units.remove(2); // it is a parent of all units of round 3
        let encoded_units = encode_all(units).into_iter().flatten().collect();

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units);
        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert!(loaded_unit_rx.await.is_err());
    }

    #[tokio::test]
    async fn backup_with_duplicate_unit_succeeds() {
        let mut units: Vec<_> = produce_units(5, SESSION_ID).into_iter().flatten().collect();
        let unit2_duplicate = units[2].clone();
        units.insert(3, unit2_duplicate);
        let encoded_units = encode_all(units.clone()).into_iter().flatten().collect();

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units);

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(Some(5)));
        assert_eq!(loaded_unit_rx.await, Ok(units));
    }

    #[tokio::test]
    async fn backup_with_units_of_one_creator_fails() {
        let units = units_of_creator(produce_units(5, SESSION_ID), NodeIndex(NODE_ID.0 + 1));
        let encoded_units = encode_all(units).into_iter().flatten().collect();

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units);

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert!(loaded_unit_rx.await.is_err());
    }

    #[tokio::test]
    async fn backup_with_wrong_session_fails() {
        let units = produce_units(5, SESSION_ID + 1)
            .into_iter()
            .flatten()
            .collect();
        let encoded_units = encode_all(units).into_iter().flatten().collect();

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units);

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert!(loaded_unit_rx.await.is_err());
    }
}
