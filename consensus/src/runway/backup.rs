use crate::{units::UncheckedSignedUnit, Data, Hasher, NodeIndex, Round, SessionId, Signature};
use codec::{Decode, Encode, Error as CodecError};
use futures::channel::oneshot;
use log::{error, info, warn};
use std::{
    fmt,
    io::{Read, Write},
    marker::PhantomData,
};

/// Backup load error. Could be either caused by io error from Reader, or by decoding.
#[derive(Debug)]
pub enum LoaderError {
    IO(std::io::Error),
    Codec(CodecError),
    RoundMissmatch(Round, Round),
    WrongCreator(Round, NodeIndex, NodeIndex),
    WrongSession(Round, SessionId, SessionId),
}

impl fmt::Display for LoaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoaderError::IO(err) => {
                write!(f, "Got IO error while reading from UnitLoader: {}", err)
            }

            LoaderError::Codec(err) => {
                write!(f, "Got Codec error while docoding backup: {}", err)
            }

            LoaderError::RoundMissmatch(expected, round) => {
                write!(
                    f,
                    "Round mismatch. Expected round {:?}. Got round {:?}",
                    expected, round
                )
            }

            LoaderError::WrongCreator(round, expected, creator) => {
                write!(
                    f,
                    "Wrong creator for unit round {:?}. We are not the creator. Expected: {:?} got: {:?}",
                    round, expected, creator
                )
            }

            LoaderError::WrongSession(round, expected, session) => {
                write!(
                    f,
                    "Wrong session for unit round {:?}. Expected: {:?} got: {:?}",
                    round, expected, session
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
    index: NodeIndex,
    session_id: SessionId,
) -> Result<Vec<UncheckedSignedUnit<H, D, S>>, LoaderError> {
    let units = unit_loader.load()?;

    for (u, expected_round) in units.iter().zip(0..) {
        let su = u.as_signable();
        let coord = su.coord();

        if coord.round() != expected_round {
            return Err(LoaderError::RoundMissmatch(expected_round, coord.round()));
        }
        if coord.creator() != index {
            return Err(LoaderError::WrongCreator(
                coord.round(),
                index,
                coord.creator(),
            ));
        }
        if su.session_id() != session_id {
            return Err(LoaderError::WrongSession(
                coord.round(),
                session_id,
                su.session_id(),
            ));
        }
    }

    Ok(units)
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
    let units = match load_backup(unit_loader, index, session_id) {
        Ok(units) => units,
        Err(e) => {
            error!(target: "AlephBFT-unit-backup", "unable to load unit backup: {}", e);
            on_shutdown(starting_round_tx);
            return;
        }
    };
    let next_round_backup: Round = units.len() as Round;
    info!(target: "AlephBFT-unit-backup", "loaded units from backup. Loaded {:?} units", units.len());

    if let Err(e) = loaded_unit_tx.send(units) {
        error!(target: "AlephBFT-unit-backup", "could not send loaded units: {:?}", e);
        on_shutdown(starting_round_tx);
        return;
    }

    let next_round_collection = match next_round_collection_rx.await {
        Ok(round) => round,
        Err(e) => {
            error!(target: "AlephBFT-unit-backup", "unable to receive response from unit collections: {}", e);
            on_shutdown(starting_round_tx);
            return;
        }
    };
    info!(target: "AlephBFT-unit-backup", "received next round from unit collection: {:?}", next_round_collection);

    if next_round_backup < next_round_collection {
        error!(target: "AlephBFT-unit-backup", "backup lower than unit collection result. Backup got: {:?}, collection got: {:?}", next_round_backup, next_round_collection);
        on_shutdown(starting_round_tx);
        return;
    };

    if next_round_collection < next_round_backup {
        warn!(target: "AlephBFT-unit-backup", "unit collection result lower than backup. Backup got: {:?}, collection got: {:?}", next_round_backup, next_round_collection);
    }

    if let Err(e) = starting_round_tx.send(Some(next_round_backup)) {
        error!(target: "AlephBFT-unit-backup", "could not send starting round: {:?}", e);
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
    use futures::channel::oneshot::{self, Receiver, Sender};

    type UncheckedSignedUnit = GenericUncheckedSignedUnit<Hasher64, Data, Signature>;

    const SESSION_ID: SessionId = 43;
    const NODE_ID: NodeIndex = NodeIndex(0);
    const N_MEMBERS: NodeCount = NodeCount(4);

    async fn produce_units(rounds: usize, session_id: SessionId) -> Vec<Vec<UncheckedSignedUnit>> {
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
                unchecked_signed_units
                    .push(preunit_to_unchecked_signed_unit(pre_unit, session_id, keychain).await)
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

    fn concatenate_encodings(unit_encodings: Vec<Vec<u8>>) -> Vec<u8> {
        unit_encodings.into_iter().flatten().collect()
    }

    async fn prepare_test<'a>(
        encoded_units: Vec<u8>,
    ) -> (
        impl futures::Future,
        Receiver<Vec<UncheckedSignedUnit>>,
        Sender<Round>,
        Receiver<Option<Round>>,
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
    async fn nothing_loaded_nothing_collected() {
        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(Vec::new()).await;

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(Some(0)));
        assert_eq!(loaded_unit_rx.await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn something_loaded_nothing_collected() {
        let units = units_of_creator(produce_units(5, SESSION_ID).await, NodeIndex(0));
        let encoded_units = concatenate_encodings(encode_all(units.clone()));

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units).await;

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(Some(5)));
        assert_eq!(loaded_unit_rx.await, Ok(units));
    }

    #[tokio::test]
    async fn something_loaded_something_collected() {
        let units = units_of_creator(produce_units(5, SESSION_ID).await, NodeIndex(0));
        let encoded_units = concatenate_encodings(encode_all(units.clone()));

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units).await;

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(5).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(Some(5)));
        assert_eq!(loaded_unit_rx.await, Ok(units));
    }

    #[tokio::test]
    async fn nothing_loaded_something_collected() {
        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(Vec::new()).await;

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(1).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert_eq!(loaded_unit_rx.await, Ok(Vec::new()));
    }

    #[tokio::test]
    async fn loaded_smaller_then_collected() {
        let units = units_of_creator(produce_units(3, SESSION_ID).await, NodeIndex(0));
        let encoded_units = concatenate_encodings(encode_all(units.clone()));

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units).await;

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(4).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert_eq!(loaded_unit_rx.await, Ok(units));
    }

    #[tokio::test]
    async fn nothing_collected() {
        let units = units_of_creator(produce_units(3, SESSION_ID).await, NodeIndex(0));
        let encoded_units = concatenate_encodings(encode_all(units.clone()));

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units).await;

        let handle = tokio::spawn(async {
            task.await;
        });

        drop(highest_response_tx);

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert_eq!(loaded_unit_rx.await, Ok(units));
    }

    #[tokio::test]
    async fn corrupted_backup_codec() {
        let units = units_of_creator(produce_units(5, SESSION_ID).await, NodeIndex(0));
        let mut unit_encodings = encode_all(units);
        let unit2_encoding_len = unit_encodings[2].len();
        unit_encodings[2].resize(unit2_encoding_len - 1, 0); // remove the last byte
        let encoded_units = concatenate_encodings(unit_encodings);

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units).await;
        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert!(loaded_unit_rx.await.is_err());
    }

    #[tokio::test]
    async fn corrupted_backup_missing() {
        let mut units = units_of_creator(produce_units(5, SESSION_ID).await, NodeIndex(0));
        units.remove(2);
        let encoded_units = concatenate_encodings(encode_all(units));

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units).await;
        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert!(loaded_unit_rx.await.is_err());
    }

    #[tokio::test]
    async fn corrupted_backup_duplicate() {
        let mut units = units_of_creator(produce_units(5, SESSION_ID).await, NodeIndex(0));
        let unit2_duplicate = units[2].clone();
        units.insert(3, unit2_duplicate);
        let encoded_units = concatenate_encodings(encode_all(units));

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units).await;

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert!(loaded_unit_rx.await.is_err());
    }

    #[tokio::test]
    async fn corrupted_backup_wrong_creator() {
        let units = units_of_creator(produce_units(5, SESSION_ID).await, NodeIndex(NODE_ID.0 + 1));
        let encoded_units = concatenate_encodings(encode_all(units));

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units).await;

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert!(loaded_unit_rx.await.is_err());
    }

    #[tokio::test]
    async fn corrupted_backup_wrong_session() {
        let units = units_of_creator(produce_units(5, SESSION_ID + 1).await, NodeIndex(0));
        let encoded_units = concatenate_encodings(encode_all(units));

        let (task, loaded_unit_rx, highest_response_tx, starting_round_rx) =
            prepare_test(encoded_units).await;

        let handle = tokio::spawn(async {
            task.await;
        });

        highest_response_tx.send(0).unwrap();

        handle.await.unwrap();

        assert_eq!(starting_round_rx.await, Ok(None));
        assert!(loaded_unit_rx.await.is_err());
    }
}
