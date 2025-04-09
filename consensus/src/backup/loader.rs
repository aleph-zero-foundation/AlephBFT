use std::{
    collections::HashSet,
    fmt::{self, Debug},
    marker::PhantomData,
    pin::Pin,
};

use codec::{Decode, Error as CodecError};
use futures::{AsyncRead, AsyncReadExt};

use crate::{
    units::{UncheckedSignedUnit, Unit, UnitCoord},
    Data, Hasher, NodeIndex, Round, SessionId, Signature,
};

/// Backup read error. Could be either caused by io error from `BackupReader`, or by decoding.
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
                    "received IO error while reading from backup source: {}",
                    err
                )
            }
            LoaderError::Codec(err) => {
                write!(f, "received Codec error while decoding backup: {}", err)
            }
            LoaderError::InconsistentData(coord) => {
                write!(
                    f,
                    "inconsistent backup data. Unit from round {:?} of creator {:?} is missing a parent in backup.",
                    coord.round(), coord.creator()
                )
            }
            LoaderError::WrongSession(coord, expected_session, actual_session) => {
                write!(
                    f,
                    "unit from round {:?} of creator {:?} has a wrong session id in backup. Expected: {:?} got: {:?}",
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

pub struct BackupLoader<H: Hasher, D: Data, S: Signature, R: AsyncRead> {
    backup: Pin<Box<R>>,
    index: NodeIndex,
    session_id: SessionId,
    _phantom: PhantomData<(H, D, S)>,
}

impl<H: Hasher, D: Data, S: Signature, R: AsyncRead> BackupLoader<H, D, S, R> {
    pub fn new(backup: R, index: NodeIndex, session_id: SessionId) -> BackupLoader<H, D, S, R> {
        BackupLoader {
            backup: Box::pin(backup),
            index,
            session_id,
            _phantom: PhantomData,
        }
    }

    async fn load(&mut self) -> Result<Vec<UncheckedSignedUnit<H, D, S>>, LoaderError> {
        let mut buf = Vec::new();
        self.backup.read_to_end(&mut buf).await?;
        let input = &mut &buf[..];
        let mut result = Vec::new();
        while !input.is_empty() {
            result.push(<UncheckedSignedUnit<H, D, S>>::decode(input)?);
        }
        Ok(result)
    }

    fn verify_units(&self, units: &Vec<UncheckedSignedUnit<H, D, S>>) -> Result<(), LoaderError> {
        let mut already_loaded_coords = HashSet::new();

        for unit in units {
            let full_unit = unit.as_signable();
            let coord = full_unit.coord();

            if full_unit.session_id() != self.session_id {
                return Err(LoaderError::WrongSession(
                    coord,
                    self.session_id,
                    full_unit.session_id(),
                ));
            }

            // Sanity check: verify that all unit's parents appeared in backup before it.
            for parent in full_unit.as_pre_unit().control_hash().parents() {
                if !already_loaded_coords.contains(&parent) {
                    return Err(LoaderError::InconsistentData(coord));
                }
            }

            already_loaded_coords.insert(coord);
        }

        Ok(())
    }

    pub async fn load_backup(
        &mut self,
    ) -> Result<(Vec<UncheckedSignedUnit<H, D, S>>, Round), LoaderError> {
        let units = self.load().await?;
        self.verify_units(&units)?;
        let next_round: Round = units
            .iter()
            .filter(|u| u.as_signable().creator() == self.index)
            .map(|u| u.as_signable().round())
            .max()
            .map(|round| round + 1)
            .unwrap_or(0);

        Ok((units, next_round))
    }
}

#[cfg(test)]
mod tests {
    use codec::Encode;

    use aleph_bft_mock::{Data, Hasher64, Keychain, Loader, Signature};

    use crate::{
        backup::{loader::LoaderError, BackupLoader as GenericLoader},
        units::{
            create_preunits, creator_set, preunit_to_full_unit, preunit_to_unchecked_signed_unit,
            UncheckedSignedUnit as GenericUncheckedSignedUnit,
        },
        NodeCount, NodeIndex, Round, SessionId,
    };

    type UncheckedSignedUnit = GenericUncheckedSignedUnit<Hasher64, Data, Signature>;
    type BackupLoader<R> = GenericLoader<Hasher64, Data, Signature, R>;

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
            let pre_units = create_preunits(creators.iter(), round as Round);

            let units: Vec<_> = pre_units
                .iter()
                .map(|pre_unit| preunit_to_full_unit(pre_unit.clone(), session_id))
                .collect();
            for creator in creators.iter_mut() {
                creator.add_units(&units);
            }

            let mut unchecked_signed_units = Vec::with_capacity(pre_units.len());
            for (pre_unit, keychain) in pre_units.into_iter().zip(keychains.iter()) {
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

    fn encode_all(items: Vec<UncheckedSignedUnit>) -> Vec<Vec<u8>> {
        items.iter().map(|u| u.encode()).collect()
    }

    #[tokio::test]
    async fn loads_nothing() {
        let (units, round) = BackupLoader::new(Loader::new(Vec::new()), NODE_ID, SESSION_ID)
            .load_backup()
            .await
            .expect("should load correctly");
        assert_eq!(round, 0);
        assert_eq!(units, Vec::new());
    }

    #[tokio::test]
    async fn loads_some_units() {
        let items: Vec<_> = produce_units(5, SESSION_ID).into_iter().flatten().collect();
        let encoded_items = encode_all(items.clone()).into_iter().flatten().collect();

        let (units, round) = BackupLoader::new(Loader::new(encoded_items), NODE_ID, SESSION_ID)
            .load_backup()
            .await
            .expect("should load correctly");
        assert_eq!(round, 5);
        assert_eq!(units, items);
    }

    #[tokio::test]
    async fn backup_with_corrupted_encoding_fails() {
        let items: Vec<_> = produce_units(5, SESSION_ID).into_iter().flatten().collect();
        let mut item_encodings = encode_all(items);
        let unit2_encoding_len = item_encodings[2].len();
        item_encodings[2].resize(unit2_encoding_len - 1, 0); // remove the last byte
        let encoded_items = item_encodings.into_iter().flatten().collect();

        assert!(matches!(
            BackupLoader::new(Loader::new(encoded_items), NODE_ID, SESSION_ID)
                .load_backup()
                .await,
            Err(LoaderError::Codec(_))
        ));
    }

    #[tokio::test]
    async fn backup_with_missing_parent_fails() {
        let mut items: Vec<_> = produce_units(5, SESSION_ID).into_iter().flatten().collect();
        items.remove(2); // it is a parent of all units of round 3
        let encoded_items = encode_all(items).into_iter().flatten().collect();

        assert!(matches!(
            BackupLoader::new(Loader::new(encoded_items), NODE_ID, SESSION_ID)
                .load_backup()
                .await,
            Err(LoaderError::InconsistentData(_))
        ));
    }

    #[tokio::test]
    async fn backup_with_units_of_one_creator_fails() {
        let items = units_of_creator(produce_units(5, SESSION_ID), NodeIndex(NODE_ID.0 + 1));
        let encoded_items = encode_all(items).into_iter().flatten().collect();

        assert!(matches!(
            BackupLoader::new(Loader::new(encoded_items), NODE_ID, SESSION_ID)
                .load_backup()
                .await,
            Err(LoaderError::InconsistentData(_))
        ));
    }

    #[tokio::test]
    async fn backup_with_wrong_session_fails() {
        let items: Vec<_> = produce_units(5, SESSION_ID + 1)
            .into_iter()
            .flatten()
            .collect();
        let encoded_items = encode_all(items).into_iter().flatten().collect();

        assert!(matches!(
            BackupLoader::new(Loader::new(encoded_items), NODE_ID, SESSION_ID)
                .load_backup()
                .await,
            Err(LoaderError::WrongSession(..))
        ));
    }
}
