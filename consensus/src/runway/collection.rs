use crate::{
    runway::Request,
    units::{UncheckedSignedUnit, ValidationError, Validator},
    Data, Hasher, Keychain, NodeCount, NodeIndex, NodeMap, Receiver, Round, Sender, Signable,
    Signature, SignatureError, UncheckedSigned,
};
use codec::{Decode, Encode};
use futures::{channel::oneshot, FutureExt, StreamExt};
use futures_timer::Delay;
use log::{debug, error, info, warn};
use std::{
    cmp::max,
    collections::hash_map::DefaultHasher,
    fmt::{Display, Formatter, Result as FmtResult},
    hash::{Hash, Hasher as _},
    time::Duration,
};

/// Salt uniquely identifying an initial unit collection instance.
pub type Salt = u64;

fn generate_salt() -> Salt {
    let mut hasher = DefaultHasher::new();
    std::time::Instant::now().hash(&mut hasher);
    hasher.finish()
}

/// A response to the request for the newest unit.
#[derive(Debug, Encode, Decode, Clone)]
pub struct NewestUnitResponse<H: Hasher, D: Data, S: Signature> {
    requester: NodeIndex,
    responder: NodeIndex,
    unit: Option<UncheckedSignedUnit<H, D, S>>,
    salt: Salt,
}

impl<H: Hasher, D: Data, S: Signature> Signable for NewestUnitResponse<H, D, S> {
    type Hash = Vec<u8>;

    fn hash(&self) -> Self::Hash {
        self.encode()
    }
}

impl<H: Hasher, D: Data, S: Signature> crate::Index for NewestUnitResponse<H, D, S> {
    fn index(&self) -> NodeIndex {
        self.responder
    }
}

impl<H: Hasher, D: Data, S: Signature> NewestUnitResponse<H, D, S> {
    /// Create a newest unit response.
    pub fn new(
        requester: NodeIndex,
        responder: NodeIndex,
        unit: Option<UncheckedSignedUnit<H, D, S>>,
        salt: Salt,
    ) -> Self {
        NewestUnitResponse {
            requester,
            responder,
            unit,
            salt,
        }
    }

    /// The data included in this message, i.e. contents of the unit if any.
    pub fn included_data(&self) -> Vec<D> {
        match &self.unit {
            Some(u) => vec![u.as_signable().data().clone()],
            None => Vec::new(),
        }
    }

    /// Who requested this response.
    pub fn requester(&self) -> NodeIndex {
        self.requester
    }
}

/// Ways in which a newest unit response might be wrong.
#[derive(Eq, PartialEq, Debug)]
pub enum Error<H: Hasher, D: Data, S: Signature> {
    WrongSignature,
    SaltMismatch(Salt, Salt),
    InvalidUnit(ValidationError<H, D, S>),
    ForeignUnit(NodeIndex),
}

impl<H: Hasher, D: Data, S: Signature> Display for Error<H, D, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        use Error::*;
        match self {
            WrongSignature => write!(f, "wrong signature"),
            SaltMismatch(expected, got) => {
                write!(f, "mismatched salt, expected {}, got {}", expected, got)
            }
            InvalidUnit(e) => write!(f, "invalid unit: {}", e),
            ForeignUnit(id) => write!(f, "unit from node {:?}", id),
        }
    }
}

impl<H: Hasher, D: Data, S: Signature> From<ValidationError<H, D, S>> for Error<H, D, S> {
    fn from(ve: ValidationError<H, D, S>) -> Self {
        Error::InvalidUnit(ve)
    }
}

impl<H: Hasher, D: Data, S: Signature> From<SignatureError<NewestUnitResponse<H, D, S>, S>>
    for Error<H, D, S>
{
    fn from(_: SignatureError<NewestUnitResponse<H, D, S>, S>) -> Self {
        Error::WrongSignature
    }
}

/// The status of an ongoing collection.
#[derive(PartialEq, Eq, Debug)]
pub enum Status {
    /// Received less than threshold responses, counting the trivial self-response.
    Pending,
    /// Received at least threshold responses, counting the trivial self-response.
    Ready(Round),
    /// Received all possible responses.
    Finished(Round),
}

/// Initial unit collection to figure out at which round we should start unit production.
/// Unfortunately this isn't quite BFT, but it's good enough in many situations.
pub struct Collection<'a, MK: Keychain> {
    keychain: &'a MK,
    validator: &'a Validator<'a, MK>,
    collected_starting_rounds: NodeMap<Round>,
    threshold: NodeCount,
    salt: Salt,
}

impl<'a, MK: Keychain> Collection<'a, MK> {
    /// Create a new collection instance ready to collect responses.
    /// The returned salt should be used to initiate newest unit requests.
    pub fn new(
        keychain: &'a MK,
        validator: &'a Validator<'a, MK>,
        threshold: NodeCount,
    ) -> (Self, Salt) {
        let salt = generate_salt();
        let mut collected_starting_rounds = NodeMap::with_size(keychain.node_count());
        collected_starting_rounds.insert(keychain.index(), 0);
        (
            Collection {
                keychain,
                validator,
                collected_starting_rounds,
                threshold,
                salt,
            },
            salt,
        )
    }

    /// Process a response to a newest unit request.
    pub fn on_newest_response<H: Hasher, D: Data>(
        &mut self,
        unchecked_response: UncheckedSigned<NewestUnitResponse<H, D, MK::Signature>, MK::Signature>,
    ) -> Result<Status, Error<H, D, MK::Signature>> {
        let response = unchecked_response.check(self.keychain)?.into_signable();
        if response.salt != self.salt {
            return Err(Error::SaltMismatch(self.salt, response.salt));
        }
        let round: Round = match response.unit {
            Some(unchecked_unit) => {
                let checked_signed_unit = self.validator.validate_unit(unchecked_unit)?;
                let checked_unit = checked_signed_unit.as_signable();
                if checked_unit.creator() != self.keychain.index() {
                    return Err(Error::ForeignUnit(checked_unit.creator()));
                }
                checked_unit.round() + 1
            }
            None => 0,
        };
        let current_round = *self
            .collected_starting_rounds
            .get(response.responder)
            .unwrap_or(&round);
        if current_round != round {
            debug!(target: "AlephBFT-runway", "Node {} responded with starting unit {}, but now says {}", response.responder.0, current_round, round);
        }
        self.collected_starting_rounds
            .insert(response.responder, max(current_round, round));
        Ok(self.status())
    }

    /// The salt associated with this collection instance.
    pub fn salt(&self) -> Salt {
        self.salt
    }

    /// The current status of the collection.
    pub fn status(&self) -> Status {
        use Status::*;
        let responders = NodeCount(self.collected_starting_rounds.item_count());
        let starting_round: Round = *self.collected_starting_rounds.values().max().unwrap_or(&0);
        if responders == self.keychain.node_count() {
            return Finished(starting_round);
        }
        if responders >= self.threshold {
            return Ready(starting_round);
        }
        Pending
    }
}

/// A runnable wrapper around initial unit collection.
pub struct IO<'a, H: Hasher, D: Data, MK: Keychain> {
    round_for_creator: oneshot::Sender<Round>,
    responses_from_network:
        Receiver<UncheckedSigned<NewestUnitResponse<H, D, MK::Signature>, MK::Signature>>,
    resolved_requests: Sender<Request<H>>,
    collection: Collection<'a, MK>,
}

impl<'a, H: Hasher, D: Data, MK: Keychain> IO<'a, H, D, MK> {
    /// Create the IO instance for the specified collection and channels associated with it.
    pub fn new(
        round_for_creator: oneshot::Sender<Round>,
        responses_from_network: Receiver<
            UncheckedSigned<NewestUnitResponse<H, D, MK::Signature>, MK::Signature>,
        >,
        resolved_requests: Sender<Request<H>>,
        collection: Collection<'a, MK>,
    ) -> Self {
        IO {
            round_for_creator,
            responses_from_network,
            resolved_requests,
            collection,
        }
    }

    fn finish(self, round: Round) {
        if self.round_for_creator.send(round).is_err() {
            error!(target: "AlephBFT-runway", "unable to send starting round to creator");
        }
        if let Err(e) = self
            .resolved_requests
            .unbounded_send(Request::NewestUnit(self.collection.salt()))
        {
            warn!(target: "AlephBFT-runway", "unable to send resolved request:  {}", e);
        }
        info!(target: "AlephBFT-runway", "Finished initial unit collection with status: {:?}", self.collection.status());
    }

    fn status_report(&self) {
        info!(target: "AlephBFT-runway", "Initial unit collection status report: status - {:?}, collected starting rounds - {}", self.collection.status(), self.collection.collected_starting_rounds);
    }

    /// Run the initial unit collection until it sends the initial round.
    pub async fn run(mut self) {
        use Status::*;
        let mut catch_up_delay = futures_timer::Delay::new(Duration::from_secs(5)).fuse();
        let mut delay_passed = false;

        let status_ticker_delay = Duration::from_secs(10);
        let mut status_ticker = Delay::new(status_ticker_delay).fuse();

        loop {
            futures::select! {
                response = self.responses_from_network.next() => {
                    let response = match response {
                        Some(response) => response,
                        None => {
                            warn!(target: "AlephBFT-runway", "Response channel closed.");
                            info!(target: "AlephBFT-runway", "Finished initial unit collection with status: {:?}", self.collection.status());
                            return;
                        }
                    };
                    match self.collection.on_newest_response(response) {
                        Ok(Pending) => (),
                        Ok(Ready(round)) => if delay_passed {
                            self.finish(round);
                            return;
                        },
                        Ok(Finished(round)) => {
                            self.finish(round);
                            return;
                        },
                        Err(e) => warn!(target: "AlephBFT-runway", "Received wrong newest unit response: {}", e),
                    }
                },
                _ = catch_up_delay => match self.collection.status() {
                    Pending => {
                        delay_passed = true;
                        info!(target: "AlephBFT-runway", "Catch up delay passed.");
                        self.status_report();
                    },
                    Ready(round) | Finished(round)  => {
                        self.finish(round);
                        return;
                    },
                },
                _ = &mut status_ticker => {
                    self.status_report();
                    status_ticker = Delay::new(status_ticker_delay).fuse();
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Collection as GenericCollection, Error, NewestUnitResponse as GenericNewestUnitResponse,
        Salt, Status::*,
    };
    use crate::{
        creation::Creator as GenericCreator,
        units::{
            FullUnit as GenericFullUnit, PreUnit as GenericPreUnit,
            UncheckedSignedUnit as GenericUncheckedSignedUnit, Validator as GenericValidator,
        },
        Index, NodeCount, NodeIndex, SessionId, Signed, UncheckedSigned,
    };
    use aleph_bft_mock::{Data, Hasher64, Keychain, Signature};
    use std::iter::{once, repeat};

    type Collection<'a> = GenericCollection<'a, Keychain>;
    type Validator<'a> = GenericValidator<'a, Keychain>;
    type Creator = GenericCreator<Hasher64>;
    type PreUnit = GenericPreUnit<Hasher64>;
    type FullUnit = GenericFullUnit<Hasher64, Data>;
    type UncheckedSignedUnit = GenericUncheckedSignedUnit<Hasher64, Data, Signature>;
    type NewestUnitResponse = GenericNewestUnitResponse<Hasher64, Data, Signature>;
    type UncheckedSignedNewestUnitResponse = UncheckedSigned<NewestUnitResponse, Signature>;

    fn keychain_set(n_members: NodeCount) -> Vec<Keychain> {
        let mut result = Vec::new();
        for i in 0..n_members.0 {
            result.push(Keychain::new(n_members, NodeIndex(i)));
        }
        result
    }

    async fn create_responses<
        'a,
        R: Iterator<Item = (&'a Keychain, Option<UncheckedSignedUnit>)>,
    >(
        presponses: R,
        salt: Salt,
        requester: NodeIndex,
    ) -> Vec<UncheckedSignedNewestUnitResponse> {
        let mut result = Vec::new();
        for (keychain, maybe_unit) in presponses {
            let response = NewestUnitResponse::new(requester, keychain.index(), maybe_unit, salt);
            result.push(Signed::sign(response, keychain).await.into_unchecked());
        }
        result
    }

    async fn preunit_to_unchecked_signed_unit(
        pu: PreUnit,
        session_id: SessionId,
        keychain: &Keychain,
    ) -> UncheckedSignedUnit {
        let full_unit = FullUnit::new(pu, 0, session_id);
        let signed_unit = Signed::sign(full_unit, keychain).await;
        signed_unit.into()
    }

    #[test]
    fn pending_with_no_messages() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychain = Keychain::new(n_members, creator_id);
        let validator = Validator::new(session_id, &keychain, max_round, threshold);
        let (collection, _) = Collection::new(&keychain, &validator, threshold);
        assert_eq!(collection.status(), Pending);
    }

    #[tokio::test]
    async fn pending_with_too_few_messages() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychains = keychain_set(n_members);
        let keychain = &keychains[0];
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (mut collection, salt) = Collection::new(keychain, &validator, threshold);
        let responses = create_responses(
            keychains.iter().skip(1).take(3).zip(repeat(None)),
            salt,
            creator_id,
        )
        .await;
        for response in responses {
            assert_eq!(collection.on_newest_response(response), Ok(Pending));
        }
        assert_eq!(collection.status(), Pending);
    }

    #[tokio::test]
    async fn pending_with_repeated_messages() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychains = keychain_set(n_members);
        let keychain = &keychains[0];
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (mut collection, salt) = Collection::new(keychain, &validator, threshold);
        let responses = create_responses(
            repeat(&keychains[1]).take(43).zip(repeat(None)),
            salt,
            creator_id,
        )
        .await;
        for response in responses {
            assert_eq!(collection.on_newest_response(response), Ok(Pending));
        }
        assert_eq!(collection.status(), Pending);
    }

    #[tokio::test]
    async fn ready_with_just_enough_messages() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychains = keychain_set(n_members);
        let keychain = &keychains[0];
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (mut collection, salt) = Collection::new(keychain, &validator, threshold);
        let responses = create_responses(
            keychains.iter().skip(1).take(4).zip(repeat(None)),
            salt,
            creator_id,
        )
        .await;
        for response in responses.iter().take(3) {
            assert_eq!(collection.on_newest_response(response.clone()), Ok(Pending));
        }
        assert_eq!(
            collection.on_newest_response(responses[3].clone()),
            Ok(Ready(0))
        );
        assert_eq!(collection.status(), Ready(0));
    }

    #[tokio::test]
    async fn finished_and_higher_starting_round_with_last_message() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychains = keychain_set(n_members);
        let keychain = &keychains[0];
        let creator = Creator::new(creator_id, n_members);
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (mut collection, salt) = Collection::new(keychain, &validator, threshold);
        let (preunit, _) = creator.create_unit(0).expect("Creation should succeed.");
        let unit = preunit_to_unchecked_signed_unit(preunit, session_id, keychain).await;
        let responses = create_responses(
            keychains
                .iter()
                .skip(1)
                .zip(repeat(None).take(5).chain(once(Some(unit)))),
            salt,
            creator_id,
        )
        .await;
        for response in responses.iter().take(3) {
            assert_eq!(collection.on_newest_response(response.clone()), Ok(Pending));
        }
        for response in responses.iter().skip(3).take(2) {
            assert_eq!(
                collection.on_newest_response(response.clone()),
                Ok(Ready(0))
            );
        }
        assert_eq!(
            collection.on_newest_response(responses[5].clone()),
            Ok(Finished(1))
        );
        assert_eq!(collection.status(), Finished(1));
    }

    #[tokio::test]
    async fn detects_salt_mismatch() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let max_round = 2;
        let keychains = keychain_set(n_members);
        let keychain = &keychains[0];
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (mut collection, salt) = Collection::new(keychain, &validator, threshold);
        let other_salt = salt + 1;
        let responses = create_responses(
            keychains.iter().skip(1).zip(repeat(None)),
            other_salt,
            creator_id,
        )
        .await;
        for response in responses {
            assert_eq!(
                collection.on_newest_response(response),
                Err(Error::SaltMismatch(salt, other_salt))
            );
        }
        assert_eq!(collection.status(), Pending);
    }

    #[tokio::test]
    async fn detects_invalid_unit() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let session_id = 0;
        let wrong_session_id = 43;
        let max_round = 2;
        let keychains = keychain_set(n_members);
        let keychain = &keychains[0];
        let creator = Creator::new(creator_id, n_members);
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (mut collection, salt) = Collection::new(keychain, &validator, threshold);
        let (preunit, _) = creator.create_unit(0).expect("Creation should succeed.");
        let unit = preunit_to_unchecked_signed_unit(preunit, wrong_session_id, keychain).await;
        let responses = create_responses(
            keychains.iter().skip(1).zip(repeat(Some(unit.clone()))),
            salt,
            creator_id,
        )
        .await;
        for response in responses {
            match collection.on_newest_response(response) {
                Err(Error::InvalidUnit(_)) => (),
                result => panic!("Expected invalid unit result got {:?}", result),
            }
        }
        assert_eq!(collection.status(), Pending);
    }

    #[tokio::test]
    async fn detects_foreign_unit() {
        let n_members = NodeCount(7);
        let threshold = NodeCount(5);
        let creator_id = NodeIndex(0);
        let other_creator_id = NodeIndex(1);
        let session_id = 0;
        let max_round = 2;
        let keychains = keychain_set(n_members);
        let keychain = &keychains[0];
        let creator = Creator::new(other_creator_id, n_members);
        let validator = Validator::new(session_id, keychain, max_round, threshold);
        let (mut collection, salt) = Collection::new(keychain, &validator, threshold);
        let (preunit, _) = creator.create_unit(0).expect("Creation should succeed.");
        let unit = preunit_to_unchecked_signed_unit(preunit, session_id, &keychains[1]).await;
        let responses = create_responses(
            keychains.iter().skip(1).zip(repeat(Some(unit.clone()))),
            salt,
            creator_id,
        )
        .await;
        for response in responses {
            assert_eq!(
                collection.on_newest_response(response),
                Err(Error::ForeignUnit(other_creator_id))
            );
        }
        assert_eq!(collection.status(), Pending);
    }
}
