use crate::{
    config::DelaySchedule,
    network::UnitMessageTo,
    units::{UncheckedSignedUnit, Validator},
    Data, Hasher, Keychain, MultiKeychain, NodeIndex, Receiver, Round, Sender, Signable, Signature,
    UncheckedSigned,
};
use codec::{Decode, Encode};
use futures::{channel::oneshot, Future};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher as _},
};

mod service;

pub use service::{Collection, IO};

const LOG_TARGET: &str = "AlephBFT-collection";

/// Salt uniquely identifying an initial unit collection instance.
pub type Salt = u64;

fn generate_salt() -> Salt {
    let mut hasher = DefaultHasher::new();
    std::time::Instant::now().hash(&mut hasher);
    hasher.finish()
}

/// A response to the request for the newest unit.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default, Decode, Encode)]
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
            Some(u) => u.as_signable().included_data(),
            None => Vec::new(),
        }
    }
}

pub type CollectionResponse<H, D, MK> = UncheckedSigned<
    NewestUnitResponse<H, D, <MK as Keychain>::Signature>,
    <MK as Keychain>::Signature,
>;

#[cfg(feature = "initial_unit_collection")]
pub fn initial_unit_collection<'a, H: Hasher, D: Data, MK: MultiKeychain>(
    keychain: &'a MK,
    validator: &'a Validator<MK>,
    messages_for_network: Sender<UnitMessageTo<H, D, MK::Signature>>,
    starting_round_sender: oneshot::Sender<Option<Round>>,
    starting_round_from_backup: Round,
    responses_from_network: Receiver<CollectionResponse<H, D, MK>>,
    request_delay: DelaySchedule,
) -> Result<impl Future<Output = ()> + 'a, ()> {
    let collection = Collection::new(keychain, validator);

    let collection = IO::new(
        starting_round_sender,
        starting_round_from_backup,
        responses_from_network,
        messages_for_network,
        collection,
        request_delay,
    );
    Ok(collection.run())
}

/// A trivial start that doesn't actually perform the initial unit collection.
#[cfg(not(feature = "initial_unit_collection"))]
pub fn initial_unit_collection(
    _keychain: &'a MK,
    _validator: &'a Validator<MK>,
    _messages_for_network: Sender<UnitMessageTo<H, D, MK::Signature>>,
    starting_round_sender: oneshot::Sender<Option<Round>>,
    starting_round_from_backup: Round,
    _responses_from_network: Receiver<CollectionResponse<H, D, MK>>,
    _request_delay: DelaySchedule,
) -> Result<impl Future<Output = ()>, ()> {
    if let Err(e) = starting_round_sender.send(Some(starting_round_from_backup)) {
        error!(target: LOG_TARGET, "Unable to send the starting round: {}", e);
        return Err(());
    }
    Ok(async {})
}
