use crate::{
    config::Config,
    units::{PreUnit, SignedUnit, Unit},
    DataProvider, MultiKeychain, Receiver, Round, Sender, Terminator,
};
use futures::{
    channel::{
        mpsc::{SendError, TrySendError},
        oneshot,
    },
    FutureExt, StreamExt,
};
use futures_timer::Delay;
use log::{debug, error, trace, warn};

mod collector;
mod creator;
mod packer;

pub use creator::Creator;
use packer::Packer;

const LOG_TARGET: &str = "AlephBFT-creator";

enum CreatorError {
    OutChannelClosed(SendError),
    ParentsChannelClosed,
}

impl<T> From<TrySendError<T>> for CreatorError {
    fn from(e: TrySendError<T>) -> Self {
        Self::OutChannelClosed(e.into_send_error())
    }
}

pub struct IO<U: Unit, MK: MultiKeychain, DP: DataProvider> {
    pub incoming_parents: Receiver<U>,
    pub outgoing_units: Sender<SignedUnit<U::Hasher, DP::Output, MK>>,
    pub data_provider: DP,
}

async fn create_unit<U: Unit>(
    round: Round,
    creator: &mut Creator<U::Hasher>,
    incoming_parents: &mut Receiver<U>,
) -> Result<PreUnit<U::Hasher>, CreatorError> {
    loop {
        match creator.create_unit(round) {
            Ok(unit) => return Ok(unit),
            Err(err) => {
                trace!(target: LOG_TARGET, "Creator unable to create a new unit at round {}: {}.", round, err)
            }
        }
        process_unit(creator, incoming_parents).await?;
    }
}

/// Tries to process a single parent from given `incoming_parents` receiver.
/// Returns error when `incoming_parents` channel is closed.
async fn process_unit<U: Unit>(
    creator: &mut Creator<U::Hasher>,
    incoming_parents: &mut Receiver<U>,
) -> anyhow::Result<(), CreatorError> {
    let unit = incoming_parents
        .next()
        .await
        .ok_or(CreatorError::ParentsChannelClosed)?;
    creator.add_unit(&unit);
    Ok(())
}

async fn keep_processing_units<U: Unit>(
    creator: &mut Creator<U::Hasher>,
    incoming_parents: &mut Receiver<U>,
) -> anyhow::Result<(), CreatorError> {
    loop {
        process_unit(creator, incoming_parents).await?;
    }
}

async fn keep_processing_units_until<U: Unit>(
    creator: &mut Creator<U::Hasher>,
    incoming_parents: &mut Receiver<U>,
    until: Delay,
) -> anyhow::Result<(), CreatorError> {
    futures::select! {
        result = keep_processing_units(creator, incoming_parents).fuse() => {
            result?
        },
        _ = until.fuse() => {
            debug!(target: LOG_TARGET, "Delay passed.");
        },
    }
    Ok(())
}

/// A process responsible for creating new units. It receives all the units added locally to the Dag
/// via the `incoming_parents` channel. It creates units according to an internal strategy respecting
/// always the following constraints: if round is equal to 0, U has no parents, otherwise for a unit U of round r > 0
/// - all U's parents are from round (r-1),
/// - all U's parents are created by different nodes,
/// - one of U's parents is the (r-1)-round unit by U's creator,
/// - U has > floor(2*N/3) parents.
/// - U will appear in the channel only if all U's parents appeared there before
///
/// The currently implemented strategy creates the unit U according to a delay schedule and when enough
/// candidates for parents are available for all the above constraints to be satisfied.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/internals.html
/// Section 5.1 for a discussion of this component.
pub async fn run<U: Unit, MK: MultiKeychain, DP: DataProvider>(
    conf: Config,
    mut io: IO<U, MK, DP>,
    keychain: MK,
    mut starting_round: oneshot::Receiver<Option<Round>>,
    mut terminator: Terminator,
) {
    futures::select! {
        _ = read_starting_round_and_run_creator(conf, &mut io, keychain, &mut starting_round).fuse() =>
            debug!(target: LOG_TARGET, "Creator is about to finish."),
        _ = terminator.get_exit().fuse() =>
            debug!(target: LOG_TARGET, "Received an exit signal."),
    }

    terminator.terminate_sync().await;
}

async fn read_starting_round_and_run_creator<U: Unit, MK: MultiKeychain, DP: DataProvider>(
    conf: Config,
    io: &mut IO<U, MK, DP>,
    keychain: MK,
    starting_round: &mut oneshot::Receiver<Option<Round>>,
) {
    let maybe_round = starting_round.await;
    let starting_round = match maybe_round {
        Ok(Some(round)) => round,
        Ok(None) => {
            warn!(target: LOG_TARGET, "None starting round provided. Exiting.");
            return;
        }
        Err(e) => {
            error!(target: LOG_TARGET, "Starting round not provided: {}", e);
            return;
        }
    };

    if let Err(err) = run_creator(conf, io, keychain, starting_round).await {
        match err {
            CreatorError::OutChannelClosed(e) => {
                warn!(target: LOG_TARGET, "Notification send error: {}. Exiting.", e)
            }
            CreatorError::ParentsChannelClosed => {
                debug!(target: LOG_TARGET, "Incoming parent channel closed, exiting.")
            }
        }
    }
}

async fn run_creator<U: Unit, MK: MultiKeychain, DP: DataProvider>(
    conf: Config,
    io: &mut IO<U, MK, DP>,
    keychain: MK,
    starting_round: Round,
) -> anyhow::Result<(), CreatorError> {
    let node_id = conf.node_ix();
    let n_members = conf.n_members();
    let create_delay = conf.delay_config().unit_creation_delay.clone();
    let max_round = conf.max_round();
    let session_id = conf.session_id();
    let mut creator = Creator::new(node_id, n_members);
    let packer = Packer::new(keychain, session_id);
    let incoming_parents = &mut io.incoming_parents;
    let outgoing_units = &io.outgoing_units;
    let data_provider = &mut io.data_provider;

    debug!(target: LOG_TARGET, "Creator starting from round {}", starting_round);
    for round in starting_round..max_round {
        // Skip waiting if someone created a unit of a higher round.
        // In such a case at least 2/3 nodes created units from this round so we aren't skipping a
        // delay we should observe.
        let skip_delay = creator.current_round() > round;
        if !skip_delay {
            let delay = Delay::new(create_delay(round.into()));

            keep_processing_units_until(&mut creator, incoming_parents, delay).await?;
        }

        let preunit = create_unit(round, &mut creator, incoming_parents).await?;
        trace!(target: LOG_TARGET, "Created a new preunit {:?} at round {:?}.", preunit, round);
        let data = data_provider.get_data().await;
        trace!(target: LOG_TARGET, "Received data: {:?}.", data);
        let unit = packer.pack(preunit, data);

        outgoing_units.unbounded_send(unit)?;
    }

    warn!(target: LOG_TARGET, "Maximum round reached. Not creating another unit.");
    Ok(())
}
