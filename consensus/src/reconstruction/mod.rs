use crate::{
    extension::ExtenderUnit,
    runway::ExplicitParents,
    units::{ControlHash, HashFor, Unit, UnitCoord, WrappedUnit},
    NodeMap, Receiver, Sender, Terminator,
};
use futures::{FutureExt, StreamExt};
use log::{debug, trace, warn};

mod dag;
mod parents;

use dag::Dag;
pub use parents::Request;
use parents::{Reconstruction, ReconstructionResult};

const LOG_TARGET: &str = "AlephBFT-reconstruction";

/// A unit with its parents represented explicitly.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ReconstructedUnit<U: Unit> {
    unit: U,
    parents: NodeMap<HashFor<U>>,
}

impl<U: Unit> ReconstructedUnit<U> {
    /// Returns a reconstructed unit if the parents agree with the hash, errors out otherwise.
    pub fn with_parents(unit: U, parents: NodeMap<HashFor<U>>) -> Result<Self, U> {
        match unit.control_hash().combined_hash
            == ControlHash::<U::Hasher>::combine_hashes(&parents)
        {
            true => Ok(ReconstructedUnit { unit, parents }),
            false => Err(unit),
        }
    }

    /// Reconstructs empty parents for a round 0 unit.
    /// Assumes obviously incorrect units with wrong control hashes have been rejected earlier.
    /// Will panic if called for any other kind of unit.
    pub fn initial(unit: U) -> Self {
        let n_members = unit.control_hash().n_members();
        assert!(unit.round() == 0, "Only the zeroth unit can be initial.");
        ReconstructedUnit {
            unit,
            parents: NodeMap::with_size(n_members),
        }
    }

    /// The reconstructed parents, guaranteed to be correct.
    pub fn parents(&self) -> &NodeMap<HashFor<U>> {
        &self.parents
    }

    /// Create an extender unit from this one.
    pub fn extender_unit(&self) -> ExtenderUnit<U::Hasher> {
        ExtenderUnit::new(
            self.unit.creator(),
            self.unit.round(),
            self.hash(),
            self.parents.clone(),
        )
    }
}

impl<U: Unit> Unit for ReconstructedUnit<U> {
    type Hasher = U::Hasher;

    fn hash(&self) -> HashFor<U> {
        self.unit.hash()
    }

    fn coord(&self) -> UnitCoord {
        self.unit.coord()
    }

    fn control_hash(&self) -> &ControlHash<Self::Hasher> {
        self.unit.control_hash()
    }
}

impl<U: Unit> WrappedUnit<U::Hasher> for ReconstructedUnit<U> {
    type Wrapped = U;

    fn unpack(self) -> U {
        self.unit
    }
}

/// The service responsible for reconstructing the structure of the Dag.
/// Receives units containing control hashes and eventually outputs versions
/// with explicit parents in an order conforming to the Dag order.
pub struct Service<U: Unit> {
    reconstruction: Reconstruction<U>,
    dag: Dag<U>,
    units_from_runway: Receiver<U>,
    parents_from_runway: Receiver<ExplicitParents<U::Hasher>>,
    requests_for_runway: Sender<Request<U::Hasher>>,
    units_for_runway: Sender<ReconstructedUnit<U>>,
    units_for_creator: Sender<ReconstructedUnit<U>>,
}

impl<U: Unit> Service<U> {
    /// Create a new reconstruction service with the provided IO channels.
    pub fn new(
        units_from_runway: Receiver<U>,
        parents_from_runway: Receiver<ExplicitParents<U::Hasher>>,
        requests_for_runway: Sender<Request<U::Hasher>>,
        units_for_runway: Sender<ReconstructedUnit<U>>,
        units_for_creator: Sender<ReconstructedUnit<U>>,
    ) -> Self {
        let reconstruction = Reconstruction::new();
        let dag = Dag::new();
        Service {
            reconstruction,
            dag,
            units_from_runway,
            parents_from_runway,
            requests_for_runway,
            units_for_runway,
            units_for_creator,
        }
    }

    fn handle_reconstruction_result(
        &mut self,
        reconstruction_result: ReconstructionResult<U>,
    ) -> bool {
        let (units, requests) = reconstruction_result.into();
        trace!(target: LOG_TARGET, "Reconstructed {} units, and have {} requests.", units.len(), requests.len());
        for request in requests {
            if self.requests_for_runway.unbounded_send(request).is_err() {
                warn!(target: LOG_TARGET, "Request channel should be open.");
                return false;
            }
        }
        for unit in units {
            for unit in self.dag.add_unit(unit) {
                if self.units_for_creator.unbounded_send(unit.clone()).is_err() {
                    warn!(target: LOG_TARGET, "Creator channel should be open.");
                    return false;
                }
                if self.units_for_runway.unbounded_send(unit).is_err() {
                    warn!(target: LOG_TARGET, "Notification channel should be open.");
                    return false;
                }
            }
        }
        true
    }

    /// Run the reconstruction service until terminated.
    pub async fn run(mut self, mut terminator: Terminator) {
        loop {
            let reconstruction_result = futures::select! {
                unit = self.units_from_runway.next() => match unit {
                    Some(unit) => self.reconstruction.add_unit(unit),
                    None => {
                        warn!(target: LOG_TARGET, "Units for reconstruction unexpectedly ended.");
                        return;
                    }
                },
                parents = self.parents_from_runway.next() => match parents {
                    Some((unit, parents)) => self.reconstruction.add_parents(unit, parents),
                    None => {
                        warn!(target: LOG_TARGET, "Parents for reconstruction unexpectedly ended.");
                        return;
                    }
                },
                _ = terminator.get_exit().fuse() => {
                    debug!(target: LOG_TARGET, "Received exit signal.");
                    break;
                }
            };
            if !self.handle_reconstruction_result(reconstruction_result) {
                return;
            }
        }
        debug!(target: LOG_TARGET, "Reconstruction decided to exit.");
        terminator.terminate_sync().await;
    }
}
