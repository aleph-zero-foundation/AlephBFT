use crate::{
    extension::ExtenderUnit,
    runway::{NotificationIn, NotificationOut},
    units::{ControlHash, Unit},
    Hasher, NodeMap, Receiver, Sender, Terminator,
};
use futures::{FutureExt, StreamExt};
use log::{debug, warn};

mod dag;
mod parents;

use dag::Dag;
use parents::{Reconstruction, ReconstructionResult, Request};

const LOG_TARGET: &str = "AlephBFT-reconstruction";

/// A unit with its parents represented explicitly.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ReconstructedUnit<H: Hasher> {
    unit: Unit<H>,
    parents: NodeMap<H::Hash>,
}

impl<H: Hasher> ReconstructedUnit<H> {
    /// Returns a reconstructed unit if the parents agree with the hash, errors out otherwise.
    pub fn with_parents(unit: Unit<H>, parents: NodeMap<H::Hash>) -> Result<Self, Unit<H>> {
        match unit.control_hash().combined_hash == ControlHash::<H>::combine_hashes(&parents) {
            true => Ok(ReconstructedUnit { unit, parents }),
            false => Err(unit),
        }
    }

    /// Reconstructs empty parents for a round 0 unit.
    /// Assumes obviously incorrect units with wrong control hashes have been rejected earlier.
    /// Will panic if called for any other kind of unit.
    pub fn initial(unit: Unit<H>) -> Self {
        let n_members = unit.control_hash().n_members();
        assert!(unit.round() == 0, "Only the zeroth unit can be initial.");
        ReconstructedUnit {
            unit,
            parents: NodeMap::with_size(n_members),
        }
    }

    /// The reconstructed parents, guaranteed to be correct.
    pub fn parents(&self) -> &NodeMap<H::Hash> {
        &self.parents
    }

    /// The hash of the unit.
    pub fn hash(&self) -> H::Hash {
        self.unit.hash()
    }

    fn unit(&self) -> Unit<H> {
        self.unit.clone()
    }

    fn extender_unit(&self) -> ExtenderUnit<H> {
        ExtenderUnit::new(
            self.unit.creator(),
            self.unit.round(),
            self.hash(),
            self.parents.clone(),
        )
    }
}

/// The service responsible for reconstructing the structure of the Dag.
/// Receives units containing control hashes and eventually outputs versions
/// with explicit parents in an order conforming to the Dag order.
pub struct Service<H: Hasher> {
    reconstruction: Reconstruction<H>,
    dag: Dag<H>,
    notifications_from_runway: Receiver<NotificationIn<H>>,
    notifications_for_runway: Sender<NotificationOut<H>>,
    units_for_creator: Sender<Unit<H>>,
    units_for_extender: Sender<ExtenderUnit<H>>,
}

enum Output<H: Hasher> {
    Unit(ReconstructedUnit<H>),
    Request(Request<H>),
}

impl<H: Hasher> Service<H> {
    /// Create a new reconstruction service with the provided IO channels.
    pub fn new(
        notifications_from_runway: Receiver<NotificationIn<H>>,
        notifications_for_runway: Sender<NotificationOut<H>>,
        units_for_creator: Sender<Unit<H>>,
        units_for_extender: Sender<ExtenderUnit<H>>,
    ) -> Self {
        let reconstruction = Reconstruction::new();
        let dag = Dag::new();
        Service {
            reconstruction,
            dag,
            notifications_from_runway,
            notifications_for_runway,
            units_for_creator,
            units_for_extender,
        }
    }

    fn handle_reconstruction_result(
        &mut self,
        reconstruction_result: ReconstructionResult<H>,
    ) -> Vec<Output<H>> {
        use Output::*;
        let mut result = Vec::new();
        let (units, requests) = reconstruction_result.into();
        result.append(&mut requests.into_iter().map(Request).collect());
        for unit in units {
            result.append(&mut self.dag.add_unit(unit).into_iter().map(Unit).collect());
        }
        result
    }

    fn handle_notification(&mut self, notification: NotificationIn<H>) -> Vec<Output<H>> {
        use NotificationIn::*;
        let mut result = Vec::new();
        match notification {
            NewUnits(units) => {
                for unit in units {
                    let reconstruction_result = self.reconstruction.add_unit(unit);
                    result.append(&mut self.handle_reconstruction_result(reconstruction_result));
                }
            }
            UnitParents(unit, parents) => {
                let reconstruction_result = self.reconstruction.add_parents(unit, parents);
                result.append(&mut self.handle_reconstruction_result(reconstruction_result))
            }
        }
        result
    }

    fn handle_output(&mut self, output: Output<H>) -> bool {
        use Output::*;
        match output {
            Request(request) => {
                if self
                    .notifications_for_runway
                    .unbounded_send(request.into())
                    .is_err()
                {
                    warn!(target: LOG_TARGET, "Notification channel should be open.");
                    return false;
                }
            }
            Unit(unit) => {
                if self.units_for_creator.unbounded_send(unit.unit()).is_err() {
                    warn!(target: LOG_TARGET, "Creator channel should be open.");
                    return false;
                }
                if self
                    .units_for_extender
                    .unbounded_send(unit.extender_unit())
                    .is_err()
                {
                    warn!(target: LOG_TARGET, "Extender channel should be open.");
                    return false;
                }
                if self
                    .notifications_for_runway
                    .unbounded_send(NotificationOut::AddedToDag(
                        unit.hash(),
                        unit.parents().clone().into_values().collect(),
                    ))
                    .is_err()
                {
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
            futures::select! {
                n = self.notifications_from_runway.next() => match n {
                    Some(notification) => for output in self.handle_notification(notification) {
                        if !self.handle_output(output) {
                            return;
                        }
                    },
                    None => {
                        warn!(target: LOG_TARGET, "Notifications for reconstruction unexpectedly ended.");
                        return;
                    }
                },
                _ = terminator.get_exit().fuse() => {
                    debug!(target: LOG_TARGET, "Received exit signal.");
                    break;
                }
            }
        }
        debug!(target: LOG_TARGET, "Reconstruction decided to exit.");
        terminator.terminate_sync().await;
    }
}
