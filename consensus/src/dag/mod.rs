//! Converts units from the network into ones that are in the Dag, in the correct order.
use std::collections::HashMap;

use crate::{
    alerts::{Alert, ForkingNotification},
    units::{
        SignedUnit, UncheckedSignedUnit, Unit, UnitStore, Validator as UnitValidator, WrappedUnit,
    },
    Data, Hasher, MultiKeychain,
};
use log::{debug, trace, warn};

mod reconstruction;
mod validation;

pub use reconstruction::{ReconstructedUnit, Request};
use reconstruction::{Reconstruction, ReconstructionResult};
pub use validation::ValidatorStatus as DagStatus;
use validation::{Error as ValidationError, Validator};

const LOG_TARGET: &str = "AlephBFT-dag";

pub type DagUnit<H, D, MK> = ReconstructedUnit<SignedUnit<H, D, MK>>;

/// The result of sending some information to the Dag.
pub struct DagResult<H: Hasher, D: Data, MK: MultiKeychain> {
    /// Units added to the dag.
    pub units: Vec<DagUnit<H, D, MK>>,
    /// Requests for more information.
    pub requests: Vec<Request<H>>,
    /// Alerts raised due to encountered forks.
    pub alerts: Vec<Alert<H, D, MK::Signature>>,
}

impl<H: Hasher, D: Data, MK: MultiKeychain> DagResult<H, D, MK> {
    fn empty() -> Self {
        DagResult {
            units: Vec::new(),
            requests: Vec::new(),
            alerts: Vec::new(),
        }
    }

    fn alert(alert: Alert<H, D, MK::Signature>) -> Self {
        DagResult {
            units: Vec::new(),
            requests: Vec::new(),
            alerts: vec![alert],
        }
    }

    fn add_alert(&mut self, alert: Alert<H, D, MK::Signature>) {
        self.alerts.push(alert);
    }

    fn accumulate(&mut self, other: DagResult<H, D, MK>) {
        let DagResult {
            mut units,
            mut requests,
            mut alerts,
        } = other;
        self.units.append(&mut units);
        self.requests.append(&mut requests);
        self.alerts.append(&mut alerts);
    }
}

impl<H: Hasher, D: Data, MK: MultiKeychain> From<ReconstructionResult<SignedUnit<H, D, MK>>>
    for DagResult<H, D, MK>
{
    fn from(other: ReconstructionResult<SignedUnit<H, D, MK>>) -> Self {
        let ReconstructionResult { units, requests } = other;
        DagResult {
            units,
            requests,
            alerts: Vec::new(),
        }
    }
}

/// The Dag ensuring that all units from the network get returned reconstructed in the correct order.
pub struct Dag<H: Hasher, D: Data, MK: MultiKeychain> {
    validator: Validator<H, D, MK>,
    reconstruction: Reconstruction<SignedUnit<H, D, MK>>,
}

impl<H: Hasher, D: Data, MK: MultiKeychain> Dag<H, D, MK> {
    /// A new dag using the provided unit validator under the hood.
    pub fn new(unit_validator: UnitValidator<MK>) -> Self {
        Dag {
            validator: Validator::new(unit_validator),
            reconstruction: Reconstruction::new(),
        }
    }

    fn handle_validation_error(error: ValidationError<H, D, MK>) -> DagResult<H, D, MK> {
        use ValidationError::*;
        match error {
            Invalid(e) => {
                warn!(target: LOG_TARGET, "Received unit failing validation: {}", e);
                DagResult::empty()
            }
            Duplicate(unit) => {
                trace!(target: LOG_TARGET, "Received unit with hash {:?} again.", unit.hash());
                DagResult::empty()
            }
            Uncommitted(unit) => {
                debug!(target: LOG_TARGET, "Received unit with hash {:?} created by known forker {:?} for which we don't have a commitment, discarding.", unit.hash(), unit.creator());
                DagResult::empty()
            }
            NewForker(alert) => {
                warn!(target: LOG_TARGET, "New forker detected.");
                trace!(target: LOG_TARGET, "Created alert: {:?}.", alert);
                DagResult::alert(*alert)
            }
        }
    }

    /// Add a unit to the Dag.
    pub fn add_unit<U: WrappedUnit<H, Wrapped = SignedUnit<H, D, MK>>>(
        &mut self,
        unit: UncheckedSignedUnit<H, D, MK::Signature>,
        store: &UnitStore<U>,
    ) -> DagResult<H, D, MK> {
        match self.validator.validate(unit, store) {
            Ok(unit) => self.reconstruction.add_unit(unit).into(),
            Err(e) => Self::handle_validation_error(e),
        }
    }

    /// Add parents of a unit to the Dag.
    pub fn add_parents<U: WrappedUnit<H, Wrapped = SignedUnit<H, D, MK>>>(
        &mut self,
        unit_hash: H::Hash,
        parents: Vec<UncheckedSignedUnit<H, D, MK::Signature>>,
        store: &UnitStore<U>,
    ) -> DagResult<H, D, MK> {
        use ValidationError::*;
        let mut result = DagResult::empty();
        let mut parent_hashes = HashMap::new();
        for unit in parents {
            let unit = match self.validator.validate(unit, store) {
                Ok(unit) => {
                    result.accumulate(self.reconstruction.add_unit(unit.clone()).into());
                    unit
                }
                Err(Invalid(e)) => {
                    warn!(target: LOG_TARGET, "Received parent failing validation: {}", e);
                    return result;
                }
                Err(Duplicate(unit)) => {
                    trace!(target: LOG_TARGET, "Received parent with hash {:?} again.", unit.hash());
                    unit
                }
                Err(Uncommitted(unit)) => {
                    debug!(target: LOG_TARGET, "Received uncommitted parent {:?}, we should get the commitment soon.", unit.hash());
                    unit
                }
                Err(NewForker(alert)) => {
                    warn!(target: LOG_TARGET, "New forker detected.");
                    trace!(target: LOG_TARGET, "Created alert: {:?}.", alert);
                    result.add_alert(*alert);
                    // technically this was a correct unit, so we could have passed it on,
                    // but this will happen at most once and we will receive the parent
                    // response again, so we just discard it now
                    return result;
                }
            };
            parent_hashes.insert(unit.coord(), unit.hash());
        }
        result.accumulate(
            self.reconstruction
                .add_parents(unit_hash, parent_hashes)
                .into(),
        );
        result
    }

    /// Process a forking notification, potentially returning a lot of unit processing results.
    pub fn process_forking_notification<U: WrappedUnit<H, Wrapped = SignedUnit<H, D, MK>>>(
        &mut self,
        notification: ForkingNotification<H, D, MK::Signature>,
        store: &UnitStore<U>,
    ) -> DagResult<H, D, MK> {
        use ForkingNotification::*;
        let mut result = DagResult::empty();
        match notification {
            Forker((unit, other_unit)) => {
                // Just treat them as normal incoming units, if they are a forking proof
                // this will either trigger a new forker or we already knew about this one.
                result.accumulate(self.add_unit(unit, store));
                result.accumulate(self.add_unit(other_unit, store));
            }
            Units(units) => {
                for unit in units {
                    result.accumulate(match self.validator.validate_committed(unit, store) {
                        Ok(unit) => self.reconstruction.add_unit(unit).into(),
                        Err(e) => Self::handle_validation_error(e),
                    })
                }
            }
        }
        result
    }

    /// Notify the dag that a unit has finished processing and can be cleared from the cache.
    pub fn finished_processing(&mut self, hash: &H::Hash) {
        self.validator.finished_processing(hash);
    }

    pub fn status(&self) -> DagStatus {
        self.validator.status()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        alerts::ForkingNotification,
        dag::{Dag, DagResult, Request},
        units::{
            random_full_parent_units_up_to, random_unit_with_parents, Unit, UnitStore,
            Validator as UnitValidator, WrappedSignedUnit,
        },
        NodeCount, NodeIndex, Signed,
    };
    use aleph_bft_mock::Keychain;

    #[test]
    fn accepts_initial_units() {
        let node_count = NodeCount(4);
        let node_id = NodeIndex(0);
        let session_id = 43;
        let max_round = 2137;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let validator = UnitValidator::new(session_id, keychains[node_id.0], max_round);
        let mut dag = Dag::new(validator);
        for unit in random_full_parent_units_up_to(0, node_count, session_id)
            .into_iter()
            .flatten()
            .map(|unit| {
                let keychain = keychains
                    .get(unit.creator().0)
                    .expect("we have the keychains");
                Signed::sign(unit, keychain)
            })
        {
            let DagResult {
                units,
                requests,
                alerts,
            } = dag.add_unit(unit.into(), &store);
            assert_eq!(units.len(), 1);
            assert!(requests.is_empty());
            assert!(alerts.is_empty());
        }
    }

    #[test]
    fn accepts_units_in_order() {
        let node_count = NodeCount(4);
        let node_id = NodeIndex(0);
        let session_id = 43;
        let max_round = 2137;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let validator = UnitValidator::new(session_id, keychains[node_id.0], max_round);
        let mut dag = Dag::new(validator);
        for unit in random_full_parent_units_up_to(13, node_count, session_id)
            .into_iter()
            .flatten()
            .map(|unit| {
                let keychain = keychains
                    .get(unit.creator().0)
                    .expect("we have the keychains");
                Signed::sign(unit, keychain)
            })
        {
            let DagResult {
                units,
                requests,
                alerts,
            } = dag.add_unit(unit.into(), &store);
            assert_eq!(units.len(), 1);
            assert!(requests.is_empty());
            assert!(alerts.is_empty());
        }
    }

    #[test]
    fn accepts_units_in_reverse_order() {
        let node_count = NodeCount(4);
        let node_id = NodeIndex(0);
        let session_id = 43;
        let max_round = 2137;
        let total_rounds = 13;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let validator = UnitValidator::new(session_id, keychains[node_id.0], max_round);
        let mut dag = Dag::new(validator);
        for unit in random_full_parent_units_up_to(total_rounds, node_count, session_id)
            .into_iter()
            .flatten()
            .rev()
            .map(|unit| {
                let keychain = keychains
                    .get(unit.creator().0)
                    .expect("we have the keychains");
                Signed::sign(unit, keychain)
            })
        {
            let unit_round = unit.round();
            let unit_creator = unit.creator();
            let DagResult {
                units,
                requests,
                alerts,
            } = dag.add_unit(unit.into(), &store);
            assert!(alerts.is_empty());
            match unit_round {
                0 => match unit_creator {
                    NodeIndex(0) => {
                        assert_eq!(units.len(), (total_rounds * 4 + 1).into());
                        assert!(requests.is_empty());
                    }
                    _ => {
                        assert_eq!(units.len(), 1);
                        assert!(requests.is_empty());
                    }
                },
                _ => {
                    assert_eq!(requests.len(), 4);
                    assert!(units.is_empty());
                }
            }
        }
    }

    #[test]
    fn alerts_on_fork() {
        let node_count = NodeCount(4);
        let node_id = NodeIndex(0);
        let session_id = 43;
        let max_round = 2137;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let mut store = UnitStore::new(node_count);
        let validator = UnitValidator::new(session_id, keychains[node_id.0], max_round);
        let mut dag = Dag::new(validator);
        let forker_id = NodeIndex(3);
        let keychain = keychains.get(forker_id.0).expect("we have the keychain");
        let unit = random_full_parent_units_up_to(0, node_count, session_id)
            .get(0)
            .expect("we have initial units")
            .get(forker_id.0)
            .expect("We have the forker")
            .clone();
        let unit = Signed::sign(unit, keychain);
        let mut fork = random_full_parent_units_up_to(0, node_count, session_id)
            .get(0)
            .expect("we have initial units")
            .get(forker_id.0)
            .expect("We have the forker")
            .clone();
        // we might have randomly created an identical "fork"
        while fork.hash() == unit.hash() {
            fork = random_full_parent_units_up_to(0, node_count, session_id)
                .get(0)
                .expect("we have initial units")
                .get(forker_id.0)
                .expect("We have the forker")
                .clone();
        }
        let fork = Signed::sign(fork, keychain);
        let DagResult {
            mut units,
            requests,
            alerts,
        } = dag.add_unit(unit.into(), &store);
        assert_eq!(units.len(), 1);
        assert!(requests.is_empty());
        assert!(alerts.is_empty());
        store.insert(units.pop().expect("just checked"));
        let DagResult {
            units,
            requests,
            alerts,
        } = dag.add_unit(fork.into(), &store);
        assert!(units.is_empty());
        assert!(requests.is_empty());
        assert_eq!(alerts.len(), 1);
    }

    #[test]
    fn detects_fork_through_notification() {
        let node_count = NodeCount(7);
        let node_id = NodeIndex(0);
        let forker_id = NodeIndex(3);
        let session_id = 0;
        let max_round = 2137;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let validator = UnitValidator::new(session_id, keychains[node_id.0], max_round);
        let mut dag = Dag::new(validator);
        let unit = random_full_parent_units_up_to(2, node_count, session_id)
            .get(2)
            .expect("we have the requested round")
            .get(forker_id.0)
            .expect("we have the unit for the forker")
            .clone();
        let unit = Signed::sign(unit, &keychains[forker_id.0]);
        let fork = random_full_parent_units_up_to(2, node_count, session_id)
            .get(2)
            .expect("we have the requested round")
            .get(forker_id.0)
            .expect("we have the unit for the forker")
            .clone();
        let fork = Signed::sign(fork, &keychains[forker_id.0]);
        let DagResult {
            units,
            requests,
            alerts,
        } = dag.process_forking_notification(
            ForkingNotification::Forker((unit.clone().into(), fork.into())),
            &store,
        );
        // parents were not passed, so the correct unit does not yet get returned
        assert!(units.is_empty());
        assert_eq!(requests.len(), node_count.0);
        assert_eq!(alerts.len(), 1);
    }

    #[test]
    fn accepts_committed() {
        let node_count = NodeCount(7);
        let node_id = NodeIndex(0);
        let forker_id = NodeIndex(3);
        let session_id = 0;
        let max_round = 2137;
        let produced_round = 4;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let validator = UnitValidator::new(session_id, keychains[node_id.0], max_round);
        let mut dag = Dag::new(validator);
        let units = random_full_parent_units_up_to(produced_round, node_count, session_id);
        let fork_parents = units
            .get(2)
            .expect("we have the requested round")
            .iter()
            .take(5)
            .cloned()
            .collect();
        let fork = random_unit_with_parents(forker_id, &fork_parents);
        let fork = Signed::sign(fork, &keychains[forker_id.0]);
        let unit = units
            .get(3)
            .expect("we have the requested round")
            .get(forker_id.0)
            .expect("we have the forker's unit")
            .clone();
        let unit = Signed::sign(unit, &keychains[forker_id.0]);
        let DagResult {
            units: reconstructed_units,
            requests,
            alerts,
        } = dag.process_forking_notification(
            ForkingNotification::Forker((unit.clone().into(), fork.clone().into())),
            &store,
        );
        assert!(reconstructed_units.is_empty());
        assert_eq!(requests.len(), node_count.0);
        assert_eq!(alerts.len(), 1);
        // normally adding forker units should no longer work now, so trying to add all units only adds initial units of non-forkers
        let mut units_added = 0;
        for unit in units.iter().flatten().map(|unit| {
            let keychain = keychains
                .get(unit.creator().0)
                .expect("we have the keychains");
            Signed::sign(unit.clone(), keychain)
        }) {
            let DagResult {
                units,
                requests: _,
                alerts,
            } = dag.add_unit(unit.into(), &store);
            units_added += units.len();
            assert!(alerts.is_empty());
        }
        assert_eq!(units_added, node_count.0 - 1);
        let committed_units = units
            .iter()
            .take(3)
            .map(|units| {
                units
                    .get(forker_id.0)
                    .expect("we have the forker's unit")
                    .clone()
            })
            .map(|unit| Signed::sign(unit, &keychains[forker_id.0]))
            .chain(Some(fork))
            .map(|unit| unit.into())
            .collect();
        let DagResult {
            units: reconstructed_units,
            requests,
            alerts,
        } = dag.process_forking_notification(ForkingNotification::Units(committed_units), &store);
        assert!(alerts.is_empty());
        // the non-fork unit was added first in the forking notif, so all units reconstruct successfully
        assert!(requests.is_empty());
        assert_eq!(reconstructed_units.len(), node_count.0 * 4 + 1);
    }

    #[test]
    fn handles_explicit_parents() {
        let node_count = NodeCount(7);
        let node_id = NodeIndex(0);
        let forker_id = NodeIndex(3);
        let session_id = 0;
        let max_round = 2137;
        let produced_round = 4;
        let keychains: Vec<_> = node_count
            .into_iterator()
            .map(|node_id| Keychain::new(node_count, node_id))
            .collect();
        let store = UnitStore::<WrappedSignedUnit>::new(node_count);
        let validator = UnitValidator::new(session_id, keychains[node_id.0], max_round);
        let mut dag = Dag::new(validator);
        let units = random_full_parent_units_up_to(produced_round, node_count, session_id);
        let fork_parents = units
            .get(2)
            .expect("we have the requested round")
            .iter()
            .take(5)
            .cloned()
            .collect();
        let fork = random_unit_with_parents(forker_id, &fork_parents);
        let fork = Signed::sign(fork, &keychains[forker_id.0]);
        let unit = units
            .get(3)
            .expect("we have the requested round")
            .get(forker_id.0)
            .expect("we have the forker's unit")
            .clone();
        let unit = Signed::sign(unit, &keychains[forker_id.0]);
        let DagResult {
            units: reconstructed_units,
            requests,
            alerts,
        } = dag.process_forking_notification(
            // note the reverse order, to create parent requests later
            ForkingNotification::Forker((fork.clone().into(), unit.clone().into())),
            &store,
        );
        assert!(reconstructed_units.is_empty());
        // the fork only has 5 parents
        assert_eq!(requests.len(), 5);
        assert_eq!(alerts.len(), 1);
        let mut units_added = 0;
        let mut all_requests = Vec::new();
        for unit in units.iter().flatten().map(|unit| {
            let keychain = keychains
                .get(unit.creator().0)
                .expect("we have the keychains");
            Signed::sign(unit.clone(), keychain)
        }) {
            let DagResult {
                units,
                mut requests,
                alerts,
            } = dag.add_unit(unit.into(), &store);
            units_added += units.len();
            all_requests.append(&mut requests);
            assert!(alerts.is_empty());
        }
        assert_eq!(units_added, node_count.0 - 1);
        let mut parent_requests: Vec<_> = all_requests
            .into_iter()
            .filter_map(|request| match request {
                Request::Coord(_) => None,
                Request::ParentsOf(hash) => Some(hash),
            })
            .collect();
        // all the round 4 non-forker units should be confused
        assert_eq!(parent_requests.len(), node_count.0 - 1);
        let committed_units = units
            .iter()
            .take(3)
            .map(|units| {
                units
                    .get(forker_id.0)
                    .expect("we have the forker's unit")
                    .clone()
            })
            .map(|unit| Signed::sign(unit, &keychains[forker_id.0]))
            .chain(Some(fork))
            .map(|unit| unit.into())
            .collect();
        let DagResult {
            units: reconstructed_units,
            requests,
            alerts,
        } = dag.process_forking_notification(ForkingNotification::Units(committed_units), &store);
        assert!(alerts.is_empty());
        // we already got the requests earlier, in parent_requests
        assert!(requests.is_empty());
        assert!(!reconstructed_units.is_empty());
        // gotta also commit to the correct unit, so that it can get imported
        let committed_units = units
            .iter()
            .take(4)
            .map(|units| {
                units
                    .get(forker_id.0)
                    .expect("we have the forker's unit")
                    .clone()
            })
            .map(|unit| Signed::sign(unit, &keychains[forker_id.0]).into())
            .collect();
        let DagResult {
            units: reconstructed_units,
            requests,
            alerts,
        } = dag.process_forking_notification(ForkingNotification::Units(committed_units), &store);
        assert!(alerts.is_empty());
        assert!(requests.is_empty());
        assert_eq!(reconstructed_units.len(), 1);
        let confused_unit = parent_requests.pop().expect("we chacked it's not empty");
        let parents = units
            .get(3)
            .expect("we have round 3 units")
            .iter()
            .map(|unit| Signed::sign(unit.clone(), &keychains[unit.creator().0]))
            .map(|unit| unit.into())
            .collect();
        let DagResult {
            units: reconstructed_units,
            requests,
            alerts,
        } = dag.add_parents(confused_unit, parents, &store);
        assert!(alerts.is_empty());
        assert!(requests.is_empty());
        assert_eq!(reconstructed_units.len(), 1);
        assert_eq!(reconstructed_units[0].hash(), confused_unit);
    }
}
