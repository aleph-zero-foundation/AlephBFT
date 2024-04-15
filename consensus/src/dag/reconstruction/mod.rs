use std::collections::HashMap;

use crate::{
    units::{ControlHash, HashFor, Unit, UnitCoord, UnitWithParents, WrappedUnit},
    Hasher, NodeMap, SessionId,
};

mod dag;
mod parents;

use dag::Dag;
use parents::Reconstruction as ParentReconstruction;

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

    fn session_id(&self) -> SessionId {
        self.unit.session_id()
    }
}

impl<U: Unit> WrappedUnit<U::Hasher> for ReconstructedUnit<U> {
    type Wrapped = U;

    fn unpack(self) -> U {
        self.unit
    }
}

impl<U: Unit> UnitWithParents for ReconstructedUnit<U> {
    fn parents(&self) -> &NodeMap<HashFor<Self>> {
        &self.parents
    }
}

/// What we need to request to reconstruct units.
#[derive(Debug, PartialEq, Eq)]
pub enum Request<H: Hasher> {
    /// We need a unit at this coordinate.
    Coord(UnitCoord),
    /// We need the explicit list of parents for the unit identified by the hash.
    /// This should only happen in the presence of forks, when optimistic reconstruction failed.
    ParentsOf(H::Hash),
}

/// The result of a reconstruction attempt. Might contain multiple reconstructed units,
/// as well as requests for some data that is needed for further reconstruction.
#[derive(Debug, PartialEq, Eq)]
pub struct ReconstructionResult<U: Unit> {
    /// All the units that got reconstructed.
    pub units: Vec<ReconstructedUnit<U>>,
    /// Any requests that now should be made.
    pub requests: Vec<Request<U::Hasher>>,
}

impl<U: Unit> ReconstructionResult<U> {
    fn new(units: Vec<ReconstructedUnit<U>>, requests: Vec<Request<U::Hasher>>) -> Self {
        ReconstructionResult { units, requests }
    }

    fn empty() -> Self {
        ReconstructionResult::new(Vec::new(), Vec::new())
    }

    fn reconstructed(unit: ReconstructedUnit<U>) -> Self {
        ReconstructionResult {
            units: vec![unit],
            requests: Vec::new(),
        }
    }

    fn request(request: Request<U::Hasher>) -> Self {
        ReconstructionResult {
            units: Vec::new(),
            requests: vec![request],
        }
    }

    fn add_unit(&mut self, unit: ReconstructedUnit<U>) {
        self.units.push(unit);
    }

    fn add_request(&mut self, request: Request<U::Hasher>) {
        self.requests.push(request);
    }

    fn accumulate(&mut self, other: ReconstructionResult<U>) {
        let ReconstructionResult {
            mut units,
            mut requests,
        } = other;
        self.units.append(&mut units);
        self.requests.append(&mut requests);
    }
}

/// The reconstruction of the structure of the Dag.
/// When passed units containing control hashes, and responses to requests it produces,
/// it eventually outputs versions with explicit parents in an order conforming to the Dag order.
pub struct Reconstruction<U: Unit> {
    parents: ParentReconstruction<U>,
    dag: Dag<ReconstructedUnit<U>>,
}

impl<U: Unit> Reconstruction<U> {
    /// Create a new reconstruction.
    pub fn new() -> Self {
        let parents = ParentReconstruction::new();
        let dag = Dag::new();
        Reconstruction { parents, dag }
    }

    fn handle_parents_reconstruction_result(
        &mut self,
        reconstruction_result: ReconstructionResult<U>,
    ) -> ReconstructionResult<U> {
        let ReconstructionResult { units, requests } = reconstruction_result;
        let units = units
            .into_iter()
            .flat_map(|unit| self.dag.add_unit(unit))
            .collect();
        ReconstructionResult::new(units, requests)
    }

    /// Add a unit to the reconstruction.
    pub fn add_unit(&mut self, unit: U) -> ReconstructionResult<U> {
        let parent_reconstruction_result = self.parents.add_unit(unit);
        self.handle_parents_reconstruction_result(parent_reconstruction_result)
    }

    /// Add an explicit list of parents to the reconstruction.
    pub fn add_parents(
        &mut self,
        unit: HashFor<U>,
        parents: HashMap<UnitCoord, HashFor<U>>,
    ) -> ReconstructionResult<U> {
        let parent_reconstruction_result = self.parents.add_parents(unit, parents);
        self.handle_parents_reconstruction_result(parent_reconstruction_result)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        dag::reconstruction::{ReconstructedUnit, Reconstruction, ReconstructionResult, Request},
        units::{random_full_parent_units_up_to, Unit, UnitCoord, UnitWithParents},
        NodeCount, NodeIndex,
    };

    #[test]
    fn reconstructs_initial_units() {
        let mut reconstruction = Reconstruction::new();
        for unit in &random_full_parent_units_up_to(0, NodeCount(4), 43)[0] {
            let ReconstructionResult {
                mut units,
                requests,
            } = reconstruction.add_unit(unit.clone());
            assert!(requests.is_empty());
            assert_eq!(units.len(), 1);
            let reconstructed_unit = units.pop().expect("just checked its there");
            assert_eq!(reconstructed_unit, ReconstructedUnit::initial(unit.clone()));
            assert_eq!(reconstructed_unit.parents().item_count(), 0);
        }
    }

    #[test]
    fn reconstructs_units_coming_in_order() {
        let mut reconstruction = Reconstruction::new();
        let dag = random_full_parent_units_up_to(7, NodeCount(4), 43);
        for units in &dag {
            for unit in units {
                let round = unit.round();
                let ReconstructionResult {
                    mut units,
                    requests,
                } = reconstruction.add_unit(unit.clone());
                assert!(requests.is_empty());
                assert_eq!(units.len(), 1);
                let reconstructed_unit = units.pop().expect("just checked its there");
                match round {
                    0 => {
                        assert_eq!(reconstructed_unit, ReconstructedUnit::initial(unit.clone()));
                        assert_eq!(reconstructed_unit.parents().item_count(), 0);
                    }
                    round => {
                        assert_eq!(reconstructed_unit.parents().item_count(), 4);
                        let parents = dag
                            .get((round - 1) as usize)
                            .expect("the parents are there");
                        for (parent, reconstructed_parent) in
                            parents.iter().zip(reconstructed_unit.parents().values())
                        {
                            assert_eq!(&parent.hash(), reconstructed_parent);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn requests_all_parents() {
        let mut reconstruction = Reconstruction::new();
        let dag = random_full_parent_units_up_to(1, NodeCount(4), 43);
        let unit = dag
            .get(1)
            .expect("just created")
            .last()
            .expect("we have a unit");
        let ReconstructionResult { units, requests } = reconstruction.add_unit(unit.clone());
        assert!(units.is_empty());
        assert_eq!(requests.len(), 4);
    }

    #[test]
    fn requests_single_parent() {
        let mut reconstruction = Reconstruction::new();
        let dag = random_full_parent_units_up_to(1, NodeCount(4), 43);
        for unit in dag.get(0).expect("just created").iter().skip(1) {
            reconstruction.add_unit(unit.clone());
        }
        let unit = dag
            .get(1)
            .expect("just created")
            .last()
            .expect("we have a unit");
        let ReconstructionResult { units, requests } = reconstruction.add_unit(unit.clone());
        assert!(units.is_empty());
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests.last().expect("just checked"),
            &Request::Coord(UnitCoord::new(0, NodeIndex(0)))
        );
    }

    #[test]
    fn reconstructs_units_coming_in_reverse_order() {
        let mut reconstruction = Reconstruction::new();
        let mut dag = random_full_parent_units_up_to(7, NodeCount(4), 43);
        dag.reverse();
        for units in dag.iter().take(7) {
            for unit in units {
                let ReconstructionResult { units, requests } =
                    reconstruction.add_unit(unit.clone());
                assert!(units.is_empty());
                assert_eq!(requests.len(), 4);
            }
        }
        for unit in dag[7].iter().take(3) {
            let ReconstructionResult { units, requests } = reconstruction.add_unit(unit.clone());
            assert!(requests.is_empty());
            assert_eq!(units.len(), 1);
        }
        let ReconstructionResult { units, requests } = reconstruction.add_unit(dag[7][3].clone());
        assert!(requests.is_empty());
        assert_eq!(units.len(), 4 * 8 - 3);
    }

    #[test]
    fn handles_bad_hash() {
        let node_count = NodeCount(7);
        let mut reconstruction = Reconstruction::new();
        let dag = random_full_parent_units_up_to(0, node_count, 43);
        for unit in dag.get(0).expect("just created") {
            reconstruction.add_unit(unit.clone());
        }
        let other_dag = random_full_parent_units_up_to(1, node_count, 43);
        let unit = other_dag
            .get(1)
            .expect("just created")
            .last()
            .expect("we have a unit");
        let unit_hash = unit.hash();
        let ReconstructionResult { units, requests } = reconstruction.add_unit(unit.clone());
        assert!(units.is_empty());
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests.last().expect("just checked"),
            &Request::ParentsOf(unit_hash),
        );
        let parent_hashes: HashMap<_, _> = other_dag
            .get(0)
            .expect("other dag has initial units")
            .iter()
            .map(|unit| (unit.coord(), unit.hash()))
            .collect();
        let ReconstructionResult { units, requests } =
            reconstruction.add_parents(unit_hash, parent_hashes.clone());
        assert!(requests.is_empty());
        assert!(units.is_empty());
        let mut all_reconstructed = Vec::new();
        for other_initial in &other_dag[0] {
            let ReconstructionResult {
                mut units,
                requests,
            } = reconstruction.add_unit(other_initial.clone());
            assert!(requests.is_empty());
            all_reconstructed.append(&mut units);
        }
        // some of the initial units may randomly be identical,
        // so all we can say that the last reconstructed unit should be the one we want
        assert!(!all_reconstructed.is_empty());
        assert_eq!(
            all_reconstructed.pop().expect("just checked").hash(),
            unit_hash
        )
    }
}
