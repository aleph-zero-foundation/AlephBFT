use crate::units::{HashFor, UnitWithParents};
use std::collections::{HashMap, HashSet, VecDeque};

struct OrphanedUnit<U: UnitWithParents> {
    unit: U,
    missing_parents: HashSet<HashFor<U>>,
}

impl<U: UnitWithParents> OrphanedUnit<U> {
    /// If there are no missing parents then returns just the internal unit.
    pub fn new(unit: U, missing_parents: HashSet<HashFor<U>>) -> Result<Self, U> {
        match missing_parents.is_empty() {
            true => Err(unit),
            false => Ok(OrphanedUnit {
                unit,
                missing_parents,
            }),
        }
    }

    /// If this was the last missing parent return the unit.
    pub fn resolve_parent(self, parent: HashFor<U>) -> Result<U, Self> {
        let OrphanedUnit {
            unit,
            mut missing_parents,
        } = self;
        missing_parents.remove(&parent);
        match missing_parents.is_empty() {
            true => Ok(unit),
            false => Err(OrphanedUnit {
                unit,
                missing_parents,
            }),
        }
    }

    /// The hash of the unit.
    pub fn hash(&self) -> HashFor<U> {
        self.unit.hash()
    }

    /// The set of still missing parents.
    pub fn missing_parents(&self) -> &HashSet<HashFor<U>> {
        &self.missing_parents
    }
}

/// A structure ensuring that units added to it are output in an order
/// in agreement with the DAG order.
pub struct Dag<U: UnitWithParents> {
    orphaned_units: HashMap<HashFor<U>, OrphanedUnit<U>>,
    waiting_for: HashMap<HashFor<U>, Vec<HashFor<U>>>,
    dag_units: HashSet<HashFor<U>>,
}

impl<U: UnitWithParents> Dag<U> {
    /// Create a new empty DAG.
    pub fn new() -> Self {
        Dag {
            orphaned_units: HashMap::new(),
            waiting_for: HashMap::new(),
            dag_units: HashSet::new(),
        }
    }

    fn move_to_dag(&mut self, unit: U) -> Vec<U> {
        let mut result = Vec::new();
        let mut ready_units = VecDeque::from([unit]);
        while let Some(unit) = ready_units.pop_front() {
            let unit_hash = unit.hash();
            self.dag_units.insert(unit_hash);
            result.push(unit);
            for child in self.waiting_for.remove(&unit_hash).iter().flatten() {
                match self
                    .orphaned_units
                    .remove(child)
                    .expect("we were waiting for parents")
                    .resolve_parent(unit_hash)
                {
                    Ok(unit) => ready_units.push_back(unit),
                    Err(orphan) => {
                        self.orphaned_units.insert(*child, orphan);
                    }
                }
            }
        }
        result
    }

    /// Add a unit to the Dag. Returns all the units that now have all their parents in the Dag,
    /// in an order agreeing with the Dag structure.
    pub fn add_unit(&mut self, unit: U) -> Vec<U> {
        if self.dag_units.contains(&unit.hash()) {
            // Deduplicate.
            return Vec::new();
        }
        let missing_parents = unit
            .parents()
            .values()
            .filter(|parent| !self.dag_units.contains(parent))
            .cloned()
            .collect();
        match OrphanedUnit::new(unit, missing_parents) {
            Ok(orphan) => {
                let unit_hash = orphan.hash();
                for parent in orphan.missing_parents() {
                    self.waiting_for.entry(*parent).or_default().push(unit_hash);
                }
                self.orphaned_units.insert(unit_hash, orphan);
                Vec::new()
            }
            Err(unit) => self.move_to_dag(unit),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        dag::reconstruction::{dag::Dag, ReconstructedUnit},
        units::{random_full_parent_units_up_to, TestingFullUnit, Unit},
        Hasher, NodeCount, NodeIndex, NodeMap,
    };
    use aleph_bft_mock::Hasher64;
    use std::collections::HashSet;

    fn full_parents_to_map(
        parents: Vec<<Hasher64 as Hasher>::Hash>,
    ) -> NodeMap<<Hasher64 as Hasher>::Hash> {
        let mut result = NodeMap::with_size(NodeCount(parents.len()));
        for (id, parent) in parents.into_iter().enumerate() {
            result.insert(NodeIndex(id), parent);
        }
        result
    }

    // silly clippy, the map below doesn't work with &[..]
    #[allow(clippy::ptr_arg)]
    fn unit_hashes(units: &Vec<TestingFullUnit>) -> Vec<<Hasher64 as Hasher>::Hash> {
        units.iter().map(|unit| unit.hash()).collect()
    }

    fn reconstructed(
        dag: Vec<Vec<TestingFullUnit>>,
    ) -> Vec<Vec<ReconstructedUnit<TestingFullUnit>>> {
        let hashes: Vec<_> = dag.iter().map(unit_hashes).collect();
        let initial_units: Vec<_> = dag
            .first()
            .expect("only called on nonempty dags")
            .iter()
            .map(|unit| ReconstructedUnit::initial(unit.clone()))
            .collect();
        let mut result = vec![initial_units];
        for (units, parents) in dag.iter().skip(1).zip(hashes) {
            let parents = full_parents_to_map(parents);
            let reconstructed = units
                .iter()
                .map(|unit| {
                    ReconstructedUnit::with_parents(unit.clone(), parents.clone())
                        .expect("parents are correct")
                })
                .collect();
            result.push(reconstructed);
        }
        result
    }

    #[test]
    fn reconstructs_initial_units() {
        let mut dag = Dag::new();
        for unit in reconstructed(random_full_parent_units_up_to(0, NodeCount(4), 43))
            .pop()
            .expect("we have initial units")
        {
            let reconstructed = dag.add_unit(unit.clone());
            assert_eq!(reconstructed, vec![unit]);
        }
    }

    #[test]
    fn reconstructs_units_in_order() {
        let mut dag = Dag::new();
        for units in reconstructed(random_full_parent_units_up_to(7000, NodeCount(4), 43)) {
            for unit in units {
                let reconstructed = dag.add_unit(unit.clone());
                assert_eq!(reconstructed, vec![unit]);
            }
        }
    }

    #[test]
    fn reconstructs_units_in_reverse_order() {
        let full_unit_dag = random_full_parent_units_up_to(7000, NodeCount(4), 43);
        let mut hash_batches: Vec<_> = full_unit_dag
            .iter()
            .map(unit_hashes)
            .map(HashSet::from_iter)
            .collect();
        hash_batches.reverse();
        let mut unit_dag = reconstructed(full_unit_dag);
        unit_dag.reverse();
        let mut initial_units = unit_dag.pop().expect("initial units are there");
        let mut dag = Dag::new();
        for units in unit_dag {
            for unit in units {
                let reconstructed = dag.add_unit(unit.clone());
                assert!(reconstructed.is_empty());
            }
        }
        let last_initial_unit = initial_units.pop().expect("there is an initial unit");
        for unit in initial_units {
            let reconstructed = dag.add_unit(unit.clone());
            let mut current_round_hashes: HashSet<_> = hash_batches.pop().expect("we are not done");
            assert!(current_round_hashes.remove(&unit.hash()));
            if !current_round_hashes.is_empty() {
                hash_batches.push(current_round_hashes);
            }
            assert_eq!(reconstructed, vec![unit]);
        }
        for unit in dag.add_unit(last_initial_unit) {
            let mut current_round_hashes = hash_batches.pop().expect("we are not done");
            assert!(current_round_hashes.remove(&unit.hash()));
            if !current_round_hashes.is_empty() {
                hash_batches.push(current_round_hashes);
            }
        }
        assert!(hash_batches.is_empty());
    }
}
