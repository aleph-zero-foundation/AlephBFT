use std::{
    collections::HashMap,
    fmt::{Display, Formatter, Result as FmtResult},
};

use crate::{
    units::{HashFor, Unit, UnitCoord},
    NodeCount, NodeIndex, NodeMap, Round,
};

/// An overview of what is in the unit store.
pub struct UnitStoreStatus {
    size: usize,
    top_row: NodeMap<Round>,
}

impl UnitStoreStatus {
    /// Highest round among units in the store.
    pub fn top_round(&self) -> Round {
        self.top_row.values().max().cloned().unwrap_or(0)
    }
}

impl Display for UnitStoreStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "total units: {}, top row: {}", self.size, self.top_row)
    }
}

/// Stores units, and keeps track of which are canonical, i.e. the first ones inserted with a given coordinate.
/// See `remove` for limitation on trusting canonical units, although they don't impact our usecases.
pub struct UnitStore<U: Unit> {
    by_hash: HashMap<HashFor<U>, U>,
    canonical_units: NodeMap<HashMap<Round, HashFor<U>>>,
}

impl<U: Unit> UnitStore<U> {
    /// Create a new unit store for the given number of nodes.
    pub fn new(node_count: NodeCount) -> Self {
        let mut canonical_units = NodeMap::with_size(node_count);
        for node_id in node_count.into_iterator() {
            canonical_units.insert(node_id, HashMap::new());
        }
        UnitStore {
            by_hash: HashMap::new(),
            canonical_units,
        }
    }

    fn mut_hashes_by(&mut self, creator: NodeIndex) -> &mut HashMap<Round, HashFor<U>> {
        self.canonical_units
            .get_mut(creator)
            .expect("all hashmaps initialized")
    }

    fn hashes_by(&self, creator: NodeIndex) -> &HashMap<Round, HashFor<U>> {
        self.canonical_units
            .get(creator)
            .expect("all hashmaps initialized")
    }

    // only call this for canonical units
    fn canonical_by_hash(&self, hash: &HashFor<U>) -> &U {
        self.by_hash.get(hash).expect("we have all canonical units")
    }

    /// Insert a unit. If no other unit with this coord is in the store it becomes canonical.
    pub fn insert(&mut self, unit: U) {
        let unit_hash = unit.hash();
        let unit_coord = unit.coord();
        if self.canonical_unit(unit_coord).is_none() {
            self.mut_hashes_by(unit_coord.creator())
                .insert(unit.coord().round(), unit_hash);
        }
        self.by_hash.insert(unit_hash, unit);
    }

    /// Remove a unit with a given hash. Notably if you remove a unit another might become canonical in its place in the future.
    pub fn remove(&mut self, hash: &HashFor<U>) {
        if let Some(unit) = self.by_hash.remove(hash) {
            let creator_hashes = self.mut_hashes_by(unit.creator());
            if creator_hashes.get(&unit.round()) == Some(&unit.hash()) {
                creator_hashes.remove(&unit.round());
            }
        }
    }

    /// The canonical unit for the given coord if it exists.
    pub fn canonical_unit(&self, coord: UnitCoord) -> Option<&U> {
        self.hashes_by(coord.creator())
            .get(&coord.round())
            .map(|hash| self.canonical_by_hash(hash))
    }

    /// All the canonical units for the given creator, in order of rounds.
    pub fn canonical_units(&self, creator: NodeIndex) -> impl Iterator<Item = &U> {
        let canonical_hashes = self.hashes_by(creator);
        let max_round = canonical_hashes.keys().max().cloned().unwrap_or(0);
        (0..=max_round)
            .filter_map(|round| canonical_hashes.get(&round))
            .map(|hash| self.canonical_by_hash(hash))
    }

    /// The unit for the given hash, if present.
    pub fn unit(&self, hash: &HashFor<U>) -> Option<&U> {
        self.by_hash.get(hash)
    }

    /// The status summary of this store.
    pub fn status(&self) -> UnitStoreStatus {
        let mut top_row = NodeMap::with_size(self.canonical_units.size());
        for (creator, units) in self.canonical_units.iter() {
            if let Some(round) = units.keys().max() {
                top_row.insert(creator, *round);
            }
        }
        UnitStoreStatus {
            size: self.by_hash.len(),
            top_row,
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use crate::{
        units::{random_full_parent_units_up_to, TestingFullUnit, Unit, UnitCoord, UnitStore},
        NodeCount, NodeIndex,
    };

    #[test]
    fn empty_has_no_units() {
        let node_count = NodeCount(7);
        let store = UnitStore::<TestingFullUnit>::new(node_count);
        assert!(store
            .canonical_unit(UnitCoord::new(0, NodeIndex(0)))
            .is_none());
        assert!(store.canonical_units(NodeIndex(0)).next().is_none());
    }

    #[test]
    fn single_unit_basic_operations() {
        let node_count = NodeCount(7);
        let mut store = UnitStore::new(node_count);
        let unit = random_full_parent_units_up_to(0, node_count, 43)
            .first()
            .expect("we have the first round")
            .first()
            .expect("we have the initial unit for the zeroth creator")
            .clone();
        store.insert(unit.clone());
        assert_eq!(store.unit(&unit.hash()), Some(&unit));
        assert_eq!(store.canonical_unit(unit.coord()), Some(&unit));
        {
            // in block to drop the iterator
            let mut canonical_units = store.canonical_units(unit.creator());
            assert_eq!(canonical_units.next(), Some(&unit));
            assert_eq!(canonical_units.next(), None);
        }
        store.remove(&unit.hash());
        assert_eq!(store.unit(&unit.hash()), None);
        assert_eq!(store.canonical_unit(unit.coord()), None);
        assert_eq!(store.canonical_units(unit.creator()).next(), None);
    }

    #[test]
    fn first_variant_is_canonical() {
        let node_count = NodeCount(7);
        let mut store = UnitStore::new(node_count);
        // only unique variants
        #[allow(clippy::mutable_key_type)]
        let variants: HashSet<_> = (0..15)
            .map(|_| {
                random_full_parent_units_up_to(0, node_count, 43)
                    .first()
                    .expect("we have the first round")
                    .first()
                    .expect("we have the initial unit for the zeroth creator")
                    .clone()
            })
            .collect();
        let variants: Vec<_> = variants.into_iter().collect();
        for unit in &variants {
            store.insert(unit.clone());
        }
        for unit in &variants {
            assert_eq!(store.unit(&unit.hash()), Some(unit));
        }
        let canonical_unit = variants.first().expect("we have the unit").clone();
        assert_eq!(
            store.canonical_unit(canonical_unit.coord()),
            Some(&canonical_unit)
        );
        {
            // in block to drop the iterator
            let mut canonical_units = store.canonical_units(canonical_unit.creator());
            assert_eq!(canonical_units.next(), Some(&canonical_unit));
            assert_eq!(canonical_units.next(), None);
        }
        store.remove(&canonical_unit.hash());
        assert_eq!(store.unit(&canonical_unit.hash()), None);
        // we don't have a canonical unit any more
        assert_eq!(store.canonical_unit(canonical_unit.coord()), None);
        assert_eq!(store.canonical_units(canonical_unit.creator()).next(), None);
        // we still have all this other units
        for unit in variants.iter().skip(1) {
            assert_eq!(store.unit(&unit.hash()), Some(unit));
        }
    }

    #[test]
    fn stores_lots_of_units() {
        let node_count = NodeCount(7);
        let mut store = UnitStore::new(node_count);
        let max_round = 15;
        let units = random_full_parent_units_up_to(max_round, node_count, 43);
        for round_units in &units {
            for unit in round_units {
                store.insert(unit.clone());
            }
        }
        for round_units in &units {
            for unit in round_units {
                assert_eq!(store.unit(&unit.hash()), Some(unit));
                assert_eq!(store.canonical_unit(unit.coord()), Some(unit));
            }
        }
        for node_id in node_count.into_iterator() {
            let mut canonical_units = store.canonical_units(node_id);
            for round in 0..=max_round {
                assert_eq!(
                    canonical_units.next(),
                    Some(&units[round as usize][node_id.0])
                );
            }
            assert_eq!(canonical_units.next(), None);
        }
    }

    #[test]
    fn handles_fragmented_canonical() {
        let node_count = NodeCount(7);
        let mut store = UnitStore::new(node_count);
        let max_round = 15;
        let units = random_full_parent_units_up_to(max_round, node_count, 43);
        for round_units in &units {
            for unit in round_units {
                store.insert(unit.clone());
            }
        }
        for round_units in &units {
            for unit in round_units {
                // remove some units with a weird criterion
                if unit.round() as usize % (unit.creator().0 + 1) == 0 {
                    store.remove(&unit.hash());
                }
            }
        }
        for node_id in node_count.into_iterator() {
            let mut canonical_units = store.canonical_units(node_id);
            for round in 0..=max_round {
                if round as usize % (node_id.0 + 1) != 0 {
                    assert_eq!(
                        canonical_units.next(),
                        Some(&units[round as usize][node_id.0])
                    );
                }
            }
            assert_eq!(canonical_units.next(), None);
        }
    }
}
