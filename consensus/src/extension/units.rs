use std::collections::{HashMap, VecDeque};

use crate::{extension::ExtenderUnit, Hasher, Round};

/// Units kept in a way optimized for easy batch extraction.
pub struct Units<H: Hasher> {
    units: HashMap<H::Hash, ExtenderUnit<H>>,
    by_round: HashMap<Round, Vec<H::Hash>>,
    highest_round: Round,
}

impl<H: Hasher> Units<H> {
    /// Create empty unit store.
    pub fn new() -> Self {
        Units {
            units: HashMap::new(),
            by_round: HashMap::new(),
            highest_round: 0,
        }
    }

    /// Add a unit to the store.
    pub fn add_unit(&mut self, u: ExtenderUnit<H>) {
        let round = u.round;
        if round > self.highest_round {
            self.highest_round = round;
        }

        self.by_round.entry(round).or_default().push(u.hash);
        self.units.insert(u.hash, u);
    }

    pub fn get(&self, hash: &H::Hash) -> Option<&ExtenderUnit<H>> {
        self.units.get(hash)
    }

    /// Get the list of unit hashes from the given round.
    /// Panics if called for a round greater or equal to the round
    /// of the highest head of a removed batch.
    pub fn in_round(&self, round: Round) -> Option<Vec<&ExtenderUnit<H>>> {
        self.by_round.get(&round).map(|hashes| {
            hashes
                .iter()
                .map(|hash| self.units.get(hash).expect("we have all the units"))
                .collect()
        })
    }

    /// The highest round among all added units, or 0 if there are none.
    pub fn highest_round(&self) -> Round {
        self.highest_round
    }

    /// Remove a batch of units, deterministically ordered based on the given head.
    pub fn remove_batch(&mut self, head: H::Hash) -> Vec<H::Hash> {
        let mut batch = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back(
            self.units
                .remove(&head)
                .expect("head is picked among units we have"),
        );
        while let Some(u) = queue.pop_front() {
            batch.push(u.hash);
            for u_hash in u.parents.into_values() {
                if let Some(v) = self.units.remove(&u_hash) {
                    queue.push_back(v);
                }
            }
        }
        // Since we construct the batch using BFS, the ordering is canonical and respects the DAG partial order.

        // We reverse for the batch to start with least recent units.
        batch.reverse();
        batch
    }
}

#[cfg(test)]
mod test {
    use crate::{
        extension::{tests::construct_unit_all_parents, units::Units},
        NodeCount, NodeIndex,
    };
    use aleph_bft_mock::Hasher64;

    #[test]
    fn initially_empty() {
        let units = Units::<Hasher64>::new();
        assert!(units.in_round(0).is_none());
        assert_eq!(units.highest_round(), 0);
    }

    #[test]
    fn accepts_unit() {
        let mut units = Units::new();
        let n_members = NodeCount(4);
        let unit = construct_unit_all_parents(NodeIndex(0), 0, n_members);
        units.add_unit(unit.clone());
        assert_eq!(units.highest_round(), 0);
        assert_eq!(units.in_round(0), Some(vec![&unit]));
        assert_eq!(units.get(&unit.hash), Some(&unit));
    }

    #[test]
    fn returns_batches_all_parents() {
        let mut units = Units::new();
        let n_members = NodeCount(4);
        let max_round = 43;
        let mut heads = Vec::new();
        for round in 0..=max_round {
            for creator in n_members.into_iterator() {
                let unit = construct_unit_all_parents(creator, round, n_members);
                if round as usize % n_members.0 == creator.0 {
                    heads.push(unit.hash)
                }
                units.add_unit(unit);
            }
        }
        assert_eq!(units.highest_round(), max_round);
        assert_eq!(units.in_round(max_round + 1), None);
        for head in heads {
            let mut batch = units.remove_batch(head);
            assert_eq!(batch.pop(), Some(head));
        }
    }

    #[test]
    fn batch_order_constant_with_different_insertion_order() {
        let mut units = Units::new();
        let mut units_but_backwards = Units::new();
        let n_members = NodeCount(4);
        let max_round = 43;
        let mut heads = Vec::new();
        for round in 0..=max_round {
            let mut round_units = Vec::new();
            for creator in n_members.into_iterator() {
                let unit = construct_unit_all_parents(creator, round, n_members);
                if round as usize % n_members.0 == creator.0 {
                    heads.push(unit.hash)
                }
                round_units.push(unit.clone());
                units.add_unit(unit);
            }
            for unit in round_units.into_iter().rev() {
                units_but_backwards.add_unit(unit);
            }
        }
        for head in heads {
            let batch1 = units.remove_batch(head);
            let batch2 = units_but_backwards.remove_batch(head);
            assert_eq!(batch1, batch2);
        }
    }
}
