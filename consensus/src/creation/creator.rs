use crate::{
    creation::collector::{ConstraintError, UnitsCollector},
    units::{ControlHash, PreUnit, Unit},
    Hasher, NodeCount, NodeIndex, NodeMap, Round,
};
use anyhow::Result;
use std::cmp;

pub struct Creator<H: Hasher> {
    round_collectors: Vec<UnitsCollector<H>>,
    node_id: NodeIndex,
    n_members: NodeCount,
}

impl<H: Hasher> Creator<H> {
    pub fn new(node_id: NodeIndex, n_members: NodeCount) -> Self {
        Creator {
            node_id,
            n_members,
            round_collectors: vec![UnitsCollector::new_initial(n_members)],
        }
    }

    pub fn current_round(&self) -> Round {
        (self.round_collectors.len() - 1) as Round
    }

    // gets or initializes a unit collector for a given round (and all between if not there)
    fn get_or_initialize_collector_for_round(&mut self, round: Round) -> &mut UnitsCollector<H> {
        while round > self.current_round() {
            let next_collector = UnitsCollector::from_previous(
                self.round_collectors
                    .last()
                    .expect("we always have at least one"),
            );
            self.round_collectors.push(next_collector);
        }
        &mut self.round_collectors[round as usize]
    }

    /// To create a new unit, we need to have at least the consensus threshold of parents available in previous round.
    /// Additionally, our unit from previous round must be available.
    pub fn create_unit(&self, round: Round) -> Result<PreUnit<H>> {
        let control_hash = match round.checked_sub(1) {
            None => ControlHash::new(&NodeMap::with_size(self.n_members)),
            Some(prev_round) => ControlHash::new(
                self.round_collectors
                    .get(usize::from(prev_round))
                    .ok_or(ConstraintError::NotEnoughParents)?
                    .prospective_parents(self.node_id)?,
            ),
        };

        Ok(PreUnit::new(self.node_id, round, control_hash))
    }

    pub fn add_unit<U: Unit<Hasher = H>>(&mut self, unit: &U) {
        let start_round = unit.round();
        let end_round = cmp::max(start_round, self.current_round());
        for round in start_round..=end_round {
            self.get_or_initialize_collector_for_round(round)
                .add_unit(unit);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        creation::creator::Creator as GenericCreator,
        units::{
            create_preunits, creator_set, preunit_to_full_unit, random_full_parent_units_up_to,
        },
        NodeCount, NodeIndex,
    };
    use aleph_bft_mock::Hasher64;

    type Creator = GenericCreator<Hasher64>;

    #[test]
    fn creates_initial_unit() {
        let n_members = NodeCount(7);
        let round = 0;
        let creator = Creator::new(NodeIndex(0), n_members);
        assert_eq!(creator.current_round(), round);
        let preunit = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        assert_eq!(preunit.round(), round);
    }

    #[test]
    fn creates_unit_with_all_parents() {
        let n_members = NodeCount(7);
        let mut creators = creator_set(n_members);
        let new_units = create_preunits(creators.iter(), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|pu| preunit_to_full_unit(pu, 0))
            .collect();
        let creator = &mut creators[0];
        creator.add_units(&new_units);
        let round = 1;
        assert_eq!(creator.current_round(), 0);
        let preunit = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        assert_eq!(preunit.round(), round);
    }

    fn create_unit_with_minimal_parents(n_members: NodeCount) {
        let n_parents = n_members.consensus_threshold().0;
        let mut creators = creator_set(n_members);
        let new_units = create_preunits(creators.iter().take(n_parents), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|pu| preunit_to_full_unit(pu, 0))
            .collect();
        let creator = &mut creators[0];
        creator.add_units(&new_units);
        let round = 1;
        assert_eq!(creator.current_round(), 0);
        let preunit = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        assert_eq!(preunit.round(), round);
    }

    #[test]
    fn creates_unit_with_minimal_parents_4() {
        create_unit_with_minimal_parents(NodeCount(4));
    }

    #[test]
    fn creates_unit_with_minimal_parents_5() {
        create_unit_with_minimal_parents(NodeCount(5));
    }

    #[test]
    fn creates_unit_with_minimal_parents_6() {
        create_unit_with_minimal_parents(NodeCount(6));
    }

    #[test]
    fn creates_unit_with_minimal_parents_7() {
        create_unit_with_minimal_parents(NodeCount(7));
    }

    fn dont_create_unit_below_parents_threshold(n_members: NodeCount) {
        let n_parents = n_members.consensus_threshold() - NodeCount(1);
        let mut creators = creator_set(n_members);
        let new_units = create_preunits(creators.iter().take(n_parents.0), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|pu| preunit_to_full_unit(pu, 0))
            .collect();
        let creator = &mut creators[0];
        creator.add_units(&new_units);
        let round = 1;
        assert_eq!(creator.current_round(), 0);
        assert!(creator.create_unit(round).is_err())
    }

    #[test]
    fn cannot_create_unit_below_parents_threshold_4() {
        dont_create_unit_below_parents_threshold(NodeCount(4));
    }

    #[test]
    fn cannot_create_unit_below_parents_threshold_5() {
        dont_create_unit_below_parents_threshold(NodeCount(5));
    }

    #[test]
    fn cannot_create_unit_below_parents_threshold_6() {
        dont_create_unit_below_parents_threshold(NodeCount(6));
    }

    #[test]
    fn cannot_create_unit_below_parents_threshold_7() {
        dont_create_unit_below_parents_threshold(NodeCount(7));
    }

    #[test]
    fn creates_two_units_when_possible() {
        let n_members = NodeCount(7);
        let mut creators = creator_set(n_members);
        for round in 0..2 {
            let new_units = create_preunits(creators.iter().skip(1), round);
            let new_units: Vec<_> = new_units
                .into_iter()
                .map(|pu| preunit_to_full_unit(pu, 0))
                .collect();
            for creator in creators.iter_mut() {
                creator.add_units(&new_units);
            }
        }
        let creator = &mut creators[0];
        assert_eq!(creator.current_round(), 1);
        for round in 0..3 {
            let preunit = creator
                .create_unit(round)
                .expect("Creation should succeed.");
            assert_eq!(preunit.round(), round);
            let unit = preunit_to_full_unit(preunit, 0);
            creator.add_unit(&unit);
        }
    }

    #[test]
    fn cannot_create_unit_without_predecessor() {
        let n_members = NodeCount(7);
        let mut creators = creator_set(n_members);
        let new_units = create_preunits(creators.iter().skip(1), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|pu| preunit_to_full_unit(pu, 0))
            .collect();
        let creator = &mut creators[0];
        creator.add_units(&new_units);
        let round = 1;
        assert!(creator.create_unit(round).is_err());
    }

    #[test]
    fn creates_unit_with_ancient_parents() {
        let n_members = NodeCount(7);
        let mut creator = Creator::new(NodeIndex(0), n_members);
        let units = random_full_parent_units_up_to(1, n_members, 43);
        creator.add_units(&units[0]);
        for unit in units[1].iter().take(5) {
            creator.add_unit(unit);
        }
        let round = 2;
        let preunit = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        assert_eq!(preunit.round(), round);
        for coord in preunit.control_hash().parents().take(5) {
            assert_eq!(coord.round(), round - 1);
        }
        assert_eq!(
            preunit
                .control_hash()
                .parents()
                .nth(5)
                .expect("there is a sixth parent")
                .round(),
            round - 2
        );
    }

    #[test]
    fn creates_unit_with_ancient_parents_weird_order() {
        let n_members = NodeCount(7);
        let mut creator = Creator::new(NodeIndex(0), n_members);
        let units = random_full_parent_units_up_to(1, n_members, 43);
        for unit in units[1].iter().take(5) {
            creator.add_unit(unit);
        }
        creator.add_units(&units[0]);
        let round = 2;
        let preunit = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        assert_eq!(preunit.round(), round);
        for coord in preunit.control_hash().parents().take(5) {
            assert_eq!(coord.round(), round - 1);
        }
        assert_eq!(
            preunit
                .control_hash()
                .parents()
                .nth(5)
                .expect("there is a sixth parent")
                .round(),
            round - 2
        );
    }

    #[test]
    fn creates_old_unit_with_best_parents() {
        let n_members = NodeCount(7);
        let mut creator = Creator::new(NodeIndex(0), n_members);
        let units = random_full_parent_units_up_to(2, n_members, 43);
        for round_units in &units {
            for unit in round_units.iter().take(5) {
                creator.add_unit(unit);
            }
        }
        creator.add_unit(&units[0][5]);
        let round = 1;
        let preunit = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        assert_eq!(preunit.round(), round);
        for coord in preunit.control_hash().parents().take(5) {
            assert_eq!(coord.round(), round - 1);
        }
        assert!(preunit.control_hash().parents().nth(5).is_some());
    }
}
