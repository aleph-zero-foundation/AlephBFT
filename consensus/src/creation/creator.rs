use crate::{
    units::{ControlHash, PreUnit, Unit},
    Hasher, NodeCount, NodeIndex, NodeMap, Round,
};
use anyhow::Result;
use thiserror::Error;

#[derive(Eq, Error, Debug, PartialEq)]
enum ConstraintError {
    #[error("Not enough parents.")]
    NotEnoughParents,
    #[error("Missing own parent.")]
    MissingOwnParent,
}

#[derive(Clone)]
struct UnitsCollector<H: Hasher> {
    candidates: NodeMap<H::Hash>,
    n_candidates: NodeCount,
}

impl<H: Hasher> UnitsCollector<H> {
    pub fn new(n_members: NodeCount) -> Self {
        Self {
            candidates: NodeMap::with_size(n_members),
            n_candidates: NodeCount(0),
        }
    }

    pub fn add_unit<U: Unit<Hasher = H>>(&mut self, unit: &U) {
        let node_id = unit.creator();
        let hash = unit.hash();

        if self.candidates.get(node_id).is_none() {
            self.candidates.insert(node_id, hash);
            self.n_candidates += NodeCount(1);
        }
    }

    pub fn prospective_parents(
        &self,
        node_id: NodeIndex,
    ) -> Result<&NodeMap<H::Hash>, ConstraintError> {
        if self.n_candidates < self.candidates.size().consensus_threshold() {
            return Err(ConstraintError::NotEnoughParents);
        }
        if self.candidates.get(node_id).is_none() {
            return Err(ConstraintError::MissingOwnParent);
        }
        Ok(&self.candidates)
    }
}

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
            round_collectors: vec![UnitsCollector::new(n_members)],
        }
    }

    pub fn current_round(&self) -> Round {
        (self.round_collectors.len() - 1) as Round
    }

    // gets or initializes a unit collector for a given round (and all between if not there)
    fn get_or_initialize_collector_for_round(&mut self, round: Round) -> &mut UnitsCollector<H> {
        let round_ix = usize::from(round);
        if round > self.current_round() {
            let new_size = round_ix + 1;
            self.round_collectors
                .resize(new_size, UnitsCollector::new(self.n_members));
        };
        &mut self.round_collectors[round_ix]
    }

    /// To create a new unit, we need to have at least the consensus threshold of parents available in previous round.
    /// Additionally, our unit from previous round must be available.
    pub fn create_unit(&self, round: Round) -> Result<PreUnit<H>> {
        let parents = match round.checked_sub(1) {
            None => NodeMap::with_size(self.n_members),
            Some(prev_round) => self
                .round_collectors
                .get(usize::from(prev_round))
                .ok_or(ConstraintError::NotEnoughParents)?
                .prospective_parents(self.node_id)?
                .clone(),
        };
        Ok(PreUnit::new(
            self.node_id,
            round,
            ControlHash::new(&parents),
        ))
    }

    pub fn add_unit<U: Unit<Hasher = H>>(&mut self, unit: &U) {
        self.get_or_initialize_collector_for_round(unit.round())
            .add_unit(unit);
    }
}

#[cfg(test)]
mod tests {
    use super::{Creator as GenericCreator, UnitsCollector};
    use crate::{
        creation::creator::ConstraintError,
        units::{create_preunits, creator_set, preunit_to_full_unit, Unit},
        NodeCount, NodeIndex,
    };
    use aleph_bft_mock::Hasher64;
    use std::collections::HashSet;

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
    fn units_collector_successfully_computes_parents() {
        let n_members = NodeCount(4);
        let creators = creator_set(n_members);
        let new_units = create_preunits(creators.iter(), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|pu| preunit_to_full_unit(pu, 0))
            .collect();

        let mut units_collector = UnitsCollector::new(n_members);
        new_units
            .iter()
            .for_each(|unit| units_collector.add_unit(unit));

        let parents = units_collector
            .prospective_parents(NodeIndex(0))
            .expect("we should be able to retrieve parents");
        assert_eq!(parents.item_count(), 4);

        let new_units: HashSet<_> = new_units.iter().map(|unit| unit.hash()).collect();
        let selected_parents = parents.values().cloned().collect();
        assert_eq!(new_units, selected_parents);
    }

    #[test]
    fn units_collector_returns_err_when_not_enough_parents() {
        let n_members = NodeCount(4);
        let creators = creator_set(n_members);
        let new_units = create_preunits(creators.iter().take(2), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|pu| preunit_to_full_unit(pu, 0))
            .collect();

        let mut units_collector = UnitsCollector::new(n_members);
        new_units
            .iter()
            .for_each(|unit| units_collector.add_unit(unit));

        let parents = units_collector.prospective_parents(NodeIndex(0));
        assert_eq!(
            parents.expect_err("should be an error"),
            ConstraintError::NotEnoughParents
        );
    }

    #[test]
    fn units_collector_returns_err_when_missing_own_parent() {
        let n_members = NodeCount(4);
        let creators = creator_set(n_members);
        let new_units = create_preunits(creators.iter().take(3), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|pu| preunit_to_full_unit(pu, 0))
            .collect();

        let mut units_collector = UnitsCollector::new(n_members);
        new_units
            .iter()
            .for_each(|unit| units_collector.add_unit(unit));

        let parents = units_collector.prospective_parents(NodeIndex(3));
        assert_eq!(
            parents.expect_err("should be an error"),
            ConstraintError::MissingOwnParent
        );
    }
}
