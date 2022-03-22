use crate::{
    units::{ControlHash, PreUnit, Unit},
    Hasher, NodeCount, NodeIndex, NodeMap, Round,
};
use log::trace;

pub(super) struct Creator<H: Hasher> {
    node_id: NodeIndex,
    n_members: NodeCount,
    candidates_by_round: Vec<NodeMap<H::Hash>>,
    n_candidates_by_round: Vec<NodeCount>, // len of this - 1 is the highest round number of all known units
}

impl<H: Hasher> Creator<H> {
    pub(super) fn new(node_id: NodeIndex, n_members: NodeCount) -> Self {
        Creator {
            node_id,
            n_members,
            candidates_by_round: vec![NodeMap::with_size(n_members)],
            n_candidates_by_round: vec![NodeCount(0)],
        }
    }

    pub(super) fn current_round(&self) -> Round {
        (self.n_candidates_by_round.len() - 1) as Round
    }

    // initializes the vectors corresponding to the given round (and all between if not there)
    fn init_round(&mut self, round: Round) {
        if round > self.current_round() {
            let new_size = (round + 1).into();
            self.candidates_by_round
                .resize(new_size, NodeMap::with_size(self.n_members));
            self.n_candidates_by_round.resize(new_size, NodeCount(0));
        }
    }

    /// Returns `None` if a unit cannot be created.
    /// To create a new unit, we need to have at least floor(2*N/3) + 1 parents available in previous round.
    /// Additionally, our unit from previous round must be available.
    pub(super) fn create_unit(&self, round: Round) -> Option<(PreUnit<H>, Vec<H::Hash>)> {
        if !self.can_create(round) {
            return None;
        }
        let parents = {
            if round == 0 {
                NodeMap::with_size(self.n_members)
            } else {
                self.candidates_by_round[(round - 1) as usize].clone()
            }
        };

        let control_hash = ControlHash::new(&parents);
        let parent_hashes = parents.into_values().collect();

        let new_preunit = PreUnit::new(self.node_id, round, control_hash);
        trace!(target: "AlephBFT-creator", "Created a new unit {:?} at round {:?}.", new_preunit, round);
        Some((new_preunit, parent_hashes))
    }

    pub(super) fn add_unit(&mut self, unit: &Unit<H>) {
        let round = unit.round();
        let pid = unit.creator();
        let hash = unit.hash();
        self.init_round(round);
        if self.candidates_by_round[round as usize].get(pid).is_none() {
            // passing the check above means that we do not have any unit for the pair (round, pid) yet
            self.candidates_by_round[round as usize].insert(pid, hash);
            self.n_candidates_by_round[round as usize] += NodeCount(1);
        }
    }

    fn can_create(&self, round: Round) -> bool {
        if round == 0 {
            return true;
        }
        let prev_round = (round - 1).into();

        let threshold = (self.n_members * 2) / 3 + NodeCount(1);

        self.n_candidates_by_round.len() > prev_round
            && self.n_candidates_by_round[prev_round] >= threshold
            && self.candidates_by_round[prev_round]
                .get(self.node_id)
                .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::Creator as GenericCreator;
    use crate::{
        testing::mock::{Data, Hasher64},
        units::{
            FullUnit as GenericFullUnit, PreUnit as GenericPreUnit, Unit as GenericUnit, UnitCoord,
        },
        Hasher, NodeCount, NodeIndex, Round,
    };
    use std::collections::HashSet;

    type Creator = GenericCreator<Hasher64>;
    type PreUnit = GenericPreUnit<Hasher64>;
    type Unit = GenericUnit<Hasher64>;
    type FullUnit = GenericFullUnit<Hasher64, Data>;

    fn creator_set(n_members: NodeCount) -> Vec<Creator> {
        let mut result = Vec::new();
        for i in 0..n_members.0 {
            result.push(Creator::new(NodeIndex(i), n_members));
        }
        result
    }

    fn create_units<'a, C: Iterator<Item = &'a Creator>>(
        creators: C,
        round: Round,
    ) -> Vec<(PreUnit, Vec<<Hasher64 as Hasher>::Hash>)> {
        let mut result = Vec::new();
        for creator in creators {
            result.push(
                creator
                    .create_unit(round)
                    .expect("Creation should succeed."),
            );
        }
        result
    }

    fn preunit_to_unit(preunit: PreUnit) -> Unit {
        FullUnit::new(
            preunit,
            // The coord is wrong, but it doesn't matter.
            Data::new(UnitCoord::new(0, 0.into()), 0),
            0,
        )
        .unit()
    }

    fn add_units(creator: &mut Creator, units: &[Unit]) {
        for unit in units {
            creator.add_unit(unit);
        }
    }

    #[test]
    fn creates_initial_unit() {
        let n_members = NodeCount(7);
        let round = 0;
        let creator = Creator::new(NodeIndex(0), n_members);
        assert_eq!(creator.current_round(), round);
        let (preunit, parent_hashes) = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        assert_eq!(preunit.round(), round);
        assert_eq!(parent_hashes.len(), 0);
    }

    #[test]
    fn creates_unit_with_all_parents() {
        let n_members = NodeCount(7);
        let mut creators = creator_set(n_members);
        let new_units = create_units(creators.iter(), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|(pu, _)| preunit_to_unit(pu))
            .collect();
        let expected_hashes: Vec<_> = new_units.iter().map(|u| u.hash()).collect();
        let creator = &mut creators[0];
        add_units(creator, &new_units);
        let round = 1;
        assert_eq!(creator.current_round(), 0);
        let (preunit, parent_hashes) = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        assert_eq!(preunit.round(), round);
        assert_eq!(parent_hashes, expected_hashes);
    }

    fn create_unit_with_minimal_parents(n_members: NodeCount) {
        let n_parents = (n_members.0 * 2) / 3 + 1;
        let mut creators = creator_set(n_members);
        let new_units = create_units(creators.iter().take(n_parents), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|(pu, _)| preunit_to_unit(pu))
            .collect();
        let expected_hashes: Vec<_> = new_units.iter().map(|u| u.hash()).collect();
        let creator = &mut creators[0];
        add_units(creator, &new_units);
        let round = 1;
        assert_eq!(creator.current_round(), 0);
        let (preunit, parent_hashes) = creator
            .create_unit(round)
            .expect("Creation should succeed.");
        assert_eq!(preunit.round(), round);
        assert_eq!(parent_hashes, expected_hashes);
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
        let n_parents = (n_members.0 * 2) / 3;
        let mut creators = creator_set(n_members);
        let new_units = create_units(creators.iter().take(n_parents), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|(pu, _)| preunit_to_unit(pu))
            .collect();
        let creator = &mut creators[0];
        add_units(creator, &new_units);
        let round = 1;
        assert_eq!(creator.current_round(), 0);
        assert!(creator.create_unit(round).is_none())
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
        let mut expected_hashes_per_round = Vec::new();
        for round in 0..2 {
            let new_units = create_units(creators.iter().skip(1), round);
            let new_units: Vec<_> = new_units
                .into_iter()
                .map(|(pu, _)| preunit_to_unit(pu))
                .collect();
            let expected_hashes: HashSet<_> = new_units.iter().map(|u| u.hash()).collect();
            for creator in creators.iter_mut() {
                add_units(creator, &new_units);
            }
            expected_hashes_per_round.push(expected_hashes);
        }
        let creator = &mut creators[0];
        assert_eq!(creator.current_round(), 1);
        for round in 0..3 {
            let (preunit, parent_hashes) = creator
                .create_unit(round)
                .expect("Creation should succeed.");
            assert_eq!(preunit.round(), round);
            let parent_hashes: HashSet<_> = parent_hashes.into_iter().collect();
            if round != 0 {
                assert_eq!(
                    parent_hashes,
                    expected_hashes_per_round[(round - 1) as usize]
                );
            }
            let unit = preunit_to_unit(preunit);
            creator.add_unit(&unit);
            if round < 2 {
                expected_hashes_per_round[round as usize].insert(unit.hash());
            }
        }
    }

    #[test]
    fn cannot_create_unit_without_predecessor() {
        let n_members = NodeCount(7);
        let mut creators = creator_set(n_members);
        let new_units = create_units(creators.iter().skip(1), 0);
        let new_units: Vec<_> = new_units
            .into_iter()
            .map(|(pu, _)| preunit_to_unit(pu))
            .collect();
        let creator = &mut creators[0];
        add_units(creator, &new_units);
        let round = 1;
        assert!(creator.create_unit(round).is_none());
    }
}
