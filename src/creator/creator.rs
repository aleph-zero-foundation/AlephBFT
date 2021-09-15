use crate::{
    nodes::{NodeCount, NodeIndex, NodeMap},
    units::{ControlHash, PreUnit, Unit},
    Hasher, Round,
};
use log::trace;

pub(super) struct Creator<H: Hasher> {
    node_id: NodeIndex,
    n_members: NodeCount,
    candidates_by_round: Vec<NodeMap<Option<H::Hash>>>,
    n_candidates_by_round: Vec<NodeCount>, // len of this - 1 is the highest round number of all known units
}

impl<H: Hasher> Creator<H> {
    pub(super) fn new(node_id: NodeIndex, n_members: NodeCount) -> Self {
        Creator {
            node_id,
            n_members,
            candidates_by_round: vec![NodeMap::new_with_len(n_members)],
            n_candidates_by_round: vec![NodeCount(0)],
        }
    }

    fn current_round(&self) -> Round {
        (self.n_candidates_by_round.len() - 1) as Round
    }

    // initializes the vectors corresponding to the given round (and all between if not there)
    fn init_round(&mut self, round: Round) {
        if round > self.current_round() {
            let new_size = (round + 1).into();
            self.candidates_by_round
                .resize(new_size, NodeMap::new_with_len(self.n_members));
            self.n_candidates_by_round
                .resize(new_size, NodeCount(0));
        }
    }

    pub(super) fn create_unit(&mut self, round: Round) -> (PreUnit<H>, Vec<H::Hash>) {
        let parents = {
            if round == 0 {
                NodeMap::new_with_len(self.n_members)
            } else {
                self.candidates_by_round[(round - 1) as usize].clone()
            }
        };

        let control_hash = ControlHash::new(&parents);
        let parent_hashes: Vec<H::Hash> = parents.into_iter().flatten().collect();

        let new_preunit = PreUnit::new(self.node_id, round, control_hash);
        trace!(target: "AlephBFT-creator", "Created a new unit {:?} at round {:?}.", new_preunit, round);
        self.init_round(round + 1);
        (new_preunit, parent_hashes)
    }

    pub(super) fn add_unit(&mut self, unit: &Unit<H>) {
        let round = unit.round();
        let pid = unit.creator();
        let hash = unit.hash();
        self.init_round(round);
        if self.candidates_by_round[round as usize][pid].is_none() {
            // passing the check above means that we do not have any unit for the pair (round, pid) yet
            self.candidates_by_round[round as usize][pid] = Some(hash);
            self.n_candidates_by_round[round as usize] += NodeCount(1);
        }
    }

    /// Check whether the provided round is far behind the current round, meaning the unit of that
    /// round should be created without delay.
    pub(super) fn is_behind(&self, round: Round) -> bool {
        round + 2 < self.current_round()
    }

    /// To create a new unit, we need to have at least floor(2*N/3) + 1 parents available in previous round.
    /// Additionally, our unit from previous round must be available.
    pub(super) fn can_create(&self, round: Round) -> bool {
        if round == 0 {
            return true;
        }
        let prev_round = (round - 1).into();

        let threshold = (self.n_members * 2) / 3 + NodeCount(1);

        self.n_candidates_by_round.len() > prev_round &&
        self.n_candidates_by_round[prev_round] >= threshold &&
        self.candidates_by_round[prev_round][self.node_id].is_some()
    }
}

