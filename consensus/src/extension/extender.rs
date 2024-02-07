use crate::{
    extension::{
        election::{ElectionResult, RoundElection},
        units::Units,
        ExtenderUnit,
    },
    Hasher, Round,
};

pub struct Extender<H: Hasher> {
    election: Option<RoundElection<H>>,
    units: Units<H>,
    round: Round,
}

impl<H: Hasher> Extender<H> {
    /// Create a new extender with no units.
    pub fn new() -> Self {
        Extender {
            election: None,
            units: Units::new(),
            round: 0,
        }
    }

    fn handle_election_result(&mut self, result: ElectionResult<H>) -> Option<Vec<H::Hash>> {
        use ElectionResult::*;
        match result {
            // Wait for more voters for this election.
            Pending(election) => {
                self.election = Some(election);
                None
            }
            // Advance to the next round and return the ordered batch.
            Elected(head) => {
                self.round += 1;
                Some(self.units.remove_batch(head))
            }
        }
    }

    /// Add a unit to the extender. Might return several batches of ordered units as a result.
    pub fn add_unit(&mut self, u: ExtenderUnit<H>) -> Vec<Vec<H::Hash>> {
        let hash = u.hash;
        self.units.add_unit(u);
        let unit = self.units.get(&hash).expect("just added");
        let mut result = Vec::new();
        // If we have an ongoing election try to finish it.
        if let Some(election) = self.election.take() {
            if let Some(batch) = self.handle_election_result(election.add_voter(unit, &self.units))
            {
                result.push(batch);
            }
        }
        // Try finding another election to be working on.
        while self.election.is_none() {
            match RoundElection::for_round(self.round, &self.units) {
                Ok(election_result) => {
                    if let Some(batch) = self.handle_election_result(election_result) {
                        result.push(batch);
                    }
                }
                // Not enough voters yet.
                Err(()) => break,
            }
        }
        result
    }
}

#[cfg(test)]
mod test {
    use crate::{
        extension::{
            extender::Extender,
            tests::{construct_unit, construct_unit_all_parents},
        },
        NodeCount, NodeIndex, Round,
    };
    use std::iter;

    #[test]
    fn easy_elections() {
        let mut extender = Extender::new();
        let n_members = NodeCount(4);
        let max_round: Round = 43;
        let mut batches = Vec::new();
        for round in 0..=max_round {
            for creator in n_members.into_iterator() {
                batches.append(
                    &mut extender.add_unit(construct_unit_all_parents(creator, round, n_members)),
                );
            }
        }
        assert_eq!(batches.len(), (max_round - 3).into());
        assert_eq!(batches[0].len(), 1);
        for batch in batches.iter().skip(1) {
            assert_eq!(batch.len(), n_members.0);
        }
    }

    // TODO(A0-1047): Rewrite this once we order all the data, even unpopular.
    #[test]
    fn ignores_sufficiently_unpopular() {
        let mut extender = Extender::new();
        let n_members = NodeCount(4);
        let max_round: Round = 43;
        let active_nodes: Vec<_> = n_members.into_iterator().skip(1).collect();
        let mut batches = Vec::new();
        for round in 0..=max_round {
            for creator in n_members.into_iterator() {
                let parent_coords = match round.checked_sub(1) {
                    None => Vec::new(),
                    Some(parent_round) => match creator {
                        NodeIndex(0) => n_members
                            .into_iterator()
                            .zip(iter::repeat(parent_round))
                            .collect(),
                        _ => active_nodes
                            .iter()
                            .cloned()
                            .zip(iter::repeat(parent_round))
                            .collect(),
                    },
                };
                batches.append(&mut extender.add_unit(construct_unit(
                    creator,
                    round,
                    parent_coords,
                    n_members,
                )));
            }
        }
        assert_eq!(batches.len(), (max_round - 3).into());
        assert_eq!(batches[0].len(), 1);
        for batch in batches.iter().skip(1) {
            assert_eq!(batch.len(), n_members.0 - 1);
        }
    }
}
