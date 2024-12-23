use crate::{
    extension::{
        election::{ElectionResult, RoundElection},
        units::Units,
    },
    units::UnitWithParents,
    Round,
};

pub struct Extender<U: UnitWithParents> {
    election: Option<RoundElection<U>>,
    units: Units<U>,
    round: Round,
}

impl<U: UnitWithParents> Extender<U> {
    /// Create a new extender with no units.
    pub fn new() -> Self {
        Extender {
            election: None,
            units: Units::new(),
            round: 0,
        }
    }

    fn handle_election_result(&mut self, result: ElectionResult<U>) -> Option<Vec<U>> {
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
                Some(self.units.remove_batch(&head))
            }
        }
    }

    /// Add a unit to the extender. Might return several batches of ordered units as a result.
    pub fn add_unit(&mut self, u: U) -> Vec<Vec<U>> {
        let hash = u.hash();
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
    use crate::units::{minimal_reconstructed_dag_units_up_to, Unit};
    use crate::{
        extension::extender::Extender, units::random_full_parent_reconstrusted_units_up_to,
        NodeCount, Round,
    };
    use aleph_bft_mock::Keychain;

    #[test]
    fn easy_elections() {
        let mut extender = Extender::new();
        let n_members = NodeCount(4);
        let max_round: Round = 43;
        let session_id = 2137;
        let keychains = Keychain::new_vec(n_members);
        let mut batches = Vec::new();
        for round_units in random_full_parent_reconstrusted_units_up_to(
            max_round, n_members, session_id, &keychains,
        ) {
            for unit in round_units {
                batches.append(&mut extender.add_unit(unit));
            }
        }
        assert_eq!(batches.len(), (max_round - 3).into());
        assert_eq!(batches[0].len(), 1);
        for batch in batches.iter().skip(1) {
            assert_eq!(batch.len(), n_members.0);
        }
    }

    #[test]
    #[ignore]
    // TODO(A0-4559) Uncomment
    fn given_minimal_dag_with_orphaned_node_when_producing_batches_have_correct_length() {
        let mut extender = Extender::new();
        let n_members = NodeCount(4);
        let threshold = n_members.consensus_threshold();
        let max_round: Round = 4;
        let session_id = 2137;
        let keychains = Keychain::new_vec(n_members);
        let mut batches = Vec::new();
        let (dag, _) =
            minimal_reconstructed_dag_units_up_to(max_round, n_members, session_id, &keychains);
        for round in dag {
            for unit in round {
                batches.append(&mut extender.add_unit(unit));
            }
        }
        assert_eq!(batches.len(), (max_round - 3).into());
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[0][0].round(), 0);
        for batch in batches.iter().skip(1) {
            assert_eq!(batch.len(), threshold.0);
        }
    }
}
