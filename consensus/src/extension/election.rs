use std::collections::HashMap;

use crate::{
    extension::units::Units,
    units::{HashFor, UnitWithParents},
    Hasher, NodeCount, NodeIndex, Round,
};

fn common_vote(relative_round: Round) -> bool {
    // This should only be called for relative round >= 2, so to be precise we start with true, false, true, and then
    if relative_round == 3 {
        return false;
    }
    if relative_round <= 4 {
        return true;
    }
    // we alternate between true and false starting from true in round 5.
    relative_round % 2 == 1
}

enum CandidateOutcome<H: Hasher> {
    Eliminate,
    ElectionDone(H::Hash),
}

struct CandidateElection<U: UnitWithParents> {
    round: Round,
    candidate_creator: NodeIndex,
    candidate_hash: HashFor<U>,
    votes: HashMap<HashFor<U>, bool>,
}

impl<U: UnitWithParents> CandidateElection<U> {
    /// Creates an election for the given candidate.
    /// The candidate will eventually either get elected or eliminated.
    /// Might immediately return an outcome.
    pub fn for_candidate(
        candidate: &U,
        units: &Units<U>,
    ) -> Result<Self, CandidateOutcome<U::Hasher>> {
        CandidateElection {
            round: candidate.round(),
            candidate_creator: candidate.creator(),
            candidate_hash: candidate.hash(),
            votes: HashMap::new(),
        }
        .compute_votes(units)
    }

    fn parent_votes(
        &mut self,
        parents: Vec<HashFor<U>>,
    ) -> Result<(NodeCount, NodeCount), CandidateOutcome<U::Hasher>> {
        let (mut votes_for, mut votes_against) = (NodeCount(0), NodeCount(0));
        for parent in parents {
            match self.votes.get(&parent).expect("units are added in order") {
                true => votes_for += NodeCount(1),
                false => votes_against += NodeCount(1),
            }
        }
        Ok((votes_for, votes_against))
    }

    fn vote_from_parents(
        &mut self,
        parents: Vec<HashFor<U>>,
        threshold: NodeCount,
        relative_round: Round,
    ) -> Result<bool, CandidateOutcome<U::Hasher>> {
        use CandidateOutcome::*;
        // Gather parents' votes.
        let (votes_for, votes_against) = self.parent_votes(parents)?;
        assert!(votes_for + votes_against >= threshold);
        let common_vote = common_vote(relative_round);
        // If the round is sufficiently high we are done voting for the candidate if
        if relative_round >= 3 {
            match common_vote {
                // the default vote is for the candidate and the parents' votes are for over the threshold,
                true if votes_for >= threshold => return Err(ElectionDone(self.candidate_hash)),
                // or the default vote is against the candidate and the parents' votes are against over the threshold.
                false if votes_against >= threshold => return Err(Eliminate),
                _ => (),
                // Note that this means the earliest we can have a head elected is round 4.
            }
        }

        // The vote is either identical to all the votes of the parents, or the default vote if that is not possible.
        Ok(match (votes_for, votes_against) {
            (NodeCount(0), _) => false,
            (_, NodeCount(0)) => true,
            _ => common_vote,
        })
    }

    fn vote(&mut self, voter: &U) -> Result<(), CandidateOutcome<U::Hasher>> {
        // If the vote is already computed we are done.
        if self.votes.contains_key(&voter.hash()) {
            return Ok(());
        }
        // Votes for old units are never used, so we just return.
        if voter.round() <= self.round {
            return Ok(());
        }
        let relative_round = voter.round() - self.round;
        let vote = match relative_round {
            0 => unreachable!("just checked that voter and election rounds are not equal"),
            // Direct descendants vote for, all other units of that round against.
            1 => voter.parent_for(self.candidate_creator) == Some(&self.candidate_hash),
            // Otherwise we compute the vote based on the parents' votes.
            _ => {
                let threshold = voter.node_count().consensus_threshold();
                let direct_parents = voter.direct_parents().cloned().collect();
                self.vote_from_parents(direct_parents, threshold, relative_round)?
            }
        };
        self.votes.insert(voter.hash(), vote);
        Ok(())
    }

    fn compute_votes(mut self, units: &Units<U>) -> Result<Self, CandidateOutcome<U::Hasher>> {
        for round in self.round + 1..=units.highest_round() {
            for voter in units.in_round(round).expect("units are added in order") {
                self.vote(voter)?;
            }
        }
        Ok(self)
    }

    /// Add a single voter and compute their vote. This might end up electing or eliminating the candidate.
    /// Might panic if called for a unit before its parents.
    pub fn add_voter(mut self, voter: &U) -> Result<Self, CandidateOutcome<U::Hasher>> {
        self.vote(voter).map(|()| self)
    }
}

/// Election for a single round.
pub struct RoundElection<U: UnitWithParents> {
    // Remaining candidates for this round's head, in reverse order.
    candidates: Vec<HashFor<U>>,
    voting: CandidateElection<U>,
}

/// An election result.
pub enum ElectionResult<U: UnitWithParents> {
    /// The election is not done yet.
    Pending(RoundElection<U>),
    /// The head has been elected.
    Elected(HashFor<U>),
}

impl<U: UnitWithParents> RoundElection<U> {
    /// Create a new round election. It might immediately be decided, so this might return an election result rather than a pending election.
    /// Returns an error when it's too early to finalize the candidate list, i.e. we are not at least 3 rounds ahead of the election round.
    ///
    /// Note: it is crucial that units are added to `Units` only when all their parents are there, otherwise this might panic.
    pub fn for_round(round: Round, units: &Units<U>) -> Result<ElectionResult<U>, ()> {
        // If we don't yet have a unit of round + 3 we might not know about the winning candidate, so we cannot start the election.
        if units.highest_round() < round + 3 {
            return Err(());
        }
        // We might be missing units from this round, but any unit that is not an ancestor of an arbitrary unit from round + 3
        // will always eventually be eliminated in the voting, so we can freely skip it.
        let mut candidates: Vec<_> = units
            .in_round(round)
            .expect("units come in order, so we definitely have units from this round")
            .iter()
            .map(|candidate| candidate.hash())
            .collect();
        candidates.sort();
        // We will be `pop`ing the candidates from the back.
        candidates.reverse();
        let candidate = units
            .get(&candidates.pop().expect("there is a candidate"))
            .expect("we have all the units we work with");
        Ok(Self::handle_candidate_election_result(
            CandidateElection::for_candidate(candidate, units),
            candidates,
            units,
        ))
    }

    fn handle_candidate_election_result(
        result: Result<CandidateElection<U>, CandidateOutcome<U::Hasher>>,
        mut candidates: Vec<HashFor<U>>,
        units: &Units<U>,
    ) -> ElectionResult<U> {
        use CandidateOutcome::*;
        use ElectionResult::*;
        match result {
            // Wait for more voters.
            Ok(voting) => Pending(RoundElection { candidates, voting }),
            // Pick the next candidate and keep trying.
            Err(Eliminate) => {
                let candidate = units
                    .get(&candidates.pop().expect("there is a candidate"))
                    .expect("we have all the units we work with");
                Self::handle_candidate_election_result(
                    CandidateElection::for_candidate(candidate, units),
                    candidates,
                    units,
                )
            }
            // Yay, we picked a head.
            Err(ElectionDone(head)) => Elected(head),
        }
    }

    /// Add a single voter to the election.
    /// Might panic if not all parents were added previously.
    pub fn add_voter(self, voter: &U, units: &Units<U>) -> ElectionResult<U> {
        let RoundElection { candidates, voting } = self;
        Self::handle_candidate_election_result(voting.add_voter(voter), candidates, units)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        extension::{
            election::{ElectionResult, RoundElection},
            units::Units,
        },
        units::{
            minimal_reconstructed_dag_units_up_to, random_full_parent_reconstrusted_units_up_to,
            random_reconstructed_unit_with_parents, TestingDagUnit, Unit,
        },
        NodeCount,
    };
    use aleph_bft_mock::Keychain;

    #[test]
    fn refuses_to_elect_without_units() {
        let units = Units::<TestingDagUnit>::new();
        assert!(RoundElection::for_round(0, &units).is_err());
    }

    #[test]
    fn refuses_to_elect_with_insufficient_units() {
        let mut units = Units::new();
        let n_members = NodeCount(4);
        let max_round = 2;
        let session_id = 2137;
        let keychains = Keychain::new_vec(n_members);
        for round_units in random_full_parent_reconstrusted_units_up_to(
            max_round, n_members, session_id, &keychains,
        ) {
            for unit in round_units {
                units.add_unit(unit);
            }
        }
        assert!(RoundElection::for_round(0, &units).is_err());
    }

    #[test]
    fn easy_election() {
        use ElectionResult::*;
        let mut units = Units::new();
        let n_members = NodeCount(4);
        let max_round = 4;
        let session_id = 2137;
        let keychains = Keychain::new_vec(n_members);
        let dag = random_full_parent_reconstrusted_units_up_to(
            max_round, n_members, session_id, &keychains,
        );
        for round_units in dag.iter().take(4) {
            for unit in round_units {
                units.add_unit(unit.clone());
            }
        }
        let election = RoundElection::for_round(0, &units).expect("we have enough rounds");
        let election = match election {
            Pending(election) => election,
            Elected(_) => panic!("elected head without units of round + 4"),
        };
        let last_voter = dag[4].last().expect("created all units").clone();
        units.add_unit(last_voter.clone());
        match election.add_voter(&last_voter, &units) {
            Pending(_) => panic!("failed to elect obvious head"),
            Elected(head) => {
                assert_eq!(units.get(&head).expect("we have the head").round(), 0);
            }
        }
    }

    #[test]
    fn immediate_election() {
        use ElectionResult::*;
        let mut units = Units::new();
        let n_members = NodeCount(4);
        let max_round = 4;
        let session_id = 2137;
        let keychains = Keychain::new_vec(n_members);
        for round_units in random_full_parent_reconstrusted_units_up_to(
            max_round, n_members, session_id, &keychains,
        ) {
            for unit in round_units {
                units.add_unit(unit.clone());
            }
        }
        let election = RoundElection::for_round(0, &units).expect("we have enough rounds");
        match election {
            Pending(_) => panic!("should have elected"),
            Elected(head) => {
                assert_eq!(units.get(&head).expect("we have the head").round(), 0);
            }
        }
    }

    #[test]
    fn eliminates_unpopular() {
        use ElectionResult::*;
        let mut units = Units::new();
        let n_members = NodeCount(4);
        let max_round = 4;
        let session_id = 2137;
        let keychains = Keychain::new_vec(n_members);
        for unit in
            random_full_parent_reconstrusted_units_up_to(0, n_members, session_id, &keychains)
                .last()
                .expect("just created")
        {
            units.add_unit(unit.clone());
        }
        let mut candidate_hashes: Vec<_> = units
            .in_round(0)
            .expect("just added these")
            .iter()
            .map(|candidate| candidate.hash())
            .collect();
        candidate_hashes.sort();
        let inactive_node = units
            .get(&candidate_hashes[0])
            .expect("we just got it")
            .creator();
        for round in 1..=max_round {
            let parents: Vec<TestingDagUnit> = units
                .in_round(round - 1)
                .expect("created in order")
                .into_iter()
                .filter(|unit| unit.creator() != inactive_node)
                .cloned()
                .collect();
            for creator in n_members
                .into_iterator()
                .filter(|node_id| node_id != &inactive_node)
            {
                units.add_unit(random_reconstructed_unit_with_parents(
                    creator,
                    &parents,
                    &keychains[creator.0],
                    round,
                ));
            }
        }
        let election = RoundElection::for_round(0, &units).expect("we have enough rounds");
        match election {
            Pending(_) => panic!("should have elected"),
            Elected(head) => {
                // This should be the second unit in order, as the first was not popular.
                assert_eq!(head, candidate_hashes[1]);
            }
        }
    }

    #[test]
    fn given_minimal_dag_with_orphaned_node_when_electing_then_orphaned_node_is_not_head() {
        use ElectionResult::*;
        let mut units = Units::new();
        let n_members = NodeCount(14);
        let max_round = 4;
        let session_id = 2137;
        let keychains = Keychain::new_vec(n_members);

        let (dag, inactive_node_first_unit) =
            minimal_reconstructed_dag_units_up_to(max_round, n_members, session_id, &keychains);
        for round in dag {
            for unit in round {
                units.add_unit(unit);
            }
        }
        let election = RoundElection::for_round(0, &units).expect("we have enough rounds");
        match election {
            Pending(_) => panic!("should have elected"),
            Elected(head) => {
                // This should be the second unit in order, as the first was not popular.
                assert_ne!(head, inactive_node_first_unit.hash());
            }
        }
    }
}
