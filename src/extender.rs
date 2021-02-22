use std::collections::{HashMap, VecDeque};

use crate::{
    nodes::{NodeCount, NodeIndex, NodeMap},
    Environment, HashT, Receiver, Round, Sender,
};

#[derive(Clone, Default)]
pub(crate) struct ExtenderUnit<B: HashT, H: HashT> {
    creator: NodeIndex,
    round: Round,
    parents: NodeMap<Option<H>>,
    hash: H,
    best_block: B,
    vote: bool,
}

impl<B: HashT, H: HashT> ExtenderUnit<B, H> {
    pub(crate) fn new(
        creator: NodeIndex,
        round: Round,
        hash: H,
        parents: NodeMap<Option<H>>,
        best_block: B,
    ) -> Self {
        ExtenderUnit {
            creator,
            round,
            hash,
            parents,
            best_block,
            vote: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct CacheState {
    highest_round: Round,
    current_round: Round,
    round_initialized: bool,
    pending_cand_id: usize,
    votes_up_to_date: bool,
}

impl CacheState {
    fn empty_dag_cache() -> Self {
        CacheState {
            highest_round: 0,
            current_round: 0,
            round_initialized: false,
            pending_cand_id: 0,
            votes_up_to_date: false,
        }
    }
}

// a process responsible for extending the partial order
pub(crate) struct Extender<E: Environment> {
    electors: Receiver<ExtenderUnit<E::BlockHash, E::Hash>>,
    finalizer_tx: Sender<Vec<E::BlockHash>>,
    state: CacheState,
    units: HashMap<E::Hash, ExtenderUnit<E::BlockHash, E::Hash>>,
    units_by_round: Vec<Vec<E::Hash>>,
    n_members: NodeCount,
    candidates: Vec<E::Hash>,
}

impl<E: Environment> Extender<E> {
    pub(crate) fn new(
        electors: Receiver<ExtenderUnit<E::BlockHash, E::Hash>>,
        finalizer_tx: Sender<Vec<E::BlockHash>>,
        n_members: NodeCount,
    ) -> Self {
        Extender {
            electors,
            finalizer_tx,
            state: CacheState::empty_dag_cache(),
            units: HashMap::new(),
            units_by_round: vec![vec![]],
            n_members,
            candidates: vec![],
        }
    }

    fn add_unit(&mut self, u: ExtenderUnit<E::BlockHash, E::Hash>) {
        let round = u.round;
        if round > self.state.highest_round {
            self.state.highest_round = round;
        }
        // need to extend the vector first to the required length
        if self.units_by_round.len() <= round {
            self.units_by_round.push(vec![]);
        }
        self.units_by_round[round].push(u.hash);
        self.units.insert(u.hash, u);
    }

    //
    fn initialize_round(&mut self, round: Round) {
        // The clone below is necessary as we take "a snapshot" of the set of units at this round and never
        // go back and never update this list. From math it follows that each unit that is added to the Dag later
        // then the moment of round initialization will be decided as false, hence they can be ignored.
        self.candidates = self.units_by_round[round].clone();
        // TODO: we sort units by hashes -- we could do some other permutation here
        self.candidates.sort();
    }

    fn common_vote(&self, relative_round: Round) -> bool {
        if relative_round == 3 {
            return false;
        }
        if relative_round <= 4 {
            return true;
        }
        // we alternate between true and false starting from round 5
        relative_round % 2 == 1
    }

    /// Prepares a batch and removes all unnecessary units from the data structures
    fn finalize_round(&mut self, round: Round, head: &E::Hash) {
        let mut batch = vec![];
        let mut queue = VecDeque::new();
        queue.push_back(self.units.remove(head).unwrap());
        while let Some(u) = queue.pop_front() {
            batch.push(u.best_block);
            for u_hash in u.parents.into_iter().flatten() {
                if let Some(v) = self.units.remove(&u_hash) {
                    queue.push_back(v);
                }
            }
        }
        // Since we construct the batch using BFS, the ordering is canonical and respects the DAG partial order.

        // TODO (?): when constructing the batch we iterate over parents in the order of indices, this creates
        // a slight bias which causes the units of high-index nodes to be slightly preferred by the linear ordering
        // algorithm. Because of the finalization mechanism there seems to be no issue with that, but I'm leaving
        // this comment here in case we would like to think about that in the future.

        // We reverse for the batch to start with least recent units.
        batch.reverse();
        let _ = self.finalizer_tx.send(batch);
        self.units_by_round[round].clear();
    }

    fn vote_and_decision(
        &self,
        candidate_hash: &E::Hash,
        voter_hash: &E::Hash,
        candidate_creator: NodeIndex,
        candidate_round: Round,
    ) -> (bool, Option<bool>) {
        // Outputs the vote and decision of a unit u, computed based on the votes of its parents
        // It is thus required the votes of the parents to be up-to-date (if relative_round>=2).
        let voter = self.units.get(voter_hash).unwrap();
        let relative_round = (voter.round) - candidate_round;
        if relative_round == 1 {
            return (
                voter.parents[candidate_creator] == Some(*candidate_hash),
                None,
            );
        }

        let mut n_votes_true = NodeCount(0);
        let mut n_votes_total = NodeCount(0);

        for p_hash in voter.parents.iter().flatten() {
            let p = self.units.get(p_hash).unwrap();
            if p.vote {
                n_votes_true += NodeCount(1);
            }
            n_votes_total += NodeCount(1);
        }
        let cv = self.common_vote(relative_round);
        let mut decision = None;
        let threshold = (self.n_members * 2) / 3;

        if relative_round >= 3
            && ((cv && n_votes_true > threshold)
                || (!cv && (n_votes_total - n_votes_true) > threshold))
        {
            decision = Some(cv);
        }

        let vote = {
            if n_votes_true == n_votes_total {
                true
            } else if n_votes_true == NodeCount(0) {
                false
            } else {
                cv
            }
        };
        (vote, decision)
    }

    // Tries to make progress in extending the partial order after adding a new unit to the Dag.
    fn progress(&mut self, u_new_hash: E::Hash) {
        loop {
            if !self.state.round_initialized {
                if self.state.highest_round >= self.state.current_round + 3 {
                    self.initialize_round(self.state.current_round);
                    self.state.round_initialized = true;
                    self.state.pending_cand_id = 0;
                    self.state.votes_up_to_date = false;
                    continue;
                } else {
                    break;
                }
            }

            let mut decision: Option<bool> = None;
            let curr_round = self.state.current_round;
            let candidate_hash = self.candidates[self.state.pending_cand_id];
            let candidate_creator = self.units.get(&candidate_hash).unwrap().creator;

            if !self.state.votes_up_to_date {
                // this means that for the unit currently considered for head we need to compute votes
                // and check for decisions
                for r in curr_round + 1..self.state.highest_round {
                    for u_hash in self.units_by_round[r].iter() {
                        let (vote, u_decision) = self.vote_and_decision(
                            &candidate_hash,
                            u_hash,
                            candidate_creator,
                            curr_round,
                        );
                        // we update the vote
                        self.units.get_mut(u_hash).unwrap().vote = vote;
                        decision = u_decision;
                        if decision.is_some() {
                            break;
                        }
                    }
                    if decision.is_some() {
                        break;
                    }
                }
            } else {
                // we only need to compute the decision and votes for the new unit u_new
                let (vote, u_decision) = self.vote_and_decision(
                    &candidate_hash,
                    &u_new_hash,
                    candidate_creator,
                    curr_round,
                );
                self.units.get_mut(&u_new_hash).unwrap().vote = vote;
                decision = u_decision;
            }

            if decision == Some(true) {
                self.finalize_round(self.state.current_round, &candidate_hash);
                self.state.current_round += 1;
                self.state.round_initialized = false;
                continue;
            }
            if decision == Some(false) {
                self.state.pending_cand_id += 1;
                self.state.votes_up_to_date = false;
                continue;
            }
            // decision = None, no progress can be done
            self.state.votes_up_to_date = true;
            break;
        }
    }

    pub(crate) async fn extend(&mut self) {
        loop {
            if let Some(v) = self.electors.recv().await {
                let v_hash = v.hash;
                self.add_unit(v);
                self.progress(v_hash);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nodes::NodeCount,
        testing::environment::{self, BlockHash, Hash},
    };
    use tokio::sync::mpsc;

    fn coord_to_number(creator: usize, round: usize, n_members: usize) -> usize {
        round * n_members + creator
    }

    fn construct_unit(
        creator: usize,
        round: usize,
        n_members: usize,
        best_block: BlockHash,
    ) -> ExtenderUnit<BlockHash, Hash> {
        let mut parents = NodeMap::new_with_len(NodeCount(n_members));
        if round > 0 {
            for i in 0..n_members {
                parents[NodeIndex(i)] = Some(Hash(coord_to_number(i, round - 1, n_members) as u32));
            }
        }

        ExtenderUnit::new(
            NodeIndex(creator),
            round,
            Hash(coord_to_number(creator, round, n_members) as u32),
            parents,
            best_block,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn finalize_rounds_01() {
        let n_members = 4;
        let rounds = 6;
        let (batch_tx, mut batch_rx) = mpsc::unbounded_channel();
        let (electors_tx, electors_rx) = mpsc::unbounded_channel();
        let mut extender =
            Extender::<environment::Environment>::new(electors_rx, batch_tx, NodeCount(n_members));
        let _extender_handle = tokio::spawn(async move { extender.extend().await });

        for round in 0..rounds {
            for creator in 0..n_members {
                let block = BlockHash(coord_to_number(creator, round, n_members) as u32 + 1000);
                let unit = construct_unit(creator, round, n_members, block);
                let _ = electors_tx.send(unit);
            }
        }
        let batch_round_0 = batch_rx.recv().await.unwrap();
        assert_eq!(batch_round_0, vec![BlockHash(1000)]);

        let batch_round_1 = batch_rx.recv().await.unwrap();
        assert_eq!(
            batch_round_1,
            vec![
                BlockHash(1003),
                BlockHash(1002),
                BlockHash(1001),
                BlockHash(1004)
            ]
        );
    }
}
