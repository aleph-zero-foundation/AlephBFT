use futures::{channel::oneshot, StreamExt};
use std::collections::{HashMap, VecDeque};

use log::debug;

use crate::{
    nodes::{NodeCount, NodeIndex, NodeMap},
    Hasher, Receiver, Round, Sender,
};

pub(crate) struct ExtenderUnit<H: Hasher> {
    creator: NodeIndex,
    round: Round,
    parents: NodeMap<Option<H::Hash>>,
    hash: H::Hash,
    vote: bool,
}

impl<H: Hasher> ExtenderUnit<H> {
    pub(crate) fn new(
        creator: NodeIndex,
        round: Round,
        hash: H::Hash,
        parents: NodeMap<Option<H::Hash>>,
    ) -> Self {
        ExtenderUnit {
            creator,
            round,
            hash,
            parents,
            vote: false,
        }
    }
}

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

/// A process responsible for executing the Consensus protocol on a local copy of the Dag.
/// It receives units via a channel `electors` which are guaranteed to be eventually in the Dags
/// of all honest nodes. The static Aleph Consensus algorithm is then run on this Dag in order
/// to finalize subsequent rounds of the Dag. More specifically whenever a new unit is received
/// this process checks whether a new round can be finalized and if so, it computes the batch of
/// units that should be finalized, unwraps them (leaving only a block hash per unit) and pushes
/// such a batch to a channel via the finalizer_tx endpoint.

pub(crate) struct Extender<H: Hasher> {
    node_id: NodeIndex,
    electors: Receiver<ExtenderUnit<H>>,
    state: CacheState,
    units: HashMap<H::Hash, ExtenderUnit<H>>,
    units_by_round: Vec<Vec<H::Hash>>,
    n_members: NodeCount,
    candidates: Vec<H::Hash>,
    finalizer_tx: Sender<Vec<H::Hash>>,
}

impl<H: Hasher> Extender<H> {
    pub(crate) fn new(
        node_id: NodeIndex,
        n_members: NodeCount,
        electors: Receiver<ExtenderUnit<H>>,
        finalizer_tx: Sender<Vec<H::Hash>>,
    ) -> Self {
        Extender {
            node_id,
            electors,
            finalizer_tx,
            state: CacheState::empty_dag_cache(),
            units: HashMap::new(),
            units_by_round: vec![vec![]],
            n_members,
            candidates: vec![],
        }
    }

    fn add_unit(&mut self, u: ExtenderUnit<H>) {
        debug!(target: "AlephBFT-extender", "{:?} New unit in Extender round {:?} creator {:?} hash {:?}.", self.node_id, u.round, u.creator, u.hash);
        let round = u.round;
        if round > self.state.highest_round {
            self.state.highest_round = round;
        }

        if round >= self.state.current_round {
            //The units in units_by_round are required for head calculation only.
            //If the round of u is too low, we don't need to update it.

            //Need to extend the vector first to the required length.
            if self.units_by_round.len() <= round {
                self.units_by_round.push(vec![]);
            }
            self.units_by_round[round].push(u.hash);
        }
        self.units.insert(u.hash, u);
    }

    //
    fn initialize_round(&mut self, round: Round) {
        // The clone below is necessary as we take "a snapshot" of the set of units at this round and never
        // go back and never update this list. From math it follows that each unit that is added to the Dag later
        // then the moment of round initialization will be decided as false, hence they can be ignored.
        self.candidates = self.units_by_round[round].clone();
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
    fn finalize_round(&mut self, round: Round, head: &H::Hash) -> Result<(), ()> {
        let mut batch = vec![];
        let mut queue = VecDeque::new();
        queue.push_back(self.units.remove(head).unwrap());
        while let Some(u) = queue.pop_front() {
            batch.push(u.hash);
            for u_hash in u.parents.into_iter().flatten() {
                if let Some(v) = self.units.remove(&u_hash) {
                    queue.push_back(v);
                }
            }
        }
        // Since we construct the batch using BFS, the ordering is canonical and respects the DAG partial order.

        // We reverse for the batch to start with least recent units.
        batch.reverse();
        self.finalizer_tx
            .unbounded_send(batch)
            .map_err(|e| {
                debug!(target: "AlephBFT-extender", "{:?} channel for batches is closed {:?}, closing", self.node_id, e);
            })?;

        debug!(target: "AlephBFT-extender", "{:?} Finalized round {:?} with head {:?}.", self.node_id, round, head);
        self.units_by_round[round].clear();

        Ok(())
    }

    fn vote_and_decision(
        &self,
        candidate_hash: &H::Hash,
        voter_hash: &H::Hash,
        candidate_creator: NodeIndex,
        candidate_round: Round,
    ) -> (bool, Option<bool>) {
        // Outputs the vote and decision of a unit u, computed based on the votes of its parents
        // It is thus required the votes of the parents to be up-to-date (if relative_round>=2).
        let voter = self.units.get(voter_hash).unwrap();
        if voter.round <= candidate_round {
            return (false, None);
        }
        let relative_round = voter.round - candidate_round;
        if relative_round == 1 {
            return (
                voter.parents[candidate_creator] == Some(*candidate_hash),
                None,
            );
        }

        let mut n_votes_true = NodeCount(0);
        let mut n_votes_false = NodeCount(0);

        for p_hash in voter.parents.iter().flatten() {
            let p = self.units.get(p_hash).unwrap();
            if p.vote {
                n_votes_true += NodeCount(1);
            } else {
                n_votes_false += NodeCount(1);
            }
        }
        let cv = self.common_vote(relative_round);
        let mut decision = None;
        let threshold = (self.n_members * 2) / 3 + NodeCount(1);
        assert!(n_votes_true + n_votes_false >= threshold);

        if relative_round >= 3
            && ((cv && n_votes_true >= threshold) || (!cv && n_votes_false >= threshold))
        {
            decision = Some(cv);
        }

        let vote = {
            if n_votes_false == NodeCount(0) {
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
    fn progress(&mut self, u_new_hash: H::Hash) -> Result<(), ()> {
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
                // We need to recompute all the votes for the current candidate.
                for r in curr_round + 1..=self.state.highest_round {
                    for u_hash in self.units_by_round[r].iter() {
                        let (vote, u_decision) = self.vote_and_decision(
                            &candidate_hash,
                            u_hash,
                            candidate_creator,
                            curr_round,
                        );
                        // We update the vote.
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
                // We don't need to recompute all the votes, but only compute the vote and possibly the
                // decision for the new unit u_new.
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
                self.finalize_round(self.state.current_round, &candidate_hash)?;
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
        Ok(())
    }

    pub(crate) async fn extend(&mut self, mut exit: oneshot::Receiver<()>) {
        loop {
            futures::select! {
                v = self.electors.next() => {
                    if let Some(v) = v {
                        let v_hash = v.hash;
                        self.add_unit(v);
                        if self.progress(v_hash).is_err(){
                            break
                        }
                    }
                }
                _ = &mut exit => {
                    debug!(target: "AlephBFT-extender", "{:?} received exit signal.", self.node_id);
                    break
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{nodes::NodeCount, testing::mock::Hasher64};
    use futures::channel::mpsc;

    fn coord_to_number(creator: usize, round: usize, n_members: usize) -> usize {
        round * n_members + creator
    }

    fn construct_unit(creator: usize, round: usize, n_members: usize) -> ExtenderUnit<Hasher64> {
        let mut parents = NodeMap::new_with_len(NodeCount(n_members));
        if round > 0 {
            for i in 0..n_members {
                parents[NodeIndex(i)] =
                    Some((coord_to_number(i, round - 1, n_members) as u64).to_ne_bytes());
            }
        }

        ExtenderUnit::new(
            NodeIndex(creator),
            round,
            (coord_to_number(creator, round, n_members) as u64).to_ne_bytes(),
            parents,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn finalize_rounds_01() {
        let n_members = 4;
        let rounds = 6;
        let (batch_tx, mut batch_rx) = mpsc::unbounded();
        let (electors_tx, electors_rx) = mpsc::unbounded();
        let mut extender =
            Extender::<Hasher64>::new(0.into(), NodeCount(n_members), electors_rx, batch_tx);
        let (exit_tx, exit_rx) = oneshot::channel();
        let extender_handle = tokio::spawn(async move { extender.extend(exit_rx).await });

        for round in 0..rounds {
            for creator in 0..n_members {
                let unit = construct_unit(creator, round, n_members);
                electors_tx
                    .unbounded_send(unit)
                    .expect("Channel should be open");
            }
        }
        let batch_round_0 = batch_rx.next().await.unwrap();
        assert!(!batch_round_0.is_empty());

        let batch_round_1 = batch_rx.next().await.unwrap();
        assert!(!batch_round_1.is_empty());
        let _ = exit_tx.send(());
        let _ = extender_handle.await;
    }
}
