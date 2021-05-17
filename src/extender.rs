use futures::{FutureExt, StreamExt};
use std::collections::{HashMap, VecDeque};
use tokio::sync::oneshot;

use log::{debug, error};

use crate::{
    nodes::{NodeCount, NodeIndex, NodeMap},
    Hasher, NodeIdT, Receiver, Round, Sender,
};

#[derive(Clone, Default, Debug)]
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

/// A process responsible for executing the Consensus protocol on a local copy of the Dag.
/// It receives units via a channel `electors` which are guaranteed to be eventually in the Dags
/// of all honest nodes. The static Aleph Consensus algorithm is then run on this Dag in order
/// to finalize subsequent rounds of the Dag. More specifically whenever a new unit is received
/// this process checks whether a new round can be finalized and if so, it computes the batch of
/// units that should be finalized, unwraps them (leaving only a block hash per unit) and pushes
/// such a batch to a channel via the finalizer_tx endpoint.

pub(crate) struct Extender<H: Hasher, NI: NodeIdT> {
    node_id: NI,
    electors: Receiver<ExtenderUnit<H>>,
    state: CacheState,
    units: HashMap<H::Hash, ExtenderUnit<H>>,
    units_by_round: Vec<Vec<H::Hash>>,
    n_members: NodeCount,
    candidates: Vec<H::Hash>,
    finalizer_tx: Sender<Vec<H::Hash>>,
}

impl<H: Hasher, NI: NodeIdT> Extender<H, NI> {
    pub(crate) fn new(
        node_id: NI,
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

    fn add_unit(&mut self, u: ExtenderUnit<H>) -> bool {
        debug!(target: "rush-extender", "{} New unit in Extender {:?}.", self.node_id, u.hash);
        let round = u.round;
        if round > self.state.highest_round {
            self.state.highest_round = round;
        }
        debug!(target: "rush-extender", "{} unit round {:?} state current_round {:?}", self.node_id, u.round, self.state.current_round);
        // need to extend the vector first to the required length
        if self.units_by_round.len() <= round {
            self.units_by_round.push(vec![]);
        }
        if round >= self.state.current_round {
            self.units_by_round[round].push(u.hash);
            self.units.insert(u.hash, u);
            true
        } else {
            false
        }
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
    fn finalize_round(&mut self, round: Round, head: &H::Hash) {
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

        // TODO (?): when constructing the batch we iterate over parents in the order of indices, this creates
        // a slight bias which causes the units of high-index nodes to be slightly preferred by the linear ordering
        // algorithm. Because of the finalization mechanism there seems to be no issue with that, but I'm leaving
        // this comment here in case we would like to think about that in the future.

        // We reverse for the batch to start with least recent units.
        batch.reverse();
        let send_result = self.finalizer_tx.send(batch);
        if let Err(e) = send_result {
            error!(target: "rush-extender", "{:?} Unable to send a batch to Finalizer: {:?}.", self.node_id, e);
        }
        debug!(target: "rush-extender", "{} Finalized round {}.", self.node_id, round);
        self.units_by_round[round].clear();
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
    fn progress(&mut self, u_new_hash: H::Hash) {
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

    pub(crate) async fn extend(&mut self, exit: oneshot::Receiver<()>) {
        let mut exit = exit.into_stream();
        loop {
            tokio::select! {
                Some(v) = self.electors.recv() =>{
                    let v_hash = v.hash;
                    if self.add_unit(v) {
                        self.progress(v_hash);
                    }
                }
                _ = exit.next() => {
                    debug!(target: "rush-extender", "{} received exit signal.", self.node_id);
                    break
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        nodes::NodeCount,
        testing::mock::{Hasher64, NodeId},
    };
    use tokio::sync::mpsc;

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

    #[tokio::test(max_threads = 3)]
    async fn finalize_rounds_01() {
        let n_members = 4;
        let rounds = 6;
        let (batch_tx, mut batch_rx) = mpsc::unbounded_channel();
        let (electors_tx, electors_rx) = mpsc::unbounded_channel();
        let mut extender = Extender::<Hasher64, NodeId>::new(
            0.into(),
            NodeCount(n_members),
            electors_rx,
            batch_tx,
        );
        let (exit_tx, exit_rx) = oneshot::channel();
        let extender_handle = tokio::spawn(async move { extender.extend(exit_rx).await });

        for round in 0..rounds {
            for creator in 0..n_members {
                let unit = construct_unit(creator, round, n_members);
                let _ = electors_tx.send(unit);
            }
        }
        let batch_round_0 = batch_rx.recv().await.unwrap();
        // TODO add better checks
        assert!(!batch_round_0.is_empty());

        let batch_round_1 = batch_rx.recv().await.unwrap();
        assert!(!batch_round_1.is_empty());
        let _ = exit_tx.send(());
        let _ = extender_handle.await;
    }
}
