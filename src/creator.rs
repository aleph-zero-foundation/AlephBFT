use crate::{
    nodes::{NodeCount, NodeIndex, NodeMap},
    Config, Environment, EpochId, Hashing, MyIndex, Receiver, Round, Sender, Unit,
};
use log::{debug, error};
use tokio::time::{sleep, Duration};

/// A process responsible for creating new units. It receives all the units added locally to the Dag
/// via the parents_rx channel endpoint. It creates units according to an internal strategy respecting
/// always the following constraints: for a unit U of round r
/// - all U's parents are from round (r-1),
/// - all U's parents are created by different nodes,
/// - one of U's parents is the (r-1)-round unit by U's creator,
/// - U has > floor(2*N/3) parents.
/// The currently implemented strategy creates the unit U at the very first moment when enough
/// candidates for parents are available for all the above constraints to be satisfied.
pub(crate) struct Creator<E: Environment> {
    node_id: E::NodeId,
    parents_rx: Receiver<Unit<E::BlockHash, E::Hash>>,
    new_units_tx: Sender<Unit<E::BlockHash, E::Hash>>,
    epoch_id: EpochId,
    n_members: NodeCount,
    current_round: Round, // current_round is the round number of our next unit
    candidates_by_round: Vec<NodeMap<Option<E::Hash>>>,
    n_candidates_by_round: Vec<NodeCount>,
    best_block: Box<dyn Fn() -> E::BlockHash + Send + Sync + 'static>,
    hashing: Hashing<E::Hash>,
    create_lag: Duration,
}

impl<E: Environment> Creator<E> {
    pub(crate) fn new(
        conf: Config<E::NodeId>,
        parents_rx: Receiver<Unit<E::BlockHash, E::Hash>>,
        new_units_tx: Sender<Unit<E::BlockHash, E::Hash>>,
        best_block: Box<dyn Fn() -> E::BlockHash + Send + Sync + 'static>,
        hashing: Hashing<E::Hash>,
    ) -> Self {
        let Config {
            node_id,
            n_members,
            epoch_id,
            create_lag,
        } = conf;
        Creator {
            node_id,
            parents_rx,
            new_units_tx,
            epoch_id,
            n_members,
            current_round: 0,
            candidates_by_round: vec![NodeMap::new_with_len(n_members)],
            n_candidates_by_round: vec![NodeCount(0)],
            best_block,
            hashing,
            create_lag,
        }
    }

    // initializes the vectors corresponding to the given round (and all between if not there)
    fn init_round(&mut self, round: Round) {
        while self.candidates_by_round.len() <= round {
            self.candidates_by_round
                .push(NodeMap::new_with_len(self.n_members));
            self.n_candidates_by_round.push(NodeCount(0));
        }
    }

    fn create_unit(&mut self) {
        let round = self.current_round;
        let parents = {
            if round == 0 {
                NodeMap::new_with_len(self.n_members)
            } else {
                self.candidates_by_round[round - 1].clone()
            }
        };

        let new_unit = Unit::new_from_parents(
            self.node_id.my_index().unwrap(),
            round,
            self.epoch_id,
            parents,
            (self.best_block)(),
            &self.hashing,
        );
        debug!(target: "rush-creator", "{:?} Created a new unit {:?} at round {}.", self.node_id, new_unit, self.current_round);
        let send_result = self.new_units_tx.send(new_unit);
        if let Err(e) = send_result {
            error!(target: "rush-creator", "{:?} Unable to send a newly created unit: {:?}.", self.node_id, e);
        }

        self.current_round += 1;
        self.init_round(self.current_round);
    }

    fn add_unit(&mut self, round: Round, pid: NodeIndex, hash: E::Hash) {
        // units that are too old are of no interest to us
        if round + 1 >= self.current_round {
            self.init_round(round);
            if self.candidates_by_round[round][pid].is_none() {
                // passing the check above means that we do not have any unit for the pair (round, pid) yet
                self.candidates_by_round[round][pid] = Some(hash);
                self.n_candidates_by_round[round] += NodeCount(1);
            }
        }
    }

    fn check_ready(&self) -> bool {
        if self.current_round == 0 {
            return true;
        }
        // To create a new unit, we need to have at least >floor(2*N/3) parents available in previous round.
        // Additionally, our unit from previous round must be available.
        let prev_round = self.current_round - 1;
        let threshold = (self.n_members * 2) / 3;

        self.n_candidates_by_round[prev_round] > threshold
            && self.candidates_by_round[prev_round][self.node_id.my_index().unwrap()].is_some()
    }

    pub(crate) async fn create(&mut self) {
        self.create_unit();
        loop {
            while let Some(u) = self.parents_rx.recv().await {
                self.add_unit(u.round(), u.creator(), u.hash());
                if self.check_ready() {
                    self.create_unit();
                    sleep(self.create_lag).await;
                }
            }
        }
    }
}
