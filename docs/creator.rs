use crate::{
    config::{Config, DelaySchedule},
    member::NotificationOut,
    nodes::{NodeCount, NodeIndex, NodeMap},
    units::{ControlHash, PreUnit, Unit},
    Hasher, Receiver, Round, Sender,
};
use futures::{channel::oneshot, FutureExt, StreamExt};
use log::{debug, error};
use tokio::time::{delay_for, Duration};

/// A process responsible for creating new units. It receives all the units added locally to the Dag
/// via the parents_rx channel endpoint. It creates units according to an internal strategy respecting
/// always the following constraints: for a unit U of round r
/// - all U's parents are from round (r-1),
/// - all U's parents are created by different nodes,
/// - one of U's parents is the (r-1)-round unit by U's creator,
/// - U has > floor(2*N/3) parents.
/// The currently implemented strategy creates the unit U at the very first moment when enough
/// candidates for parents are available for all the above constraints to be satisfied.
pub(crate) struct Creator<H: Hasher> {
    node_ix: NodeIndex,
    parents_rx: Receiver<Unit<H>>,
    new_units_tx: Sender<NotificationOut<H>>,
    n_members: NodeCount,
    current_round: Round, // current_round is the round number of our next unit
    candidates_by_round: Vec<NodeMap<Option<H::Hash>>>,
    n_candidates_by_round: Vec<NodeCount>,
    create_lag: DelaySchedule,
}

impl<H: Hasher> Creator<H> {
    pub(crate) fn new(
        conf: Config,
        parents_rx: Receiver<Unit<H>>,
        new_units_tx: Sender<NotificationOut<H>>,
    ) -> Self {
        let node_ix = conf.node_ix;
        let n_members = conf.n_members;
        let create_lag = conf.delay_config.unit_creation_delay;
        Creator {
            node_ix,
            parents_rx,
            new_units_tx,
            n_members,
            current_round: 0,
            candidates_by_round: vec![NodeMap::new_with_len(n_members)],
            n_candidates_by_round: vec![NodeCount(0)],
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

        let control_hash = ControlHash::new(&parents);

        let new_preunit = PreUnit::new(self.node_ix, round, control_hash);
        debug!(target: "rush-creator", "{:?} Created a new unit {:?} at round {:?}.", self.node_ix, new_preunit, self.current_round);
        let send_result = self
            .new_units_tx
            .unbounded_send(NotificationOut::CreatedPreUnit(new_preunit));
        if let Err(e) = send_result {
            error!(target: "rush-creator", "{:?} Unable to send a newly created unit: {:?}.", self.node_ix, e);
        }

        self.current_round += 1;
        self.init_round(self.current_round);
    }

    fn add_unit(&mut self, round: Round, pid: NodeIndex, hash: H::Hash) {
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
            && self.candidates_by_round[prev_round][self.node_ix].is_some()
    }

    pub(crate) async fn create(&mut self, exit: oneshot::Receiver<()>) {
        let half_hour = Duration::from_secs(30 * 60);
        let mut round: usize = 0;
        let mut exit = exit.into_stream();
        let mut delay_fut = delay_for(Duration::from_millis(0));
        let mut delay_passed = false;
        loop {
            tokio::select! {
                Some(u) = self.parents_rx.next() => {
                    self.add_unit(u.round(), u.creator(), u.hash());
                }
                _ = &mut delay_fut => {
                    if delay_passed {
                        error!(target: "rush-creator", "{:?} more than half hour has passed since we created the previous unit.", self.node_ix);
                    }
                    delay_passed = true;
                    delay_fut = delay_for(half_hour);
                }
                _ = exit.next() => {
                    debug!(target: "rush-creator", "{:?} received exit signal.", self.node_ix);
                    break;
                }
            }
            if delay_passed && self.check_ready() {
                self.create_unit();
                delay_fut = delay_for((self.create_lag)(round));
                round += 1;
                delay_passed = false;
            }
        }
    }
}
