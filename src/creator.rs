use std::{
    pin::Pin,
    task::{self, Poll},
};

use crate::{
    nodes::{NodeCount, NodeIndex, NodeMap},
    skeleton::{CHUnit, Receiver, Sender},
    traits::Environment,
};

use futures::prelude::*;

// a process responsible for creating new units
pub(crate) struct Creator<E: Environment> {
    parents_rx: Receiver<CHUnit<E::Hash>>,
    new_units_tx: Sender<CHUnit<E::Hash>>,
    epoch_id: u32,
    pid: NodeIndex,
    n_members: NodeCount,
    // current_round is the round number of our next unit
    current_round: usize,
    candidates_by_round: Vec<NodeMap<Option<E::Hash>>>,
    n_candidates_by_round: Vec<NodeCount>,
    best_block: Box<dyn Fn() -> E::Hash + Send + Sync + 'static>,
}

impl<E: Environment> Unpin for Creator<E> {}

impl<E: Environment> Creator<E> {
    pub(crate) fn new(
        parents_rx: Receiver<CHUnit<E::Hash>>,
        new_units_tx: Sender<CHUnit<E::Hash>>,
        epoch_id: u32,
        pid: NodeIndex,
        n_members: NodeCount,
        best_block: Box<dyn Fn() -> E::Hash + Send + Sync + 'static>,
    ) -> Self {
        Creator {
            parents_rx,
            new_units_tx,
            epoch_id,
            pid,
            n_members,
            current_round: 0,
            candidates_by_round: vec![NodeMap::new_with_len(n_members)],
            n_candidates_by_round: vec![NodeCount(0)],
            best_block,
        }
    }

    // initializes the vectors corresponding to the given round (and all between if not there)
    fn init_round(&mut self, round: usize) {
        while self.candidates_by_round.len() <= round {
            self.candidates_by_round
                .push(NodeMap::new_with_len(self.n_members));
            self.n_candidates_by_round.push(NodeCount(0));
        }
    }

    fn _current_round(&self) -> usize {
        self.current_round
    }

    fn create_unit(&mut self) {
        let round = self.current_round;
        let new_unit = CHUnit::new_from_parents(
            self.pid,
            round as u32,
            self.epoch_id,
            self.candidates_by_round[round].clone(),
            (self.best_block)(),
        );
        let _ = self.new_units_tx.send(new_unit);
        self.current_round += 1;
    }

    fn add_unit(&mut self, round: usize, pid: NodeIndex, hash: E::Hash) {
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
        let prev_round: usize = self.current_round - 1;
        let threshold = (self.n_members * 2) / 3;

        self.n_candidates_by_round[prev_round] > threshold
            && self.candidates_by_round[prev_round][self.pid].is_some()
    }
}

impl<E: Environment> Future for Creator<E> {
    type Output = Result<(), E::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        while let Poll::Ready(Some(u)) = self.parents_rx.poll_recv(cx) {
            self.add_unit(u.round() as usize, u.creator(), u.hash());
        }
        while self.check_ready() {
            self.create_unit();
        }
        Poll::Pending
    }
}
