use futures::{FutureExt, StreamExt};

use log::{debug, warn};

use crate::{Hasher, NodeIndex, NodeMap, Receiver, Round, Sender, Terminator};

mod election;
mod extender;
mod units;

use extender::Extender;

const LOG_TARGET: &str = "AlephBFT-extender";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtenderUnit<H: Hasher> {
    creator: NodeIndex,
    round: Round,
    parents: NodeMap<H::Hash>,
    hash: H::Hash,
}

impl<H: Hasher> ExtenderUnit<H> {
    pub fn new(creator: NodeIndex, round: Round, hash: H::Hash, parents: NodeMap<H::Hash>) -> Self {
        ExtenderUnit {
            creator,
            round,
            hash,
            parents,
        }
    }
}

/// A process responsible for executing the Consensus protocol on a local copy of the Dag.
/// It receives units via a channel `electors` which are guaranteed to be eventually in the Dags
/// of all honest nodes. The static Aleph Consensus algorithm is then run on this Dag in order
/// to finalize subsequent rounds of the Dag. More specifically whenever a new unit is received
/// this process checks whether a new round can be finalized and if so, it computes the batch of
/// hashes of units that should be finalized, and pushes such a batch to a channel via the
/// finalizer_tx endpoint.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/internals.html
/// Section 5.4 for a discussion of this component.

pub struct Service<H: Hasher> {
    node_id: NodeIndex,
    extender: Extender<H>,
    electors: Receiver<ExtenderUnit<H>>,
    finalizer_tx: Sender<Vec<H::Hash>>,
}

impl<H: Hasher> Service<H> {
    pub fn new(
        node_id: NodeIndex,
        electors: Receiver<ExtenderUnit<H>>,
        finalizer_tx: Sender<Vec<H::Hash>>,
    ) -> Self {
        let extender = Extender::new();
        Service {
            node_id,
            extender,
            electors,
            finalizer_tx,
        }
    }

    pub async fn run(mut self, mut terminator: Terminator) {
        let mut exiting = false;
        let mut round = 0;
        loop {
            futures::select! {
                v = self.electors.next() => match v {
                    Some(u) => {
                        debug!(target: LOG_TARGET, "{:?} New unit in Extender round {:?} creator {:?} hash {:?}.", self.node_id, u.round, u.creator, u.hash);
                        for batch in self.extender.add_unit(u) {
                            let head = *batch.last().expect("all batches are nonempty");
                            if self.finalizer_tx.unbounded_send(batch).is_err() {
                                warn!(target: LOG_TARGET, "{:?} Channel for batches should be open", self.node_id);
                                exiting = true;
                            }
                            debug!(target: LOG_TARGET, "{:?} Finalized round {:?} with head {:?}.", self.node_id, round, head);
                            round += 1;
                        }
                    },
                    None => {
                        warn!(target: LOG_TARGET, "{:?} Units for extender unexpectedly ended.", self.node_id);
                        exiting = true;
                    }
                },
                _ = terminator.get_exit().fuse() => {
                    debug!(target: LOG_TARGET, "{:?} received exit signal.", self.node_id);
                    exiting = true;
                }
            }
            if exiting {
                debug!(target: LOG_TARGET, "{:?} Extender decided to exit.", self.node_id);
                terminator.terminate_sync().await;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        extension::{ExtenderUnit, Service},
        NodeCount, NodeIndex, NodeMap, Round, Terminator,
    };
    use aleph_bft_mock::Hasher64;
    use futures::{
        channel::{mpsc, oneshot},
        StreamExt,
    };
    use std::iter;

    fn coord_to_number(creator: NodeIndex, round: Round, n_members: NodeCount) -> u64 {
        (round as usize * n_members.0 + creator.0) as u64
    }

    pub fn construct_unit(
        creator: NodeIndex,
        round: Round,
        parent_coords: Vec<(NodeIndex, Round)>,
        n_members: NodeCount,
    ) -> ExtenderUnit<Hasher64> {
        assert!(round > 0 || parent_coords.is_empty());
        let mut parents = NodeMap::with_size(n_members);
        for (creator, round) in parent_coords {
            parents.insert(
                creator,
                coord_to_number(creator, round, n_members).to_ne_bytes(),
            );
        }
        ExtenderUnit::new(
            creator,
            round,
            coord_to_number(creator, round, n_members).to_ne_bytes(),
            parents,
        )
    }

    pub fn construct_unit_all_parents(
        creator: NodeIndex,
        round: Round,
        n_members: NodeCount,
    ) -> ExtenderUnit<Hasher64> {
        let parent_coords = match round.checked_sub(1) {
            None => Vec::new(),
            Some(parent_round) => n_members
                .into_iterator()
                .zip(iter::repeat(parent_round))
                .collect(),
        };

        construct_unit(creator, round, parent_coords, n_members)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn finalize_rounds_01() {
        let n_members = NodeCount(4);
        let rounds = 6;
        let (batch_tx, mut batch_rx) = mpsc::unbounded();
        let (electors_tx, electors_rx) = mpsc::unbounded();
        let extender = Service::<Hasher64>::new(0.into(), electors_rx, batch_tx);
        let (exit_tx, exit_rx) = oneshot::channel();
        let extender_handle = tokio::spawn(async move {
            extender
                .run(Terminator::create_root(exit_rx, "AlephBFT-extender"))
                .await
        });

        for round in 0..rounds {
            for creator in n_members.into_iterator() {
                let unit = construct_unit_all_parents(creator, round, n_members);
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
