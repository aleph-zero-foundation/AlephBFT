use crate::{
    consensus,
    nodes::{NodeCount, NodeIndex, NodeMap},
    runway::{NotificationIn, NotificationOut},
    testing::mock::{gen_config, Hash64, Hasher64, Spawner},
    units::{ControlHash, PreUnit, Unit},
    Receiver, Round, Sender, SpawnHandle,
};
use futures::{
    channel::{mpsc, oneshot},
    stream::StreamExt,
    FutureExt,
};
use futures_timer::Delay;
use log::{debug, error, trace};
use rand::{distributions::Open01, prelude::*};
use std::{cmp, time::Duration};

use std::collections::HashMap;

#[derive(Clone)]
struct UnitWithParents {
    unit: Unit<Hasher64>,
    parent_hashes: NodeMap<Option<Hash64>>,
}

fn unit_hash(round: Round, creator: NodeIndex, variant: usize) -> Hash64 {
    let mut hash = Hash64::default();
    hash[0] = round as u8;
    hash[1] = creator.0 as u8;
    hash[2] = variant as u8;
    hash
}

impl UnitWithParents {
    fn new(
        round: Round,
        creator: NodeIndex,
        variant: usize,
        parent_hashes: NodeMap<Option<Hash64>>,
    ) -> Self {
        let control_hash = ControlHash::new(&parent_hashes);
        let pre_unit = PreUnit::new(creator, round, control_hash);
        let hash = unit_hash(round, creator, variant);
        let unit = Unit::new(pre_unit, hash);
        UnitWithParents {
            unit,
            parent_hashes,
        }
    }
    fn hash(&self) -> Hash64 {
        self.unit.hash()
    }

    fn parent_hashes_vec(&self) -> Vec<Hash64> {
        self.parent_hashes.iter().cloned().flatten().collect()
    }
}

struct ConsensusDagFeeder {
    tx_in: Sender<NotificationIn<Hasher64>>,
    rx_out: Receiver<NotificationOut<Hasher64>>,
    units: Vec<UnitWithParents>,
    units_map: HashMap<Hash64, UnitWithParents>,
}

impl ConsensusDagFeeder {
    fn new(
        units: Vec<UnitWithParents>,
    ) -> (
        Self,
        Receiver<NotificationIn<Hasher64>>,
        Sender<NotificationOut<Hasher64>>,
    ) {
        let units_map = units.iter().map(|u| (u.hash(), u.clone())).collect();
        let (tx_in, rx_in) = mpsc::unbounded();
        let (tx_out, rx_out) = mpsc::unbounded();
        let cdf = ConsensusDagFeeder {
            tx_in,
            rx_out,
            units,
            units_map,
        };
        (cdf, rx_in, tx_out)
    }

    fn on_consensus_notification(&self, notification: NotificationOut<Hasher64>) {
        match notification {
            NotificationOut::WrongControlHash(h) => {
                // We need to answer these requests as otherwise terminal cannot make progress
                let parent_hashes = self.units_map.get(&h).unwrap().parent_hashes_vec();
                let notification = NotificationIn::UnitParents(h, parent_hashes);
                self.tx_in.unbounded_send(notification).unwrap();
            }
            NotificationOut::AddedToDag(h, p_hashes) => {
                let expected_hashes = self.units_map.get(&h).unwrap().parent_hashes_vec();
                assert!(p_hashes == expected_hashes);
            }
            _ => {
                //We ignore the remaining notifications. We don't need to answer missing units requests.
            }
        }
    }

    async fn run(mut self) {
        for unit in &self.units {
            let notification = NotificationIn::NewUnits(vec![unit.unit.clone()]);
            self.tx_in.unbounded_send(notification).unwrap();
        }

        loop {
            let notification = self.rx_out.next().await;
            match notification {
                Some(notification) => self.on_consensus_notification(notification),
                None => {
                    error!(target: "dag-test", "Consensus notification stream closed.");
                    break;
                }
            }
        }
    }
}

async fn run_consensus_on_dag(
    units: Vec<UnitWithParents>,
    n_members: NodeCount,
    deadline_ms: u64,
) -> Vec<Vec<Hash64>> {
    let (feeder, rx_in, tx_out) = ConsensusDagFeeder::new(units);
    let conf = gen_config(NodeIndex(0), n_members);
    let (_exit_tx, exit_rx) = oneshot::channel();
    let (batch_tx, mut batch_rx) = mpsc::unbounded();
    let spawner = Spawner::new();
    spawner.spawn(
        "consensus",
        consensus::run(conf, rx_in, tx_out, batch_tx, spawner.clone(), exit_rx),
    );
    spawner.spawn("feeder", feeder.run());
    let mut batches = Vec::new();
    let mut delay_fut = Delay::new(Duration::from_millis(deadline_ms)).fuse();
    loop {
        futures::select! {
            batch = batch_rx.next() => {
                batches.push(batch.unwrap());
            },
            _ = &mut delay_fut => {
                break;
            }
        };
    }
    batches
}

fn generate_random_dag(n_members: NodeCount, height: Round, seed: u64) -> Vec<UnitWithParents> {
    // The below asserts are mainly because these numbers must fit in 8 bits for hashing but also: this is
    // meant to be run for small dags only -- it's not optimized for large dags.
    assert!(n_members < 100.into());
    assert!(height < 100);

    let mut rng = StdRng::seed_from_u64(seed);
    let max_forkers = NodeCount((n_members.0 - 1) / 3);
    let n_forkers = NodeCount(rng.gen_range(0..=max_forkers.0));
    let mut forker_bitmap = NodeMap::<bool>::new_with_len(n_members);
    // below we select n_forkers forkers at random
    for forker_ix in n_members
        .into_iterator()
        .choose_multiple(&mut rng, n_forkers.into())
    {
        forker_bitmap[forker_ix] = true;
    }
    // The probability that a node stops creating units at a given round.
    // For a fixed node the probability that it will terminate before height is a constant around 0.1
    let prob_terminate = 0.15 / ((height + 1) as f64);
    // Maximum number of forks per round per forker.
    let max_variants = rng.gen_range(1..=4);

    let threshold = NodeCount((2 * n_members.0) / 3 + 1);

    let mut dag: Vec<Vec<Vec<UnitWithParents>>> =
        vec![vec![vec![]; n_members.into()]; height.into()];
    // dag is a (height x n_members)-dimensional array consisting of empty vectors.

    let mut all_ixs: Vec<_> = n_members.into_iterator().collect();

    for r in 0..height {
        for node_ix in n_members.into_iterator() {
            let mut n_variants = if forker_bitmap[node_ix] {
                rng.gen_range(1..=max_variants)
            } else {
                1
            };
            let rand_val: f64 = rng.sample(Open01);
            if rand_val < prob_terminate {
                // this node terminates at this round
                n_variants = 0;
            }
            for variant in 0..n_variants {
                let mut parents = NodeMap::new_with_len(n_members);
                if r != 0 {
                    let previous_round_index = (r - 1) as usize;
                    if dag[previous_round_index][node_ix.0].is_empty() {
                        //Impossible to create a valid unit because we cannot refer to parent from previous round.
                        break;
                    }
                    let mut n_max_parents = NodeCount(0);
                    for p_ix in n_members.into_iterator() {
                        if !dag[previous_round_index][p_ix.0].is_empty() {
                            n_max_parents += 1.into();
                        }
                    }
                    if n_max_parents < threshold {
                        //Impossible to create a valid unit -- not enough parents;
                        break;
                    }

                    all_ixs.shuffle(&mut rng);
                    // The loop below makes the first element of all_ixs equal to node_ix (the currently considered creator)
                    // This is to make sure that it will be chosen as a parent
                    for i in n_members.into_iterator() {
                        if all_ixs[i.0] == node_ix {
                            all_ixs.swap(0, i.0);
                            break;
                        }
                    }

                    let n_parents = NodeCount(rng.gen_range(threshold.0..=n_max_parents.0));
                    let mut curr_n_parents = NodeCount(0);

                    for parent_ix in all_ixs.iter() {
                        if dag[previous_round_index][parent_ix.0].is_empty() {
                            continue;
                        }
                        let parent = dag[previous_round_index][parent_ix.0]
                            .choose(&mut rng)
                            .unwrap();
                        parents[*parent_ix] = Some(parent.hash());
                        curr_n_parents += 1.into();
                        if curr_n_parents == n_parents {
                            break;
                        }
                    }
                }
                let unit = UnitWithParents::new(r, node_ix, variant, parents);
                dag[r as usize][node_ix.0].push(unit);
            }
        }
    }
    let mut dag_units = Vec::new();
    for round_units in dag.iter().take(height.into()) {
        for coord_units in round_units.iter().take(n_members.into()) {
            for unit in coord_units {
                dag_units.push(unit.clone());
            }
        }
    }
    dag_units
}

fn batch_lists_consistent(batches1: &[Vec<Hash64>], batches2: &[Vec<Hash64>]) -> bool {
    for i in 0..cmp::min(batches1.len(), batches2.len()) {
        if batches1[i] != batches2[i] {
            return false;
        }
    }
    true
}

#[tokio::test]
async fn ordering_random_dag_consistency_under_permutations() {
    for seed in 0..4u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let n_members = NodeCount(rng.gen_range(1..11));
        let height = rng.gen_range(3..11);
        let mut units = generate_random_dag(n_members, height, seed);
        let batch_on_sorted =
            run_consensus_on_dag(units.clone(), n_members, 80 + (n_members.0 as u64) * 5).await;
        debug!(target: "dag-test",
            "seed {:?} n_members {:?} height {:?} batch_len {:?}",
            seed,
            n_members,
            height,
            batch_on_sorted.len()
        );
        for i in 0..8 {
            units.shuffle(&mut rng);
            let mut batch =
                run_consensus_on_dag(units.clone(), n_members, 25 + (n_members.0 as u64) * 5).await;
            if batch != batch_on_sorted {
                if batch_lists_consistent(&batch, &batch_on_sorted) {
                    // there might be some timing issue here, we run it with more time
                    batch = run_consensus_on_dag(units.clone(), n_members, 200).await;
                }
                if batch != batch_on_sorted {
                    debug!(target: "dag-test",
                        "seed {:?} n_members {:?} height {:?} i {:?}",
                        seed, n_members, height, i
                    );
                    debug!(target: "dag-test",
                        "batch lens {:?} \n {:?}",
                        batch_on_sorted.len(),
                        batch.len()
                    );
                    trace!(target: "dag-test", "batches {:?} \n {:?}", batch_on_sorted, batch);
                    assert!(batch == batch_on_sorted);
                } else {
                    debug!(
                        "False alarm at seed {:?} n_members {:?} height {:?}!",
                        seed, n_members, height
                    );
                }
            }
        }
    }
}
