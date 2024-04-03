use crate::{
    consensus,
    extension::Service as Extender,
    reconstruction::{ReconstructedUnit as GenericReconstructedUnit, Request as GenericRequest},
    runway::ExplicitParents as GenericExplicitParents,
    testing::{complete_oneshot, gen_config, gen_delay_config},
    units::{ControlHash, FullUnit, PreUnit, SignedUnit as GenericSignedUnit, Unit, UnitCoord},
    NodeCount, NodeIndex, NodeMap, NodeSubset, Receiver, Round, Sender, Signed, SpawnHandle,
    Terminator,
};
use aleph_bft_mock::{Data, DataProvider, Hash64, Hasher64, Keychain, Spawner};
use futures::{
    channel::{mpsc, oneshot},
    stream::StreamExt,
    FutureExt,
};
use futures_timer::Delay;
use log::{debug, trace};
use rand::{distributions::Open01, prelude::*};
use std::{cmp, collections::HashMap, time::Duration};

type Request = GenericRequest<Hasher64>;
type ExplicitParents = GenericExplicitParents<Hasher64>;
type SignedUnit = GenericSignedUnit<Hasher64, Data, Keychain>;
type ReconstructedUnit = GenericReconstructedUnit<SignedUnit>;

#[derive(Clone)]
struct UnitWithParents {
    unit: SignedUnit,
    parent_hashes: NodeMap<Hash64>,
}

impl UnitWithParents {
    fn new(
        round: Round,
        creator: NodeIndex,
        variant: Data,
        parent_hashes: NodeMap<Hash64>,
    ) -> Self {
        let keychain = Keychain::new(parent_hashes.size(), creator);
        let control_hash = ControlHash::new(&parent_hashes);
        let pre_unit = PreUnit::new(creator, round, control_hash);
        let unit = Signed::sign(FullUnit::new(pre_unit, Some(variant), 0), &keychain);
        UnitWithParents {
            unit,
            parent_hashes,
        }
    }
    fn hash(&self) -> Hash64 {
        self.unit.hash()
    }

    fn parent_hashes_map(&self) -> HashMap<UnitCoord, Hash64> {
        let mut result = HashMap::new();
        let round = self.unit.round();
        for (creator, hash) in self.parent_hashes.iter() {
            result.insert(UnitCoord::new(round - 1, creator), *hash);
        }
        result
    }
}

struct ConsensusDagFeeder {
    units_for_consensus: Sender<SignedUnit>,
    parents_for_consensus: Sender<ExplicitParents>,
    requests_from_consensus: Receiver<Request>,
    reconstructed_units_from_consensus: Receiver<ReconstructedUnit>,
    units_from_creator: Receiver<SignedUnit>,
    units: Vec<UnitWithParents>,
    units_map: HashMap<Hash64, UnitWithParents>,
}

type DagFeederParts = (
    ConsensusDagFeeder,
    Receiver<SignedUnit>,
    Receiver<ExplicitParents>,
    Sender<Request>,
    Sender<ReconstructedUnit>,
    Sender<SignedUnit>,
);

impl ConsensusDagFeeder {
    fn new(units: Vec<UnitWithParents>) -> DagFeederParts {
        let units_map = units.iter().map(|u| (u.hash(), u.clone())).collect();
        let (units_for_consensus, units_from_feeder) = mpsc::unbounded();
        let (parents_for_consensus, parents_from_feeder) = mpsc::unbounded();
        let (requests_for_feeder, requests_from_consensus) = mpsc::unbounded();
        let (reconstructed_units_for_feeder, reconstructed_units_from_consensus) =
            mpsc::unbounded();
        let (units_for_feeder, units_from_creator) = mpsc::unbounded();
        let cdf = ConsensusDagFeeder {
            units_for_consensus,
            parents_for_consensus,
            requests_from_consensus,
            reconstructed_units_from_consensus,
            units_from_creator,
            units,
            units_map,
        };
        (
            cdf,
            units_from_feeder,
            parents_from_feeder,
            requests_for_feeder,
            reconstructed_units_for_feeder,
            units_for_feeder,
        )
    }

    fn on_request(&self, request: Request) {
        use GenericRequest::*;
        match request {
            ParentsOf(h) => {
                // We need to answer these requests as otherwise reconstruction cannot make progress
                let parent_hashes = self.units_map.get(&h).unwrap().parent_hashes_map();
                let parents = (h, parent_hashes);
                self.parents_for_consensus.unbounded_send(parents).unwrap();
            }
            Coord(_) => {
                // We don't need to answer missing units requests.
            }
        }
    }

    fn on_reconstructed_unit(&self, unit: ReconstructedUnit) {
        let h = unit.hash();
        let round = unit.round();
        let parents = unit.parents();
        let expected_hashes = self
            .units_map
            .get(&h)
            .expect("we have the unit")
            .parent_hashes_map();
        assert_eq!(parents.item_count(), expected_hashes.len());
        for (creator, hash) in parents {
            assert_eq!(
                Some(hash),
                expected_hashes.get(&UnitCoord::new(round - 1, creator))
            );
        }
    }

    async fn run(mut self) {
        for unit in &self.units {
            self.units_for_consensus
                .unbounded_send(unit.unit.clone())
                .expect("channel should be open");
        }

        loop {
            futures::select! {
                request = self.requests_from_consensus.next() => match request {
                    Some(request) => self.on_request(request),
                    None => break,
                },
                unit = self.reconstructed_units_from_consensus.next() => match unit {
                    Some(unit) => self.on_reconstructed_unit(unit),
                    None => break,
                },
                _ = self.units_from_creator.next() => continue,
            };
        }
        debug!(target: "dag-test", "Consensus stream closed.");
    }
}

async fn run_consensus_on_dag(
    units: Vec<UnitWithParents>,
    n_members: NodeCount,
    deadline_ms: u64,
) -> Vec<Vec<Hash64>> {
    let node_id = NodeIndex(0);
    let (
        feeder,
        units_from_feeder,
        parents_from_feeder,
        requests_for_feeder,
        reconstructed_units_for_feeder,
        units_for_feeder,
    ) = ConsensusDagFeeder::new(units);
    let conf = gen_config(node_id, n_members, gen_delay_config());
    let keychain = Keychain::new(n_members, node_id);
    let (_exit_tx, exit_rx) = oneshot::channel();
    let (_extender_exit_tx, extender_exit_rx) = oneshot::channel();
    let (reconstructed_units_for_us, mut reconstructed_units_from_consensus) = mpsc::unbounded();
    let (batch_tx, mut batch_rx) = mpsc::unbounded();
    let spawner = Spawner::new();
    let starting_round = complete_oneshot(Some(0));
    let (units_for_extender, units_from_us) = mpsc::unbounded();
    let extender = Extender::<Hasher64>::new(node_id, units_from_us, batch_tx);
    spawner.spawn(
        "consensus",
        consensus::run(
            conf,
            consensus::IO {
                units_from_runway: units_from_feeder,
                parents_from_runway: parents_from_feeder,
                units_for_runway: reconstructed_units_for_us,
                requests_for_runway: requests_for_feeder,
                new_units_for_runway: units_for_feeder,
                data_provider: DataProvider::new(),
                starting_round,
            },
            keychain,
            spawner,
            Terminator::create_root(exit_rx, "AlephBFT-consensus"),
        ),
    );
    spawner.spawn(
        "extender",
        extender.run(Terminator::create_root(
            extender_exit_rx,
            "AlephBFT-extender",
        )),
    );
    spawner.spawn("feeder", feeder.run());
    let mut batches = Vec::new();
    let mut delay_fut = Delay::new(Duration::from_millis(deadline_ms)).fuse();
    loop {
        futures::select! {
            batch = batch_rx.next() => {
                batches.push(batch.unwrap());
            },
            unit = reconstructed_units_from_consensus.next() => {
                let unit = unit.expect("consensus is operating");
                units_for_extender.unbounded_send(unit.extender_unit()).expect("extender is operating");
                reconstructed_units_for_feeder.unbounded_send(unit).expect("feeder is operating");
            }
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
    let max_forkers = n_members - n_members.consensus_threshold();
    let n_forkers = NodeCount(rng.gen_range(0..=max_forkers.0));
    let mut forker_bitmap = NodeSubset::with_size(n_members);
    // below we select n_forkers forkers at random
    for forker_ix in n_members
        .into_iterator()
        .choose_multiple(&mut rng, n_forkers.into())
    {
        forker_bitmap.insert(forker_ix);
    }
    // The probability that a node stops creating units at a given round.
    // For a fixed node the probability that it will terminate before height is a constant around 0.1
    let prob_terminate = 0.15 / ((height + 1) as f64);
    // Maximum number of forks per round per forker.
    let max_variants: u32 = rng.gen_range(1..=4);

    let threshold = n_members.consensus_threshold();

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
                let mut parents = NodeMap::with_size(n_members);
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
                        parents.insert(*parent_ix, parent.hash());
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
