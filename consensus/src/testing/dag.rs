use crate::{
    alerts::ForkingNotification,
    dag::{
        Dag as GenericDag, DagResult, ReconstructedUnit as GenericReconstructedUnit,
        Request as GenericRequest,
    },
    extension::Ordering,
    units::{
        ControlHash, FullUnit, PreUnit, SignedUnit as GenericSignedUnit, Unit, UnitStore,
        UnitWithParents as _, Validator,
    },
    NodeCount, NodeIndex, NodeMap, NodeSubset, OrderedUnit, Round, Signed, UnitFinalizationHandler,
};
use aleph_bft_mock::{Data, Hash64, Hasher64, Keychain};
use log::debug;
use parking_lot::Mutex;
use rand::{distributions::Open01, prelude::*};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

type Request = GenericRequest<Hasher64>;
type SignedUnit = GenericSignedUnit<Hasher64, Data, Keychain>;
type ReconstructedUnit = GenericReconstructedUnit<SignedUnit>;
type Dag = GenericDag<Hasher64, Data, Keychain>;

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

    fn parent_hashes(&self) -> Vec<Hash64> {
        self.parent_hashes.values().cloned().collect()
    }
}

struct DagFeeder {
    units: Vec<UnitWithParents>,
    units_map: HashMap<Hash64, UnitWithParents>,
    // we hold all forker units, to accept all forks
    // this is not realistic, but simulates a kinda "worse than worst case scenario"
    forker_units: HashMap<NodeIndex, Vec<UnitWithParents>>,
    store: UnitStore<ReconstructedUnit>,
    dag: Dag,
    result: Vec<ReconstructedUnit>,
}

impl DagFeeder {
    fn new(
        node_id: NodeIndex,
        units: Vec<UnitWithParents>,
        forker_units: HashMap<NodeIndex, Vec<UnitWithParents>>,
    ) -> DagFeeder {
        let units_map = units.iter().map(|u| (u.hash(), u.clone())).collect();
        let node_count = units
            .first()
            .expect("we have at least one unit")
            .unit
            .control_hash()
            .n_members();
        // the index is unimportant, since we don't actually actively use signing here
        let dag = Dag::new(Validator::new(0, Keychain::new(node_count, node_id), 2137));
        let store = UnitStore::new(node_count);
        DagFeeder {
            units,
            units_map,
            forker_units,
            store,
            dag,
            result: Vec::new(),
        }
    }

    fn on_request(&mut self, request: Request) {
        use GenericRequest::*;
        match request {
            ParentsOf(h) => {
                // We need to answer these requests as otherwise reconstruction cannot make progress
                let parents = self
                    .units_map
                    .get(&h)
                    .expect("we have all the units")
                    .parent_hashes()
                    .iter()
                    .map(|hash| {
                        self.units_map
                            .get(hash)
                            .expect("we have all the units")
                            .unit
                            .clone()
                            .into()
                    })
                    .collect();
                let DagResult {
                    units,
                    requests,
                    alerts,
                } = self.dag.add_parents(h, parents, &self.store);
                for unit in units {
                    self.on_reconstructed_unit(unit);
                }
                for alert in alerts {
                    self.on_alert(alert.forker());
                    // have to repeat it, as it wasn't properly accepted because of the alert
                    self.on_request(ParentsOf(h));
                }
                for request in requests {
                    self.on_request(request);
                }
            }
            Coord(_) => {
                // We don't need to answer missing units requests.
            }
        }
    }

    fn on_reconstructed_unit(&mut self, unit: ReconstructedUnit) {
        let h = unit.hash();
        let parents: HashSet<_> = unit.parents().cloned().collect();
        let expected_hashes: HashSet<_> = self
            .units_map
            .get(&h)
            .expect("we have the unit")
            .parent_hashes()
            .into_iter()
            .collect();

        assert_eq!(parents, expected_hashes);
        self.result.push(unit.clone());
        self.store.insert(unit);
    }

    fn on_alert(&mut self, forker: NodeIndex) {
        let committed_units = self
            .forker_units
            .get(&forker)
            .expect("we have units for forkers")
            .iter()
            .map(|unit| unit.unit.clone().into())
            .collect();
        let DagResult {
            units,
            requests,
            alerts,
        } = self
            .dag
            .process_forking_notification(ForkingNotification::Units(committed_units), &self.store);
        assert!(alerts.is_empty());
        for unit in units {
            self.on_reconstructed_unit(unit);
        }
        for request in requests {
            self.on_request(request);
        }
    }

    fn feed(mut self) -> Vec<ReconstructedUnit> {
        let units = self.units.clone();
        for unit in units {
            let DagResult {
                units,
                requests,
                alerts,
            } = self.dag.add_unit(unit.unit.into(), &self.store);
            for unit in units {
                self.on_reconstructed_unit(unit);
            }
            for alert in alerts {
                self.on_alert(alert.forker());
            }
            for request in requests {
                self.on_request(request);
            }
        }
        self.result
    }
}

struct RecordingHandler {
    finalized: Arc<Mutex<Vec<Data>>>,
}

impl RecordingHandler {
    fn new() -> (Self, Arc<Mutex<Vec<Data>>>) {
        let finalized = Arc::new(Mutex::new(Vec::new()));
        (
            RecordingHandler {
                finalized: finalized.clone(),
            },
            finalized,
        )
    }
}

impl UnitFinalizationHandler for RecordingHandler {
    type Data = Data;
    type Hasher = Hasher64;

    fn batch_finalized(&mut self, batch: Vec<OrderedUnit<Self::Data, Self::Hasher>>) {
        let mut batch_of_data = batch.into_iter().filter_map(|unit| unit.data).collect();
        self.finalized.lock().append(&mut batch_of_data)
    }
}

fn run_consensus_on_dag(
    units: Vec<UnitWithParents>,
    forker_units: HashMap<NodeIndex, Vec<UnitWithParents>>,
) -> Vec<Data> {
    let node_id = NodeIndex(0);
    let feeder = DagFeeder::new(node_id, units, forker_units);
    let (recording_handler, finalized) = RecordingHandler::new();
    let mut ordering = Ordering::new(recording_handler);
    for unit in feeder.feed() {
        ordering.add_unit(unit);
    }
    let finalized = finalized.lock().clone();
    finalized
}

fn generate_random_dag(
    n_members: NodeCount,
    height: Round,
    seed: u64,
) -> (
    Vec<UnitWithParents>,
    HashMap<NodeIndex, Vec<UnitWithParents>>,
) {
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

    let mut forker_units: HashMap<NodeIndex, Vec<UnitWithParents>> = HashMap::new();
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
                if forker_bitmap[node_ix] {
                    forker_units.entry(node_ix).or_default().push(unit.clone());
                }
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
    (dag_units, forker_units)
}

#[tokio::test]
async fn ordering_random_dag_consistency_under_permutations() {
    for seed in 0..4u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let n_members = NodeCount(rng.gen_range(1..11));
        let height = rng.gen_range(3..11);
        let (mut units, forker_units) = generate_random_dag(n_members, height, seed);
        let finalized_data = run_consensus_on_dag(units.clone(), forker_units.clone());
        debug!(target: "dag-test",
            "seed {:?} n_members {:?} height {:?} data_len {:?}",
            seed,
            n_members,
            height,
            finalized_data.len()
        );
        for i in 0..8 {
            units.shuffle(&mut rng);
            let other_finalized_data = run_consensus_on_dag(units.clone(), forker_units.clone());
            if other_finalized_data != finalized_data {
                debug!(target: "dag-test",
                    "seed {:?} n_members {:?} height {:?} i {:?}",
                    seed, n_members, height, i
                );
                debug!(target: "dag-test",
                    "batch lens {:?} \n {:?}",
                    finalized_data.len(),
                    other_finalized_data.len()
                );
                assert_eq!(other_finalized_data, finalized_data);
            }
        }
    }
}
