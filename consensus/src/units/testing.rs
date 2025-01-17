use crate::units::TestingDagUnit;
use crate::{
    creation::Creator as GenericCreator,
    dag::ReconstructedUnit,
    units::{
        ControlHash as GenericControlHash, FullUnit as GenericFullUnit, PreUnit as GenericPreUnit,
        SignedUnit as GenericSignedUnit, UncheckedSignedUnit as GenericUncheckedSignedUnit, Unit,
        UnitCoord, WrappedUnit,
    },
    NodeCount, NodeIndex, NodeMap, Round, SessionId, Signed,
};
use aleph_bft_mock::{Data, Hash64, Hasher64, Keychain, Signature};
use rand::prelude::IteratorRandom;

type ControlHash = GenericControlHash<Hasher64>;
type Creator = GenericCreator<Hasher64>;
type PreUnit = GenericPreUnit<Hasher64>;
pub type FullUnit = GenericFullUnit<Hasher64, Data>;
type UncheckedSignedUnit = GenericUncheckedSignedUnit<Hasher64, Data, Signature>;
pub type SignedUnit = GenericSignedUnit<Hasher64, Data, Keychain>;
pub type DagUnit = ReconstructedUnit<SignedUnit>;

#[derive(Clone)]
pub struct WrappedSignedUnit(pub SignedUnit);

impl Unit for WrappedSignedUnit {
    type Hasher = Hasher64;

    fn hash(&self) -> Hash64 {
        self.0.hash()
    }

    fn coord(&self) -> UnitCoord {
        self.0.coord()
    }

    fn control_hash(&self) -> &ControlHash {
        self.0.control_hash()
    }

    fn session_id(&self) -> SessionId {
        self.0.session_id()
    }
}

impl WrappedUnit<Hasher64> for WrappedSignedUnit {
    type Wrapped = SignedUnit;

    fn unpack(self) -> Self::Wrapped {
        self.0
    }
}

pub fn creator_set(n_members: NodeCount) -> Vec<Creator> {
    (0..n_members.0)
        .map(|i| Creator::new(NodeIndex(i), n_members))
        .collect()
}

pub fn create_preunits<'a, C: Iterator<Item = &'a Creator>>(
    creators: C,
    round: Round,
) -> Vec<PreUnit> {
    creators
        .map(|c| c.create_unit(round).expect("Creation should succeed."))
        .collect()
}

pub fn preunit_to_full_unit(preunit: PreUnit, session_id: SessionId) -> FullUnit {
    FullUnit::new(preunit, rand::random(), session_id)
}

impl Creator {
    pub fn add_units<U: Unit<Hasher = Hasher64>>(&mut self, units: &[U]) {
        for unit in units {
            self.add_unit(unit);
        }
    }
}

pub fn full_unit_to_signed_unit(full_unit: FullUnit, keychain: &Keychain) -> SignedUnit {
    Signed::sign(full_unit, keychain)
}

pub fn preunit_to_signed_unit(
    pu: PreUnit,
    session_id: SessionId,
    keychain: &Keychain,
) -> SignedUnit {
    full_unit_to_signed_unit(preunit_to_full_unit(pu, session_id), keychain)
}

pub fn full_unit_to_unchecked_signed_unit(
    full_unit: FullUnit,
    keychain: &Keychain,
) -> UncheckedSignedUnit {
    full_unit_to_signed_unit(full_unit, keychain).into()
}

pub fn preunit_to_unchecked_signed_unit(
    pu: PreUnit,
    session_id: SessionId,
    keychain: &Keychain,
) -> UncheckedSignedUnit {
    preunit_to_signed_unit(pu, session_id, keychain).into()
}

fn initial_preunit(n_members: NodeCount, node_id: NodeIndex) -> PreUnit {
    PreUnit::new(
        node_id,
        0,
        ControlHash::new(&vec![None; n_members.0].into()),
    )
}

fn random_initial_units(n_members: NodeCount, session_id: SessionId) -> Vec<FullUnit> {
    n_members
        .into_iterator()
        .map(|node_id| initial_preunit(n_members, node_id))
        .map(|preunit| preunit_to_full_unit(preunit, session_id))
        .collect()
}

fn random_initial_reconstructed_units(
    n_members: NodeCount,
    session_id: SessionId,
    keychains: &[Keychain],
) -> Vec<DagUnit> {
    random_initial_units(n_members, session_id)
        .into_iter()
        .map(|full_unit| {
            let keychain = &keychains[full_unit.creator().0];
            ReconstructedUnit::initial(full_unit_to_signed_unit(full_unit, keychain))
        })
        .collect()
}

fn parent_map<U: Unit<Hasher = Hasher64>>(parents: &Vec<U>) -> NodeMap<(Hash64, Round)> {
    let n_members = parents
        .last()
        .expect("there are parents")
        .control_hash()
        .n_members();
    let mut result = NodeMap::with_size(n_members);
    for parent in parents {
        result.insert(parent.creator(), (parent.hash(), parent.round()));
    }
    result
}

pub fn random_unit_with_parents<U: Unit<Hasher = Hasher64>>(
    creator: NodeIndex,
    parents: &Vec<U>,
    round: Round,
) -> FullUnit {
    let representative_parent = parents.last().expect("there are parents");
    let session_id = representative_parent.session_id();
    let parent_map = parent_map(parents);
    let control_hash = ControlHash::new(&parent_map);
    preunit_to_full_unit(PreUnit::new(creator, round, control_hash), session_id)
}

pub fn random_reconstructed_unit_with_parents<U: Unit<Hasher = Hasher64>>(
    creator: NodeIndex,
    parents: &Vec<U>,
    keychain: &Keychain,
    round: Round,
) -> DagUnit {
    assert!(round > 0);
    ReconstructedUnit::with_parents(
        full_unit_to_signed_unit(random_unit_with_parents(creator, parents, round), keychain),
        parent_map(parents),
    )
    .expect("correct parents")
}

pub fn random_full_parent_units_up_to(
    round: Round,
    n_members: NodeCount,
    session_id: SessionId,
) -> Vec<Vec<FullUnit>> {
    let mut result = vec![random_initial_units(n_members, session_id)];
    for r in 1..=round {
        let units = n_members
            .into_iterator()
            .map(|node_id| {
                random_unit_with_parents(node_id, result.last().expect("previous round present"), r)
            })
            .collect();
        result.push(units);
    }
    result
}

/// Constructs a DAG so that in each round (except round 0) it has all N parents, where N is number
/// of nodes in the DAG
pub fn random_full_parent_reconstrusted_units_up_to(
    round: Round,
    n_members: NodeCount,
    session_id: SessionId,
    keychains: &[Keychain],
) -> Vec<Vec<DagUnit>> {
    let mut result = vec![random_initial_reconstructed_units(
        n_members, session_id, keychains,
    )];
    for r in 1..=round {
        let units = n_members
            .into_iterator()
            .map(|node_id| {
                random_reconstructed_unit_with_parents(
                    node_id,
                    result.last().expect("previous round present"),
                    &keychains[node_id.0],
                    r,
                )
            })
            .collect();
        result.push(units);
    }
    result
}

/// Constructs a DAG so that in each round (except round 0) it has at least 2N/3 + 1 parents, where
/// N is number of nodes in the DAG. At least one node from N/3 group has some non-direct parents.
pub fn minimal_reconstructed_dag_units_up_to(
    round: Round,
    n_members: NodeCount,
    session_id: SessionId,
    keychains: &[Keychain],
) -> (Vec<Vec<DagUnit>>, DagUnit) {
    let mut rng = rand::thread_rng();
    let threshold = n_members.consensus_threshold().0;

    let mut dag = vec![random_initial_reconstructed_units(
        n_members, session_id, keychains,
    )];
    let inactive_node_first_and_last_seen_unit = dag
        .last()
        .expect("previous round present")
        .last()
        .expect("there is at least one node")
        .clone();
    let inactive_node = inactive_node_first_and_last_seen_unit.creator();
    for r in 1..=round {
        let mut parents: Vec<TestingDagUnit> = dag
            .last()
            .expect("previous round present")
            .clone()
            .into_iter()
            .filter(|unit| unit.creator() != inactive_node)
            .choose_multiple(&mut rng, threshold)
            .into_iter()
            .collect();
        if r == round {
            let ancestor_unit = dag
                .first()
                .expect("first round present")
                .get(inactive_node.0)
                .expect("inactive node unit present");
            parents.push(ancestor_unit.clone());
        }
        let units = n_members
            .into_iterator()
            .filter(|node_id| node_id != &inactive_node)
            .map(|node_id| {
                random_reconstructed_unit_with_parents(node_id, &parents, &keychains[node_id.0], r)
            })
            .collect();
        dag.push(units);
    }
    (dag, inactive_node_first_and_last_seen_unit)
}
