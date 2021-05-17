use crate::{
    member::NotificationOut,
    signed::{Signable, Signed, UncheckedSigned},
    Data, Hasher, Index, KeyBox, NodeCount, NodeIndex, NodeMap, Round, SessionId,
};
use codec::{Decode, Encode};
use log::error;
use std::{collections::HashMap, hash::Hash as StdHash};

// TODO: need to make sure we never accept units of round > MAX_ROUND
pub(crate) const MAX_ROUND: usize = 5000;

#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct FullUnit<H: Hasher, D: Data> {
    pub(crate) inner: PreUnit<H>,
    pub(crate) data: D,
    pub(crate) session_id: SessionId,
}

impl<H: Hasher, D: Data> FullUnit<H, D> {
    pub(crate) fn creator(&self) -> NodeIndex {
        self.inner.creator
    }
    pub(crate) fn round(&self) -> Round {
        self.inner.round()
    }

    pub(crate) fn coord(&self) -> UnitCoord {
        (self.round(), self.creator()).into()
    }

    pub(crate) fn hash(&self) -> H::Hash {
        H::hash(&self.encode())
    }
}

impl<H: Hasher, D: Data> Signable for FullUnit<H, D> {
    type Hash = H::Hash;
    fn hash(&self) -> H::Hash {
        self.using_encoded(H::hash)
    }
}

impl<H: Hasher, D: Data> Index for FullUnit<H, D> {
    fn index(&self) -> NodeIndex {
        self.inner.creator
    }
}

pub(crate) type UncheckedSignedUnit<H, D, S> = UncheckedSigned<FullUnit<H, D>, S>;

pub(crate) type SignedUnit<'a, H, D, KB> = Signed<'a, FullUnit<H, D>, KB>;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Encode, Decode, StdHash)]
pub(crate) struct UnitCoord {
    pub(crate) creator: NodeIndex,
    pub(crate) round: u64,
}

impl<H: Hasher> From<Unit<H>> for UnitCoord {
    fn from(unit: Unit<H>) -> Self {
        UnitCoord {
            creator: unit.creator(),
            round: unit.round() as u64,
        }
    }
}

impl<H: Hasher> From<&Unit<H>> for UnitCoord {
    fn from(unit: &Unit<H>) -> Self {
        UnitCoord {
            creator: unit.creator(),
            round: unit.round() as u64,
        }
    }
}

impl From<(usize, NodeIndex)> for UnitCoord {
    fn from(coord: (usize, NodeIndex)) -> Self {
        UnitCoord {
            creator: coord.1,
            round: coord.0 as u64,
        }
    }
}

type UnitRound = u64;

#[derive(Clone, Debug, PartialEq, Encode, Decode)]
pub(crate) struct PreUnit<H: Hasher> {
    pub(crate) creator: NodeIndex,
    pub(crate) round: UnitRound,
    pub(crate) control_hash: ControlHash<H>,
}

impl<H: Hasher> PreUnit<H> {
    pub(crate) fn creator(&self) -> NodeIndex {
        self.creator
    }

    pub(crate) fn round(&self) -> Round {
        self.round as Round
    }

    pub(crate) fn new_from_parents(
        creator: NodeIndex,
        round: Round,
        parents: NodeMap<Option<H::Hash>>,
    ) -> Self {
        let control_hash = ControlHash::new(&parents);
        PreUnit {
            creator,
            round: round as u64,
            control_hash,
        }
    }

    pub(crate) fn n_parents(&self) -> NodeCount {
        self.control_hash.n_parents()
    }

    pub(crate) fn n_members(&self) -> NodeCount {
        self.control_hash.n_members()
    }
}

impl<H: Hasher> From<PreUnit<H>> for NotificationOut<H> {
    fn from(pu: PreUnit<H>) -> Self {
        NotificationOut::CreatedPreUnit(pu)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Encode, Decode)]
pub(crate) struct Unit<H: Hasher> {
    pub(crate) creator: NodeIndex,
    round: UnitRound,
    pub(crate) hash: H::Hash,
    pub(crate) control_hash: ControlHash<H>,
}

impl<H: Hasher> Unit<H> {
    pub(crate) fn hash(&self) -> H::Hash {
        self.hash
    }

    pub(crate) fn creator(&self) -> NodeIndex {
        self.creator
    }
    pub(crate) fn round(&self) -> Round {
        self.round as Round
    }

    pub(crate) fn new_from_preunit(pu: PreUnit<H>, hash: H::Hash) -> Self {
        Unit {
            creator: pu.creator,
            round: pu.round,
            hash,
            control_hash: pu.control_hash,
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Encode, Decode)]
pub(crate) struct ControlHash<H: Hasher> {
    // TODO we need to optimize it for it to take O(N) bits of memory not O(N) words.
    pub(crate) parents: NodeMap<bool>,
    pub(crate) hash: H::Hash,
}

impl<H: Hasher> ControlHash<H> {
    fn new(parent_map: &NodeMap<Option<H::Hash>>) -> Self {
        let hash = Self::combine_hashes(&parent_map);
        let parents = parent_map.iter().map(|h| h.is_some()).collect();

        ControlHash { parents, hash }
    }

    pub(crate) fn combine_hashes(parent_map: &NodeMap<Option<H::Hash>>) -> H::Hash {
        parent_map.using_encoded(H::hash)
    }

    pub(crate) fn n_parents(&self) -> NodeCount {
        NodeCount(self.parents.iter().filter(|&b| *b).count())
    }

    pub(crate) fn n_members(&self) -> NodeCount {
        NodeCount(self.parents.len())
    }
}

pub(crate) struct UnitStore<'a, H: Hasher, D: Data, KB: KeyBox> {
    by_coord: HashMap<UnitCoord, SignedUnit<'a, H, D, KB>>,
    by_hash: HashMap<H::Hash, SignedUnit<'a, H, D, KB>>,
    parents: HashMap<H::Hash, Vec<H::Hash>>,
    //this is the smallest r, such that round r-1 is saturated, i.e., it has at least threshold (~(2/3)N) units
    round_in_progress: usize,
    threshold: NodeCount,
    //the number of unique nodes that we hold units for a given round
    n_units_per_round: Vec<NodeCount>,
    is_forker: NodeMap<bool>,
    legit_buffer: Vec<SignedUnit<'a, H, D, KB>>,
}

impl<'a, H: Hasher, D: Data, KB: KeyBox> UnitStore<'a, H, D, KB> {
    pub(crate) fn new(n_nodes: NodeCount, threshold: NodeCount) -> Self {
        UnitStore {
            by_coord: HashMap::new(),
            by_hash: HashMap::new(),
            parents: HashMap::new(),
            round_in_progress: 0,
            threshold,
            n_units_per_round: vec![NodeCount(0); MAX_ROUND + 1],
            // is_forker is initialized with default values for bool, i.e., false
            is_forker: NodeMap::new_with_len(n_nodes),
            legit_buffer: Vec::new(),
        }
    }

    pub(crate) fn unit_by_coord(&self, coord: UnitCoord) -> Option<&SignedUnit<'a, H, D, KB>> {
        self.by_coord.get(&coord)
    }

    pub(crate) fn unit_by_hash(&self, hash: &H::Hash) -> Option<&SignedUnit<'a, H, D, KB>> {
        self.by_hash.get(hash)
    }

    pub(crate) fn contains_hash(&self, hash: &H::Hash) -> bool {
        self.by_hash.contains_key(hash)
    }

    pub(crate) fn contains_coord(&self, coord: &UnitCoord) -> bool {
        self.by_coord.contains_key(coord)
    }

    // Outputs new legit units that are supposed to be sent to Consensus and emties the buffer.
    pub(crate) fn yield_buffer_units(&mut self) -> Vec<SignedUnit<'a, H, D, KB>> {
        std::mem::take(&mut self.legit_buffer)
    }

    fn update_round_in_progress(&mut self, candidate_round: usize) {
        if candidate_round >= self.round_in_progress
            && self.n_units_per_round[candidate_round] >= self.threshold
        {
            let old_round = self.round_in_progress;
            self.round_in_progress = candidate_round + 1;
            for round in (old_round + 1)..(self.round_in_progress + 1) {
                for (id, forker) in self.is_forker.enumerate() {
                    if !*forker {
                        let coord = (round, id).into();
                        if let Some(su) = self.unit_by_coord(coord).cloned() {
                            self.legit_buffer.push(su);
                        }
                    }
                }
            }
        }
    }
    // Outputs None if this is not a newly-discovered fork or Some(sv) where (su, sv) form a fork
    pub(crate) fn is_new_fork(
        &self,
        su: &SignedUnit<'a, H, D, KB>,
    ) -> Option<SignedUnit<'a, H, D, KB>> {
        // TODO: optimize so that unit's hash is computed once only, after it is received
        let hash = su.as_signable().hash();
        if self.contains_hash(&hash) {
            return None;
        }
        let coord = su.as_signable().coord();
        self.unit_by_coord(coord).cloned()
    }

    pub(crate) fn get_round_in_progress(&self) -> usize {
        self.round_in_progress
    }

    pub(crate) fn is_forker(&self, node_id: NodeIndex) -> bool {
        self.is_forker[node_id]
    }

    // Marks a node as a forker and outputs units in store of round <= round_in_progress created by this node.
    // The returned vector is sorted w.r.t. increasing rounds. Units of higher round created by this node are removed from store.
    pub(crate) fn mark_forker(&mut self, forker: NodeIndex) -> Vec<SignedUnit<'a, H, D, KB>> {
        if self.is_forker[forker] {
            error!(target: "env", "Trying to mark the node {:?} as forker for the second time.", forker);
        }
        self.is_forker[forker] = true;
        let forkers_units = (0..=self.round_in_progress)
            .filter_map(|r| self.unit_by_coord((r, forker).into()).cloned())
            .collect();

        for round in self.round_in_progress + 1..=MAX_ROUND {
            let coord = (round, forker).into();
            if let Some(su) = self.unit_by_coord(coord).cloned() {
                // We get rid of this unit. This is safe because it has not been sent to Consensus yet.
                // The reason we do that, is to be in a "clean" situation where we alert all forker's
                // units in the store and the only way this forker's unit is sent to Consensus is when
                // it arrives in an alert for the *first* time.
                // If we didn't do that, then there would be some awkward issues with duplicates.
                self.by_coord.remove(&coord);
                let hash = su.as_signable().hash();
                self.by_hash.remove(&hash);
                self.parents.remove(&hash);
                // Now we are in a state as if the unit never arrived.
            }
        }
        forkers_units
    }

    pub(crate) fn add_unit(&mut self, su: SignedUnit<'a, H, D, KB>, alert: bool) {
        // TODO: optimize so that unit's hash is computed once only, after it is received
        let hash = su.as_signable().hash();
        let round = su.as_signable().round();
        let creator = su.as_signable().creator();
        if alert {
            assert!(
                self.is_forker[creator],
                "The forker must be marked before adding alerted units."
            );
        }
        if self.contains_hash(&hash) {
            // Ignoring a duplicate.
            return;
        }
        self.by_hash.insert(hash, su.clone());
        let coord = su.as_signable().coord();
        // We do not store multiple forks of a unit by coord, as there is never a need to
        // fetch all units corresponding to a particular coord.
        if self.by_coord.insert(coord, su.clone()).is_none() {
            // This means that this unit is not a fork (even though the creator might be a forker)
            self.n_units_per_round[round] += NodeCount(1);
        }
        // NOTE: a minor inefficiency is that we send alerted units of high rounds that are possibly
        // way beyond round_in_progress right away to Consensus. This could be perhaps corrected so that
        // we wait until the round is in progress, but this does not seem to help vs actual attacks and in
        // "accidental" forks the rounds will never be much higher than round_in_progress.
        if alert || (round <= self.round_in_progress && !self.is_forker[creator]) {
            self.legit_buffer.push(su);
        }
        self.update_round_in_progress(round);
    }

    pub(crate) fn add_parents(&mut self, hash: H::Hash, parents: Vec<H::Hash>) {
        self.parents.insert(hash, parents);
    }

    pub(crate) fn get_parents(&mut self, hash: H::Hash) -> Option<&Vec<H::Hash>> {
        self.parents.get(&hash)
    }

    pub(crate) fn limit_per_node(&self) -> Round {
        MAX_ROUND
    }
}
