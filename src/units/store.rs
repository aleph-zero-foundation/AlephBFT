use super::*;
use log::{trace, warn};

/// A component for temporarily storing units before they are declared "legit" and sent
/// to the Terminal. We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/internals.html
/// Section 5.4 for a discussion of this component and the notion of "legit" units.

pub(crate) struct UnitStore<'a, H: Hasher, D: Data, KB: KeyBox> {
    by_coord: HashMap<UnitCoord, SignedUnit<'a, H, D, KB>>,
    by_hash: HashMap<H::Hash, SignedUnit<'a, H, D, KB>>,
    parents: HashMap<H::Hash, Vec<H::Hash>>,
    //this is the smallest r, such that round r-1 is saturated, i.e., it has at least threshold (~(2/3)N) units
    round_in_progress: Round,
    threshold: NodeCount,
    //the number of unique nodes that we hold units for a given round
    n_units_per_round: Vec<NodeCount>,
    is_forker: NodeMap<bool>,
    legit_buffer: Vec<SignedUnit<'a, H, D, KB>>,
    max_round: Round,
}

impl<'a, H: Hasher, D: Data, KB: KeyBox> UnitStore<'a, H, D, KB> {
    pub(crate) fn new(n_nodes: NodeCount, threshold: NodeCount, max_round: Round) -> Self {
        UnitStore {
            by_coord: HashMap::new(),
            by_hash: HashMap::new(),
            parents: HashMap::new(),
            round_in_progress: 0,
            threshold,
            n_units_per_round: vec![NodeCount(0); (max_round + 1).into()],
            // is_forker is initialized with default values for bool, i.e., false
            is_forker: NodeMap::new_with_len(n_nodes),
            legit_buffer: Vec::new(),
            max_round,
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

    // Outputs new legit units that are supposed to be sent to Consensus and empties the buffer.
    pub(crate) fn yield_buffer_units(&mut self) -> Vec<SignedUnit<'a, H, D, KB>> {
        std::mem::take(&mut self.legit_buffer)
    }

    fn update_round_in_progress(&mut self, candidate_round: Round) {
        if candidate_round >= self.round_in_progress
            && self.n_units_per_round[candidate_round as usize] >= self.threshold
        {
            let old_round = self.round_in_progress;
            self.round_in_progress = candidate_round + 1;
            // The loop below will normally just go over a single round, but theoretically the jump between
            // old_round and candidate_round could be >1.
            for round in (old_round + 1)..(self.round_in_progress + 1) {
                for (id, forker) in self.is_forker.enumerate() {
                    if !*forker {
                        let coord = UnitCoord::new(round, id);
                        if let Some(su) = self.unit_by_coord(coord).cloned() {
                            self.legit_buffer.push(su);
                        }
                    }
                }
            }
        }
    }
    // Outputs None if this is not a newly-discovered fork or Some(sv) where (su, sv) form a fork
    pub(crate) fn is_new_fork(&self, fu: &FullUnit<H, D>) -> Option<SignedUnit<'a, H, D, KB>> {
        if self.contains_hash(&fu.hash()) {
            return None;
        }
        self.unit_by_coord(fu.coord()).cloned()
    }

    pub(crate) fn get_round_in_progress(&self) -> Round {
        self.round_in_progress
    }

    pub(crate) fn is_forker(&self, node_id: NodeIndex) -> bool {
        self.is_forker[node_id]
    }

    // Marks a node as a forker and outputs units in store of round <= round_in_progress created by this node.
    // The returned vector is sorted w.r.t. increasing rounds. Units of higher round created by this node are removed from store.
    pub(crate) fn mark_forker(&mut self, forker: NodeIndex) -> Vec<SignedUnit<'a, H, D, KB>> {
        if self.is_forker[forker] {
            warn!(target: "AlephBFT-unit-store", "Trying to mark the node {:?} as forker for the second time.", forker);
        }
        self.is_forker[forker] = true;
        let forkers_units = (0..=self.round_in_progress)
            .filter_map(|r| self.unit_by_coord(UnitCoord::new(r, forker)).cloned())
            .collect();

        for round in self.round_in_progress + 1..=self.max_round {
            let coord = UnitCoord::new(round, forker);
            if let Some(su) = self.unit_by_coord(coord).cloned() {
                // We get rid of this unit. This is safe because it has not been sent to Consensus yet.
                // The reason we do that, is to be in a "clean" situation where we alert all forker's
                // units in the store and the only way this forker's unit is sent to Consensus is when
                // it arrives in an alert for the *first* time.
                // If we didn't do that, then there would be some awkward issues with duplicates.
                trace!(target: "AlephBFT-unit-store", "Removing unit from forker {:?}  {:?}.", coord, su.as_signable());
                self.by_coord.remove(&coord);
                let hash = su.as_signable().hash();
                self.by_hash.remove(&hash);
                self.parents.remove(&hash);
                self.n_units_per_round[round as usize] -= NodeCount(1);
                // Now we are in a state as if the unit never arrived.
            }
        }
        forkers_units
    }

    pub(crate) fn add_unit(&mut self, su: SignedUnit<'a, H, D, KB>, alert: bool) {
        let hash = su.as_signable().hash();
        let round = su.as_signable().round();
        let creator = su.as_signable().creator();

        if alert {
            trace!(target: "AlephBFT-unit-store", "Adding unit with alert {:?}.", su.as_signable());
            assert!(
                self.is_forker[creator],
                "The forker must be marked before adding alerted units."
            );
        }
        if self.contains_hash(&hash) {
            // Ignoring a duplicate.
            trace!(target: "AlephBFT-unit-store", "A unit ignored as a duplicate {:?}.", su.as_signable());
            return;
        }
        self.by_hash.insert(hash, su.clone());
        let coord = su.as_signable().coord();
        // We do not store multiple forks of a unit by coord, as there is never a need to
        // fetch all units corresponding to a particular coord.
        if self.by_coord.insert(coord, su.clone()).is_none() {
            // This means that this unit is not a fork (even though the creator might be a forker)
            self.n_units_per_round[round as usize] += NodeCount(1);
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
        self.max_round
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        nodes::NodeMap,
        testing::mock::{Data, Hasher64, KeyBox},
        units::{ControlHash, FullUnit, PreUnit, SignedUnit, UnitCoord, UnitStore},
        NodeCount, NodeIndex, Round, Signed,
    };

    async fn create_unit<'a>(
        round: Round,
        node_idx: NodeIndex,
        count: NodeCount,
        session_id: u64,
        keybox: &'_ KeyBox,
    ) -> SignedUnit<'_, Hasher64, Data, KeyBox> {
        let preunit = PreUnit::<Hasher64>::new(
            node_idx,
            round,
            ControlHash::new(&NodeMap::new_with_len(count)),
        );
        let coord = UnitCoord::new(round, node_idx);
        let data = Data::new(coord, 0);
        let full_unit = FullUnit::new(preunit, data, session_id);
        Signed::sign(full_unit, keybox).await
    }

    #[tokio::test]
    async fn add_unit_increment_count_test() {
        let n_nodes = NodeCount(10);
        let threshold = NodeCount(4);

        let mut store = UnitStore::<Hasher64, Data, KeyBox>::new(n_nodes, threshold, 100);

        let keybox = KeyBox::new(n_nodes, NodeIndex(0));
        let unit = create_unit(0, NodeIndex(0), n_nodes, 0, &keybox).await;

        assert_eq!(NodeCount(0), store.n_units_per_round[0]);
        store.add_unit(unit, false);
        assert_eq!(NodeCount(1), store.n_units_per_round[0]);
    }

    #[tokio::test]
    async fn add_units_increment_round_test() {
        let n_nodes = NodeCount(10);
        let threshold = NodeCount(4);

        let mut store = UnitStore::<Hasher64, Data, KeyBox>::new(n_nodes, threshold, 100);

        let keyboxes: Vec<_> = (0..=4)
            .map(|i| KeyBox::new(n_nodes, NodeIndex(i)))
            .collect();

        assert_eq!(0, store.round_in_progress);
        assert_eq!(NodeCount(0), store.n_units_per_round[0]);

        for (i, keybox) in keyboxes.iter().enumerate() {
            let unit = create_unit(0, NodeIndex(i), n_nodes, 0, keybox).await;
            store.add_unit(unit, false);
        }

        assert_eq!(1, store.round_in_progress);
        assert_eq!(NodeCount(5), store.n_units_per_round[0]);
    }

    #[tokio::test]
    async fn mark_forker_restore_state() {
        let n_nodes = NodeCount(10);
        let threshold = NodeCount(4);

        let mut store = UnitStore::<Hasher64, Data, KeyBox>::new(n_nodes, threshold, 100);

        let keyboxes: Vec<_> = (0..=4)
            .map(|i| KeyBox::new(n_nodes, NodeIndex(i)))
            .collect();

        let mut forker_hashes = Vec::new();

        for round in 0..4 {
            for (i, keybox) in keyboxes.iter().enumerate() {
                let unit = create_unit(round, NodeIndex(i), n_nodes, 0, keybox).await;
                if i == 0 {
                    forker_hashes.push(unit.as_signable().hash());
                }
                store.add_unit(unit, false);
            }
        }

        // Forker's units
        for round in 4..7 {
            let unit = create_unit(round, NodeIndex(0), n_nodes, 0, &keyboxes[0]).await;
            forker_hashes.push(unit.as_signable().hash());
            store.add_unit(unit, false);
        }

        let forker_units: Vec<_> = store
            .mark_forker(NodeIndex(0))
            .iter()
            .map(|unit| unit.clone().into_unchecked().as_signable().round())
            .collect();

        assert_eq!(vec![0, 1, 2, 3, 4], forker_units);
        assert!(store.is_forker[NodeIndex(0)]);

        // Rounds that are not in progress still have forker's units
        for (round, hash) in forker_hashes[0..4].iter().enumerate() {
            let round = round as Round;
            let coord = UnitCoord::new(round, NodeIndex(0));
            assert_eq!(NodeCount(5), store.n_units_per_round[round as usize]);
            assert!(store.by_coord.contains_key(&coord));
            assert!(store.by_hash.contains_key(hash));
        }

        // Round in progress still has forker's unit
        assert_eq!(NodeCount(1), store.n_units_per_round[4]);
        assert!(store
            .by_coord
            .contains_key(&UnitCoord::new(4, NodeIndex(0))));
        assert!(store.by_hash.contains_key(&forker_hashes[4]));

        // Rounds after round in progress are "free" of forker's units;
        for (round, hash) in forker_hashes[5..7].iter().enumerate() {
            let round = round as Round;
            let round = round + 5;
            let coord = UnitCoord::new(round, NodeIndex(0));
            assert_eq!(NodeCount(0), store.n_units_per_round[round as usize]);
            assert!(!store.by_coord.contains_key(&coord));
            assert!(!store.by_hash.contains_key(hash));
        }
    }
}
