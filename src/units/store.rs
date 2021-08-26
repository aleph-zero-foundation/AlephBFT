use super::*;
use log::{trace, warn};

/// A component for temporarily storing units before they are declared "legit" and sent
/// to the Terminal. We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/internals.html
/// Section 5.4 for a discussion of this component and the notion of "legit" units.

pub(crate) struct UnitStore<'a, H: Hasher, D: Data, KB: KeyBox> {
    by_coord: HashMap<UnitCoord, SignedUnit<'a, H, D, KB>>,
    by_hash: HashMap<H::Hash, SignedUnit<'a, H, D, KB>>,
    parents: HashMap<H::Hash, Vec<H::Hash>>,
    //the number of unique nodes that we hold units for a given round
    is_forker: NodeMap<bool>,
    legit_buffer: Vec<SignedUnit<'a, H, D, KB>>,
    max_round: Round,
}

impl<'a, H: Hasher, D: Data, KB: KeyBox> UnitStore<'a, H, D, KB> {
    pub(crate) fn new(n_nodes: NodeCount, max_round: Round) -> Self {
        UnitStore {
            by_coord: HashMap::new(),
            by_hash: HashMap::new(),
            parents: HashMap::new(),
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

    // Outputs None if this is not a newly-discovered fork or Some(sv) where (su, sv) form a fork
    pub(crate) fn is_new_fork(&self, fu: &FullUnit<H, D>) -> Option<SignedUnit<'a, H, D, KB>> {
        if self.contains_hash(&fu.hash()) {
            return None;
        }
        self.unit_by_coord(fu.coord()).cloned()
    }

    pub(crate) fn is_forker(&self, node_id: NodeIndex) -> bool {
        self.is_forker[node_id]
    }

    // Marks a node as a forker and outputs all units in store created by this node.
    // The returned vector is sorted w.r.t. increasing rounds.
    pub(crate) fn mark_forker(&mut self, forker: NodeIndex) -> Vec<SignedUnit<'a, H, D, KB>> {
        if self.is_forker[forker] {
            warn!(target: "AlephBFT-unit-store", "Trying to mark the node {:?} as forker for the second time.", forker);
        }
        self.is_forker[forker] = true;
        (0..=self.max_round)
            .filter_map(|r| self.unit_by_coord(UnitCoord::new(r, forker)).cloned())
            .collect()
    }

    pub(crate) fn add_unit(&mut self, su: SignedUnit<'a, H, D, KB>, alert: bool) {
        let hash = su.as_signable().hash();
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
        self.by_coord.insert(su.as_signable().coord(), su.clone());

        if alert || !self.is_forker[creator] {
            self.legit_buffer.push(su);
        }
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
    async fn mark_forker_restore_state() {
        let n_nodes = NodeCount(10);

        let mut store = UnitStore::<Hasher64, Data, KeyBox>::new(n_nodes, 100);

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

        assert_eq!(vec![0, 1, 2, 3, 4, 5, 6], forker_units);
        assert!(store.is_forker[NodeIndex(0)]);

        // All rounds still have forker's units
        for (round, hash) in forker_hashes[0..4].iter().enumerate() {
            let round = round as Round;
            let coord = UnitCoord::new(round, NodeIndex(0));
            assert!(store.by_coord.contains_key(&coord));
            assert!(store.by_hash.contains_key(hash));
        }

        assert!(store
            .by_coord
            .contains_key(&UnitCoord::new(4, NodeIndex(0))));
        assert!(store.by_hash.contains_key(&forker_hashes[4]));

        for (round, hash) in forker_hashes[5..7].iter().enumerate() {
            let round = round as Round;
            let round = round + 5;
            let coord = UnitCoord::new(round, NodeIndex(0));
            assert!(store.by_coord.contains_key(&coord));
            assert!(store.by_hash.contains_key(hash));
        }
    }
}
