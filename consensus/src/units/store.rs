use super::*;
use itertools::Itertools;
use log::{trace, warn};
use std::{collections::HashSet, fmt};

pub struct UnitStoreStatus<'a> {
    forkers: &'a NodeSubset,
    size: usize,
    height: Option<Round>,
    top_row: NodeMap<Round>,
    first_missing_rounds: NodeMap<Round>,
}

impl<'a> UnitStoreStatus<'a> {
    fn new(
        forkers: &'a NodeSubset,
        size: usize,
        height: Option<Round>,
        top_row: NodeMap<Round>,
        first_missing_rounds: NodeMap<Round>,
    ) -> Self {
        Self {
            forkers,
            size,
            height,
            top_row,
            first_missing_rounds,
        }
    }
}

impl<'a> fmt::Display for UnitStoreStatus<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DAG size - {}", self.size)?;
        if let Some(r) = self.height {
            write!(f, "; DAG height - {}", r)?;
        }
        if self.first_missing_rounds.item_count() > 0 {
            write!(
                f,
                "; DAG first missing rounds - {}",
                self.first_missing_rounds
            )?;
        }
        write!(f, "; DAG top row - {}", self.top_row)?;
        if !self.forkers.is_empty() {
            write!(f, "; forkers - {}", self.forkers)?;
        }
        Ok(())
    }
}

/// A component for temporarily storing units before they are declared "legit" and sent
/// to the Terminal. We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/internals.html
/// Section 5.4 for a discussion of this component and the notion of "legit" units.

pub(crate) struct UnitStore<'a, H: Hasher, D: Data, K: Keychain> {
    by_coord: HashMap<UnitCoord, SignedUnit<'a, H, D, K>>,
    by_hash: HashMap<H::Hash, SignedUnit<'a, H, D, K>>,
    parents: HashMap<H::Hash, Vec<H::Hash>>,
    //the number of unique nodes that we hold units for a given round
    is_forker: NodeSubset,
    legit_buffer: Vec<SignedUnit<'a, H, D, K>>,
    max_round: Round,
}

impl<'a, H: Hasher, D: Data, K: Keychain> UnitStore<'a, H, D, K> {
    pub(crate) fn new(n_nodes: NodeCount, max_round: Round) -> Self {
        UnitStore {
            by_coord: HashMap::new(),
            by_hash: HashMap::new(),
            parents: HashMap::new(),
            // is_forker is initialized with default values for bool, i.e., false
            is_forker: NodeSubset::with_size(n_nodes),
            legit_buffer: Vec::new(),
            max_round,
        }
    }

    pub fn get_status(&self) -> UnitStoreStatus {
        let n_nodes: NodeCount = self.is_forker.size().into();
        let gm = self
            .by_coord
            .keys()
            .map(|c| (c.creator, c.round))
            .into_grouping_map();
        let top_row = NodeMap::from_hashmap(n_nodes, gm.clone().max());
        let first_missing_rounds = NodeMap::from_hashmap(
            n_nodes,
            gm.collect::<HashSet<_>>()
                .into_iter()
                .filter_map(|(id, rounds)| match top_row.get(id) {
                    Some(&row) => (0..row)
                        .position(|round| !rounds.contains(&round))
                        .map(|round| (id, round as Round)),
                    None => None,
                })
                .collect(),
        );
        UnitStoreStatus::new(
            &self.is_forker,
            self.by_coord.len(),
            self.by_coord.keys().map(|k| k.round).max(),
            top_row,
            first_missing_rounds,
        )
    }

    pub(crate) fn unit_by_coord(&self, coord: UnitCoord) -> Option<&SignedUnit<'a, H, D, K>> {
        self.by_coord.get(&coord)
    }

    pub(crate) fn unit_by_hash(&self, hash: &H::Hash) -> Option<&SignedUnit<'a, H, D, K>> {
        self.by_hash.get(hash)
    }

    pub(crate) fn contains_hash(&self, hash: &H::Hash) -> bool {
        self.by_hash.contains_key(hash)
    }

    pub(crate) fn contains_coord(&self, coord: &UnitCoord) -> bool {
        self.by_coord.contains_key(coord)
    }

    pub(crate) fn newest_unit(
        &self,
        index: NodeIndex,
    ) -> Option<UncheckedSignedUnit<H, D, K::Signature>> {
        Some(
            self.by_coord
                .values()
                .filter(|su| su.as_signable().creator() == index)
                .max_by_key(|su| su.as_signable().round())?
                .clone()
                .into_unchecked(),
        )
    }

    // Outputs new legit units that are supposed to be sent to Consensus and empties the buffer.
    pub(crate) fn yield_buffer_units(&mut self) -> Vec<SignedUnit<'a, H, D, K>> {
        std::mem::take(&mut self.legit_buffer)
    }

    // Outputs None if this is not a newly-discovered fork or Some(sv) where (su, sv) form a fork
    pub(crate) fn is_new_fork(&self, fu: &FullUnit<H, D>) -> Option<SignedUnit<'a, H, D, K>> {
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
    pub(crate) fn mark_forker(&mut self, forker: NodeIndex) -> Vec<SignedUnit<'a, H, D, K>> {
        if self.is_forker[forker] {
            warn!(target: "AlephBFT-unit-store", "Trying to mark the node {:?} as forker for the second time.", forker);
        }
        self.is_forker.insert(forker);
        (0..=self.max_round)
            .filter_map(|r| self.unit_by_coord(UnitCoord::new(r, forker)).cloned())
            .collect()
    }

    pub(crate) fn add_unit(&mut self, su: SignedUnit<'a, H, D, K>, alert: bool) {
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
}

#[cfg(test)]
mod tests {
    use crate::{
        units::{ControlHash, FullUnit, PreUnit, SignedUnit, UnitCoord, UnitStore},
        NodeCount, NodeIndex, NodeMap, Round, Signed,
    };
    use aleph_bft_mock::{Data, Hasher64, Keychain};

    async fn create_unit<'a>(
        round: Round,
        node_idx: NodeIndex,
        count: NodeCount,
        session_id: u64,
        keychain: &'_ Keychain,
    ) -> SignedUnit<'_, Hasher64, Data, Keychain> {
        let preunit = PreUnit::<Hasher64>::new(
            node_idx,
            round,
            ControlHash::new(&NodeMap::with_size(count)),
        );
        let full_unit = FullUnit::new(preunit, 0, session_id);
        Signed::sign(full_unit, keychain).await
    }

    #[tokio::test]
    async fn mark_forker_restore_state() {
        let n_nodes = NodeCount(10);

        let mut store = UnitStore::<Hasher64, Data, Keychain>::new(n_nodes, 100);

        let keychains: Vec<_> = (0..=4)
            .map(|i| Keychain::new(n_nodes, NodeIndex(i)))
            .collect();

        let mut forker_hashes = Vec::new();

        for round in 0..4 {
            for (i, keychain) in keychains.iter().enumerate() {
                let unit = create_unit(round, NodeIndex(i), n_nodes, 0, keychain).await;
                if i == 0 {
                    forker_hashes.push(unit.as_signable().hash());
                }
                store.add_unit(unit, false);
            }
        }

        // Forker's units
        for round in 4..7 {
            let unit = create_unit(round, NodeIndex(0), n_nodes, 0, &keychains[0]).await;
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
