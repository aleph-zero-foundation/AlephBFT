use crate::{
    network::NetworkDataInner::Units,
    network::UnitMessage,
    testing::{init_log, spawn_honest_member, HonestMember, Network, NetworkData},
    units::{ControlHash, FullUnit, PreUnit, SignedUnit, Unit, UnitCoord},
    Hasher, Network as NetworkT, NetworkData as NetworkDataT, NodeCount, NodeIndex, NodeMap,
    Recipient, Round, SessionId, Signed, SpawnHandle, TaskHandle,
};
use aleph_bft_mock::{
    Data, DataProvider, Hash64, Hasher64, Keychain, NetworkHook, Router, Spawner,
};
use futures::{channel::oneshot, StreamExt};
use log::{debug, error, trace};
use parking_lot::Mutex;
use serial_test::serial;
use std::{collections::HashMap, sync::Arc};

struct MaliciousMember<'a> {
    node_ix: NodeIndex,
    n_members: NodeCount,
    session_id: SessionId,
    forking_round: Round,
    keychain: &'a Keychain,
    network: Network,
    unit_store: HashMap<UnitCoord, SignedUnit<Hasher64, Data, Keychain>>,
}

impl<'a> MaliciousMember<'a> {
    fn new(
        keychain: &'a Keychain,
        network: Network,
        node_ix: NodeIndex,
        n_members: NodeCount,
        session_id: SessionId,
        forking_round: Round,
    ) -> Self {
        MaliciousMember {
            node_ix,
            n_members,
            session_id,
            forking_round,
            keychain,
            network,
            unit_store: HashMap::new(),
        }
    }

    fn unit_to_data(su: SignedUnit<Hasher64, Data, Keychain>) -> NetworkData {
        NetworkDataT(Units(UnitMessage::Unit(su.into())))
    }

    fn threshold(&self) -> NodeCount {
        self.n_members.consensus_threshold()
    }

    fn pick_parents(&self, round: Round) -> Option<NodeMap<Hash64>> {
        // Outputs a parent map if there are enough of them to create a new unit.
        let mut parents = NodeMap::with_size(self.n_members);
        if round == 0 {
            return Some(parents);
        }
        let mut count = 0;
        let our_prev_coord = UnitCoord::new(round - 1, self.node_ix);
        if !self.unit_store.contains_key(&our_prev_coord) {
            return None;
        }
        for i in 0..self.n_members.0 {
            let ix = NodeIndex(i);
            let coord = UnitCoord::new(round - 1, ix);
            if let Some(su) = self.unit_store.get(&coord) {
                let hash: <Hasher64 as Hasher>::Hash = su.as_signable().hash();
                parents.insert(ix, hash);
                count += 1;
            }
        }
        if NodeCount(count) >= self.threshold() {
            Some(parents)
        } else {
            None
        }
    }

    fn send_legit_unit(&mut self, su: SignedUnit<Hasher64, Data, Keychain>) {
        let message = Self::unit_to_data(su);
        self.network.send(message, Recipient::Everyone);
    }

    fn send_two_variants(
        &mut self,
        su0: SignedUnit<Hasher64, Data, Keychain>,
        su1: SignedUnit<Hasher64, Data, Keychain>,
    ) {
        // We send variant k \in {0,1} to each node with index = k (mod 2)
        // We also send to ourselves, it does not matter much.
        let message0 = Self::unit_to_data(su0);
        let message1 = Self::unit_to_data(su1);
        for ix in 0..self.n_members.0 {
            let node_ix = NodeIndex(ix);
            if ix % 2 == 0 {
                self.network
                    .send(message0.clone(), Recipient::Node(node_ix));
            } else {
                self.network
                    .send(message1.clone(), Recipient::Node(node_ix));
            }
        }
    }

    fn create_if_possible(&mut self, round: Round) -> bool {
        if let Some(parents) = self.pick_parents(round) {
            debug!(target: "malicious-member", "Creating a legit unit for round {}.", round);
            let mut node_with_parents = NodeMap::with_size(parents.size());
            if round > 0 {
                for (node_index, &hash) in parents.iter() {
                    node_with_parents.insert(node_index, (hash, round - 1));
                }
            }
            let control_hash = ControlHash::<Hasher64>::new(&node_with_parents);
            let new_preunit = PreUnit::<Hasher64>::new(self.node_ix, round, control_hash);
            if round != self.forking_round {
                let full_unit = FullUnit::new(new_preunit, Some(0), self.session_id);
                let signed_unit = Signed::sign(full_unit, self.keychain);
                self.on_unit_received(signed_unit.clone());
                self.send_legit_unit(signed_unit);
            } else {
                // FORKING HAPPENS HERE!
                debug!(target: "malicious-member", "Creating forks for round {}.", round);
                let mut variants = Vec::new();
                for data in 0u32..2u32 {
                    let full_unit = FullUnit::new(new_preunit.clone(), Some(data), self.session_id);
                    let signed = Signed::sign(full_unit, self.keychain);
                    variants.push(signed);
                }
                self.send_two_variants(variants[0].clone(), variants[1].clone());
            }
            return true;
        }
        false
    }

    fn on_unit_received(&mut self, su: SignedUnit<Hasher64, Data, Keychain>) {
        let full_unit = su.as_signable();
        let coord: UnitCoord = full_unit.coord();
        // We don't care if we overwrite something as long as we keep at least one version of a unit
        // at a given coord.
        self.unit_store.insert(coord, su);
    }

    fn on_network_data(&mut self, data: NetworkData) {
        // We ignore all messages except those carrying new units.
        if let NetworkDataT(Units(UnitMessage::Unit(unchecked))) = data {
            trace!(target: "malicious-member", "New unit received {:?}.", &unchecked);
            match unchecked.check(self.keychain) {
                Ok(su) => self.on_unit_received(su),
                Err(unchecked) => {
                    panic!("Wrong signature received {:?}.", &unchecked);
                }
            }
        }
        // else we stay silent
    }

    pub async fn run_session(mut self, mut exit: oneshot::Receiver<()>) {
        let mut round: Round = 0;
        loop {
            if self.create_if_possible(round) {
                round += 1;
            }
            tokio::select! {
                event = self.network.next_event() => match event {
                    Some(data) => {
                        self.on_network_data(data);
                    },
                    None => {
                        error!(target: "malicious-member", "Network message stream closed.");
                        break;
                    }
                },
                _ = &mut exit => break,
            }
        }
    }
}

fn spawn_malicious_member(
    spawner: Spawner,
    node_index: NodeIndex,
    n_members: NodeCount,
    round_to_fork: Round,
    network: Network,
) -> (oneshot::Sender<()>, TaskHandle) {
    let (exit_tx, exit_rx) = oneshot::channel();
    let member_task = async move {
        let keychain = Keychain::new(n_members, node_index);
        let session_id = 0u64;
        let lesniak = MaliciousMember::new(
            &keychain,
            network,
            node_index,
            n_members,
            session_id,
            round_to_fork,
        );
        lesniak.run_session(exit_rx).await;
    };
    let task_handle = spawner.spawn_essential("malicious-member", member_task);
    (exit_tx, task_handle)
}

#[derive(Clone)]
pub(crate) struct AlertHook {
    alerts_sent_by_connection: Arc<Mutex<HashMap<(NodeIndex, NodeIndex), usize>>>,
}

impl AlertHook {
    pub(crate) fn new() -> Self {
        AlertHook {
            alerts_sent_by_connection: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn count(&self, sender: NodeIndex, recipient: NodeIndex) -> usize {
        match self
            .alerts_sent_by_connection
            .lock()
            .get(&(sender, recipient))
        {
            Some(count) => *count,
            None => 0,
        }
    }
}

impl NetworkHook<NetworkData> for AlertHook {
    fn process_message(
        &mut self,
        data: NetworkData,
        sender: NodeIndex,
        recipient: NodeIndex,
    ) -> Vec<(NetworkData, NodeIndex, NodeIndex)> {
        use crate::{alerts::AlertMessage::*, network::NetworkDataInner::*};
        if let crate::NetworkData(Alert(ForkAlert(_))) = data {
            *self
                .alerts_sent_by_connection
                .lock()
                .entry((sender, recipient))
                .or_insert(0) += 1;
        }
        vec![(data, sender, recipient)]
    }
}

async fn honest_members_agree_on_batches_byzantine(
    n_members: NodeCount,
    n_honest: NodeCount,
    n_batches: usize,
) {
    init_log();
    let spawner = Spawner::new();
    let mut batch_rxs = Vec::new();
    let mut exits = Vec::new();
    let mut handles = Vec::new();
    let (mut net_hub, networks) = Router::new(n_members);

    let alert_hook = AlertHook::new();
    net_hub.add_hook(alert_hook.clone());

    spawner.spawn("network-hub", net_hub);

    for (network, _) in networks {
        let ix = network.index();
        let (exit_tx, handle) = if !n_honest.into_range().contains(&ix) {
            spawn_malicious_member(spawner, ix, n_members, 2, network)
        } else {
            let HonestMember {
                finalization_rx,
                exit_tx,
                handle,
                ..
            } = spawn_honest_member(spawner, ix, n_members, vec![], DataProvider::new(), network);
            batch_rxs.push(finalization_rx);
            (exit_tx, handle)
        };
        exits.push(exit_tx);
        handles.push(handle);
    }

    let mut batches = Vec::new();
    for mut rx in batch_rxs.drain(..) {
        let mut batches_per_ix = Vec::new();
        for _ in 0..n_batches {
            let batch = rx.next().await.unwrap();
            batches_per_ix.push(batch);
        }
        batches.push(batches_per_ix);
    }

    let expected_forkers = n_members - n_honest;
    for node_ix in n_honest.into_iterator().skip(1) {
        debug!(target: "byzantine-test", "batch {:?} received", node_ix);
        assert_eq!(batches[0], batches[node_ix.0]);
        for recipient_id in n_honest.into_iterator().skip(1) {
            if node_ix != recipient_id {
                let alerts_sent = alert_hook.count(node_ix, recipient_id);
                assert!(
                    alerts_sent >= expected_forkers.into(),
                    "Node {:?} sent only {:?} alerts to {:?}, expected at least {:?}.",
                    node_ix,
                    alerts_sent,
                    recipient_id,
                    expected_forkers
                );
            }
        }
    }
    for exit in exits {
        let _ = exit.send(());
    }
    for handle in handles {
        let _ = handle.await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn small_byzantine_one_forker() {
    honest_members_agree_on_batches_byzantine(4.into(), 3.into(), 5).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn small_byzantine_two_forkers() {
    honest_members_agree_on_batches_byzantine(7.into(), 5.into(), 5).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn medium_byzantine_ten_forkers() {
    honest_members_agree_on_batches_byzantine(31.into(), 21.into(), 5).await;
}
