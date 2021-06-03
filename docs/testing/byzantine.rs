use log::{debug, error};

use futures::{channel::oneshot, FutureExt, StreamExt};

use std::collections::HashMap;

use crate::{
    network::NetworkDataInner::Units,
    nodes::NodeMap,
    signed::Signed,
    testing::mock::{
        configure_network, init_log, spawn_honest_member, AlertHook, Data, Hasher64, KeyBox,
        Network, Signature, Spawner,
    },
    units::{ControlHash, FullUnit, PreUnit, SignedUnit, UnitCoord},
    Hasher, Network as NetworkT, NetworkData as NetworkDataT, NodeCount, NodeIndex, SessionId,
    SpawnHandle,
};

type Hash64 = <Hasher64 as Hasher>::Hash;

use crate::member::UnitMessage::NewUnit;

type NetworkData = NetworkDataT<Hasher64, Data, Signature>;

struct MaliciousMember<'a> {
    node_ix: NodeIndex,
    n_members: NodeCount,
    threshold: NodeCount,
    session_id: SessionId,
    forking_round: usize,
    round_in_progress: usize,
    keybox: &'a KeyBox,
    network: Network,
    unit_store: HashMap<UnitCoord, SignedUnit<'a, Hasher64, Data, KeyBox>>,
}

impl<'a> MaliciousMember<'a> {
    fn new(
        keybox: &'a KeyBox,
        network: Network,
        node_ix: NodeIndex,
        n_members: NodeCount,
        session_id: SessionId,
        forking_round: usize,
    ) -> Self {
        let threshold = (n_members * 2) / 3 + NodeCount(1);
        MaliciousMember {
            node_ix,
            n_members,
            threshold,
            session_id,
            forking_round,
            round_in_progress: 0,
            keybox,
            network,
            unit_store: HashMap::new(),
        }
    }

    fn unit_to_data(su: SignedUnit<'a, Hasher64, Data, KeyBox>) -> NetworkData {
        NetworkDataT(Units(NewUnit(su.into())))
    }

    fn pick_parents(&self, round: usize) -> Option<NodeMap<Option<Hash64>>> {
        // Outputs a parent map if there are enough of them to create a new unit.
        let mut parents = NodeMap::new_with_len(self.n_members);
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
                parents[ix] = Some(hash);
                count += 1;
            }
        }
        if NodeCount(count) >= self.threshold {
            Some(parents)
        } else {
            None
        }
    }

    fn send_legit_unit(&mut self, su: SignedUnit<'a, Hasher64, Data, KeyBox>) {
        let message = Self::unit_to_data(su);
        let _ = self.network.broadcast(message);
    }

    fn send_two_variants(
        &mut self,
        su0: SignedUnit<'a, Hasher64, Data, KeyBox>,
        su1: SignedUnit<'a, Hasher64, Data, KeyBox>,
    ) {
        // We send variant k \in {0,1} to each node with index = k (mod 2)
        // We also send to ourselves, it does not matter much.
        let message0 = Self::unit_to_data(su0);
        let message1 = Self::unit_to_data(su1);
        for ix in 0..self.n_members.0 {
            let node_ix = NodeIndex(ix);
            let _ = if ix % 2 == 0 {
                self.network.send(message0.clone(), node_ix)
            } else {
                self.network.send(message1.clone(), node_ix)
            };
        }
    }

    fn create_if_possible(&mut self) {
        if let Some(parents) = self.pick_parents(self.round_in_progress) {
            debug!(target: "malicious-member", "Creating a legit unit for round {}.", self.round_in_progress);
            let round = self.round_in_progress;
            let control_hash = ControlHash::<Hasher64>::new(&parents);
            let new_preunit = PreUnit::<Hasher64>::new(self.node_ix, round, control_hash);
            let coord = UnitCoord::new(round, self.node_ix);
            if round != self.forking_round {
                let data = Data::new(coord, 0);
                let full_unit = FullUnit::new(new_preunit, data, self.session_id);
                let signed_unit = Signed::sign(full_unit, self.keybox);
                self.on_unit_received(signed_unit.clone());
                self.send_legit_unit(signed_unit);
            } else {
                // FORKING HAPPENS HERE!
                debug!(target: "malicious-member", "Creating forks for round {}.", self.round_in_progress);
                let variants: Vec<SignedUnit<Hasher64, Data, KeyBox>> = (0u32..2u32)
                    .map(|var| {
                        let data = Data::new(coord, var);
                        let full_unit = FullUnit::new(new_preunit.clone(), data, self.session_id);
                        Signed::sign(full_unit, self.keybox)
                    })
                    .collect();
                self.send_two_variants(variants[0].clone(), variants[1].clone());
            }
            self.round_in_progress += 1;
        }
    }

    fn on_unit_received(&mut self, su: SignedUnit<'a, Hasher64, Data, KeyBox>) {
        let full_unit = su.as_signable();
        let coord: UnitCoord = full_unit.coord();
        // We don't care if we overwrite something as long as we keep at least one version of a unit
        // at a given coord.
        self.unit_store.insert(coord, su);
    }

    fn on_network_data(&mut self, data: NetworkData) {
        // We ignore all messages except those carrying new units.
        if let NetworkDataT(Units(NewUnit(unchecked))) = data {
            debug!(target: "malicious-member", "New unit received {:?}.", &unchecked);
            match unchecked.check(self.keybox) {
                Ok(su) => self.on_unit_received(su),
                Err(unchecked) => {
                    panic!("Wrong signature received {:?}.", &unchecked);
                }
            }
        }
        // else we stay silent
    }

    pub async fn run_session(mut self, exit: oneshot::Receiver<()>) {
        let mut exit = exit.into_stream();
        self.create_if_possible();
        loop {
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
                _ = exit.next() => break,
            }
            self.create_if_possible();
        }
    }
}

fn spawn_malicious_member(
    spawner: Spawner,
    ix: usize,
    n_members: usize,
    round_to_fork: usize,
    network: Network,
) -> oneshot::Sender<()> {
    let node_index = NodeIndex(ix);
    let (exit_tx, exit_rx) = oneshot::channel();
    let member_task = async move {
        let keybox = KeyBox::new(node_index);
        let session_id = 0u64;
        let lesniak = MaliciousMember::new(
            &keybox,
            network,
            node_index,
            NodeCount(n_members),
            session_id,
            round_to_fork,
        );
        lesniak.run_session(exit_rx).await;
    };
    spawner.spawn("malicious-member", member_task);
    exit_tx
}

async fn honest_members_agree_on_batches_byzantine(
    n_members: usize,
    n_honest: usize,
    n_batches: usize,
    network_reliability: f64,
) {
    init_log();
    let spawner = Spawner::new();
    let mut exits = vec![];
    let mut batch_rxs = Vec::new();
    let (net_hub, mut networks) = configure_network(n_members, network_reliability);

    let alert_hook = AlertHook::new();
    net_hub.add_hook(alert_hook.clone());

    spawner.spawn("network-hub", net_hub);

    for (ix, network) in networks.iter_mut().enumerate() {
        if ix >= n_honest {
            let exit_tx =
                spawn_malicious_member(spawner.clone(), ix, n_members, 2, network.take().unwrap());
            exits.push(exit_tx);
        } else {
            let (batch_rx, exit_tx) =
                spawn_honest_member(spawner.clone(), ix, n_members, network.take().unwrap());
            batch_rxs.push(batch_rx);
            exits.push(exit_tx);
        }
    }

    let mut batches = vec![];
    for mut rx in batch_rxs.drain(..) {
        let mut batches_per_ix = vec![];
        for _ in 0..n_batches {
            let batch = rx.next().await.unwrap();
            batches_per_ix.push(batch);
        }
        batches.push(batches_per_ix);
    }

    for node_ix in 1..n_honest {
        debug!("batch {} received", node_ix);
        assert_eq!(batches[0], batches[node_ix]);
    }

    // each honest node sends alert once for each malicious member (to each one except himself)
    assert_eq!(
        alert_hook.count(),
        (n_members - n_honest) * n_honest * (n_members - 1)
    );
}

#[tokio::test(max_threads = 1)]
async fn small_byzantine_one_forker() {
    honest_members_agree_on_batches_byzantine(4, 3, 5, 1.0).await;
}

#[tokio::test(max_threads = 1)]
async fn small_byzantine_two_forkers() {
    honest_members_agree_on_batches_byzantine(7, 5, 5, 1.0).await;
}
