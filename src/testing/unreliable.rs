use std::sync::Arc;

use crate::{
    member::UnitMessage,
    network::NetworkDataInner,
    testing::mock::{
        configure_network, init_log, spawn_honest_member, NetworkData, NetworkHook, Spawner,
    },
    NodeCount, NodeIndex, Round, SpawnHandle,
};
use futures::StreamExt;
use parking_lot::Mutex;

struct CorruptPacket {
    recipient: NodeIndex,
    sender: NodeIndex,
    creator: NodeIndex,
    round: Round,
}

impl NetworkHook for CorruptPacket {
    fn update_state(&mut self, data: &mut NetworkData, sender: NodeIndex, recipient: NodeIndex) {
        if self.recipient != recipient || self.sender != sender {
            return;
        }
        if let crate::NetworkData(NetworkDataInner::Units(UnitMessage::NewUnit(us))) = data {
            let full_unit = &mut us.as_signable_mut();
            if full_unit.round() == self.round && full_unit.creator() == self.creator {
                full_unit.set_round(0);
            }
        }
    }
}

struct NoteRequest {
    sender: NodeIndex,
    creator: NodeIndex,
    round: Round,
    requested: Arc<Mutex<bool>>,
}

impl NetworkHook for NoteRequest {
    fn update_state(&mut self, data: &mut NetworkData, sender: NodeIndex, _: NodeIndex) {
        use NetworkDataInner::Units;
        use UnitMessage::RequestCoord;
        if sender == self.sender {
            if let crate::NetworkData(Units(RequestCoord(_, co))) = data {
                if co.round() == self.round && co.creator() == self.creator {
                    *self.requested.lock() = true;
                }
            }
        }
    }
}

#[tokio::test]
async fn request_missing_coord() {
    init_log();

    let n_members = NodeCount(4);
    let censored_node = NodeIndex(0);
    let censoring_node = NodeIndex(1);
    let censoring_round = 5;

    let (mut net_hub, networks) = configure_network(n_members, 1.0);
    net_hub.add_hook(CorruptPacket {
        recipient: censored_node,
        sender: censoring_node,
        creator: censoring_node,
        round: censoring_round,
    });
    let requested = Arc::new(Mutex::new(false));
    net_hub.add_hook(NoteRequest {
        sender: censored_node,
        creator: censoring_node,
        round: censoring_round,
        requested: requested.clone(),
    });
    let spawner = Spawner::new();
    spawner.spawn("network-hub", net_hub);

    let mut exits = vec![];
    let mut batch_rxs = Vec::new();
    for network in networks {
        let ix = network.index();
        let (batch_rx, exit_tx) = spawn_honest_member(spawner.clone(), ix, n_members, network);
        batch_rxs.push(batch_rx);
        exits.push(exit_tx);
    }

    let n_batches = 10;
    let mut batches = vec![];
    for mut rx in batch_rxs.drain(..) {
        let mut batches_per_ix = vec![];
        for _ in 0..n_batches {
            let batch = rx.next().await.unwrap();
            batches_per_ix.push(batch);
        }
        batches.push(batches_per_ix);
    }
    for node_ix in n_members.into_iterator().skip(1) {
        assert_eq!(batches[0], batches[node_ix.0]);
    }

    assert!(*requested.lock())
}
