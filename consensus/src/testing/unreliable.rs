use crate::{
    network::NetworkDataInner,
    network::UnitMessage,
    testing::{init_log, spawn_honest_member, HonestMember, NetworkData},
    units::Unit,
    Index, NodeCount, NodeIndex, Round, Signed, SpawnHandle,
};
use aleph_bft_mock::{BadSigning, DataProvider, Keychain, NetworkHook, Router, Spawner};
use futures::StreamExt;
use parking_lot::Mutex;
use std::sync::Arc;

struct CorruptPacket {
    recipient: NodeIndex,
    sender: NodeIndex,
    creator: NodeIndex,
    round: Round,
}

impl NetworkHook<NetworkData> for CorruptPacket {
    fn process_message(
        &mut self,
        mut data: NetworkData,
        sender: NodeIndex,
        recipient: NodeIndex,
    ) -> Vec<(NetworkData, NodeIndex, NodeIndex)> {
        if self.recipient != recipient || self.sender != sender {
            return vec![(data, sender, recipient)];
        }
        if let crate::NetworkData(NetworkDataInner::Units(UnitMessage::Unit(us))) = &mut data {
            let full_unit = us.clone().into_signable();
            let index = full_unit.index();
            if full_unit.round() == self.round && full_unit.creator() == self.creator {
                let bad_keychain: BadSigning<Keychain> = Keychain::new(0.into(), index).into();
                *us = Signed::sign(full_unit, &bad_keychain).into();
            }
        }
        vec![(data, sender, recipient)]
    }
}

struct NoteRequest {
    sender: NodeIndex,
    creator: NodeIndex,
    round: Round,
    requested: Arc<Mutex<bool>>,
}

impl NetworkHook<NetworkData> for NoteRequest {
    fn process_message(
        &mut self,
        data: NetworkData,
        sender: NodeIndex,
        recipient: NodeIndex,
    ) -> Vec<(NetworkData, NodeIndex, NodeIndex)> {
        use NetworkDataInner::Units;
        use UnitMessage::CoordRequest;
        if sender == self.sender {
            if let crate::NetworkData(Units(CoordRequest(_, co))) = &data {
                if co.round() == self.round && co.creator() == self.creator {
                    *self.requested.lock() = true;
                }
            }
        }
        vec![(data, sender, recipient)]
    }
}

#[tokio::test]
async fn request_missing_coord() {
    init_log();

    let n_members = NodeCount(4);
    let censored_node = NodeIndex(0);
    let censoring_node = NodeIndex(1);
    let censoring_round = 5;

    let (mut net_hub, networks) = Router::new(n_members);
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

    let mut exits = Vec::new();
    let mut handles = Vec::new();
    let mut batch_rxs = Vec::new();
    for (network, _) in networks {
        let ix = network.index();
        let HonestMember {
            finalization_rx,
            exit_tx,
            handle,
            ..
        } = spawn_honest_member(spawner, ix, n_members, vec![], DataProvider::new(), network);
        batch_rxs.push(finalization_rx);
        exits.push(exit_tx);
        handles.push(handle);
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
    for exit in exits {
        let _ = exit.send(());
    }
    for handle in handles {
        let _ = handle.await;
    }

    assert!(*requested.lock())
}
