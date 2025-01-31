use std::{
    collections::{HashSet, VecDeque},
    time::{Duration, Instant},
};

use crate::{
    testing::{init_log, spawn_honest_member, HonestMember, NetworkData},
    NodeCount, NodeIndex, SpawnHandle,
};
use aleph_bft_mock::{DataProvider, NetworkHook, Router, Spawner};
use futures::StreamExt;
use log::info;

struct Latency {
    who: NodeIndex,
    buffer: VecDeque<(Instant, (NetworkData, NodeIndex, NodeIndex))>,
}

const LATENCY: Duration = Duration::from_millis(300);

impl Latency {
    pub fn new(who: NodeIndex) -> Self {
        Latency {
            who,
            buffer: VecDeque::new(),
        }
    }

    fn add_message(
        &mut self,
        data: NetworkData,
        sender: NodeIndex,
        recipient: NodeIndex,
    ) -> Vec<(NetworkData, NodeIndex, NodeIndex)> {
        match sender == self.who || recipient == self.who {
            true => {
                self.buffer
                    .push_back((Instant::now(), (data, sender, recipient)));
                Vec::new()
            }
            false => vec![(data, sender, recipient)],
        }
    }

    fn messages_to_send(&mut self) -> Vec<(NetworkData, NodeIndex, NodeIndex)> {
        let mut result = Vec::new();
        while !self.buffer.is_empty() {
            let (when, msg) = self
                .buffer
                .pop_front()
                .expect("just checked it is not empty");
            if Instant::now().duration_since(when) < LATENCY {
                self.buffer.push_front((when, msg));
                break;
            }
            result.push(msg);
        }
        result
    }
}

impl NetworkHook<NetworkData> for Latency {
    fn process_message(
        &mut self,
        data: NetworkData,
        sender: NodeIndex,
        recipient: NodeIndex,
    ) -> Vec<(NetworkData, NodeIndex, NodeIndex)> {
        let mut result = self.add_message(data, sender, recipient);
        result.append(&mut self.messages_to_send());
        result
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_finalized() {
    let n_members = NodeCount(7);
    let australian = NodeIndex(0);
    init_log();
    let spawner = Spawner::new();
    let mut batch_rxs = Vec::new();
    let mut exits = Vec::new();
    let mut handles = Vec::new();
    let (mut net_hub, networks) = Router::new(n_members);

    net_hub.add_hook(Latency::new(australian));

    spawner.spawn("network-hub", net_hub);

    for (network, _) in networks {
        let ix = network.index();
        let HonestMember {
            finalization_rx,
            exit_tx,
            handle,
            ..
        } = spawn_honest_member(
            spawner,
            ix,
            n_members,
            vec![],
            DataProvider::new_range(ix.0 * 50, (ix.0 + 1) * 50),
            network,
        );
        batch_rxs.push(finalization_rx);
        exits.push(exit_tx);
        handles.push(handle);
    }
    let to_finalize: HashSet<u32> = (0..((n_members.0) * 50))
        .map(|number| number as u32)
        .collect();

    for mut rx in batch_rxs.drain(..) {
        let mut to_finalize_local = to_finalize.clone();
        while !to_finalize_local.is_empty() {
            let number = rx.next().await.unwrap();
            info!("finalizing {}", number);
            assert!(to_finalize_local.remove(&number));
        }
        info!("finished one node");
    }

    for exit in exits {
        let _ = exit.send(());
    }
    for handle in handles {
        let _ = handle.await;
    }
}
