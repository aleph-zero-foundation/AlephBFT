use crate::{
    testing::{init_log, spawn_honest_member, HonestMember, Network, ReconnectSender},
    units::{UncheckedSignedUnit, Unit, UnitCoord},
    NodeCount, NodeIndex, SpawnHandle, TaskHandle,
};
use aleph_bft_mock::{Data, DataProvider, Hasher64, Router, Signature, Spawner};
use codec::Decode;
use futures::{
    channel::{mpsc, oneshot},
    StreamExt,
};
use parking_lot::Mutex;
use serial_test::serial;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

struct NodeData {
    batch_rx: mpsc::UnboundedReceiver<Data>,
    exit_tx: oneshot::Sender<()>,
    reconnect_tx: mpsc::UnboundedSender<(NodeIndex, oneshot::Sender<Network>)>,
    handle: TaskHandle,
    saved_units: Arc<Mutex<Vec<u8>>>,
    batches: Vec<Data>,
}

impl NodeData {
    /// Receives the next unit finalized by this node if one is ready and appends it to `batches`.
    /// Returns `Some(batch)` if a unit is ready, otherwise `None`.
    fn try_receive(&mut self) -> Option<Data> {
        match self.batch_rx.try_next() {
            Ok(Some(batch)) => {
                self.batches.push(batch);
                Some(batch)
            }
            _ => None,
        }
    }

    /// Receives the next unit finalized by this node. Appends it to `batches` and returns it.
    async fn receive(&mut self) -> Data {
        let batch = self.batch_rx.next().await.unwrap();
        self.batches.push(batch);
        batch
    }

    /// Kills this node.
    async fn kill(self) {
        let _ = self.exit_tx.send(());
        let _ = self.handle.await;
    }
}

fn connect_nodes(
    spawner: &Spawner,
    n_members: NodeCount,
    networks: Vec<(Network, ReconnectSender)>,
) -> HashMap<NodeIndex, NodeData> {
    networks
        .into_iter()
        .map(|(network, reconnect_tx)| {
            let ix = network.index();
            let HonestMember {
                finalization_rx,
                saved_state,
                exit_tx,
                handle,
            } = spawn_honest_member(
                *spawner,
                ix,
                n_members,
                vec![],
                DataProvider::new(),
                network,
            );
            (
                ix,
                NodeData {
                    batch_rx: finalization_rx,
                    exit_tx,
                    reconnect_tx,
                    handle,
                    saved_units: saved_state,
                    batches: vec![],
                },
            )
        })
        .collect()
}

/// Kill all of the nodes in `node_data`.
async fn shutdown(mut node_data: HashMap<NodeIndex, NodeData>) {
    for (_, data) in node_data.drain() {
        data.kill().await;
    }
}

async fn reconnect_nodes(
    spawner: &Spawner,
    n_members: NodeCount,
    killed: &HashMap<NodeIndex, (ReconnectSender, Vec<u8>)>,
) -> Vec<(NodeIndex, NodeData)> {
    let mut reconnected_nodes = Vec::new();

    for (node_id, (reconnect_tx, saved_units)) in killed.iter() {
        let (tx, rx) = oneshot::channel();
        reconnect_tx
            .unbounded_send((*node_id, tx))
            .expect("receiver should exist");

        let network = rx.await.expect("channel should be open");
        let HonestMember {
            finalization_rx,
            saved_state,
            exit_tx,
            handle,
        } = spawn_honest_member(
            *spawner,
            *node_id,
            n_members,
            saved_units.clone(),
            DataProvider::new(),
            network,
        );
        reconnected_nodes.push((
            *node_id,
            NodeData {
                batch_rx: finalization_rx,
                exit_tx,
                reconnect_tx: reconnect_tx.clone(),
                handle,
                saved_units: saved_state,
                batches: vec![],
            },
        ));
    }
    reconnected_nodes
}

fn verify_backup(buf: &mut &[u8]) -> HashSet<UnitCoord> {
    let mut already_saved = HashSet::new();

    while !buf.is_empty() {
        let unit = <UncheckedSignedUnit<Hasher64, Data, Signature>>::decode(buf).unwrap();
        let full_unit = unit.as_signable();
        let coord = full_unit.coord();
        let control_hash = &full_unit.as_pre_unit().control_hash();

        for parent_coord in control_hash.parents() {
            assert!(already_saved.contains(&parent_coord));
        }

        already_saved.insert(coord);
    }

    already_saved
}

/// Tests that finalization continues after some nodes restart.
///
/// Performs the following steps:
///
/// 1. Spawns `n_members` nodes.
/// 2. Waits for at least `n_members * n_batches` items to be finalized.
/// 3. Kills _more than f nodes_ (where `n_members = 3 * f + 1`). This should cause finalization to
///    stop.
/// 4. Notes the list of finalized items.
/// 5. Restarts the killed nodes.
/// 6. Checks that (after some time) at least twice as many items are finalized and that all nodes
///    finalized the same items.
///
/// The reason it kills more than f nodes is that we want to check that (at least some of) the
/// restarted nodes take part in finalization. As it stands, the system does not guarantee that a
/// restarted node will ever catch up, so if less than `f` nodes are restarted, the restarted nodes
/// might never be actually needed to finalize anything.
async fn crashed_nodes_recover(n_members: NodeCount, n_batches: usize) {
    init_log();

    let n_kill = (n_members - n_members.consensus_threshold()) + 1.into();
    let spawner = Spawner::new();
    let (net_hub, networks) = Router::new(n_members);
    spawner.spawn("network-hub", net_hub);

    let mut node_data = connect_nodes(&spawner, n_members, networks);

    for data in node_data.values_mut() {
        for _ in 0..n_batches * n_members.0 {
            data.receive().await;
        }
    }

    let mut killed = HashMap::new();

    for i in 0..n_kill.0 {
        let NodeData {
            exit_tx,
            reconnect_tx,
            handle,
            saved_units,
            ..
        } = node_data
            .remove(&NodeIndex(i))
            .expect("should contain killed node");
        let _ = exit_tx.send(());
        let _ = handle.await;
        killed.insert(NodeIndex(i), (reconnect_tx, (*saved_units.lock()).clone()));
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    for (_, data) in node_data.iter_mut() {
        while data.try_receive().is_some() {}
    }

    let finalized_before_kill = node_data
        .values()
        .map(|x| &x.batches)
        .max_by(|x, y| x.len().cmp(&y.len()))
        .unwrap()
        .clone();

    for (node_id, data) in reconnect_nodes(&spawner, n_members, &killed).await {
        node_data.insert(node_id, data);
    }

    for (_, data) in node_data.iter_mut() {
        while data.batches.len() < 2 * finalized_before_kill.len() {
            data.receive().await;
        }
    }

    let expected_batches = &node_data[&NodeIndex(0)].batches;
    for (_, data) in node_data.iter() {
        assert_eq!(expected_batches, &data.batches);
    }

    for (ix, (_, saved_units_before)) in killed {
        let saved_before_coords = verify_backup(&mut &saved_units_before[..]);
        let NodeData { saved_units, .. } = node_data.get(&ix).expect("should contain killed node");

        let saved_after_coords = verify_backup(&mut &saved_units.lock()[..]);
        assert!(saved_before_coords.is_subset(&saved_after_coords));
    }

    shutdown(node_data).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn saves_units_properly() {
    init_log();
    let n_batches = 2;
    let n_members = NodeCount(4);
    let spawner = Spawner::new();
    let (net_hub, networks) = Router::new(n_members);
    spawner.spawn("network-hub", net_hub);

    let mut node_data = connect_nodes(&spawner, n_members, networks);

    for data in node_data.values_mut() {
        for _ in 0..n_batches * n_members.0 {
            data.receive().await;
        }
    }

    let mut killed = HashMap::new();

    for (i, data) in node_data.drain() {
        let saved_units = data.saved_units.lock().clone();
        data.kill().await;
        killed.insert(i, saved_units);
    }

    for (_, saved_units) in killed {
        let _ = verify_backup(&mut &saved_units[..]);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn small_node_crash_recovery_small() {
    crashed_nodes_recover(7.into(), 2).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn small_node_crash_recovery_medium() {
    crashed_nodes_recover(10.into(), 2).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn medium_node_crash_recovery_large() {
    crashed_nodes_recover(28.into(), 2).await;
}
