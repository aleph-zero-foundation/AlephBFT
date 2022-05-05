use crate::{
    testing::{init_log, spawn_honest_member, Network, ReconnectSender},
    units::UncheckedSignedUnit,
    NodeCount, NodeIndex, SpawnHandle, TaskHandle,
};
use aleph_bft_mock::{Data, Hasher64, Router, Signature, Spawner};
use codec::Decode;
use futures::{
    channel::{mpsc, oneshot},
    StreamExt,
};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};

struct NodeData {
    batch_rx: mpsc::UnboundedReceiver<Data>,
    exit_tx: oneshot::Sender<()>,
    reconnect_tx: mpsc::UnboundedSender<(NodeIndex, oneshot::Sender<Network>)>,
    handle: TaskHandle,
    saved_units: Arc<Mutex<Vec<u8>>>,
    batches: Vec<Data>,
    batches_from_reconnected: Vec<Data>,
}

impl NodeData {
    async fn receive(&mut self) {
        let batch = self.batch_rx.next().await.unwrap();
        self.batches.push(batch);
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
            let (batch_rx, saved_units, exit_tx, handle) =
                spawn_honest_member(spawner.clone(), ix, n_members, vec![], network);
            (
                ix,
                NodeData {
                    batch_rx,
                    exit_tx,
                    reconnect_tx,
                    handle,
                    saved_units,
                    batches: vec![],
                    batches_from_reconnected: vec![],
                },
            )
        })
        .collect()
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
        let (batch_rx, saved_units, exit_tx, handle) = spawn_honest_member(
            spawner.clone(),
            *node_id,
            n_members,
            saved_units.clone(),
            network,
        );
        reconnected_nodes.push((
            *node_id,
            NodeData {
                batch_rx,
                exit_tx,
                reconnect_tx: reconnect_tx.clone(),
                handle,
                saved_units,
                batches: vec![],
                batches_from_reconnected: vec![],
            },
        ));
    }
    reconnected_nodes
}

async fn crashed_nodes_recover(
    n_members: NodeCount,
    n_kill: NodeCount,
    n_batches: usize,
    n_batches_upper_bound: usize,
    _network_reliability: f64,
) {
    init_log();
    let spawner = Spawner::new();
    let (net_hub, networks) = Router::new(n_members, 1.0);
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

    for (node_id, data) in reconnect_nodes(&spawner, n_members, &killed).await {
        node_data.insert(node_id, data);
    }

    for (ix, data) in node_data.iter_mut() {
        if n_kill.into_range().contains(ix) {
            for _ in 0..n_batches * n_members.0 {
                data.receive().await;
            }
        }
    }

    let expected_batches = &node_data
        .get(&NodeIndex(0))
        .expect("should contain node")
        .batches;
    for (ix, data) in node_data.iter().skip(1) {
        assert_eq!((ix, expected_batches), (ix, &data.batches));
    }

    for data in node_data.values_mut() {
        for _ in 0..n_batches_upper_bound * n_members.0 {
            let batch = data.batch_rx.next().await.unwrap();
            if batch <= n_batches as u32 {
                data.batches_from_reconnected.push(batch);
            }
            data.batches.push(batch);
            if data.batches_from_reconnected.len() == n_kill.0 * n_batches {
                break;
            }
        }
    }

    let expected_batches = &node_data
        .get(&NodeIndex(0))
        .expect("should contain node")
        .batches;
    let expected_batches_from_reconnected = &node_data
        .get(&NodeIndex(0))
        .expect("should contain node")
        .batches_from_reconnected;
    for (ix, data) in node_data.iter().skip(1) {
        assert_eq!((ix, expected_batches), (ix, &data.batches));
        assert_eq!(
            (ix, n_kill.0 * n_batches),
            (ix, data.batches_from_reconnected.len())
        );
        assert_eq!(
            (ix, expected_batches_from_reconnected),
            (ix, &data.batches_from_reconnected)
        );
    }

    for (ix, (_, saved_units_before)) in killed {
        let buf = &mut &saved_units_before[..];
        let mut counter = 0;
        while !buf.is_empty() {
            let u = UncheckedSignedUnit::<Hasher64, Data, Signature>::decode(buf)
                .expect("should be correct");
            let su = u.as_signable();
            let coord = su.coord();
            assert_eq!(coord.creator(), ix);
            assert_eq!(coord.round(), counter);
            counter += 1;
        }
        let NodeData { saved_units, .. } = node_data.get(&ix).expect("should contain killed node");

        let buf = &mut &saved_units.lock()[..];
        while !buf.is_empty() {
            let u = UncheckedSignedUnit::<Hasher64, Data, Signature>::decode(buf)
                .expect("should be correct");
            let su = u.as_signable();
            let coord = su.coord();
            assert_eq!(coord.creator(), ix);
            assert_eq!(coord.round(), counter,);
            counter += 1;
        }
    }

    for (_, data) in node_data.drain() {
        let _ = data.exit_tx.send(());
        let _ = data.handle.await;
    }
}

#[tokio::test]
async fn saves_created_units() {
    init_log();
    let n_batches = 2;
    let n_members = NodeCount(4);
    let spawner = Spawner::new();
    let (net_hub, networks) = Router::new(n_members, 1.0);
    spawner.spawn("network-hub", net_hub);

    let mut node_data = connect_nodes(&spawner, n_members, networks);

    for data in node_data.values_mut() {
        for _ in 0..n_batches * n_members.0 {
            data.receive().await;
        }
    }

    let mut killed = HashMap::new();

    for (i, data) in node_data.drain() {
        let NodeData {
            exit_tx,
            handle,
            saved_units,
            ..
        } = data;
        let _ = exit_tx.send(());
        let _ = handle.await;
        killed.insert(i, saved_units.lock().clone());
    }

    for (ix, saved_units) in killed {
        let buf = &mut &saved_units[..];
        let mut counter = 0;
        while !buf.is_empty() {
            let u = UncheckedSignedUnit::<Hasher64, Data, Signature>::decode(buf)
                .expect("should be correct");
            let su = u.as_signable();
            let coord = su.coord();
            assert_eq!(coord.creator(), ix);
            assert_eq!(coord.round(), counter);
            counter += 1;
        }
    }
}

#[tokio::test]
async fn small_node_crash_recovery_one_killed() {
    crashed_nodes_recover(6.into(), 1.into(), 2, 500, 1.0).await;
}

#[tokio::test]
async fn small_node_crash_recovery_three_killed() {
    crashed_nodes_recover(10.into(), 3.into(), 2, 500, 1.0).await;
}

#[tokio::test]
async fn medium_node_crash_recovery_nine_killed() {
    crashed_nodes_recover(28.into(), 9.into(), 2, 500, 1.0).await;
}
