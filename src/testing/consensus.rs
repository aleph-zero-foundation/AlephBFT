use crate::{
    consensus,
    runway::{NotificationIn, NotificationOut},
    testing::mock::{gen_config, Hasher64, HonestHub, Spawner},
    units::{ControlHash, PreUnit, Unit},
    Hasher, NodeIndex, SpawnHandle,
};
use futures::{
    channel::{mpsc, oneshot},
    sink::SinkExt,
    stream::StreamExt,
};
use log::trace;

fn init_log() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .is_test(true)
        .try_init();
}

#[tokio::test]
async fn agree_on_first_batch() {
    init_log();
    let n_members: usize = 16;
    let mut hub = HonestHub::new(n_members);

    let mut exits = vec![];
    let mut batch_rxs = vec![];
    let spawner = Spawner::new();

    let mut handles = vec![];

    for node_ix in 0..n_members {
        let (tx, rx) = hub.connect(NodeIndex(node_ix));
        let conf = gen_config(NodeIndex(node_ix), n_members.into());
        let (exit_tx, exit_rx) = oneshot::channel();
        exits.push(exit_tx);
        let (batch_tx, batch_rx) = mpsc::unbounded();
        batch_rxs.push(batch_rx);
        handles.push(spawner.spawn_essential(
            "consensus",
            consensus::run(conf, rx, tx, batch_tx, spawner.clone(), exit_rx),
        ));
    }

    spawner.spawn("hub", hub);

    let mut batches = vec![];
    for mut rx in batch_rxs.drain(..) {
        let batch = rx.next().await.unwrap();
        assert!(!batch.is_empty());
        batches.push(batch);
    }

    for node_ix in 1..n_members {
        assert_eq!(batches[0], batches[node_ix]);
    }
}

#[tokio::test]
async fn catches_wrong_control_hash() {
    init_log();
    let n_nodes = 4;
    let spawner = Spawner::new();
    let node_ix = 0;
    let (mut tx_in, rx_in) = mpsc::unbounded();
    let (tx_out, mut rx_out) = mpsc::unbounded();

    let conf = gen_config(NodeIndex(node_ix), n_nodes.into());
    let (exit_tx, exit_rx) = oneshot::channel();
    let (batch_tx, _batch_rx) = mpsc::unbounded();

    let consensus_handle = spawner.spawn_essential(
        "consensus",
        consensus::run(conf, rx_in, tx_out, batch_tx, spawner.clone(), exit_rx),
    );
    let control_hash = ControlHash::new(&(vec![None; n_nodes]).into());
    let bad_pu = PreUnit::<Hasher64>::new(1.into(), 0, control_hash);
    let bad_control_hash: <Hasher64 as Hasher>::Hash = [0, 1, 0, 1, 0, 1, 0, 1];
    assert!(
        bad_control_hash != bad_pu.control_hash().combined_hash,
        "Bad control hash cannot be the correct one."
    );
    let mut control_hash = bad_pu.control_hash().clone();
    control_hash.combined_hash = bad_control_hash;
    let bad_pu = PreUnit::new(bad_pu.creator(), bad_pu.round(), control_hash);
    let bad_hash: <Hasher64 as Hasher>::Hash = [0, 1, 0, 1, 0, 1, 0, 1];
    let bad_unit = Unit::new(bad_pu, bad_hash);
    let _ = tx_in.send(NotificationIn::NewUnits(vec![bad_unit])).await;
    loop {
        let notification = rx_out.next().await.unwrap();
        trace!(target: "consensus-test", "notification {:?}", notification);
        if let NotificationOut::WrongControlHash(h) = notification {
            assert_eq!(h, bad_hash, "Expected notification for our bad unit.");
            break;
        }
    }

    let _ = exit_tx.send(());

    consensus_handle.await.expect("The node is honest.");
}
