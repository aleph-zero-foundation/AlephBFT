use futures::StreamExt;

use crate::{
    testing::mock::{configure_network, init_log, spawn_honest_member, Spawner},
    SpawnHandle,
};

async fn honest_members_agree_on_batches(
    n_members: usize,
    n_alive: usize,
    n_batches: usize,
    network_reliability: f64,
) {
    init_log();
    let spawner = Spawner::new();
    let mut exits = vec![];
    let mut batch_rxs = Vec::new();
    let (net_hub, mut networks) = configure_network(n_members, network_reliability);
    spawner.spawn("network-hub", net_hub);

    for (ix, network) in networks.iter_mut().enumerate() {
        if ix < n_alive {
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

    for node_ix in 1..n_alive {
        assert_eq!(batches[0], batches[node_ix]);
    }
}

#[tokio::test]
async fn small_honest_all_alive() {
    honest_members_agree_on_batches(4, 4, 5, 1.0).await;
}

#[tokio::test]
async fn small_honest_one_crash() {
    honest_members_agree_on_batches(4, 3, 5, 1.0).await;
}

#[tokio::test]
async fn small_honest_one_crash_unreliable_network() {
    honest_members_agree_on_batches(4, 3, 5, 0.9).await;
}

#[tokio::test]
async fn medium_honest_all_alive() {
    honest_members_agree_on_batches(31, 31, 5, 1.0).await;
}

#[tokio::test]
async fn medium_honest_ten_crashes() {
    honest_members_agree_on_batches(31, 21, 5, 1.0).await;
}

#[tokio::test]
async fn medium_honest_ten_crashes_unreliable_network() {
    honest_members_agree_on_batches(31, 21, 5, 0.9).await;
}
