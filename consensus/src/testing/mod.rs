#![cfg(test)]
mod alerts;
mod byzantine;
mod consensus;
mod crash;
mod crash_recovery;
mod creation;
mod dag;
mod unreliable;

use crate::{
    exponential_slowdown, run_session, Config, DelayConfig, LocalIO, Network as NetworkT,
    NodeCount, NodeIndex, SpawnHandle, TaskHandle,
};
use aleph_bft_mock::{
    Data, DataProvider, FinalizationHandler, Hasher64, Keychain, Loader, Network as MockNetwork,
    PartialMultisignature, ReconnectSender as ReconnectSenderGeneric, Saver, Signature, Spawner,
};
use futures::channel::{mpsc::UnboundedReceiver, oneshot};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};

pub type NetworkData = crate::NetworkData<Hasher64, Data, Signature, PartialMultisignature>;

pub type Network = MockNetwork<NetworkData>;
pub type ReconnectSender = ReconnectSenderGeneric<NetworkData>;

pub fn init_log() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .is_test(true)
        .try_init();
}

pub fn complete_oneshot<T: std::fmt::Debug>(t: T) -> oneshot::Receiver<T> {
    let (tx, rx) = oneshot::channel();
    tx.send(t).unwrap();
    rx
}

pub fn gen_config(node_ix: NodeIndex, n_members: NodeCount) -> Config {
    let delay_config = DelayConfig {
        tick_interval: Duration::from_millis(5),
        requests_interval: Duration::from_millis(50),
        unit_broadcast_delay: Arc::new(|t| exponential_slowdown(t, 100.0, 1, 3.0)),
        //100, 100, 300, 900, 2700, ...
        unit_creation_delay: Arc::new(|t| exponential_slowdown(t, 50.0, usize::MAX, 1.000)),
        //50, 50, 50, 50, ...
    };
    Config {
        node_ix,
        session_id: 0,
        n_members,
        delay_config,
        max_round: 5000,
    }
}

pub fn spawn_honest_member(
    spawner: Spawner,
    node_index: NodeIndex,
    n_members: NodeCount,
    units: Vec<u8>,
    network: impl 'static + NetworkT<NetworkData>,
) -> (
    UnboundedReceiver<Data>,
    Arc<Mutex<Vec<u8>>>,
    oneshot::Sender<()>,
    TaskHandle,
) {
    let data_provider = DataProvider::new();
    let (finalization_handler, finalization_rx) = FinalizationHandler::new();
    let config = gen_config(node_index, n_members);
    let (exit_tx, exit_rx) = oneshot::channel();
    let spawner_inner = spawner.clone();
    let unit_loader = Loader::new(units);
    let saved_state = Arc::new(Mutex::new(vec![]));
    let unit_saver = Saver::new(saved_state.clone());
    let local_io = LocalIO::new(data_provider, finalization_handler, unit_saver, unit_loader);
    let member_task = async move {
        let keybox = Keychain::new(n_members, node_index);
        run_session(
            config,
            local_io,
            network,
            keybox,
            spawner_inner.clone(),
            exit_rx,
        )
        .await
    };
    let handle = spawner.spawn_essential("member", member_task);
    (finalization_rx, saved_state, exit_tx, handle)
}
