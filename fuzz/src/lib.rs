use aleph_bft::{
    create_config, run_session, Config, DelayConfig, LocalIO, Network as NetworkT, NetworkData,
    NodeCount, NodeIndex, Recipient, SpawnHandle, TaskHandle, Terminator,
};
use aleph_bft_mock::{
    Data, DataProvider, FinalizationHandler, Hasher64, Keychain, Loader, NetworkHook,
    PartialMultisignature, Router, Saver, Signature,
};
use codec::{Decode, Encode, IoReader};
use futures::{
    channel::{oneshot, oneshot::Receiver},
    task::{Context, Poll},
    Future, StreamExt,
};
use futures_timer::Delay;
use log::{error, info};
use parking_lot::Mutex;
use std::{
    io::{BufRead, BufReader, Read, Result as IOResult, Write},
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use tokio::{
    runtime::{Builder, Runtime},
    task::yield_now,
};

#[derive(Clone)]
pub struct MockSpawner {
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl SpawnHandle for MockSpawner {
    fn spawn(&self, _name: &str, task: impl Future<Output = ()> + Send + 'static) {
        self.handles.lock().push(tokio::spawn(task))
    }

    fn spawn_essential(
        &self,
        _: &str,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> TaskHandle {
        let (res_tx, res_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            task.await;
            res_tx.send(()).expect("We own the rx.");
        });
        self.handles.lock().push(task);
        Box::pin(async move { res_rx.await.map_err(|_| ()) })
    }
}

//This code is used only for testing.
#[allow(clippy::await_holding_lock)]
impl MockSpawner {
    pub async fn wait(&self) {
        for h in self.handles.lock().iter_mut() {
            let _ = h.await;
        }
    }
    pub fn new() -> Self {
        MockSpawner {
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Default for MockSpawner {
    fn default() -> Self {
        MockSpawner::new()
    }
}

#[derive(Clone)]
pub struct Spawner {
    spawner: Arc<MockSpawner>,
    idle_mx: Arc<Mutex<()>>,
    wake_flag: Arc<AtomicBool>,
    delay: Duration,
}

#[derive(Clone)]
struct SpawnFuture<T> {
    task: Pin<Box<T>>,
    wake_flag: Arc<AtomicBool>,
}

impl<T> SpawnFuture<T> {
    fn new(task: T, wake_flag: Arc<AtomicBool>) -> Self {
        SpawnFuture {
            task: Box::pin(task),
            wake_flag,
        }
    }
}

impl<T: Future<Output = ()> + Send + 'static> Future for SpawnFuture<T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.wake_flag
            .store(false, std::sync::atomic::Ordering::SeqCst);
        Future::poll(self.task.as_mut(), cx)
    }
}

impl SpawnHandle for Spawner {
    fn spawn(&self, name: &'static str, task: impl Future<Output = ()> + Send + 'static) {
        // NOTE this is magic - member creates too much background noise
        if name == "member" {
            self.spawner.spawn(name, task)
        } else {
            let wrapped = self.wrap_task(task);
            self.spawner.spawn(name, wrapped)
        }
    }

    fn spawn_essential(
        &self,
        name: &'static str,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> TaskHandle {
        // NOTE this is magic - member creates too much background noise
        if name == "member" {
            self.spawner.spawn_essential(name, task)
        } else {
            let wrapped = self.wrap_task(task);
            self.spawner.spawn_essential(name, wrapped)
        }
    }
}

impl Spawner {
    pub async fn wait(&self) {
        self.spawner.wait().await
    }

    pub fn new(delay_config: &DelayConfig) -> Self {
        Spawner {
            spawner: Arc::new(MockSpawner::new()),
            idle_mx: Arc::new(Mutex::new(())),
            wake_flag: Arc::new(AtomicBool::new(false)),
            // NOTE this is a magic value used to allow fuzzing tests be able to process enough messages from the PlaybackNetwork
            delay: 10 * delay_config.tick_interval,
        }
    }

    //This code is used only for testing.
    #[allow(clippy::await_holding_lock)]
    pub async fn wait_idle(&self) {
        let _idle_mx_lock = self.idle_mx.lock();

        self.wake_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
        loop {
            Delay::new(self.delay).await;
            yield_now().await;
            // try to verify if any other task was attempting to wake up
            // it assumes that we are using a single-threaded runtime for scheduling our Futures
            if self
                .wake_flag
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                break;
            }
        }
        self.wake_flag
            .store(false, std::sync::atomic::Ordering::SeqCst)
    }

    fn wrap_task(
        &self,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> impl Future<Output = ()> + Send + 'static {
        SpawnFuture::new(task, self.wake_flag.clone())
    }
}

type UReceiver<T> = futures::channel::mpsc::UnboundedReceiver<T>;

fn init_log() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .is_test(true)
        .try_init();
}

pub fn gen_delay_config() -> DelayConfig {
    DelayConfig {
        tick_interval: Duration::from_millis(5),
        unit_rebroadcast_interval_min: Duration::from_millis(400),
        unit_rebroadcast_interval_max: Duration::from_millis(500),
        // 50, 50, 50, 50, ...
        unit_creation_delay: Arc::new(|_| Duration::from_millis(50)),
        // 100, 100, 100, ...
        coord_request_delay: Arc::new(|_| Duration::from_millis(100)),
        // 3, 1, 1, 1, ...
        coord_request_recipients: Arc::new(|t| if t == 0 { 3 } else { 1 }),
        // 50, 50, 50, 50, ...
        parent_request_delay: Arc::new(|_| Duration::from_millis(50)),
        // 1, 1, 1, ...
        parent_request_recipients: Arc::new(|_| 1),
        // 50, 50, 50, 50, ...
        newest_request_delay: Arc::new(|_| Duration::from_millis(50)),
    }
}

pub fn gen_config(node_ix: NodeIndex, n_members: NodeCount, delay_config: DelayConfig) -> Config {
    create_config(n_members, node_ix, 0, 5000, delay_config, Duration::ZERO)
        .expect("Should always succeed with Duration::ZERO")
}

pub fn spawn_honest_member_with_config(
    spawner: impl SpawnHandle + Sync,
    config: Config,
    network: impl 'static + NetworkT<FuzzNetworkData>,
    mk: Keychain,
) -> (oneshot::Sender<()>, UReceiver<Data>) {
    let unit_loader = Loader::new(vec![]);
    let unit_saver = Saver::new();
    let data_provider = DataProvider::new();
    let (finalization_handler, finalization_rx) = FinalizationHandler::new();
    let local_io = LocalIO::new(data_provider, finalization_handler, unit_saver, unit_loader);
    let (exit_tx, exit_rx) = oneshot::channel();
    let spawner_inner = spawner.clone();
    let member_task = async move {
        run_session(
            config,
            local_io,
            network,
            mk,
            spawner_inner.clone(),
            Terminator::create_root(exit_rx, "AlephBFT-member"),
        )
        .await
    };
    spawner.spawn("member", member_task);
    (exit_tx, finalization_rx)
}

pub type FuzzNetworkData = NetworkData<Hasher64, Data, Signature, PartialMultisignature>;

#[derive(Clone, Eq, PartialEq, Hash, Debug, Default, Decode, Encode)]
struct SpyingNetworkHook<W: Write> {
    node: NodeIndex,
    encoder: NetworkDataEncoding,
    output: W,
}

impl<W: Write> SpyingNetworkHook<W> {
    fn new(node: NodeIndex, output: W) -> Self {
        SpyingNetworkHook {
            node,
            encoder: NetworkDataEncoding::default(),
            output,
        }
    }
}

impl<W: Write + Send> NetworkHook<FuzzNetworkData> for SpyingNetworkHook<W> {
    fn update_state(&mut self, data: &mut FuzzNetworkData, _: NodeIndex, recipient: NodeIndex) {
        if self.node == recipient {
            self.encoder.encode_into(data, &mut self.output).unwrap();
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Decode, Encode)]
pub(crate) struct NetworkDataEncoding {}

impl NetworkDataEncoding {
    pub(crate) fn encode_into<W: Write>(
        &self,
        data: &FuzzNetworkData,
        writer: &mut W,
    ) -> IOResult<()> {
        writer.write_all(&data.encode()[..])
    }

    pub(crate) fn decode_from<R: Read>(
        &self,
        reader: &mut R,
    ) -> Result<FuzzNetworkData, codec::Error> {
        let mut reader = IoReader(reader);
        <FuzzNetworkData>::decode(&mut reader)
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug, Default, Decode, Encode)]
struct NetworkDataIterator<R> {
    input: R,
    encoding: NetworkDataEncoding,
}

impl<R: Read> NetworkDataIterator<R> {
    fn new(read: R) -> Self {
        NetworkDataIterator {
            input: read,
            encoding: NetworkDataEncoding::default(),
        }
    }
}

impl<R: Read> Iterator for NetworkDataIterator<R> {
    type Item = FuzzNetworkData;

    fn next(&mut self) -> Option<Self::Item> {
        match self.encoding.decode_from(&mut self.input) {
            Ok(v) => Some(v),
            Err(e) => {
                error!("Error while decoding NetworkData: {:?}.", e);
                None
            }
        }
    }
}

#[derive(Debug)]
struct PlaybackNetwork<I, C> {
    data: I,
    exit: Receiver<()>,
    finished_callback: Option<C>,
}

impl<I, C> PlaybackNetwork<I, C> {
    fn new(data: I, exit: Receiver<()>, finished: C) -> Self {
        PlaybackNetwork {
            data,
            exit,
            finished_callback: Some(finished),
        }
    }
}

#[async_trait::async_trait]
impl<I: Iterator<Item = FuzzNetworkData> + Send, C: FnOnce() + Send> NetworkT<FuzzNetworkData>
    for PlaybackNetwork<I, C>
{
    fn send(&self, _: FuzzNetworkData, _: Recipient) {}

    async fn next_event(&mut self) -> Option<FuzzNetworkData> {
        match self.data.next() {
            Some(v) => Some(v),
            None => {
                if let Some(finished_call) = self.finished_callback.take() {
                    (finished_call)();
                }
                // wait for an exit call when no more data is available
                let _ = (&mut self.exit).await;
                None
            }
        }
    }
}

#[derive(Debug, Decode, Encode)]
pub struct ReadToNetworkDataIterator<R> {
    read: BufReader<R>,
    decoder: NetworkDataEncoding,
}

impl<R: Read> ReadToNetworkDataIterator<R> {
    pub fn new(read: R) -> Self {
        ReadToNetworkDataIterator {
            read: BufReader::new(read),
            decoder: NetworkDataEncoding::default(),
        }
    }
}

impl<R: Read> Iterator for ReadToNetworkDataIterator<R> {
    type Item = FuzzNetworkData;

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(buf) = self.read.fill_buf() {
            if buf.is_empty() {
                return None;
            }
        }
        match self.decoder.decode_from(&mut self.read) {
            Ok(v) => Some(v),
            // otherwise try to read until you reach END-OF-FILE
            Err(e) => {
                error!(target: "fuzz", "Unable to parse NetworkData: {:?}.", e);
                self.next()
            }
        }
    }
}

async fn execute_generate_fuzz<'a, W: Write + Send + 'static>(
    output: W,
    n_members: usize,
    n_batches: usize,
) {
    let peer_id = NodeIndex(0);
    let spy = SpyingNetworkHook::new(peer_id, output);
    // spawn only byzantine-threshold of nodes and networks so all enabled nodes are required to finish each round
    let threshold = (n_members * 2) / 3 + 1;
    let (mut router, networks) = Router::new(n_members.into(), 1.0);
    router.add_hook(spy);

    let delay_config = gen_delay_config();
    let spawner = Spawner::new(&delay_config);
    spawner.spawn("network", router);

    let mut batch_rxs = Vec::new();
    let mut exits = Vec::new();
    for (network, _) in networks.into_iter().take(threshold) {
        let keychain = Keychain::new(NodeCount(n_members), network.index());
        let config = gen_config(network.index(), n_members.into(), delay_config.clone());
        let (exit_tx, batch_rx) =
            spawn_honest_member_with_config(spawner.clone(), config, network, keychain);
        exits.push(exit_tx);
        batch_rxs.push(batch_rx);
    }

    for mut rx in batch_rxs {
        for _ in 0..n_batches {
            rx.next().await.expect("unable to retrieve a batch");
        }
    }
    for e in exits {
        let _ = e.send(());
    }
    spawner.wait().await;
}

async fn execute_fuzz(
    data: impl Iterator<Item = FuzzNetworkData> + Send + 'static,
    n_members: usize,
    n_batches: Option<usize>,
) {
    let delay_config = gen_delay_config();
    let config = gen_config(0.into(), n_members.into(), delay_config.clone());
    let (net_exit, net_exit_rx) = oneshot::channel();
    let (playback_finished_tx, mut playback_finished_rx) = oneshot::channel();
    let finished_callback = move || {
        playback_finished_tx
            .send(())
            .expect("finished_callback channel already closed");
    };
    let network = PlaybackNetwork::new(data, net_exit_rx, finished_callback);

    let spawner = Spawner::new(&delay_config);
    let node_index = NodeIndex(0);
    let keychain = Keychain::new(NodeCount(n_members), node_index);
    let (exit_tx, mut batch_rx) =
        spawn_honest_member_with_config(spawner.clone(), config, network, keychain);

    let (n_batches, batches_expected) = {
        if let Some(batches) = n_batches {
            (batches, true)
        } else {
            (usize::MAX, false)
        }
    };
    let mut batches_count = 0;
    while batches_count < n_batches {
        futures::select! {
            batch = batch_rx.next() => {
                if batch.is_some() {
                    batches_count += 1;
                } else {
                    break;
                }
            }
            _ = playback_finished_rx => {
                if !batches_expected {
                    spawner.wait_idle().await;
                    break;
                }
            }
        }
    }

    if batches_expected {
        assert!(
            batches_count >= n_batches,
            "Expected at least {:?} batches, but received {:?}.",
            n_batches,
            batches_count
        );
    }

    if net_exit.send(()).is_err() {
        info!("net_exit channel is already closed");
    }
    if exit_tx.send(()).is_err() {
        info!("exit channel is already closed");
    }
    spawner.wait().await;
}

fn get_runtime() -> Runtime {
    Builder::new_current_thread().build().unwrap()
}

pub fn generate_fuzz<W: Write + Send + 'static>(output: W, n_members: usize, n_batches: usize) {
    let runtime = get_runtime();
    runtime.block_on(execute_generate_fuzz(output, n_members, n_batches));
}

pub fn check_fuzz(input: impl Read + Send + 'static, n_members: usize, n_batches: Option<usize>) {
    init_log();
    let data_iter = NetworkDataIterator::new(input);
    let runtime = get_runtime();
    runtime.block_on(execute_fuzz(data_iter, n_members, n_batches));
}

pub fn fuzz(data: Vec<FuzzNetworkData>, n_members: usize, n_batches: Option<usize>) {
    let runtime = get_runtime();
    runtime.block_on(execute_fuzz(data.into_iter(), n_members, n_batches));
}
