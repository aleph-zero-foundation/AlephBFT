pub use aleph_bft::testing::mock::NetworkData;

use aleph_bft::{
    testing::mock::{
        configure_network, init_log, spawn_honest_member, Data, Hasher64, NetworkHook,
        PartialMultisignature, Signature, Spawner,
    },
    Network, NodeIndex, SpawnHandle,
};

use codec::{Decode, Encode, IoReader};

use futures::{
    channel::oneshot::{self, Receiver},
    StreamExt,
};

use futures_timer::Delay;
use log::{error, info};

use std::{
    io::{BufRead, BufReader, Read, Result as IOResult, Write},
    time::Duration,
};

use tokio::runtime::{Builder, Runtime};

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

impl<W: Write + Send> NetworkHook for SpyingNetworkHook<W> {
    fn update_state(&mut self, data: &mut NetworkData, _: NodeIndex, recipient: NodeIndex) {
        if self.node == recipient {
            self.encoder.encode_into(data, &mut self.output).unwrap();
        }
    }
}

#[derive(Default)]
pub(crate) struct NetworkDataEncoding {}

impl NetworkDataEncoding {
    pub(crate) fn encode_into<W: Write>(&self, data: &NetworkData, writer: &mut W) -> IOResult<()> {
        writer.write_all(&data.encode()[..])
    }

    pub(crate) fn decode_from<R: Read>(
        &self,
        reader: &mut R,
    ) -> core::result::Result<NetworkData, codec::Error> {
        let mut reader = IoReader(reader);
        <NetworkData>::decode(&mut reader)
    }
}

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
    type Item = NetworkData;

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

struct PlaybackNetwork<I, C> {
    data: I,
    delay: Duration,
    next_delay: Delay,
    exit: Receiver<()>,
    finished_callback: Option<C>,
}

impl<I, C> PlaybackNetwork<I, C> {
    fn new(data: I, delay_millis: u64, exit: Receiver<()>, finished: C) -> Self {
        PlaybackNetwork {
            data,
            delay: Duration::from_millis(delay_millis),
            next_delay: Delay::new(Duration::from_millis(delay_millis)),
            exit,
            finished_callback: Some(finished),
        }
    }
}

#[async_trait::async_trait]
impl<I: Iterator<Item = NetworkData> + Send, C: FnOnce() + Send>
    Network<Hasher64, Data, Signature, PartialMultisignature> for PlaybackNetwork<I, C>
{
    type Error = ();

    fn send(&self, _: NetworkData, _: NodeIndex) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn broadcast(&self, _: NetworkData) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<NetworkData> {
        (&mut self.next_delay).await;
        self.next_delay.reset(self.delay);
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
    type Item = NetworkData;

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
                error!(target: "fuzz_target_1", "Unable to parse NetworkData: {:?}.", e);
                self.next()
            }
        }
    }
}

async fn execute_generate_fuzz<W: Write + Send + 'static>(
    output: W,
    n_members: usize,
    n_batches: usize,
) {
    let peer_id = NodeIndex(0);
    let spy = SpyingNetworkHook::new(peer_id, output);
    // spawn only byzantine-threshold of nodes and networks so all enabled nodes are required to finish each round
    let threshold = (n_members * 2) / 3 + 1;
    let (mut router, networks) = configure_network(n_members, 1.0);
    router.add_hook(spy);

    let spawner = Spawner::new();
    spawner.spawn("network", async move { router.run().await });

    let mut batch_rxs = Vec::new();
    let mut exits = Vec::new();
    for network in networks.into_iter().take(threshold) {
        let (batch_rx, exit_tx) =
            spawn_honest_member(spawner.clone(), network.index().into(), n_members, network);
        exits.push(exit_tx);
        batch_rxs.push(batch_rx);
    }

    for mut rx in batch_rxs.drain(..) {
        for _ in 0..n_batches {
            rx.next().await.expect("unable to retrieve a batch");
        }
    }
    for e in exits.drain(..) {
        let _ = e.send(());
    }
    spawner.wait().await;
}

async fn execute_fuzz(
    data: impl Iterator<Item = NetworkData> + Send + 'static,
    n_members: usize,
    n_batches: Option<usize>,
) {
    const NETWORK_DELAY: u64 = 1;

    let (net_exit, net_exit_rx) = oneshot::channel();
    let (playback_finished_tx, mut playback_finished_rx) = oneshot::channel();
    let finished_callback = move || {
        playback_finished_tx
            .send(())
            .expect("finished_callback channel already closed");
    };
    let network = PlaybackNetwork::new(data, NETWORK_DELAY, net_exit_rx, finished_callback);

    let spawner = Spawner::new();
    let (mut batch_rx, exit_tx) = spawn_honest_member(spawner.clone(), 0, n_members, network);

    let (n_batches, batches_expected) = {
        if let Some(batches) = n_batches {
            (batches, true)
        } else {
            (usize::max_value(), false)
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
                    // let it process all received data
                    Delay::new(Duration::from_secs(1)).await;
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
    Builder::new_current_thread().enable_all().build().unwrap()
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

pub fn fuzz(data: Vec<NetworkData>, n_members: usize, n_batches: Option<usize>) {
    let runtime = get_runtime();
    runtime.block_on(execute_fuzz(data.into_iter(), n_members, n_batches));
}
