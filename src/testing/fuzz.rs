pub use crate::testing::mock::NetworkData;
use crate::{
    nodes::NodeIndex,
    testing::mock::{
        configure_network, spawn_honest_member, Data, Hasher64, NetworkHook, PartialMultisignature,
        Signature, Spawner,
    },
    utils::_after_iter,
    Network, NetworkData as ND, SpawnHandle,
};
use codec::{Decode, Encode, IoReader};
use futures::{
    channel::oneshot::{self, Receiver},
    StreamExt,
};
use futures_timer::Delay;
use log::{error, info};
use std::{
    io::{Read, Result as IOResult, Write},
    time::Duration,
};
use tokio::runtime::Builder;

struct ClosureHook<CT> {
    wrapped: CT,
}

impl<F: FnMut(&NetworkData, NodeIndex, NodeIndex) + Send> ClosureHook<F> {
    pub(crate) fn new(wrapped: F) -> Self {
        ClosureHook { wrapped }
    }
}

impl<F: FnMut(&NetworkData, NodeIndex, NodeIndex) + Send> NetworkHook for ClosureHook<F> {
    fn update_state(&mut self, data: &mut NetworkData, sender: NodeIndex, recipient: NodeIndex) {
        (self.wrapped)(data, sender, recipient);
    }
}

#[derive(Default)]
pub struct NetworkDataEncoding {}

impl NetworkDataEncoding {
    pub fn encode_into<W: Write>(&self, data: &NetworkData, writer: &mut W) -> IOResult<()> {
        writer.write_all(&data.encode()[..])
    }

    pub fn decode_from<R: Read>(
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

struct RecorderNetwork<I: Iterator<Item = NetworkData>> {
    data: I,
    delay: Duration,
    next_delay: Delay,
    exit: Receiver<()>,
}

impl<I: Iterator<Item = NetworkData> + Send> RecorderNetwork<I> {
    fn new(data: I, delay_millis: u64, exit: Receiver<()>) -> Self {
        RecorderNetwork {
            data,
            delay: Duration::from_millis(delay_millis),
            next_delay: Delay::new(Duration::from_millis(delay_millis)),
            exit,
        }
    }
}

#[async_trait::async_trait]
impl<I: Iterator<Item = NetworkData> + Send>
    Network<Hasher64, Data, Signature, PartialMultisignature> for RecorderNetwork<I>
{
    type Error = ();

    fn send(
        &self,
        _: ND<Hasher64, Data, Signature, PartialMultisignature>,
        _: NodeIndex,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn broadcast(
        &self,
        _: ND<Hasher64, Data, Signature, PartialMultisignature>,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn next_event(&mut self) -> Option<ND<Hasher64, Data, Signature, PartialMultisignature>> {
        (&mut self.next_delay).await;
        self.next_delay.reset(self.delay);
        match self.data.next() {
            Some(v) => Some(v),
            None => {
                // just for an exit call when no more data is available
                let _ = (&mut self.exit).await;
                None
            }
        }
    }
}

async fn execute_generate_fuzz<W: Write + Send + 'static>(
    mut output: W,
    n_members: usize,
    n_batches: usize,
) {
    let peer_id = NodeIndex(0);
    let encoder = NetworkDataEncoding::default();
    let spy = move |data: &NetworkData, _: NodeIndex, recipient: NodeIndex| {
        if recipient == peer_id {
            encoder.encode_into(data, &mut output).unwrap();
        }
    };
    let network_hook = ClosureHook::new(spy);
    // spawn only byzantine-threshold of nodes and networks so all enabled nodes are required to finish each round
    let threshold = (n_members * 2) / 3 + 1;
    let (mut router, networks) = configure_network(n_members, 1.0, (0..threshold).map(NodeIndex));
    router.add_hook(network_hook);

    let spawner = Spawner::new();
    spawner.spawn("network", async move { router.run().await });

    let mut batch_rxs = Vec::new();
    let mut exits = Vec::new();
    for network in networks {
        let (batch_rx, exit_tx) =
            spawn_honest_member(spawner.clone(), network.index.into(), n_members, network);
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
    let (empty_tx, mut empty_rx) = oneshot::channel();
    let data = _after_iter(data, move || {
        empty_tx.send(()).expect("empty_rx was already closed");
    });

    let (net_exit, net_exit_rx) = oneshot::channel();
    let network = RecorderNetwork::new(data, NETWORK_DELAY, net_exit_rx);

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
            _ = empty_rx => {
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

pub fn generate_fuzz<W: Write + Send + 'static>(output: W, n_members: usize, n_batches: usize) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(execute_generate_fuzz(output, n_members, n_batches));
}

pub fn check_fuzz(input: impl Read + Send + 'static, n_members: usize, n_batches: Option<usize>) {
    let data_iter = NetworkDataIterator::new(input);
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        execute_fuzz(data_iter, n_members, n_batches).await;
    });
}

pub fn fuzz(data: Vec<NetworkData>, n_members: usize, n_batches: Option<usize>) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(execute_fuzz(data.into_iter(), n_members, n_batches));
}
