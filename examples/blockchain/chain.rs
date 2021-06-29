use aleph_bft::{DataState, NodeIndex, OrderedBatch};
use codec::{Decode, Encode};
use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    FutureExt, StreamExt,
};
use futures_timer::Delay;
use log::{debug, error, info};
use parking_lot::Mutex;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{self, Duration},
};

type BlockNum = u64;

#[derive(Clone, Encode, Decode)]
pub(crate) struct Block {
    pub(crate) num: BlockNum,
    pub(crate) data: Vec<u8>,
}

impl Block {
    pub(crate) fn new(num: BlockNum, size: usize) -> Self {
        debug!(target: "Blockchain-chain", "Started creating block {:?}", num);
        // Not extremely random, but good enough.
        let data: Vec<u8> = (0..size)
            .map(|i| ((i + i / 999 + (i >> 12)) % 8) as u8)
            .collect();
        debug!(target: "Blockchain-chain", "Finished creating block {:?}", num);
        Block { num, data }
    }
}

pub(crate) type BlockPlan = Arc<dyn Fn(BlockNum) -> NodeIndex + Sync + Send + 'static>;

pub(crate) struct ChainConfig {
    // Our NodeIndex.
    pub(crate) node_ix: NodeIndex,
    // Number of random bytes to include in the block.
    pub(crate) data_size: usize,
    // Delay between blocks
    pub(crate) blocktime_ms: u64,
    // Delay before the first block should be created
    pub(crate) init_delay_ms: u64,
    // f(k) means who should author the kth block
    pub(crate) authorship_plan: BlockPlan,
}

pub(crate) fn gen_chain_config(
    node_ix: NodeIndex,
    n_members: usize,
    data_size: usize,
    blocktime_ms: u64,
    init_delay_ms: u64,
) -> ChainConfig {
    //Round robin block authorship plan.
    let authorship_plan = Arc::new(move |num: u64| NodeIndex((num as usize) % n_members));
    ChainConfig {
        node_ix,
        data_size,
        blocktime_ms,
        init_delay_ms,
        authorship_plan,
    }
}

pub(crate) type Data = BlockNum;

struct InnerDataIO {
    current_block: BlockNum,
    available_blocks: HashSet<BlockNum>,
    finalized_tx: UnboundedSender<OrderedBatch<Data>>,
}

#[derive(Clone)]
pub(crate) struct DataIO {
    inner: Arc<Mutex<InnerDataIO>>,
}

impl aleph_bft::DataIO<Data> for DataIO {
    type Error = ();
    fn get_data(&self) -> Data {
        self.inner.lock().current_block
    }

    fn check_availability(&self, data: &Data) -> DataState<Self::Error> {
        if self.inner.lock().available_blocks.contains(data) {
            return DataState::Available;
        }
        let data_io = self.inner.clone();
        let data = *data;
        let fut = async move {
            loop {
                if data_io.lock().available_blocks.contains(&data) {
                    return Ok(());
                }
                Delay::new(Duration::from_millis(100)).await;
            }
        };
        DataState::Missing(Box::pin(fut))
    }

    fn send_ordered_batch(&mut self, data: OrderedBatch<Data>) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .finalized_tx
            .unbounded_send(data)
            .map_err(|_| ())
    }
}

impl DataIO {
    pub(crate) fn new() -> (Self, UnboundedReceiver<OrderedBatch<Data>>) {
        let (finalized_tx, finalized_rx) = mpsc::unbounded();
        // We initialize with the "Genesis Block" => number 0.
        let available_blocks: HashSet<_> = [0u64].iter().cloned().collect();
        let inner = InnerDataIO {
            current_block: 0,
            available_blocks,
            finalized_tx,
        };

        (
            DataIO {
                inner: Arc::new(Mutex::new(inner)),
            },
            finalized_rx,
        )
    }

    pub(crate) fn contains_block(&self, num: BlockNum) -> bool {
        self.inner.lock().available_blocks.contains(&num)
    }

    pub(crate) fn add_block(&self, num: BlockNum) {
        debug!(target: "Blockchain", "Added block {:?}.", num);
        let mut data_io = self.inner.lock();
        data_io.available_blocks.insert(num);
        while data_io
            .available_blocks
            .contains(&(data_io.current_block + 1))
        {
            data_io.current_block += 1;
        }
    }
}

// Runs a process that maintains a simple blockchain. The blocks are created every config.blocktime_ms
// milliseconds and the block authors are determined by config.authorship_plan. The default config
// uses round robin authorship: node k creates blocks number n if n%n_members = k.
// A node will create a block n only if:
// 1) it received the previous block (n-1)
// 2) it is the nth block author
// 3) enough time has passed -- to maintain blocktime of roughly config.blocktime_ms milliseconds.
// This process holds two channel endpoints: block_rx to receive blocks from the network and
// block_tx to push created blocks to the network (to send them to all the remaining nodes).
pub(crate) async fn run_blockchain(
    config: ChainConfig,
    data_io: DataIO,
    mut block_rx: UnboundedReceiver<Block>,
    block_tx: UnboundedSender<Block>,
    mut exit: oneshot::Receiver<()>,
) {
    let start_time = time::Instant::now();
    for block_num in 1u64.. {
        loop {
            let curr_author = (config.authorship_plan)(block_num);
            if curr_author == config.node_ix {
                // We need to create the block, but at the right time
                let curr_time = time::Instant::now();
                let block_delay_ms = (block_num - 1) * config.blocktime_ms + config.init_delay_ms;
                let block_creation_time = start_time + Duration::from_millis(block_delay_ms);
                if curr_time >= block_creation_time {
                    let block = Block::new(block_num, config.data_size);
                    if let Err(e) = block_tx.unbounded_send(block) {
                        error!(target: "Blockchain-chain", "Error when sending block {:?}. Exiting.", e);
                        return;
                    }
                    data_io.add_block(block_num);
                }
            }
            // We tick every 10ms.
            let mut delay_fut = Delay::new(Duration::from_millis(10)).fuse();

            futures::select! {
                maybe_block = block_rx.next() => {
                    if let Some(block) = maybe_block {
                        data_io.add_block(block.num);
                        //We drop the block at this point, only keep track of the fact that we received it.
                    }
                }
                _ = &mut delay_fut => {
                    //We do nothing, but this takes us out of the select.
                }
                _ = &mut exit => {
                    info!(target: "Blockchain-chain", "Received exit signal.");
                    return;
                },
            }

            if data_io.contains_block(block_num) {
                break;
            }
        }
    }
}
