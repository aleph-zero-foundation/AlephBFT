use crate::{network::NetworkData, DataStore};
use aleph_bft::NodeIndex;
use aleph_bft_types::Terminator;
use codec::{Decode, Encode};
use futures::{
    channel::mpsc::{UnboundedReceiver, UnboundedSender},
    FutureExt, StreamExt,
};
use futures_timer::Delay;
use log::{debug, info};
use parking_lot::Mutex;
use std::{
    fmt,
    fmt::{Debug, Formatter},
    sync::Arc,
    time::{self, Duration},
};

pub type BlockNum = u32;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Encode, Decode)]
pub struct Block {
    pub num: BlockNum,
    pub data: Vec<u8>,
}

impl Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Block").field("num", &self.num).finish()
    }
}

impl Block {
    pub fn new(num: BlockNum, size: usize) -> Self {
        debug!(target: "Blockchain-chain", "Started creating block {:?}", num);
        // Not extremely random, but good enough.
        let data: Vec<u8> = (0..size)
            .map(|i| ((i + i / 999 + (i >> 12)) % 8) as u8)
            .collect();
        debug!(target: "Blockchain-chain", "Finished creating block {:?}", num);
        Block { num, data }
    }
}

pub type BlockPlan = Arc<dyn Fn(BlockNum) -> NodeIndex + Sync + Send + 'static>;

#[derive(Clone)]
pub struct ChainConfig {
    // Our NodeIndex.
    pub node_ix: NodeIndex,
    // Number of random bytes to include in the block.
    pub data_size: usize,
    // Delay between blocks
    pub blocktime: Duration,
    // Delay before the first block should be created
    pub init_delay: Duration,
    // f(k) means who should author the kth block
    pub authorship_plan: BlockPlan,
}

impl Debug for ChainConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChainConfig")
            .field("node index", &self.node_ix)
            .field("data size", &self.data_size)
            .field("blocktime", &self.blocktime)
            .field("initial delay", &self.init_delay)
            .finish()
    }
}

impl ChainConfig {
    pub fn new(
        node_ix: NodeIndex,
        n_members: usize,
        data_size: usize,
        blocktime: Duration,
        init_delay: Duration,
    ) -> ChainConfig {
        //Round robin block authorship plan.
        let authorship_plan = Arc::new(move |num: BlockNum| NodeIndex((num as usize) % n_members));
        ChainConfig {
            node_ix,
            data_size,
            blocktime,
            init_delay,
            authorship_plan,
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
pub async fn run_blockchain(
    config: ChainConfig,
    mut data_store: DataStore,
    current_block: Arc<Mutex<BlockNum>>,
    mut blocks_from_network: UnboundedReceiver<Block>,
    blocks_for_network: UnboundedSender<Block>,
    mut messages_from_network: UnboundedReceiver<NetworkData>,
    mut terminator: Terminator,
) {
    let start_time = time::Instant::now();
    for block_num in 1.. {
        while *current_block.lock() < block_num {
            let curr_author = (config.authorship_plan)(block_num);
            if curr_author == config.node_ix {
                // We need to create the block, but at the right time
                let curr_time = time::Instant::now();
                let block_delay = (block_num - 1) * config.blocktime + config.init_delay;
                let block_creation_time = start_time + block_delay;
                if curr_time >= block_creation_time {
                    let block = Block::new(block_num, config.data_size);
                    blocks_for_network
                        .unbounded_send(block)
                        .expect("network should accept blocks");
                    data_store.add_block(block_num);
                }
            }
            // We tick every 10ms.
            let mut delay_fut = Delay::new(Duration::from_millis(10)).fuse();

            futures::select! {
                maybe_block = blocks_from_network.next() => {
                    if let Some(block) = maybe_block {
                        data_store.add_block(block.num);
                        //We drop the block at this point, only keep track of the fact that we received it.
                    }
                }
                maybe_message = messages_from_network.next() => {
                    if let Some(message) = maybe_message {
                        data_store.add_message(message);
                    }
                }
                _ = &mut delay_fut => {
                    //We do nothing, but this takes us out of the select.
                }
                _ = terminator.get_exit().fuse() => {
                    info!(target: "Blockchain-chain", "Received exit signal.");
                    terminator.terminate_sync().await;
                    return;
                },
            }
        }
    }
}
