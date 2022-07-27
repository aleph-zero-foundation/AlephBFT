use std::{
    collections::HashMap,
    io::Write,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::Parser;
use futures::{channel::oneshot, StreamExt};
use log::{debug, error, info};
use parking_lot::Mutex;
use time::{macros::format_description, OffsetDateTime};

use aleph_bft::{run_session, NodeIndex, Terminator};
use aleph_bft_mock::{FinalizationHandler, Keychain, Loader, Saver, Spawner};
use chain::{run_blockchain, Block, BlockNum, ChainConfig};
use data::{Data, DataProvider, DataStore};
use network::{Address, NetworkData, NetworkManager};

mod chain;
mod data;
mod network;

const TXS_PER_BLOCK: usize = 50000;
const TX_SIZE: usize = 300;
const BLOCK_TIME: Duration = Duration::from_millis(1000);
const INITIAL_DELAY: Duration = Duration::from_millis(5000);

/// Blockchain example.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Our index
    #[clap(long, value_parser)]
    my_id: usize,

    /// IP address of the node
    #[clap(default_value = "127.0.0.1:0", long, value_parser)]
    ip_addr: String,

    /// Bootnodes indices
    #[clap(long, value_parser, value_delimiter = ',')]
    bootnodes_id: Vec<usize>,

    /// Bootnodes addresses
    #[clap(long, value_parser, value_delimiter = ',')]
    bootnodes_ip_addr: Vec<String>,

    /// Size of the committee
    #[clap(long, value_parser)]
    n_members: usize,

    /// Number of data to be finalized
    #[clap(long, value_parser)]
    n_finalized: usize,
}

#[tokio::main]
async fn main() {
    let time_format =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]");
    env_logger::builder()
        .format(move |buf, record| {
            writeln!(
                buf,
                "{} {} {}: {}",
                record.level(),
                OffsetDateTime::now_local()
                    .unwrap_or_else(|_| OffsetDateTime::now_utc())
                    .format(&time_format)
                    .unwrap(),
                record.target(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Debug)
        .init();

    let args = Args::parse();
    let start_time = Instant::now();
    info!(target: "Blockchain-main", "Getting network up.");
    let bootnodes: HashMap<NodeIndex, Address> = args
        .bootnodes_id
        .into_iter()
        .zip(args.bootnodes_ip_addr)
        .map(|(id, addr)| (id.into(), Address::from_str(&addr).unwrap()))
        .collect();
    let (
        mut manager,
        network,
        block_from_data_io_tx,
        block_from_network_rx,
        message_for_network,
        message_from_network,
    ) = NetworkManager::new(args.my_id.into(), args.ip_addr, args.n_members, bootnodes)
        .await
        .expect("Network set-up should succeed.");
    let (data_provider, current_block) = DataProvider::new();
    let (finalization_handler, mut finalized_rx) = FinalizationHandler::new();
    let data_store = DataStore::new(current_block.clone(), message_for_network);

    let (terminator_tx, terminator_rx) = oneshot::channel();
    let mut terminator = Terminator::create_root(terminator_rx, "Blockchain example");
    let network_terminator = terminator.add_offspring_connection("blockchain network");
    let network_handle = tokio::spawn(async move { manager.run(network_terminator).await });

    let data_size: usize = TXS_PER_BLOCK * TX_SIZE;
    let chain_config = ChainConfig::new(
        args.my_id.into(),
        args.n_members,
        data_size,
        BLOCK_TIME,
        INITIAL_DELAY,
    );
    let chain_terminator = terminator.add_offspring_connection("chain");
    let chain_handle = tokio::spawn(async move {
        run_blockchain(
            chain_config,
            data_store,
            current_block,
            block_from_network_rx,
            block_from_data_io_tx,
            message_from_network,
            chain_terminator,
        )
        .await
    });

    let member_terminator = terminator.add_offspring_connection("AlephBFT-member");
    let member_handle = tokio::spawn(async move {
        let keychain = Keychain::new(args.n_members.into(), args.my_id.into());
        let config = aleph_bft::default_config(args.n_members.into(), args.my_id.into(), 0);
        let backup_loader = Loader::new(vec![]);
        let backup_saver = Saver::new(Arc::new(Mutex::new(vec![])));
        let local_io = aleph_bft::LocalIO::new(
            data_provider,
            finalization_handler,
            backup_saver,
            backup_loader,
        );
        run_session(
            config,
            local_io,
            network,
            keychain,
            Spawner {},
            member_terminator,
        )
        .await
    });

    let mut max_block_finalized = 0;
    while let Some(block_num) = finalized_rx.next().await {
        if max_block_finalized < block_num {
            max_block_finalized = block_num;
        }
        debug!(target: "Blockchain-main",
            "Got new batch. Highest finalized = {:?}",
            max_block_finalized
        );
        if max_block_finalized >= args.n_finalized as u32 {
            break;
        }
    }
    if max_block_finalized < args.n_finalized as u32 {
        error!(target: "Blockchain-main", "Finalization stream finished too soon. Highest finalized = {:?}, expected {:?}", max_block_finalized, args.n_finalized);
        panic!("Finalization stream finished too soon.");
    }

    let stop_time = Instant::now();
    let tot_millis = (stop_time - start_time - INITIAL_DELAY).as_millis();
    let tps = (args.n_finalized as f64) * (TXS_PER_BLOCK as f64) / (0.001 * (tot_millis as f64));
    info!(target: "Blockchain-main", "Achieved {:?} tps.", tps);

    terminator_tx.send(()).expect("should send");
    terminator.get_exit().await.expect("should receive");
    terminator.terminate_sync().await;

    member_handle.await.unwrap();
    chain_handle.await.unwrap();
    network_handle.await.unwrap();
}
