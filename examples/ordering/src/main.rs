use std::io::Write;
mod dataio;
mod network;

use aleph_bft::{default_delay_config, run_session, NodeIndex, Terminator};
use aleph_bft_mock::{Keychain, Spawner};
use clap::Parser;
use dataio::{Data, DataProvider, FinalizationHandler};
use futures::{channel::oneshot, io, StreamExt};
use log::{debug, error, info};
use network::Network;
use std::sync::Arc;
use std::{path::Path, time::Duration};
use time::{macros::format_description, OffsetDateTime};
use tokio::fs::{self, File};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

/// Example node producing linear order.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Index of the node
    #[clap(long, value_parser)]
    id: usize,

    /// Ports
    #[clap(long, value_parser, value_delimiter = ',')]
    ports: Vec<usize>,

    /// Number of items to be ordered
    #[clap(long, value_parser)]
    data_items: u32,

    /// Number of the first created item
    #[clap(long, value_parser)]
    starting_data_item: u32,

    /// Should the node stall after providing all its items
    #[clap(long, value_parser)]
    should_stall: bool,

    /// Value which denotes range of integers that must be seen as finalized from all nodes
    /// ie all nodes must finalize integer sequence [0; required_finalization_value)
    #[clap(long, value_parser)]
    required_finalization_value: u32,

    /// Unit creation delay (milliseconds)
    #[clap(long, default_value = "200", value_parser)]
    unit_creation_delay: u64,
}

async fn create_backup(
    node_id: NodeIndex,
) -> Result<(Compat<File>, io::Cursor<Vec<u8>>), io::Error> {
    let stash_path = Path::new("./aleph-bft-examples-ordering-backup");
    fs::create_dir_all(stash_path).await?;
    let file_path = stash_path.join(format!("{}.units", node_id.0));
    let loader = if file_path.exists() {
        io::Cursor::new(fs::read(&file_path).await?)
    } else {
        io::Cursor::new(Vec::new())
    };
    let saver = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)
        .await?;
    Ok((saver.compat_write(), loader))
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

    let Args {
        id,
        ports,
        data_items,
        starting_data_item,
        should_stall,
        required_finalization_value,
        unit_creation_delay,
    } = Args::parse();

    let id: NodeIndex = id.into();

    info!("Getting network up.");
    let network = Network::new(id, &ports)
        .await
        .expect("Could not create a Network instance.");
    let n_members = ports.len().into();
    let data_provider = DataProvider::new(id, starting_data_item, data_items, should_stall);
    let (finalization_handler, mut finalized_rx) = FinalizationHandler::new();
    let (backup_saver, backup_loader) = create_backup(id)
        .await
        .expect("Error setting up unit saving");
    let local_io = aleph_bft::LocalIO::new(
        data_provider,
        finalization_handler,
        backup_saver,
        backup_loader,
    );

    let (exit_tx, exit_rx) = oneshot::channel();
    let member_terminator = Terminator::create_root(exit_rx, "AlephBFT-member");
    let mut delay_config = default_delay_config();
    delay_config.unit_creation_delay =
        Arc::new(move |_| Duration::from_millis(unit_creation_delay));
    let member_handle = tokio::spawn(async move {
        let keychain = Keychain::new(n_members, id);
        let config = aleph_bft::create_config(n_members, id, 0, 5000, delay_config, Duration::ZERO)
            .expect("Should always succeed with Duration::ZERO");
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

    let node_count = ports.len();
    let mut count_finalized = vec![0; node_count];

    let mut finalized_items = vec![0; required_finalization_value as usize];

    loop {
        match finalized_rx.next().await {
            Some((id, number)) => {
                count_finalized[id.0] += 1;
                finalized_items[number as usize] += 1;
                debug!(
                    "Finalized new item: node {:?}, number {:?}; total: {:?}",
                    id.0, number, &count_finalized,
                );
            }
            None => {
                error!(
                    "Finalization stream finished too soon. Got {:?} items, wanted {:?} items",
                    &count_finalized, data_items
                );
                panic!("Finalization stream finished too soon.");
            }
        }
        if finalized_items.iter().all(|item| *item >= 1) {
            info!(
                "Finalized all items from 0 to {}, at least once.",
                required_finalization_value - 1
            );
            info!("Waiting 10 seconds for other nodes...");
            tokio::time::sleep(Duration::from_secs(10)).await;
            info!("Shutdown.");
            break;
        }
    }

    exit_tx.send(()).expect("should send");
    member_handle.await.unwrap();
}
