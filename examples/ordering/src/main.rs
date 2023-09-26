mod dataio;
mod network;

use aleph_bft::{run_session, NodeIndex};
use aleph_bft_mock::{Keychain, Spawner};
use aleph_bft_types::Terminator;
use clap::Parser;
use dataio::{Data, DataProvider, FinalizationHandler};
use futures::{channel::oneshot, StreamExt};
use log::{debug, error, info};
use network::Network;
use std::{collections::HashMap, fs, fs::File, io, io::Write, path::Path, time::Duration};
use time::{macros::format_description, OffsetDateTime};

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
    n_data: u32,

    /// Number of the first created item
    #[clap(default_value = "0", long, value_parser)]
    n_starting: u32,

    /// Indices of nodes having stalling DataProviders
    #[clap(default_value = "", long, value_parser, value_delimiter = ',')]
    stalled: Vec<usize>,

    /// Should the node crash after finalizing its items
    #[clap(long, value_parser)]
    crash: bool,
}

fn create_backup(node_id: NodeIndex) -> Result<(File, io::Cursor<Vec<u8>>), io::Error> {
    let stash_path = Path::new("./aleph-bft-examples-ordering-backup");
    fs::create_dir_all(stash_path)?;
    let file_path = stash_path.join(format!("{}.units", node_id.0));
    let loader = if file_path.exists() {
        io::Cursor::new(fs::read(&file_path)?)
    } else {
        io::Cursor::new(Vec::new())
    };
    let saver = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)?;
    Ok((saver, loader))
}

fn finalized_counts(cf: &HashMap<NodeIndex, u32>) -> Vec<u32> {
    let mut v = cf
        .iter()
        .map(|(id, n)| (id.0, n))
        .collect::<Vec<(usize, &u32)>>();
    v.sort();
    v.iter().map(|(_, n)| **n).collect()
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
        n_data,
        n_starting,
        stalled,
        crash,
    } = Args::parse();
    let stalled = stalled.contains(&id);
    let id: NodeIndex = id.into();

    info!("Getting network up.");
    let network = Network::new(id, &ports)
        .await
        .expect("Could not create a Network instance.");
    let n_members = ports.len().into();
    let data_provider = DataProvider::new(id, n_starting, n_data - n_starting, stalled);
    let (finalization_handler, mut finalized_rx) = FinalizationHandler::new();
    let (backup_saver, backup_loader) = create_backup(id).expect("Error setting up unit saving");
    let local_io = aleph_bft::LocalIO::new(
        data_provider,
        finalization_handler,
        backup_saver,
        backup_loader,
    );

    let (exit_tx, exit_rx) = oneshot::channel();
    let member_terminator = Terminator::create_root(exit_rx, "AlephBFT-member");
    let member_handle = tokio::spawn(async move {
        let keychain = Keychain::new(n_members, id);
        let config = aleph_bft::default_config(n_members, id, 0, 5000, Duration::ZERO)
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

    let mut count_finalized: HashMap<NodeIndex, u32> =
        (0..ports.len()).map(|c| (c.into(), 0)).collect();

    loop {
        match finalized_rx.next().await {
            Some((id, number)) => {
                *count_finalized.get_mut(&id).unwrap() += 1;
                debug!(
                    "Finalized new item: node {:?}, number {:?}; total: {:?}",
                    id.0,
                    number,
                    finalized_counts(&count_finalized)
                );
            }
            None => {
                error!(
                    "Finalization stream finished too soon. Got {:?} items, wanted {:?} items",
                    finalized_counts(&count_finalized),
                    n_data
                );
                panic!("Finalization stream finished too soon.");
            }
        }
        if crash && count_finalized.get(&id).unwrap() >= &(n_data) {
            panic!(
                "Forced crash - items finalized so far: {:?}.",
                finalized_counts(&count_finalized)
            );
        } else if count_finalized.values().all(|c| c >= &(n_data)) {
            info!("Finalized required number of items.");
            info!("Waiting 10 seconds for other nodes...");
            tokio::time::sleep(Duration::from_secs(10)).await;
            info!("Shutdown.");
            break;
        }
    }

    exit_tx.send(()).expect("should send");
    member_handle.await.unwrap();
}
