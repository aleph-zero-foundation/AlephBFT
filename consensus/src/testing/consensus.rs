use crate::{
    consensus,
    reconstruction::Request as GenericRequest,
    testing::{complete_oneshot, gen_config, gen_delay_config, init_log},
    units::{preunit_to_full_unit, random_full_parent_units_up_to, ControlHash, PreUnit, Unit},
    Hasher, NodeCount, NodeIndex, NodeMap, Signed, SpawnHandle, Terminator,
};
use aleph_bft_mock::{DataProvider, Hasher64, Keychain, Spawner};
use futures::{
    channel::{mpsc::unbounded, oneshot},
    sink::SinkExt,
    stream::StreamExt,
};
use log::trace;

type Request = GenericRequest<Hasher64>;

#[tokio::test]
async fn catches_wrong_control_hash() {
    init_log();
    let n_nodes = NodeCount(4);
    let spawner = Spawner::new();
    let node_ix = NodeIndex(0);
    let (mut units_for_consensus, units_from_us) = unbounded();
    let (_parents_for_consensus, parents_from_us) = unbounded();
    let (units_for_us, _units_from_consensus) = unbounded();
    let (requests_for_us, mut requests_from_consensus) = unbounded();
    let (new_units_for_us, _new_units_from_consensus) = unbounded();

    let conf = gen_config(node_ix, n_nodes, gen_delay_config());
    let (exit_tx, exit_rx) = oneshot::channel();
    let starting_round = complete_oneshot(Some(0));
    let keychains: Vec<_> = n_nodes
        .into_iterator()
        .map(|node_id| Keychain::new(n_nodes, node_id))
        .collect();
    let keychain = keychains[node_ix.0];
    let data_provider = DataProvider::new();

    let consensus_handle = spawner.spawn_essential(
        "consensus",
        consensus::run(
            conf,
            consensus::IO {
                units_from_runway: units_from_us,
                parents_from_runway: parents_from_us,
                units_for_runway: units_for_us,
                requests_for_runway: requests_for_us,
                new_units_for_runway: new_units_for_us,
                data_provider,
                starting_round,
            },
            keychain,
            spawner,
            Terminator::create_root(exit_rx, "AlephBFT-consensus"),
        ),
    );
    let other_initial_units: Vec<_> = random_full_parent_units_up_to(0, n_nodes, 0)
        .pop()
        .expect("created initial units")
        .into_iter()
        .map(|unit| {
            let keychain = keychains
                .get(unit.creator().0)
                .expect("we have the keychains");
            Signed::sign(unit, keychain)
        })
        .collect();
    for unit in &other_initial_units {
        units_for_consensus
            .send(unit.clone())
            .await
            .expect("channel works");
    }
    let mut parent_hashes = NodeMap::with_size(n_nodes);
    for unit in other_initial_units.into_iter() {
        parent_hashes.insert(unit.creator(), unit.hash());
    }
    let bad_pu = PreUnit::<Hasher64>::new(1.into(), 1, ControlHash::new(&parent_hashes));
    let bad_control_hash: <Hasher64 as Hasher>::Hash = [0, 1, 0, 1, 0, 1, 0, 1];
    assert!(
        bad_control_hash != bad_pu.control_hash().combined_hash,
        "Bad control hash cannot be the correct one."
    );
    let mut control_hash = bad_pu.control_hash().clone();
    control_hash.combined_hash = bad_control_hash;
    let bad_pu = PreUnit::new(bad_pu.creator(), bad_pu.round(), control_hash);
    let keychain = &keychains[bad_pu.creator().0];
    let bad_unit = Signed::sign(preunit_to_full_unit(bad_pu, 0), keychain);
    let unit_hash = bad_unit.hash();
    units_for_consensus
        .send(bad_unit)
        .await
        .expect("channel is open");
    loop {
        let request = requests_from_consensus
            .next()
            .await
            .expect("channel is open");
        trace!(target: "consensus-test", "request {:?}", request);
        if let Request::ParentsOf(h) = request {
            assert_eq!(h, unit_hash, "Expected notification for our bad unit.");
            break;
        }
    }

    let _ = exit_tx.send(());

    consensus_handle.await.expect("The node is honest.");
}
