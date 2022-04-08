use crate::{
    consensus,
    runway::{NotificationIn, NotificationOut},
    testing::{complete_oneshot, gen_config, init_log},
    units::{ControlHash, PreUnit, Unit, UnitCoord},
    Hasher, NodeIndex, SpawnHandle,
};
use aleph_bft_mock::{Hasher64, Spawner};
use codec::Encode;
use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    sink::SinkExt,
    stream::StreamExt,
    Future,
};
use log::trace;
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
};

// This struct allows to create a Hub to interconnect several instances of the Consensus engine, without
// requiring the Member wrapper. The Hub notifies all connected instances about newly created units and
// is able to answer unit requests as well. WrongControlHashes are not supported, which means that this
// Hub should be used to run simple tests in honest scenarios only.
// Usage: 1) create an instance using new(n_members), 2) connect all n_members instances, 0, 1, 2, ..., n_members - 1.
// 3) run the HonestHub instance as a Future.
pub(crate) struct HonestHub {
    n_members: usize,
    ntfct_out_rxs: HashMap<NodeIndex, UnboundedReceiver<NotificationOut<Hasher64>>>,
    ntfct_in_txs: HashMap<NodeIndex, UnboundedSender<NotificationIn<Hasher64>>>,
    units_by_coord: HashMap<UnitCoord, Unit<Hasher64>>,
}

impl HonestHub {
    pub(crate) fn new(n_members: usize) -> Self {
        HonestHub {
            n_members,
            ntfct_out_rxs: HashMap::new(),
            ntfct_in_txs: HashMap::new(),
            units_by_coord: HashMap::new(),
        }
    }

    pub(crate) fn connect(
        &mut self,
        node_ix: NodeIndex,
    ) -> (
        UnboundedSender<NotificationOut<Hasher64>>,
        UnboundedReceiver<NotificationIn<Hasher64>>,
    ) {
        let (tx_in, rx_in) = unbounded();
        let (tx_out, rx_out) = unbounded();
        self.ntfct_in_txs.insert(node_ix, tx_in);
        self.ntfct_out_rxs.insert(node_ix, rx_out);
        (tx_out, rx_in)
    }

    fn send_to_all(&mut self, ntfct: NotificationIn<Hasher64>) {
        assert!(
            self.ntfct_in_txs.len() == self.n_members,
            "Must connect to all nodes before running the hub."
        );
        for (_ix, tx) in self.ntfct_in_txs.iter() {
            tx.unbounded_send(ntfct.clone()).ok();
        }
    }

    fn send_to_node(&mut self, node_ix: NodeIndex, ntfct: NotificationIn<Hasher64>) {
        let tx = self
            .ntfct_in_txs
            .get(&node_ix)
            .expect("Must connect to all nodes before running the hub.");
        tx.unbounded_send(ntfct).expect("Channel should be open");
    }

    fn on_notification(&mut self, node_ix: NodeIndex, ntfct: NotificationOut<Hasher64>) {
        match ntfct {
            NotificationOut::CreatedPreUnit(pu, _parent_hashes) => {
                let hash = pu.using_encoded(Hasher64::hash);
                let u = Unit::new(pu, hash);
                let coord = UnitCoord::new(u.round(), u.creator());
                self.units_by_coord.insert(coord, u.clone());
                self.send_to_all(NotificationIn::NewUnits(vec![u]));
            }
            NotificationOut::MissingUnits(coords) => {
                let mut response_units = Vec::new();
                for coord in coords {
                    match self.units_by_coord.get(&coord) {
                        Some(unit) => {
                            response_units.push(unit.clone());
                        }
                        None => {
                            panic!("Unit requested that the hub does not know.");
                        }
                    }
                }
                let ntfct = NotificationIn::NewUnits(response_units);
                self.send_to_node(node_ix, ntfct);
            }
            NotificationOut::WrongControlHash(_u_hash) => {
                panic!("No support for forks in testing.");
            }
            NotificationOut::AddedToDag(_u_hash, _hashes) => {
                // Safe to ignore in testing.
                // Normally this is used in Member to answer parents requests.
            }
        }
    }
}

impl Future for HonestHub {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut ready_ixs: Vec<NodeIndex> = Vec::new();
        let mut buffer = Vec::new();
        for (ix, rx) in self.ntfct_out_rxs.iter_mut() {
            loop {
                match rx.poll_next_unpin(cx) {
                    Poll::Ready(Some(ntfct)) => {
                        buffer.push((*ix, ntfct));
                    }
                    Poll::Ready(None) => {
                        ready_ixs.push(*ix);
                        break;
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            }
        }
        for (ix, ntfct) in buffer {
            self.on_notification(ix, ntfct);
        }
        for ix in ready_ixs {
            self.ntfct_out_rxs.remove(&ix);
        }
        if self.ntfct_out_rxs.is_empty() {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

#[tokio::test]
async fn agree_on_first_batch() {
    init_log();
    let n_members: usize = 16;
    let mut hub = HonestHub::new(n_members);

    let mut exits = vec![];
    let mut batch_rxs = vec![];
    let spawner = Spawner::new();

    let mut handles = vec![];

    for node_ix in 0..n_members {
        let (tx, rx) = hub.connect(NodeIndex(node_ix));
        let conf = gen_config(NodeIndex(node_ix), n_members.into());
        let (exit_tx, exit_rx) = oneshot::channel();
        exits.push(exit_tx);
        let (batch_tx, batch_rx) = unbounded();
        batch_rxs.push(batch_rx);
        let starting_round = complete_oneshot(0);
        handles.push(spawner.spawn_essential(
            "consensus",
            consensus::run(
                conf,
                rx,
                tx,
                batch_tx,
                spawner.clone(),
                starting_round,
                exit_rx,
            ),
        ));
    }

    spawner.spawn("hub", hub);

    let mut batches = vec![];
    for mut rx in batch_rxs.drain(..) {
        let batch = rx.next().await.unwrap();
        assert!(!batch.is_empty());
        batches.push(batch);
    }

    for node_ix in 1..n_members {
        assert_eq!(batches[0], batches[node_ix]);
    }
}

#[tokio::test]
async fn catches_wrong_control_hash() {
    init_log();
    let n_nodes = 4;
    let spawner = Spawner::new();
    let node_ix = 0;
    let (mut tx_in, rx_in) = unbounded();
    let (tx_out, mut rx_out) = unbounded();

    let conf = gen_config(NodeIndex(node_ix), n_nodes.into());
    let (exit_tx, exit_rx) = oneshot::channel();
    let (batch_tx, _batch_rx) = unbounded();
    let starting_round = complete_oneshot(0);

    let consensus_handle = spawner.spawn_essential(
        "consensus",
        consensus::run(
            conf,
            rx_in,
            tx_out,
            batch_tx,
            spawner.clone(),
            starting_round,
            exit_rx,
        ),
    );
    let control_hash = ControlHash::new(&(vec![None; n_nodes]).into());
    let bad_pu = PreUnit::<Hasher64>::new(1.into(), 0, control_hash);
    let bad_control_hash: <Hasher64 as Hasher>::Hash = [0, 1, 0, 1, 0, 1, 0, 1];
    assert!(
        bad_control_hash != bad_pu.control_hash().combined_hash,
        "Bad control hash cannot be the correct one."
    );
    let mut control_hash = bad_pu.control_hash().clone();
    control_hash.combined_hash = bad_control_hash;
    let bad_pu = PreUnit::new(bad_pu.creator(), bad_pu.round(), control_hash);
    let bad_hash: <Hasher64 as Hasher>::Hash = [0, 1, 0, 1, 0, 1, 0, 1];
    let bad_unit = Unit::new(bad_pu, bad_hash);
    let _ = tx_in.send(NotificationIn::NewUnits(vec![bad_unit])).await;
    loop {
        let notification = rx_out.next().await.unwrap();
        trace!(target: "consensus-test", "notification {:?}", notification);
        if let NotificationOut::WrongControlHash(h) = notification {
            assert_eq!(h, bad_hash, "Expected notification for our bad unit.");
            break;
        }
    }

    let _ = exit_tx.send(());

    consensus_handle.await.expect("The node is honest.");
}
