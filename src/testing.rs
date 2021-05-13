#[cfg(test)]
pub mod mock {
    use crate::{
        member::{NotificationIn, NotificationOut},
        units::{Unit, UnitCoord},
        Hasher, Index, NodeIndex,
    };
    use codec::{Decode, Encode, Error as CodecError, Input, Output};
    use derive_more::{From, Into};
    use futures::{
        channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        Future, StreamExt,
    };

    use std::{
        collections::{hash_map::DefaultHasher, HashMap},
        fmt,
        hash::Hasher as StdHasher,
        pin::Pin,
        task::{Context, Poll},
    };

    #[derive(Hash, From, Into, Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
    pub struct NodeId(pub usize);

    impl Encode for NodeId {
        fn encode_to<T: Output + ?Sized>(&self, dest: &mut T) {
            let val = self.0 as u64;
            let bytes = val.to_le_bytes().to_vec();
            Encode::encode_to(&bytes, dest)
        }
    }

    impl Decode for NodeId {
        fn decode<I: Input>(value: &mut I) -> Result<Self, CodecError> {
            let mut arr = [0u8; 8];
            value.read(&mut arr)?;
            let val: u64 = u64::from_le_bytes(arr);
            Ok(NodeId(val as usize))
        }
    }

    impl fmt::Display for NodeId {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "Node-{}", self.0)
        }
    }

    impl Index for NodeId {
        fn index(&self) -> Option<NodeIndex> {
            Some(NodeIndex(self.0))
        }
    }

    // A hasher from the standard library that hashes to u64, should be enough to
    // avoid collisions in testing.
    #[derive(PartialEq, Eq, Clone, Debug)]
    pub struct Hasher64;

    impl Hasher for Hasher64 {
        type Hash = u64;

        fn hash(x: &[u8]) -> Self::Hash {
            let mut hasher = DefaultHasher::new();
            hasher.write(x);
            hasher.finish()
        }
    }

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
                let _ = tx.unbounded_send(ntfct.clone());
            }
        }

        fn send_to_node(&mut self, node_ix: NodeIndex, ntfct: NotificationIn<Hasher64>) {
            let tx = self
                .ntfct_in_txs
                .get(&node_ix)
                .expect("Must connect to all nodes before running the hub.");
            let _ = tx.unbounded_send(ntfct);
        }

        fn on_notification(&mut self, node_ix: NodeIndex, ntfct: NotificationOut<Hasher64>) {
            match ntfct {
                NotificationOut::CreatedPreUnit(pu) => {
                    let hash = pu.using_encoded(Hasher64::hash);
                    let u = Unit::new_from_preunit(pu, hash);
                    self.units_by_coord.insert(u.clone().into(), u.clone());
                    self.send_to_all(NotificationIn::NewUnits(vec![u]));
                }
                NotificationOut::MissingUnits(coords, _aux_data) => {
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
}
