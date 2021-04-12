#[cfg(test)]
pub mod mock {
    use crate::{MyIndex, NodeIndex, NotificationIn, NotificationOut, Unit, UnitCoord};
    use codec::{Decode, Encode, Error as CodecError, Input, Output};
    use derive_more::{Display, From, Into};
    use futures::{Sink, Stream};
    use parking_lot::Mutex;

    use std::{
        collections::{hash_map::DefaultHasher, HashMap},
        fmt,
        hash::Hasher,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };
    use tokio::sync::mpsc::*;

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

    impl MyIndex for NodeId {
        fn my_index(&self) -> Option<NodeIndex> {
            Some(NodeIndex(self.0))
        }
    }

    #[derive(
        Hash, Debug, Default, Display, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Encode, Decode,
    )]
    pub struct Hash(pub u32);

    impl From<u32> for Hash {
        fn from(h: u32) -> Self {
            Hash(h)
        }
    }

    type Units = Arc<Mutex<HashMap<UnitCoord, Unit<Hash>>>>;

    #[derive(Clone)]
    pub(crate) struct Network {
        senders: Senders,
        units: Units,
    }

    impl Network {
        pub(crate) fn new() -> Self {
            Network {
                senders: Arc::new(Mutex::new(vec![])),
                units: Arc::new(Mutex::new(HashMap::new())),
            }
        }
        pub(crate) fn consensus_data(
            &self,
            node_id: NodeId,
        ) -> (
            impl Sink<NotificationOut<Hash>, Error = Box<dyn std::error::Error>>,
            impl Stream<Item = NotificationIn<Hash>> + Send + Unpin + 'static,
        ) {
            let stream;
            {
                let (tx, rx) = unbounded_channel();
                stream = BcastStream(rx);
                self.senders.lock().push((node_id, tx));
            }
            let sink = BcastSink {
                node_id,
                senders: self.senders.clone(),
                units: self.units.clone(),
            };

            (sink, stream)
        }
    }

    type Sender = (NodeId, UnboundedSender<NotificationIn<Hash>>);
    type Senders = Arc<Mutex<Vec<Sender>>>;

    #[derive(Clone)]
    struct BcastSink {
        node_id: NodeId,
        senders: Senders,
        units: Units,
    }

    impl BcastSink {
        fn do_send(&self, msg: NotificationIn<Hash>, recipient: &Sender) {
            let (_node_id, tx) = recipient;
            let _ = tx.send(msg);
        }
        fn send_to_all(&self, msg: NotificationIn<Hash>) {
            self.senders
                .lock()
                .iter()
                .for_each(|r| self.do_send(msg.clone(), r));
        }
        fn send_to_peer(&self, msg: NotificationIn<Hash>, peer: NodeId) {
            let _ = self.senders.lock().iter().for_each(|r| {
                if r.0 == peer {
                    self.do_send(msg.clone(), r);
                }
            });
        }
    }

    impl Sink<NotificationOut<Hash>> for BcastSink {
        type Error = Box<dyn std::error::Error>;
        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, m: NotificationOut<Hash>) -> Result<(), Self::Error> {
            match m {
                NotificationOut::CreatedPreUnit(pu) => {
                    let hash = pu.using_encoded(hashing);
                    let u = Unit::new(pu.creator, pu.round(), hash, pu.control_hash);
                    self.units.lock().insert(u.clone().into(), u.clone());
                    self.send_to_all(NotificationIn::NewUnits(vec![u]));
                }
                NotificationOut::MissingUnits(coords, _aux_data) => {
                    let units: Vec<Unit<Hash>> = coords
                        .iter()
                        .map(|coord| self.units.lock().get(coord).cloned().unwrap())
                        .collect();
                    for u in units {
                        let response = NotificationIn::NewUnits(vec![u]);
                        self.send_to_peer(response, self.node_id);
                    }
                }
            }
            Ok(())
        }
    }

    struct BcastStream(UnboundedReceiver<NotificationIn<Hash>>);

    impl Stream for BcastStream {
        type Item = NotificationIn<Hash>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            // here we may add custom logic for dropping/changing messages
            self.0.poll_recv(cx)
        }
    }

    pub(crate) fn hashing(x: &[u8]) -> Hash {
        let mut hasher = DefaultHasher::new();
        hasher.write(x);
        Hash(hasher.finish() as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::mock::*;
    use crate::{NotificationIn, PreUnit, Unit};
    use codec::Encode;
    use futures::{sink::SinkExt, stream::StreamExt};

    #[tokio::test(max_threads = 3)]
    async fn comm() {
        let n = Network::new();
        let (mut out0, mut in0) = n.consensus_data(NodeId(0));
        let (mut out1, mut in1) = n.consensus_data(NodeId(1));
        let pu0 = PreUnit {
            creator: 0.into(),
            ..PreUnit::default()
        };

        let hash = pu0.using_encoded(hashing);
        let u = Unit::new(pu0.creator, pu0.round(), hash, pu0.control_hash.clone());
        let u0 = u.clone();
        let h0 = tokio::spawn(async move {
            assert_eq!(
                in0.next().await.unwrap(),
                NotificationIn::NewUnits(vec![u0]),
            );
        });

        let h1 = tokio::spawn(async move {
            assert_eq!(in1.next().await.unwrap(), NotificationIn::NewUnits(vec![u]));
        });

        assert!(out0.send(pu0.clone().into()).await.is_ok());
        assert!(out1.send(pu0.into()).await.is_ok());
        assert!(h0.await.is_ok());
        assert!(h1.await.is_ok());
    }
}
