#[cfg(test)]
pub mod environment {
    use crate::{MyIndex, NodeIndex, NotificationIn, NotificationOut, Round, Unit};
    use codec::{Encode, Output};
    use derive_more::{Display, From, Into};
    use futures::{Future, Sink, Stream};
    use parking_lot::Mutex;

    use std::{
        collections::{hash_map::DefaultHasher, HashMap},
        fmt,
        hash::Hasher,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };
    use tokio::sync::{mpsc::*, oneshot};

    type Error = ();

    #[derive(Hash, From, Into, Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
    pub struct NodeId(pub usize);

    impl Encode for NodeId {
        fn encode_to<T: Output>(&self, dest: &mut T) {
            let bytes = self.0.to_le_bytes().to_vec();
            Encode::encode_to(&bytes, dest)
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
        Hash, Debug, Default, Display, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Encode,
    )]
    pub struct Hash(pub u32);

    impl From<u32> for Hash {
        fn from(h: u32) -> Self {
            Hash(h)
        }
    }

    #[derive(
        Hash, Debug, Default, Display, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Encode,
    )]
    pub struct BlockHash(pub u32);

    impl From<u32> for BlockHash {
        fn from(h: u32) -> Self {
            BlockHash(h)
        }
    }

    type Out = Box<dyn Sink<NotificationOut<BlockHash, Hash>, Error = Error> + Send + Unpin>;
    type In = Box<dyn Stream<Item = NotificationIn<BlockHash, Hash>> + Send + Unpin>;

    pub(crate) struct Environment {
        node_id: NodeId,
        chain: Chain,
        network: Network,
        finalized_notifier: UnboundedSender<Block>,
        calls_to_finalize: Vec<BlockHash>,
        to_notify: Arc<Mutex<HashMap<BlockHash, Vec<Option<oneshot::Sender<()>>>>>>,
    }

    impl Environment {
        pub(crate) fn new_with_chain(
            node_id: NodeId,
            network: Network,
            chain: Chain,
        ) -> (Self, UnboundedReceiver<Block>) {
            let (finalized_tx, finalized_rx) = unbounded_channel();
            (
                Environment {
                    node_id,
                    chain,
                    network,
                    finalized_notifier: finalized_tx,
                    calls_to_finalize: Vec::new(),
                    to_notify: Arc::new(Mutex::new(HashMap::new())),
                },
                finalized_rx,
            )
        }

        pub(crate) fn new(node_id: NodeId, network: Network) -> (Self, UnboundedReceiver<Block>) {
            let chain = Chain::new();
            Self::new_with_chain(node_id, network, chain)
        }

        pub(crate) fn get_log_finalize_calls(&self) -> Vec<BlockHash> {
            self.calls_to_finalize.clone()
        }

        pub(crate) fn gen_chain(&mut self, branches: Vec<(BlockHash, Vec<BlockHash>)>) {
            for (h, branch) in branches {
                self.chain.import_branch(h, branch);
            }
        }

        pub(crate) fn import_block(&mut self, block: BlockHash, parent: BlockHash) {
            let mut to_notify = self.to_notify.lock();
            self.chain.import_block(block, parent);
            if let Some(mut notifiers) = to_notify.remove(&block) {
                notifiers.iter_mut().for_each(|n| {
                    n.take()
                        .unwrap()
                        .send(())
                        .expect("should send a notification that a block arrived")
                })
            };
        }
    }

    impl crate::Environment for Environment {
        type NodeId = NodeId;
        type Hash = Hash;
        type BlockHash = BlockHash;
        type InstanceId = u32;
        type Crypto = ();
        type Error = Error;
        type Out = Out;
        type In = In;
        type Hashing = Box<dyn Fn(&[u8]) -> Self::Hash + Send + Sync + 'static>;

        fn finalize_block(&mut self, h: Self::BlockHash) {
            self.calls_to_finalize.push(h);
            let finalized_blocks = self.chain.finalize(h);
            for block in finalized_blocks {
                let _ = self.finalized_notifier.send(block);
            }
        }

        fn check_available(
            &self,
            h: Self::BlockHash,
        ) -> Box<dyn Future<Output = Result<(), Self::Error>> + Send + Sync + Unpin> {
            if self.chain.block(&h).is_some() {
                return Box::new(futures::future::ok(()));
            }
            let (tx, rx) = oneshot::channel();
            self.to_notify.lock().entry(h).or_default().push(Some(tx));

            Box::new(Box::pin(async move { rx.await.map_err(|_| ()) }))
        }

        fn check_extends_finalized(&self, h: Self::BlockHash) -> bool {
            let last_finalized = self.chain.best_finalized();
            h != last_finalized && self.chain.is_descendant(&h, &last_finalized)
        }

        fn best_block(&self) -> Self::BlockHash {
            self.chain.best_block()
        }

        fn hash(_data: &[u8]) -> Self::Hash {
            Default::default()
        }

        fn consensus_data(&self) -> (Self::Out, Self::In) {
            self.network.consensus_data(self.node_id)
        }

        fn hashing() -> Self::Hashing {
            Box::new(|x: &[u8]| {
                let mut hasher = DefaultHasher::new();
                hasher.write(x);
                Hash(hasher.finish() as u32)
            })
        }
    }

    #[derive(Copy, Clone, Debug)]
    pub(crate) struct Block {
        number: usize,
        parent: Option<BlockHash>,
        hash: BlockHash,
    }

    impl Block {
        pub(crate) fn hash(&self) -> BlockHash {
            self.hash
        }
    }

    #[derive(Clone)]
    pub(crate) struct Chain {
        tree: HashMap<BlockHash, Block>,
        best_finalized: BlockHash,
        longest_chain: BlockHash,
    }

    impl Chain {
        pub(crate) fn new() -> Self {
            let genesis = Block {
                number: 0,
                parent: None,
                hash: BlockHash::default(),
            };
            let genesis_hash = genesis.hash;
            let mut tree = HashMap::new();
            tree.insert(genesis.hash, genesis);

            Chain {
                tree,
                best_finalized: genesis_hash,
                longest_chain: genesis_hash,
            }
        }

        fn update_longest_chain(&mut self, candidate_hash: BlockHash) {
            let old_longest = self
                .tree
                .get(&self.longest_chain)
                .expect("Longest chain block not in tree.");
            let cand_longest = self
                .tree
                .get(&candidate_hash)
                .expect("Candidate block not in tree.");
            if cand_longest.number > old_longest.number
                || (cand_longest.number == old_longest.number
                    && cand_longest.hash < old_longest.hash)
            {
                self.longest_chain = candidate_hash;
            }
        }

        pub(crate) fn import_block(&mut self, block_hash: BlockHash, parent_hash: BlockHash) {
            let parent_block = self
                .tree
                .get(&parent_hash)
                .expect("Parent not in block tree.");
            assert!(
                !self.tree.contains_key(&block_hash),
                "The imported block is already in the tree."
            );
            let block_number = parent_block.number + 1;
            let new_block = Block {
                number: block_number,
                parent: Some(parent_hash),
                hash: block_hash,
            };
            self.tree.insert(block_hash, new_block);
            self.update_longest_chain(block_hash);
        }

        pub(crate) fn import_branch(&mut self, base: BlockHash, branch: Vec<BlockHash>) {
            let mut parent = base;
            for block_hash in branch {
                self.import_block(block_hash, parent);
                parent = block_hash;
            }
        }

        fn is_descendant(&self, child: &BlockHash, parent: &BlockHash) -> bool {
            // Equal or strict descendant returns true.
            // Panics if either block does not exist.
            let mut child_block = self.tree.get(child).expect("Child block not in tree.");
            let parent_block = self.tree.get(parent).expect("Parent block not in tree.");
            if child_block.number < parent_block.number {
                return false;
            }
            while child_block.number > parent_block.number {
                child_block = self.tree.get(&child_block.parent.unwrap()).unwrap();
            }
            child_block.hash == parent_block.hash
        }

        fn block(&self, h: &BlockHash) -> Option<Block> {
            self.tree.get(h).copied()
        }

        fn best_block(&self) -> BlockHash {
            self.longest_chain
        }

        fn best_finalized(&self) -> BlockHash {
            self.best_finalized
        }

        fn finalize(&mut self, h: BlockHash) -> Vec<Block> {
            // Will panic if h does not exist or we finalize a block that is not a strict descendant of best_finalized
            // Outputs a vector of newly finalized blocks.
            assert!(
                self.tree.contains_key(&h),
                "Block to finalize does not exist"
            );
            assert!(
                self.is_descendant(&h, &self.best_finalized),
                "Block to finalize is not a descendant of the last finalized block."
            );
            assert!(
                h != self.best_finalized,
                "Block to finalize is equal to the last finalized block."
            );
            let mut list_finalized = Vec::new();

            let mut block = self.tree.get(&h).unwrap();
            while block.hash != self.best_finalized {
                list_finalized.push(*block);
                block = self.tree.get(&block.parent.unwrap()).unwrap();
            }
            self.best_finalized = h;
            list_finalized.reverse();
            list_finalized
        }
    }
    type Units = Arc<Mutex<HashMap<(Round, NodeIndex), Unit<BlockHash, Hash>>>>;

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
        pub(crate) fn consensus_data(&self, node_id: NodeId) -> (Out, In) {
            let stream;
            {
                let (tx, rx) = unbounded_channel();
                stream = BcastStream(rx);
                self.senders.lock().push((node_id, tx));
            }
            let sink = Box::new(BcastSink {
                node_id,
                senders: self.senders.clone(),
                units: self.units.clone(),
            });

            (sink, Box::new(stream))
        }
    }

    type Sender = (NodeId, UnboundedSender<NotificationIn<BlockHash, Hash>>);
    type Senders = Arc<Mutex<Vec<Sender>>>;

    #[derive(Clone)]
    struct BcastSink {
        node_id: NodeId,
        senders: Senders,
        units: Units,
    }

    impl BcastSink {
        fn do_send(&self, msg: NotificationIn<BlockHash, Hash>, recipient: &Sender) {
            let (_node_id, tx) = recipient;
            let _ = tx.send(msg);
        }
        fn send_to_all(&self, msg: NotificationIn<BlockHash, Hash>) {
            self.senders
                .lock()
                .iter()
                .for_each(|r| self.do_send(msg.clone(), r));
        }
        fn send_to_peer(&self, msg: NotificationIn<BlockHash, Hash>, peer: NodeId) {
            let _ = self.senders.lock().iter().for_each(|r| {
                if r.0 == peer {
                    self.do_send(msg.clone(), r);
                }
            });
        }
    }

    impl Sink<NotificationOut<BlockHash, Hash>> for BcastSink {
        type Error = Error;
        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(
            self: Pin<&mut Self>,
            m: NotificationOut<BlockHash, Hash>,
        ) -> Result<(), Self::Error> {
            match m {
                NotificationOut::CreatedUnit(u) => {
                    let coord = (u.round(), u.creator());
                    self.units.lock().insert(coord, u.clone());
                    self.send_to_all(NotificationIn::NewUnit(u));
                }
                NotificationOut::MissingUnits(coords, _aux_data) => {
                    let units: Vec<Unit<BlockHash, Hash>> = coords
                        .iter()
                        .map(|coord| self.units.lock().get(coord).cloned().unwrap())
                        .collect();
                    for u in units {
                        let response = NotificationIn::NewUnit(u);
                        self.send_to_peer(response, self.node_id);
                    }
                }
            }
            Ok(())
        }
    }

    struct BcastStream(UnboundedReceiver<NotificationIn<BlockHash, Hash>>);

    impl Stream for BcastStream {
        type Item = NotificationIn<BlockHash, Hash>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            // here we may add custom logic for dropping/changing messages
            self.0.poll_recv(cx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::environment::*;
    use crate::{NotificationIn, NotificationOut, Unit};
    use futures::{sink::SinkExt, stream::StreamExt};

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn comm() {
        let n = Network::new();
        let (mut out0, mut in0) = n.consensus_data(NodeId(0));
        let (mut out1, mut in1) = n.consensus_data(NodeId(1));
        let u0 = Unit {
            creator: 0.into(),
            ..Unit::default()
        };

        let u = u0.clone();
        let h0 = tokio::spawn(async move {
            assert_eq!(in0.next().await.unwrap(), NotificationIn::NewUnit(u),);
        });

        let u = u0.clone();
        let h1 = tokio::spawn(async move {
            assert_eq!(in1.next().await.unwrap(), NotificationIn::NewUnit(u));
        });

        assert!(out0
            .send(NotificationOut::CreatedUnit(u0.clone()))
            .await
            .is_ok());
        assert!(out1.send(NotificationOut::CreatedUnit(u0)).await.is_ok());
        assert!(h0.await.is_ok());
        assert!(h1.await.is_ok());
    }
}
