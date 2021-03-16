#[cfg(test)]
pub mod environment {
    use crate::{Hashing, MyIndex, NodeIndex, NotificationIn, NotificationOut, Round, Unit};
    use codec::{Decode, Encode, Error as CodecError, Input, Output};
    use derive_more::{Display, From, Into};
    use futures::{Future, Sink, Stream};
    use log::debug;
    use parking_lot::Mutex;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use tokio::time::{sleep, Duration};

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

    #[derive(
        Hash, Debug, Default, Display, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Encode, Decode,
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
        chain: Arc<Mutex<Chain>>,
        network: Network,
        finalized_notifier: UnboundedSender<Block>,
        calls_to_finalize: Arc<Mutex<Vec<BlockHash>>>,
    }

    impl Environment {
        pub(crate) fn new_with_chain(
            node_id: NodeId,
            network: Network,
            chain: Arc<Mutex<Chain>>,
        ) -> (Arc<Self>, UnboundedReceiver<Block>) {
            let (finalized_tx, finalized_rx) = unbounded_channel();
            (
                Arc::new(Environment {
                    node_id,
                    chain,
                    network,
                    finalized_notifier: finalized_tx,
                    calls_to_finalize: Arc::new(Mutex::new(Vec::new())),
                }),
                finalized_rx,
            )
        }

        pub(crate) fn new(
            node_id: NodeId,
            network: Network,
        ) -> (Arc<Self>, UnboundedReceiver<Block>) {
            let chain = Arc::new(Mutex::new(Chain::new()));
            Self::new_with_chain(node_id, network, chain)
        }

        pub(crate) fn get_log_finalize_calls(&self) -> Vec<BlockHash> {
            self.calls_to_finalize.lock().clone()
        }

        pub(crate) fn gen_chain(&self, branches: Vec<(BlockHash, Vec<BlockHash>)>) {
            let mut chain = self.chain.lock();
            for (h, branch) in branches {
                chain.import_branch(h, branch);
            }
        }

        pub(crate) fn import_block(&self, block: BlockHash, parent: BlockHash) {
            self.chain.lock().import_block(block, parent);
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

        fn finalize_block(&self, h: Self::BlockHash) {
            self.calls_to_finalize.lock().push(h);
            let finalized_blocks = self.chain.lock().finalize(h);
            for block in finalized_blocks {
                let _ = self.finalized_notifier.send(block);
            }
        }

        fn check_available(
            &self,
            h: Self::BlockHash,
        ) -> Box<dyn Future<Output = Result<(), Self::Error>> + Send + Sync + Unpin> {
            let mut chain = self.chain.lock();
            if chain.block(&h).is_some() {
                return Box::new(futures::future::ok(()));
            }
            debug!("{} Block {:?} not yet available.", self.node_id, h);
            let (tx, rx) = oneshot::channel();
            chain.add_observer(&h, tx);

            Box::new(Box::pin(async move { rx.await.map_err(|_| ()) }))
        }

        fn check_extends_finalized(&self, h: Self::BlockHash) -> bool {
            let chain = self.chain.lock();
            let last_finalized = chain.best_finalized();
            h != last_finalized && chain.is_descendant(&h, &last_finalized)
        }

        fn best_block(&self) -> Self::BlockHash {
            self.chain.lock().best_block()
        }

        fn consensus_data(&self) -> (Self::Out, Self::In) {
            self.network.consensus_data(self.node_id)
        }

        fn hashing() -> Hashing<Self::Hash> {
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
        pub(crate) tree: HashMap<BlockHash, Block>,
        best_finalized: BlockHash,
        longest_chain: BlockHash,
        to_notify: Arc<Mutex<HashMap<BlockHash, Vec<oneshot::Sender<()>>>>>,
        finalized_chain: Vec<BlockHash>,
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
            let finalized_chain = vec![genesis_hash];

            Chain {
                tree,
                best_finalized: genesis_hash,
                longest_chain: genesis_hash,
                to_notify: Arc::new(Mutex::new(HashMap::new())),
                finalized_chain,
            }
        }

        fn update_longest_chain(&mut self, candidate_hash: &BlockHash) {
            let old_longest = self
                .tree
                .get(&self.longest_chain)
                .expect("Longest chain block not in tree.");
            let cand_longest = self
                .tree
                .get(&candidate_hash)
                .expect("Candidate block not in tree.");
            if !self.is_descendant(&candidate_hash, &self.best_finalized) {
                return;
            }
            if cand_longest.number > old_longest.number
                || (cand_longest.number == old_longest.number
                    && cand_longest.hash < old_longest.hash)
            {
                self.longest_chain = *candidate_hash;
            }
        }

        fn update_longest_chain_on_finalize(&mut self) {
            if self.is_descendant(&self.longest_chain, &self.best_finalized) {
                return;
            }
            self.longest_chain = self.best_finalized;
            // below a borrow checker hack -- it should not affect performance much, though
            for (block_hash, _) in self.tree.clone().iter() {
                self.update_longest_chain(block_hash);
            }
        }

        fn add_observer(&mut self, block_hash: &BlockHash, tx: oneshot::Sender<()>) {
            self.to_notify
                .lock()
                .entry(*block_hash)
                .or_default()
                .push(tx);
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
            self.update_longest_chain(&block_hash);

            let mut to_notify = self.to_notify.lock();
            if let Some(mut notifiers) = to_notify.remove(&block_hash) {
                notifiers.drain(0..).for_each(|n| {
                    n.send(())
                        .expect("should send a notification that a block arrived")
                })
            };
        }

        pub(crate) fn import_branch(&mut self, base: BlockHash, branch: Vec<BlockHash>) {
            let mut parent = base;
            for block_hash in branch {
                self.import_block(block_hash, parent);
                parent = block_hash;
            }
        }

        pub(crate) fn is_descendant(&self, child: &BlockHash, parent: &BlockHash) -> bool {
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

        pub(crate) fn finalized_at(&self, height: usize) -> Option<BlockHash> {
            self.finalized_chain.get(height).copied()
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
            self.update_longest_chain_on_finalize();
            list_finalized.reverse();
            for block in &list_finalized {
                self.finalized_chain.push(block.hash());
            }
            list_finalized
        }

        // This grows the chain according to a random seeded expansion strategy. If two nodes
        // finalize the same blocks then their chains will grow the same way, with the only difference
        // being the time at which they see subsequent blocks (random, seeded independently).
        pub(crate) async fn grow_chain(
            growing_chain: Arc<Mutex<Chain>>,
            expected_delay_ms: u64,
            seed_delay: u64,
        ) {
            let mut rng_delay = StdRng::seed_from_u64(seed_delay);
            let mut rng_chain = StdRng::seed_from_u64(0);
            let mut tip_height: usize = 0;
            let mut last_block_num: u32 = 0;
            loop {
                let maybe_finalized_hash = growing_chain.lock().finalized_at(tip_height);
                if let Some(finalized_hash) = maybe_finalized_hash {
                    // Block at height tip_height has been finalized, need to generate a couple of blocks
                    for _ in 0..3 {
                        loop {
                            let parent_candidate = rng_chain.gen_range(0..(last_block_num + 1));
                            if growing_chain
                                .lock()
                                .is_descendant(&(parent_candidate.into()), &finalized_hash)
                            {
                                last_block_num += 1;
                                growing_chain
                                    .lock()
                                    .import_block(last_block_num.into(), parent_candidate.into());
                                break;
                            }
                        }
                        sleep(Duration::from_millis(
                            rng_delay.gen_range(0..2 * expected_delay_ms),
                        ))
                        .await;
                    }
                    tip_height += 1;
                }
                sleep(Duration::from_millis(
                    rng_delay.gen_range(0..2 * expected_delay_ms),
                ))
                .await;
            }
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
