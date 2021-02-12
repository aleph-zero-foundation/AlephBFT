#[cfg(test)]
pub mod environment {
    use crate::skeleton::Message;
    use derive_more::Display;
    use futures::{Sink, Stream};
    use parking_lot::Mutex;
    use serde::{Deserialize, Serialize};
    use std::{
        collections::BTreeMap,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };
    use tokio::sync::{broadcast::*, mpsc};

    type Error = ();

    #[derive(Hash, Debug, Display, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
    pub struct Id(pub u32);

    #[derive(
        Hash,
        Debug,
        Default,
        Display,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Ord,
        PartialOrd,
        Serialize,
        Deserialize,
    )]
    pub struct Hash(pub u32);

    impl From<u32> for Hash {
        fn from(h: u32) -> Self {
            Hash(h)
        }
    }

    #[derive(
        Hash,
        Debug,
        Default,
        Display,
        Clone,
        Copy,
        PartialEq,
        Eq,
        Ord,
        PartialOrd,
        Serialize,
        Deserialize,
    )]
    pub struct BlockHash(pub u32);

    impl From<u32> for BlockHash {
        fn from(h: u32) -> Self {
            BlockHash(h)
        }
    }

    type Out = Box<dyn Sink<Message<BlockHash, Hash>, Error = Error> + Send + Unpin>;
    type In = Box<dyn Stream<Item = Message<BlockHash, Hash>> + Send + Unpin>;

    #[derive(Clone)]
    pub(crate) struct Environment {
        chain: Arc<Mutex<Chain>>,
        network: Network,
        finalized_notifier: mpsc::UnboundedSender<Block>,
        calls_to_finalize: Vec<BlockHash>,
    }

    impl Environment {
        pub(crate) fn new(network: Network) -> (Self, mpsc::UnboundedReceiver<Block>) {
            let (finalized_tx, finalized_rx) = mpsc::unbounded_channel();

            (
                Environment {
                    chain: Arc::new(Mutex::new(Chain::new())),
                    network,
                    finalized_notifier: finalized_tx,
                    calls_to_finalize: Vec::new(),
                },
                finalized_rx,
            )
        }

        pub(crate) fn get_log_finalize_calls(&self) -> Vec<BlockHash> {
            self.calls_to_finalize.clone()
        }

        pub(crate) fn gen_chain(&self, branches: Vec<(BlockHash, Vec<BlockHash>)>) {
            for (h, branch) in branches {
                self.chain.lock().import_blocks(h, branch);
            }
        }

        pub(crate) fn import_block(&self, block: BlockHash, parent: BlockHash) {
            self.chain.lock().import_blocks(parent, vec![block]);
        }
    }

    impl crate::traits::Environment for Environment {
        type NodeId = Id;
        type Hash = Hash;
        type BlockHash = BlockHash;
        type InstanceId = u32;
        type Crypto = ();
        type Error = Error;
        type Out = Out;
        type In = In;

        fn finalize_block(&mut self, h: Self::BlockHash) {
            self.calls_to_finalize.push(h);
            let mut chain = self.chain.lock();
            let last_finalized = chain.best_finalized();
            if !chain.is_descendant(&h, &last_finalized) {
                return;
            }
            chain.finalize(h);
            let mut new_finalized = h;
            while new_finalized != last_finalized {
                let block = chain.block(&new_finalized).unwrap();
                new_finalized = block.parent.unwrap();
                let _ = self.finalized_notifier.send(block);
            }
        }

        fn check_extends_finalized(&self, h: Self::BlockHash) -> bool {
            let chain = self.chain.lock();
            let last_finalized = chain.best_finalized();
            h != last_finalized && chain.is_descendant(&h, &last_finalized)
        }

        fn best_block(&self) -> Self::BlockHash {
            self.chain.lock().best_block()
        }

        fn crypto(&self) -> Self::Crypto {}

        fn hash(_data: &[u8]) -> Self::Hash {
            Default::default()
        }

        fn consensus_data(&self) -> (Self::Out, Self::In) {
            self.network.consensus_data()
        }
    }

    #[derive(Copy, Clone)]
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
    struct Chain {
        tree: BTreeMap<BlockHash, Block>,
        leaves: Vec<BlockHash>,
        best_finalized: BlockHash,
    }

    impl Chain {
        fn new() -> Self {
            let genesis = Block {
                number: 0,
                parent: None,
                hash: BlockHash::default(),
            };
            let genesis_hash = genesis.hash;
            let mut tree = BTreeMap::new();
            tree.insert(genesis.hash, genesis);
            let leaves = vec![genesis_hash];

            Chain {
                tree,
                leaves,
                best_finalized: genesis_hash,
            }
        }

        fn import_blocks(&mut self, base: BlockHash, branch: Vec<BlockHash>) {
            if branch.is_empty() || !self.tree.contains_key(&base) {
                return;
            }

            let base = *self.tree.get(&base).unwrap();
            let pos = match self
                .leaves
                .binary_search_by(|leaf| self.tree.get(leaf).unwrap().number.cmp(&base.number))
            {
                Ok(pos) => pos,
                Err(_) => return,
            };
            self.leaves.remove(pos);

            let new_leaf_hash = *branch.last().unwrap();
            let mut parent_hash = base.hash;
            let mut number = base.number + 1;
            for block_hash in branch {
                let block = Block {
                    number,
                    parent: Some(parent_hash),
                    hash: block_hash,
                };
                number += 1;
                parent_hash = block.hash;
                self.tree.insert(block.hash, block);
            }

            let pos = match self
                .leaves
                .binary_search_by(|leaf| self.tree.get(leaf).unwrap().number.cmp(&number))
            {
                Ok(pos) => pos,
                Err(pos) => pos,
            };

            self.leaves.insert(pos, new_leaf_hash);
        }

        fn is_descendant(&self, child: &BlockHash, parent: &BlockHash) -> bool {
            let mut child = match self.tree.get(child) {
                Some(block) => block,
                None => return false,
            };
            let parent = match self.tree.get(parent) {
                Some(block) => block,
                None => return false,
            };
            if child.number < parent.number {
                return true;
            }
            while child.number > parent.number {
                child = &*self.tree.get(&child.parent.unwrap()).unwrap();
            }

            child.hash == parent.hash
        }

        fn block(&self, h: &BlockHash) -> Option<Block> {
            self.tree.get(h).copied()
        }

        fn best_block(&self) -> BlockHash {
            *self.leaves.last().unwrap()
        }

        fn best_finalized(&self) -> BlockHash {
            self.best_finalized
        }

        fn finalize(&mut self, h: BlockHash) -> bool {
            if !self.tree.contains_key(&h) {
                return false;
            }

            let finalized = self.tree.get(&self.best_finalized).unwrap();
            let candidate = self.tree.get(&h).unwrap();
            if candidate.number < finalized.number {
                return false;
            }

            self.best_finalized = h;

            true
        }
    }

    #[derive(Clone)]
    pub(crate) struct Network {
        sender: Sender<Message<BlockHash, Hash>>,
    }

    impl Network {
        pub(crate) fn new(capacity: usize) -> Self {
            let (sender, _) = channel(capacity);

            Network { sender }
        }
        pub(crate) fn consensus_data(&self) -> (Out, In) {
            let sink = Box::new(BSink(self.sender.clone()));
            let stream = Box::new(BStream(self.sender.subscribe()));

            (sink, stream)
        }
    }

    struct BSink(Sender<Message<BlockHash, Hash>>);

    impl Sink<Message<BlockHash, Hash>> for BSink {
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
            m: Message<BlockHash, Hash>,
        ) -> Result<(), Self::Error> {
            self.0.send(m).map(|_r| ()).map_err(|_e| {})
        }
    }

    struct BStream(Receiver<Message<BlockHash, Hash>>);

    impl BStream {}

    impl Stream for BStream {
        type Item = Message<BlockHash, Hash>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let mut f = Box::pin(self.0.recv());
            match futures::Future::poll(Pin::as_mut(&mut f), cx) {
                // here we may add custom logic for dropping/changing messages
                Poll::Ready(Ok(m)) => Poll::Ready(Some(m)),
                Poll::Ready(Err(_)) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    use crate::skeleton::{Consensus, ConsensusConfig};
    pub(crate) async fn dispatch_nodes(
        n_nodes: u32,
        conf: ConsensusConfig,
        env: Environment,
        check: Box<dyn FnOnce() + Send + Sync + 'static>,
    ) {
        let mut handles = vec![];
        for ix in 0..n_nodes {
            let mut conf = conf.clone();
            conf.ix = ix.into();
            handles.push(tokio::spawn(
                Consensus::new(conf.clone(), env.clone()).run(),
            ));
        }

        check();

        // TODO this returns immediately since we don't join in consensus, add exit signal
        for h in handles {
            let _ = h.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::environment::*;
    use crate::skeleton::Message;
    use futures::{sink::SinkExt, stream::StreamExt};

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn comm() {
        let n_members = 2;
        let rounds = 1;
        let capacity = n_members * rounds;
        let n = Network::new(capacity);
        let (mut out0, mut in0) = n.consensus_data();
        let (mut out1, mut in1) = n.consensus_data();

        let h0 = tokio::spawn(async move {
            assert_eq!(
                in0.next().await.unwrap(),
                Message::FetchRequest(vec![], 0.into()),
            );
            assert_eq!(
                in0.next().await.unwrap(),
                Message::FetchRequest(vec![], 1.into()),
            );
        });

        let h1 = tokio::spawn(async move {
            assert_eq!(
                in1.next().await.unwrap(),
                Message::FetchRequest(vec![], 0.into()),
            );
            assert_eq!(
                in1.next().await.unwrap(),
                Message::FetchRequest(vec![], 1.into()),
            );
        });

        assert!(out0
            .send(Message::FetchRequest(vec![], 0.into()))
            .await
            .is_ok());
        assert!(out1
            .send(Message::FetchRequest(vec![], 1.into()))
            .await
            .is_ok());
        assert!(h0.await.is_ok());
        assert!(h1.await.is_ok());
    }
}
