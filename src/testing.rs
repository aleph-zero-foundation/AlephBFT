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

    type Out = Box<dyn Sink<Message<Hash>, Error = Error> + Send + Unpin>;
    type In = Box<dyn Stream<Item = Message<Hash>> + Send + Unpin>;

    pub(crate) struct Environment {
        chain: Arc<Mutex<Chain>>,
        network: Network,
        finlized_notifier: mpsc::UnboundedSender<Block>,
    }

    impl Environment {
        pub(crate) fn new(network: Network) -> (Self, mpsc::UnboundedReceiver<Block>) {
            let (finalized_tx, finalized_rx) = mpsc::unbounded_channel();

            (
                Environment {
                    chain: Arc::new(Mutex::new(Chain::new())),
                    network,
                    finlized_notifier: finalized_tx,
                },
                finalized_rx,
            )
        }

        pub(crate) fn gen_chain(&self, branches: Vec<(Hash, Vec<Hash>)>) {
            for (h, branch) in branches {
                self.chain.lock().import_blocks(h, branch);
            }
        }
    }

    impl crate::traits::Environment for Environment {
        type NodeId = Id;
        type Hash = Hash;
        type InstanceId = u32;
        type Crypto = ();
        type Error = Error;
        type Out = Out;
        type In = In;

        fn finalize_block(&self, h: Self::Hash) {
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
                let _ = self.finlized_notifier.send(block);
            }
        }

        fn best_block(&self) -> Self::Hash {
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
        parent: Option<Hash>,
        hash: Hash,
    }

    impl Block {
        pub(crate) fn hash(&self) -> Hash {
            self.hash
        }
    }

    struct Chain {
        tree: BTreeMap<Hash, Block>,
        leaves: Vec<Hash>,
        best_finalized: Hash,
    }

    impl Chain {
        fn new() -> Self {
            let genesis = Block {
                number: 0,
                parent: None,
                hash: Hash::default(),
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

        fn import_blocks(&mut self, base: Hash, branch: Vec<Hash>) {
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

        fn is_descendant(&self, child: &Hash, parent: &Hash) -> bool {
            let mut child = match self.tree.get(child) {
                Some(block) => block.clone(),
                None => return false,
            };
            let parent = match self.tree.get(parent) {
                Some(block) => block.clone(),
                None => return false,
            };
            if child.number < parent.number {
                return true;
            }
            while child.number > parent.number {
                child = self.tree.get(&child.parent.unwrap()).unwrap().clone();
            }

            return child.hash == parent.hash;
        }

        fn block(&self, h: &Hash) -> Option<Block> {
            self.tree.get(h).map(|b| b.clone())
        }

        fn best_block(&self) -> Hash {
            self.leaves.last().unwrap().clone()
        }

        fn best_finalized(&self) -> Hash {
            self.best_finalized
        }

        fn finalize(&mut self, h: Hash) -> bool {
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
        sender: Sender<Message<Hash>>,
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

    struct BSink(Sender<Message<Hash>>);

    impl Sink<Message<Hash>> for BSink {
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

        fn start_send(self: Pin<&mut Self>, m: Message<Hash>) -> Result<(), Self::Error> {
            self.0.send(m).map(|_r| ()).map_err(|_e| return ())
        }
    }

    struct BStream(Receiver<Message<Hash>>);

    impl BStream {}

    impl Stream for BStream {
        type Item = Message<Hash>;
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
                Message::FetchRequest(vec![], 0.into())
            );
            assert_eq!(
                in0.next().await.unwrap(),
                Message::FetchRequest(vec![], 1.into())
            );
        });

        let h1 = tokio::spawn(async move {
            assert_eq!(
                in1.next().await.unwrap(),
                Message::FetchRequest(vec![], 0.into())
            );
            assert_eq!(
                in1.next().await.unwrap(),
                Message::FetchRequest(vec![], 1.into())
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
