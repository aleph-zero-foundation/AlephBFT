use futures::{
    channel::{mpsc::unbounded, oneshot},
    future::ready,
    stream::iter,
    Future, FutureExt, Stream, StreamExt,
};
use log::warn;
use std::iter::repeat;

use crate::{Receiver, Sender};

pub(crate) fn into_infinite_stream<F: Future>(f: F) -> impl Stream<Item = ()> {
    f.then(|_| ready(iter(repeat(())))).flatten_stream()
}

pub(crate) struct Barrier {
    barriers: Vec<(oneshot::Sender<()>, String)>,
    pre_barrier: Receiver<()>,
    pre_barrier_tx: Sender<()>,
}

impl Barrier {
    pub(crate) fn new() -> Self {
        let (pre_barrier_tx, pre_barrier) = unbounded();
        Self {
            barriers: Vec::new(),
            pre_barrier,
            pre_barrier_tx,
        }
    }

    pub(crate) fn clone(&mut self, name: &str) -> SubBarrier {
        let (sub_barrier, its_barrier) =
            SubBarrier::new(name.to_string(), self.pre_barrier_tx.clone());
        self.barriers.push((its_barrier, name.to_string()));
        sub_barrier
    }

    pub(crate) async fn wait(mut self) {
        // wait till each task reaches its barrier
        for _ in self.barriers.iter() {
            if self.pre_barrier.next().await.is_none() {
                warn!(target: "AlephBFT-barrier", "some-task: exit-pre-barrier already dropped." );
            }
        }
        // let all tasks know that other tasks already reached the barrier
        for (barrier, name) in self.barriers {
            if barrier.send(()).is_err() {
                warn!(target: "AlephBFT-runway", "{:?}-task: exit-barrier already dropped.", name);
            }
        }
    }
}

pub(crate) struct SubBarrier {
    name: String,
    pre_barrier: Sender<()>,
    barrier: oneshot::Receiver<()>,
}

impl SubBarrier {
    fn new(name: String, pre_barrier: Sender<()>) -> (Self, oneshot::Sender<()>) {
        let (barrier_tx, barrier_rx) = oneshot::channel();
        (
            Self {
                name,
                pre_barrier,
                barrier: barrier_rx,
            },
            barrier_tx,
        )
    }

    pub(crate) async fn wait(self) {
        if self.pre_barrier.unbounded_send(()).is_err() {
            warn!(target: "AlephBFT-barrier", "{:?}-pre-barrier already dropped.", self.name);
        }
        if self.barrier.await.is_err() {
            warn!(target: "AlephBFT-barrier", "{:?}-exit-barrier already dropped.", self.name);
        }
    }
}
