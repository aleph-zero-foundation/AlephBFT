use futures::{channel::oneshot, future::ready, stream::iter, Future, FutureExt, Stream};
use log::warn;
use std::iter::repeat;

pub(crate) fn into_infinite_stream<F: Future>(f: F) -> impl Stream<Item = ()> {
    f.then(|_| ready(iter(repeat(())))).flatten_stream()
}

pub(crate) struct Barrier {
    barriers: Vec<(oneshot::Receiver<()>, oneshot::Sender<()>, String)>,
}

impl Barrier {
    pub(crate) fn new() -> Self {
        Self {
            barriers: Vec::new(),
        }
    }

    pub(crate) fn clone(&mut self, name: &str) -> SubBarrier {
        let (sub_barrier, its_pre_barrier, its_barrier) = SubBarrier::new(name.to_string());
        self.barriers
            .push((its_pre_barrier, its_barrier, name.to_string()));
        sub_barrier
    }

    pub(crate) async fn wait(self) {
        let (pre_barriers, barriers): (
            Vec<(oneshot::Receiver<()>, String)>,
            Vec<(oneshot::Sender<()>, String)>,
        ) = self
            .barriers
            .into_iter()
            .map(|(pre_barrier, barrier, name)| ((pre_barrier, name.clone()), (barrier, name)))
            .unzip();

        // wait till each task reaches its barrier
        for (pre_barrier, name) in pre_barriers {
            if pre_barrier.await.is_err() {
                warn!(target: "AlephBFT-barrier", "{:?}-task: pre-exit-barrier already dropped.", name);
            }
        }
        // let all tasks know that other tasks already reached the barrier
        for (barrier, name) in barriers {
            if barrier.send(()).is_err() {
                warn!(target: "AlephBFT-runway", "{:?}-task: exit-barrier already dropped.", name);
            }
        }
    }
}

pub(crate) struct SubBarrier {
    name: String,
    pre_barrier: oneshot::Sender<()>,
    barrier: oneshot::Receiver<()>,
}

impl SubBarrier {
    fn new(name: String) -> (Self, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (pre_barrier_tx, pre_barrier_rx) = oneshot::channel();
        let (barrier_tx, barrier_rx) = oneshot::channel();
        (
            Self {
                name,
                pre_barrier: pre_barrier_tx,
                barrier: barrier_rx,
            },
            pre_barrier_rx,
            barrier_tx,
        )
    }

    pub(crate) async fn wait(self) {
        if self.pre_barrier.send(()).is_err() {
            warn!(target: "AlephBFT-barrier", "{:?}-pre-barrier already dropped.", self.name);
        }
        if self.barrier.await.is_err() {
            warn!(target: "AlephBFT-barrier", "{:?}-exit-barrier already dropped.", self.name);
        }
    }
}
