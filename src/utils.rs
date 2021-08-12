use futures::{channel::oneshot, future::ready, stream::iter, Future, FutureExt, Stream};
use std::{
    iter::repeat,
    mem::swap,
    sync::{atomic::AtomicUsize, Arc},
};

pub(crate) fn into_infinite_stream<F: Future>(f: F) -> impl Stream<Item = ()> {
    f.then(|_| ready(iter(repeat(())))).flatten_stream()
}

pub(crate) struct Barrier {
    counter: Arc<AtomicUsize>,
    limit: usize,
    prev: (oneshot::Sender<()>, oneshot::Receiver<()>),
    next: (oneshot::Sender<()>, oneshot::Receiver<()>),
}

impl Barrier {
    pub(crate) fn new(size: usize) -> Self {
        let prev = oneshot::channel();
        let next = oneshot::channel();
        Self {
            counter: Arc::new(AtomicUsize::new(1)),
            limit: size,
            prev,
            next,
        }
    }

    pub(crate) fn clone(&mut self) -> Self {
        let (new_tx, new_rx) = oneshot::channel();
        let (old_tx, old_rx) = oneshot::channel();
        let mut new_next = (new_tx, old_rx);
        swap(&mut new_next, &mut self.next);
        Self {
            next: new_next,
            prev: (old_tx, new_rx),
            limit: self.limit,
            counter: self.counter.clone(),
        }
    }

    pub(crate) async fn wait(self) {
        if self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            == self.limit
        {
            self.notify();
        } else {
            self.wait_and_forward().await;
        }
    }

    fn notify(self) {
        let _ = self.prev.0.send(());
        let _ = self.next.0.send(());
    }

    async fn wait_and_forward(self) {
        let _ = futures::select! {
            _ = self.next.1 => {
                self.prev.0.send(())
            },
            _ = self.prev.1 => {
                self.next.0.send(())
            },
        };
    }
}
