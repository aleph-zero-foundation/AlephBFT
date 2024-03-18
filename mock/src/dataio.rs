use aleph_bft_types::{DataProvider as DataProviderT, FinalizationHandler as FinalizationHandlerT};
use async_trait::async_trait;
use codec::{Decode, Encode};
use futures::{channel::mpsc::unbounded, future::pending, AsyncWrite};
use log::error;
use parking_lot::Mutex;
use std::{
    io::{self},
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

type Receiver<T> = futures::channel::mpsc::UnboundedReceiver<T>;
type Sender<T> = futures::channel::mpsc::UnboundedSender<T>;

pub type Data = u32;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct DataProvider {
    counter: usize,
    n_data: Option<usize>,
}

impl DataProvider {
    pub fn new() -> Self {
        Self {
            counter: 0,
            n_data: None,
        }
    }

    pub fn new_finite(n_data: usize) -> Self {
        Self {
            counter: 0,
            n_data: Some(n_data),
        }
    }
}

#[async_trait]
impl DataProviderT<Data> for DataProvider {
    async fn get_data(&mut self) -> Option<Data> {
        self.counter += 1;
        if let Some(n_data) = self.n_data {
            if n_data < self.counter {
                return None;
            }
        }
        Some(self.counter as u32)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Decode, Encode)]
pub struct StalledDataProvider {}

impl StalledDataProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl DataProviderT<Data> for StalledDataProvider {
    async fn get_data(&mut self) -> Option<Data> {
        pending().await
    }
}

#[derive(Clone, Debug)]
pub struct FinalizationHandler {
    tx: Sender<Data>,
}

impl FinalizationHandlerT<Data> for FinalizationHandler {
    fn data_finalized(&mut self, d: Data) {
        if let Err(e) = self.tx.unbounded_send(d) {
            error!(target: "finalization-handler", "Error when sending data from FinalizationHandler {:?}.", e);
        }
    }
}

impl FinalizationHandler {
    pub fn new() -> (Self, Receiver<Data>) {
        let (tx, rx) = unbounded();

        (Self { tx }, rx)
    }
}

#[derive(Clone, Debug, Default)]
pub struct Saver {
    data: Arc<Mutex<Vec<u8>>>,
}

impl Saver {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(vec![])),
        }
    }
}

impl AsyncWrite for Saver {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.data.lock().extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl From<Arc<Mutex<Vec<u8>>>> for Saver {
    fn from(data: Arc<Mutex<Vec<u8>>>) -> Self {
        Self { data }
    }
}

pub type Loader = futures::io::Cursor<Vec<u8>>;
