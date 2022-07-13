use aleph_bft_types::{DataProvider as DataProviderT, FinalizationHandler as FinalizationHandlerT};
use async_trait::async_trait;
use futures::{channel::mpsc::unbounded, future::pending};
use log::error;
use parking_lot::Mutex;
use std::{
    io::{Cursor, Write},
    sync::Arc,
};

type Receiver<T> = futures::channel::mpsc::UnboundedReceiver<T>;
type Sender<T> = futures::channel::mpsc::UnboundedSender<T>;

pub type Data = u32;

#[derive(Default)]
pub struct DataProvider {
    counter: u32,
}

impl DataProvider {
    pub fn new() -> Self {
        Self { counter: 0 }
    }
}

#[async_trait]
impl DataProviderT<Data> for DataProvider {
    async fn get_data(&mut self) -> Data {
        self.counter += 1;
        self.counter
    }
}

#[derive(Default)]
pub struct StalledDataProvider {}

impl StalledDataProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl DataProviderT<Data> for StalledDataProvider {
    async fn get_data(&mut self) -> Data {
        pending().await
    }
}

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

pub struct Saver {
    data: Arc<Mutex<Vec<u8>>>,
}

impl Saver {
    pub fn new(data: Arc<Mutex<Vec<u8>>>) -> Self {
        Self { data }
    }
}

impl Write for Saver {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.data.lock().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

pub type Loader = Cursor<Vec<u8>>;
