use aleph_bft_types::{
    DataProvider as DataProviderT, FinalizationHandler as FinalizationHandlerT, NodeIndex,
};
use async_trait::async_trait;
use futures::channel::mpsc::unbounded;
use log::{error, info};

type Receiver<T> = futures::channel::mpsc::UnboundedReceiver<T>;
type Sender<T> = futures::channel::mpsc::UnboundedSender<T>;

pub type Data = (NodeIndex, Option<u32>);

#[derive(Default)]
pub struct DataProvider {
    id: NodeIndex,
    counter: u32,
    n_data: u32,
}

impl DataProvider {
    pub fn new(id: NodeIndex, counter: u32, n_data: u32) -> Self {
        Self {
            id,
            counter,
            n_data,
        }
    }
}

#[async_trait]
impl DataProviderT<Data> for DataProvider {
    async fn get_data(&mut self) -> Data {
        if self.n_data == 0 {
            info!("Providing empty data");
            (self.id, None)
        } else {
            let data = (self.id, Some(self.counter));
            info!("Providing data: {}", self.counter);
            self.counter += 1;
            self.n_data -= 1;
            data
        }
    }
}

pub struct FinalizationHandler {
    tx: Sender<Data>,
}

#[async_trait]
impl FinalizationHandlerT<Data> for FinalizationHandler {
    async fn data_finalized(&mut self, d: Data) {
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
