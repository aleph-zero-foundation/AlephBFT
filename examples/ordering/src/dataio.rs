use aleph_bft_types::{
    DataProvider as DataProviderT, FinalizationHandler as FinalizationHandlerT, NodeIndex,
};
use async_trait::async_trait;
use codec::{Decode, Encode};
use futures::{channel::mpsc::unbounded, future::pending};
use log::{error, info};

type Receiver<T> = futures::channel::mpsc::UnboundedReceiver<T>;
type Sender<T> = futures::channel::mpsc::UnboundedSender<T>;

pub type Data = (NodeIndex, u32);

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default, Decode, Encode)]
pub struct DataProvider {
    id: NodeIndex,
    starting_data_item: u32,
    data_items: u32,
    current_data: u32,
    stalled: bool,
}

impl DataProvider {
    pub fn new(id: NodeIndex, starting_data_item: u32, data_items: u32, stalled: bool) -> Self {
        Self {
            id,
            starting_data_item,
            current_data: starting_data_item,
            data_items,
            stalled,
        }
    }
}

#[async_trait]
impl DataProviderT for DataProvider {
    type Output = Data;

    async fn get_data(&mut self) -> Option<Data> {
        if self.starting_data_item + self.data_items == self.current_data {
            if self.stalled {
                info!("Awaiting DataProvider::get_data forever");
                pending::<()>().await;
            }
            info!("Providing None");
            None
        } else {
            let data = (self.id, self.current_data);
            info!("Providing data: {}", self.current_data);
            self.current_data += 1;
            Some(data)
        }
    }
}

#[derive(Clone)]
pub struct FinalizationHandler {
    tx: Sender<Data>,
}

impl FinalizationHandlerT<Data> for FinalizationHandler {
    fn data_finalized(&mut self, data: Data) {
        if let Err(e) = self.tx.unbounded_send(data) {
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
