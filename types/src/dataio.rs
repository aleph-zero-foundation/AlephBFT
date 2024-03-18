use async_trait::async_trait;

use crate::NodeIndex;
use crate::Round;

/// The source of data items that consensus should order.
///
/// AlephBFT internally calls [`DataProvider::get_data`] whenever a new unit is created and data needs to be placed inside.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/aleph_bft_api.html for a discussion
/// and examples of how this trait can be implemented.
#[async_trait]
pub trait DataProvider<Data>: Sync + Send + 'static {
    /// Outputs a new data item to be ordered
    async fn get_data(&mut self) -> Option<Data>;
}

/// The source of finalization of the units that consensus produces.
///
/// The [`FinalizationHandler::data_finalized`] method is called whenever a piece of data input to the algorithm
/// using [`DataProvider::get_data`] has been finalized, in order of finalization.
pub trait FinalizationHandler<Data>: Sync + Send + 'static {
    /// Data, provided by [DataProvider::get_data], has been finalized.
    /// The calls to this function follow the order of finalization.
    fn data_finalized(&mut self, data: Data);
    /// A unit has been finalized. You can overwrite the default implementation for advanced finalization handling
    /// in which case the method [`FinalizationHandler::data_finalized`] will not be called anymore if a unit is finalized.
    /// Please note that this interface is less stable as it exposes intrinsics which migh be subject to change.
    /// Do not implement this method and only implement [`FinalizationHandler::data_finalized`] unless you
    /// absolutely know what you are doing.
    fn unit_finalized(&mut self, _creator: NodeIndex, _round: Round, data: Option<Data>) {
        if let Some(d) = data {
            self.data_finalized(d);
        }
    }
}
