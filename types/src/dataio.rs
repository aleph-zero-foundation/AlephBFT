use async_trait::async_trait;

/// The source of data items that consensus should order.
///
/// AlephBFT internally calls [`DataProvider::get_data`] whenever a new unit is created and data needs to be placed inside.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/aleph_bft_api.html for a discussion
/// and examples of how this trait can be implemented.
#[async_trait]
pub trait DataProvider<Data> {
    /// Outputs a new data item to be ordered
    async fn get_data(&mut self) -> Option<Data>;
}

/// The source of finalization of the units that consensus produces.
///
/// The [`FinalizationHandler::data_finalized`] method is called whenever a piece of data input to the algorithm
/// using [`DataProvider::get_data`] has been finalized, in order of finalization.
pub trait FinalizationHandler<Data> {
    /// Data, provided by [DataProvider::get_data], has been finalized.
    /// The calls to this function follow the order of finalization.
    fn data_finalized(&mut self, data: Data);
}
