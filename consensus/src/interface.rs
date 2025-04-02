use crate::{
    Data, DataProvider, FinalizationHandler, Hasher, OrderedUnit, UnitFinalizationHandler,
};
use futures::{AsyncRead, AsyncWrite};
use std::marker::PhantomData;

/// This adapter allows to map an implementation of [`FinalizationHandler`] onto implementation of [`UnitFinalizationHandler`].
pub struct FinalizationHandlerAdapter<FH, D, H> {
    finalization_handler: FH,
    _phantom: PhantomData<(D, H)>,
}

impl<FH, D, H> From<FH> for FinalizationHandlerAdapter<FH, D, H> {
    fn from(value: FH) -> Self {
        Self {
            finalization_handler: value,
            _phantom: PhantomData,
        }
    }
}

impl<D: Data, H: Hasher, FH: FinalizationHandler<D>> UnitFinalizationHandler
    for FinalizationHandlerAdapter<FH, D, H>
{
    type Data = D;
    type Hasher = H;

    fn batch_finalized(&mut self, batch: Vec<OrderedUnit<Self::Data, Self::Hasher>>) {
        for unit in batch {
            if let Some(data) = unit.data {
                self.finalization_handler.data_finalized(data)
            }
        }
    }
}

/// The local interface of the consensus algorithm. Contains a [`DataProvider`] as a source of data
/// to order, a [`UnitFinalizationHandler`] for handling ordered units, and a pair of read/write
/// structs intended for saving and restorin the state of the algorithm within the session, as a
/// contingency in the case of a crash.
#[derive(Clone)]
pub struct LocalIO<DP: DataProvider, UFH: UnitFinalizationHandler, US: AsyncWrite, UL: AsyncRead> {
    data_provider: DP,
    finalization_handler: UFH,
    unit_saver: US,
    unit_loader: UL,
}

impl<
        H: Hasher,
        DP: DataProvider,
        FH: FinalizationHandler<DP::Output>,
        US: AsyncWrite,
        UL: AsyncRead,
    > LocalIO<DP, FinalizationHandlerAdapter<FH, DP::Output, H>, US, UL>
{
    /// Create a new local interface. Note that this uses the simplified, and recommended,
    /// finalization handler that only deals with ordered data.
    pub fn new(
        data_provider: DP,
        finalization_handler: FH,
        unit_saver: US,
        unit_loader: UL,
    ) -> Self {
        Self {
            data_provider,
            finalization_handler: finalization_handler.into(),
            unit_saver,
            unit_loader,
        }
    }
}

impl<DP: DataProvider, UFH: UnitFinalizationHandler, US: AsyncWrite, UL: AsyncRead>
    LocalIO<DP, UFH, US, UL>
{
    /// Create a new local interface, providing a full implementation of a
    /// [`UnitFinalizationHandler`].Implementing [`UnitFinalizationHandler`] directly is more
    /// complex, and should be unnecessary for most usecases. Implement [`FinalizationHandler`]
    /// and use `new` instead, unless you absolutely know what you are doing.
    pub fn new_with_unit_finalization_handler(
        data_provider: DP,
        finalization_handler: UFH,
        unit_saver: US,
        unit_loader: UL,
    ) -> Self {
        Self {
            data_provider,
            finalization_handler,
            unit_saver,
            unit_loader,
        }
    }

    /// Disassemble the interface into components.
    pub fn into_components(self) -> (DP, UFH, US, UL) {
        let LocalIO {
            data_provider,
            finalization_handler,
            unit_saver,
            unit_loader,
        } = self;
        (data_provider, finalization_handler, unit_saver, unit_loader)
    }
}
