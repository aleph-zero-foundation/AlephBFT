use crate::{dag::DagUnit, MultiKeychain};

mod election;
mod extender;
mod units;

use aleph_bft_types::UnitFinalizationHandler;
use extender::Extender;

/// A struct responsible for executing the Consensus protocol on a local copy of the Dag.
/// It receives units which are guaranteed to eventually appear in the Dags
/// of all honest nodes. The static Aleph Consensus algorithm is then run on this Dag in order
/// to finalize subsequent rounds of the Dag. More specifically whenever a new unit is received
/// this process checks whether a new round can be finalized and if so, it computes the batch of
/// units that should be finalized, and uses the finalization handler to report that to the user.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/internals.html
/// Section 5.4 for a discussion of this component.
pub struct Ordering<MK: MultiKeychain, UFH: UnitFinalizationHandler> {
    extender: Extender<DagUnit<UFH::Hasher, UFH::Data, MK>>,
    finalization_handler: UFH,
}

impl<MK: MultiKeychain, UFH: UnitFinalizationHandler> Ordering<MK, UFH> {
    pub fn new(finalization_handler: UFH) -> Self {
        let extender = Extender::new();
        Ordering {
            extender,
            finalization_handler,
        }
    }

    pub fn add_unit(&mut self, unit: DagUnit<UFH::Hasher, UFH::Data, MK>) {
        for batch in self.extender.add_unit(unit) {
            self.finalization_handler
                .batch_finalized(batch.into_iter().map(|unit| unit.into()).collect());
        }
    }
}
