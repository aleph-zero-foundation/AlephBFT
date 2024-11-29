use crate::{
    runway::{NewestUnitResponse, Salt},
    units::{UncheckedSignedUnit, UnitCoord},
    Data, Hasher, NodeIndex, Signature, UncheckedSigned,
};

mod responder;

pub use responder::Responder;

/// Possible requests for information from other nodes.
#[derive(Debug)]
pub enum Request<H: Hasher> {
    Coord(UnitCoord),
    Parents(H::Hash),
    NewestUnit(NodeIndex, Salt),
}

/// Responses to requests.
#[derive(Debug)]
pub enum Response<H: Hasher, D: Data, S: Signature> {
    Coord(UncheckedSignedUnit<H, D, S>),
    Parents(H::Hash, Vec<UncheckedSignedUnit<H, D, S>>),
    NewestUnit(UncheckedSigned<NewestUnitResponse<H, D, S>, S>),
}
