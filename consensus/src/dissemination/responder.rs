use crate::{
    dag::{DagUnit, Request},
    dissemination::{DisseminationRequest, DisseminationResponse},
    runway::{NewestUnitResponse, Salt},
    units::{UnitCoord, UnitStore, UnitWithParents, WrappedUnit},
    Data, Hasher, MultiKeychain, NodeIndex, Signed,
};
use std::marker::PhantomData;
use thiserror::Error;

/// A responder that is able to answer requests for data about units.
pub struct Responder<H: Hasher, D: Data, MK: MultiKeychain> {
    keychain: MK,
    _phantom: PhantomData<(H, D)>,
}

/// Ways in which it can be impossible for us to respond to a request.
#[derive(Eq, Error, Debug, PartialEq)]
pub enum Error<H: Hasher> {
    #[error("no canonical unit at {0}")]
    NoCanonicalAt(UnitCoord),
    #[error("unit with hash {0:?} not known")]
    UnknownUnit(H::Hash),
}

impl<H: Hasher, D: Data, MK: MultiKeychain> Responder<H, D, MK> {
    /// Create a new responder.
    pub fn new(keychain: MK) -> Self {
        Responder {
            keychain,
            _phantom: PhantomData,
        }
    }

    fn index(&self) -> NodeIndex {
        self.keychain.index()
    }

    fn on_request_coord(
        &self,
        coord: UnitCoord,
        units: &UnitStore<DagUnit<H, D, MK>>,
    ) -> Result<DisseminationResponse<H, D, MK::Signature>, Error<H>> {
        units
            .canonical_unit(coord)
            .map(|unit| DisseminationResponse::Coord(unit.clone().unpack().into()))
            .ok_or(Error::NoCanonicalAt(coord))
    }

    fn on_request_parents(
        &self,
        hash: H::Hash,
        units: &UnitStore<DagUnit<H, D, MK>>,
    ) -> Result<DisseminationResponse<H, D, MK::Signature>, Error<H>> {
        units
            .unit(&hash)
            .map(|unit| {
                let parents = unit
                    .parents()
                    .map(|parent_hash| {
                        units
                            .unit(parent_hash)
                            .expect("Units are added to the store in order.")
                            .clone()
                            .unpack()
                            .into_unchecked()
                    })
                    .collect();
                DisseminationResponse::Parents(hash, parents)
            })
            .ok_or(Error::UnknownUnit(hash))
    }

    fn on_request_newest(
        &self,
        requester: NodeIndex,
        salt: Salt,
        units: &UnitStore<DagUnit<H, D, MK>>,
    ) -> DisseminationResponse<H, D, MK::Signature> {
        let unit = units
            .canonical_units(requester)
            .last()
            .map(|unit| unit.clone().unpack().into_unchecked());
        let response = NewestUnitResponse::new(requester, self.index(), unit, salt);

        let signed_response = Signed::sign(response, &self.keychain).into_unchecked();
        DisseminationResponse::NewestUnit(signed_response)
    }

    /// Handle an incoming request returning either the appropriate response or an error if we
    /// aren't able to help.
    pub fn handle_request(
        &self,
        request: DisseminationRequest<H>,
        units: &UnitStore<DagUnit<H, D, MK>>,
    ) -> Result<DisseminationResponse<H, D, MK::Signature>, Error<H>> {
        use DisseminationRequest::*;
        match request {
            Unit(unit_request) => match unit_request {
                Request::Coord(coord) => self.on_request_coord(coord, units),
                Request::ParentsOf(hash) => self.on_request_parents(hash, units),
            },
            NewestUnit(node_id, salt) => Ok(self.on_request_newest(node_id, salt, units)),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        dag::Request,
        dissemination::{
            responder::{Error, Responder},
            DisseminationRequest, DisseminationResponse,
        },
        units::{
            random_full_parent_reconstrusted_units_up_to, TestingDagUnit, Unit, UnitCoord,
            UnitStore, UnitWithParents, WrappedUnit,
        },
        NodeCount, NodeIndex,
    };
    use aleph_bft_mock::{Data, Hasher64, Keychain};
    use std::iter::zip;

    const NODE_ID: NodeIndex = NodeIndex(0);
    const NODE_COUNT: NodeCount = NodeCount(7);

    fn setup() -> (
        Responder<Hasher64, Data, Keychain>,
        UnitStore<TestingDagUnit>,
        Vec<Keychain>,
    ) {
        let keychains = Keychain::new_vec(NODE_COUNT);
        (
            Responder::new(keychains[NODE_ID.0]),
            UnitStore::new(NODE_COUNT),
            keychains,
        )
    }

    #[test]
    fn empty_fails_to_respond_to_coords() {
        let (responder, store, _) = setup();
        let coord = UnitCoord::new(0, NodeIndex(1));
        let request = Request::Coord(coord).into();
        match responder.handle_request(request, &store) {
            Ok(response) => panic!("Unexpected response: {:?}.", response),
            Err(err) => assert_eq!(err, Error::NoCanonicalAt(coord)),
        }
    }

    #[test]
    fn empty_fails_to_respond_to_parents() {
        let (responder, store, keychains) = setup();
        let session_id = 2137;
        let hash =
            random_full_parent_reconstrusted_units_up_to(1, NODE_COUNT, session_id, &keychains)
                .last()
                .expect("just created this round")
                .last()
                .expect("the round has at least one unit")
                .hash();
        let request = Request::ParentsOf(hash).into();
        match responder.handle_request(request, &store) {
            Ok(response) => panic!("Unexpected response: {:?}.", response),
            Err(err) => assert_eq!(err, Error::UnknownUnit(hash)),
        }
    }

    #[test]
    fn empty_newest_responds_with_no_units() {
        let (responder, store, keychains) = setup();
        let requester = NodeIndex(1);
        let request = DisseminationRequest::NewestUnit(requester, rand::random());
        let response = responder
            .handle_request(request, &store)
            .expect("newest unit requests always get a response");
        match response {
            DisseminationResponse::NewestUnit(newest_unit_response) => {
                let checked_newest_unit_response = newest_unit_response
                    .check(&keychains[NODE_ID.0])
                    .expect("should sign correctly");
                assert!(checked_newest_unit_response
                    .as_signable()
                    .included_data()
                    .is_empty());
            }
            other => panic!("Unexpected response: {:?}.", other),
        }
    }

    #[test]
    fn responds_to_coords_when_possible() {
        let (responder, mut store, keychains) = setup();
        let session_id = 2137;
        let coord = UnitCoord::new(3, NodeIndex(1));
        let units = random_full_parent_reconstrusted_units_up_to(
            coord.round() + 1,
            NODE_COUNT,
            session_id,
            &keychains,
        );
        for round_units in &units {
            for unit in round_units {
                store.insert(unit.clone());
            }
        }
        let request = Request::Coord(coord).into();
        let response = responder
            .handle_request(request, &store)
            .expect("should successfully respond");
        match response {
            DisseminationResponse::Coord(unit) => assert_eq!(
                unit,
                units[coord.round() as usize][coord.creator().0]
                    .clone()
                    .unpack()
                    .into_unchecked()
            ),
            other => panic!("Unexpected response: {:?}.", other),
        }
    }

    #[test]
    fn fails_to_responds_to_too_new_coords() {
        let (responder, mut store, keychains) = setup();
        let session_id = 2137;
        let coord = UnitCoord::new(3, NodeIndex(1));
        let units = random_full_parent_reconstrusted_units_up_to(
            coord.round() - 1,
            NODE_COUNT,
            session_id,
            &keychains,
        );
        for round_units in &units {
            for unit in round_units {
                store.insert(unit.clone());
            }
        }
        let request = Request::Coord(coord).into();
        match responder.handle_request(request, &store) {
            Ok(response) => panic!("Unexpected response: {:?}.", response),
            Err(err) => assert_eq!(err, Error::NoCanonicalAt(coord)),
        }
    }

    #[test]
    fn responds_to_parents_when_possible() {
        let (responder, mut store, keychains) = setup();
        let session_id = 2137;
        let units =
            random_full_parent_reconstrusted_units_up_to(5, NODE_COUNT, session_id, &keychains);
        for round_units in &units {
            for unit in round_units {
                store.insert(unit.clone());
            }
        }
        let requested_unit = units
            .last()
            .expect("just created this round")
            .last()
            .expect("the round has at least one unit")
            .clone();
        let request = Request::ParentsOf(requested_unit.hash()).into();
        let response = responder
            .handle_request(request, &store)
            .expect("should successfully respond");
        match response {
            DisseminationResponse::Parents(response_hash, parents) => {
                assert_eq!(response_hash, requested_unit.hash());
                assert_eq!(parents.len(), requested_unit.parents().count());
                for (parent, parent_hash) in zip(parents, requested_unit.parents()) {
                    assert_eq!(&parent.as_signable().hash(), parent_hash);
                }
            }
            other => panic!("Unexpected response: {:?}.", other),
        }
    }

    #[test]
    fn fails_to_respond_to_unknown_parents() {
        let (responder, mut store, keychains) = setup();
        let session_id = 2137;
        let units =
            random_full_parent_reconstrusted_units_up_to(5, NODE_COUNT, session_id, &keychains);
        for round_units in &units {
            for unit in round_units {
                store.insert(unit.clone());
            }
        }
        let hash =
            random_full_parent_reconstrusted_units_up_to(1, NODE_COUNT, session_id, &keychains)
                .last()
                .expect("just created this round")
                .last()
                .expect("the round has at least one unit")
                .hash();
        let request = Request::ParentsOf(hash).into();
        match responder.handle_request(request, &store) {
            Ok(response) => panic!("Unexpected response: {:?}.", response),
            Err(err) => assert_eq!(err, Error::UnknownUnit(hash)),
        }
    }

    #[test]
    fn responds_to_existing_newest() {
        let (responder, mut store, keychains) = setup();
        let session_id = 2137;
        let units =
            random_full_parent_reconstrusted_units_up_to(5, NODE_COUNT, session_id, &keychains);
        for round_units in &units {
            for unit in round_units {
                store.insert(unit.clone());
            }
        }
        let requester = NodeIndex(1);
        let request = DisseminationRequest::NewestUnit(requester, rand::random());
        let response = responder
            .handle_request(request, &store)
            .expect("newest unit requests always get a response");
        match response {
            DisseminationResponse::NewestUnit(newest_unit_response) => {
                newest_unit_response
                    .check(&keychains[NODE_ID.0])
                    .expect("should sign correctly");
            }
            other => panic!("Unexpected response: {:?}.", other),
        }
    }
}
