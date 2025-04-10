use crate::{alerts::AlertMessage, Data, Hasher, PartialMultisignature, Recipient, Signature};
use codec::{Decode, Encode};
use std::fmt::Debug;

mod hub;
mod unit;

pub use hub::Hub;
pub use unit::UnitMessage;

pub type UnitMessageTo<H, D, S> = (UnitMessage<H, D, S>, Recipient);

#[derive(Clone, Eq, PartialEq, Debug, Decode, Encode)]
pub(crate) enum NetworkDataInner<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature> {
    Units(UnitMessage<H, D, S>),
    Alert(AlertMessage<H, D, S, MS>),
}

impl<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature> NetworkDataInner<H, D, S, MS> {
    pub(crate) fn included_data(&self) -> Vec<D> {
        match self {
            Self::Units(message) => message.included_data(),
            Self::Alert(message) => message.included_data(),
        }
    }
}

/// NetworkData is the opaque format for all data that a committee member needs to send to other nodes.
#[derive(Clone, Eq, PartialEq, Debug, Decode, Encode)]
pub struct NetworkData<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature>(
    pub(crate) NetworkDataInner<H, D, S, MS>,
);

impl<H: Hasher, D: Data, S: Signature, MS: PartialMultisignature> NetworkData<H, D, S, MS> {
    /// Returns all the Data in the network message that might end up in the ordering as a result
    /// of accepting this message. Useful for ensuring data availability, if Data only represents
    /// the objects the user wants to order, and facilitates access to the Data before it is
    /// ordered for optimization purposes.
    pub fn included_data(&self) -> Vec<D> {
        self.0.included_data()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        alerts::AlertMessage,
        network::{
            NetworkDataInner::{Alert, Units},
            UnitMessage,
        },
        units::{ControlHash, FullUnit, PreUnit, UncheckedSignedUnit, UnitCoord},
        Hasher, NodeIndex, Round, Signed,
    };
    use aleph_bft_mock::{Data, Hasher64, Keychain, PartialMultisignature, Signature};
    use aleph_bft_types::NodeMap;
    use codec::{Decode, Encode};

    fn test_unchecked_unit(
        creator: NodeIndex,
        round: Round,
        data: Data,
    ) -> UncheckedSignedUnit<Hasher64, Data, Signature> {
        let control_hash = ControlHash::new(&NodeMap::with_size(7.into()));
        let pu = PreUnit::new(creator, round, control_hash);
        let signable = FullUnit::new(pu, Some(data), 0);
        Signed::sign(signable, &Keychain::new(0.into(), creator)).into_unchecked()
    }

    type TestNetworkData = super::NetworkData<Hasher64, Data, Signature, PartialMultisignature>;
    impl TestNetworkData {
        fn new(
            inner: super::NetworkDataInner<Hasher64, Data, Signature, PartialMultisignature>,
        ) -> Self {
            super::NetworkData::<Hasher64, Data, Signature, PartialMultisignature>(inner)
        }
    }

    #[test]
    fn decoding_network_data_units_new_unit() {
        use UnitMessage::Unit;

        let uu = test_unchecked_unit(5.into(), 43, 1729);
        let included_data = uu.as_signable().included_data();
        let nd = TestNetworkData::new(Units(Unit(uu.clone())));
        let decoded = TestNetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in encode/decode for Unit");
        let decoded = decoded.unwrap();
        assert_eq!(
            decoded.included_data(),
            included_data,
            "data decoded incorrectly"
        );
        if let Units(Unit(decoded_unchecked)) = decoded.0 {
            assert_eq!(
                uu.as_signable(),
                decoded_unchecked.as_signable(),
                "decoded should equal encoded"
            );
        } else {
            panic!("Decoded Unit as something else");
        }
    }

    #[test]
    fn decoding_network_data_units_request_coord() {
        use UnitMessage::CoordRequest;

        let ni = 7.into();
        let uc = UnitCoord::new(3, 13.into());
        let nd = TestNetworkData::new(Units(CoordRequest(ni, uc)));
        let decoded = TestNetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in encode/decode for CoordRequest");
        let decoded = decoded.unwrap();
        assert!(
            decoded.included_data().is_empty(),
            "data returned from a coord request"
        );
        if let Units(CoordRequest(dni, duc)) = decoded.0 {
            assert!(ni == dni && uc == duc, "decoded should equal encoded");
        } else {
            panic!("Decoded CoordRequest as something else");
        }
    }

    #[test]
    fn decoding_network_data_units_request_parents() {
        use UnitMessage::ParentsRequest;

        let ni = 7.into();
        let h = 43.using_encoded(Hasher64::hash);
        let nd = TestNetworkData::new(Units(ParentsRequest(ni, h)));
        let decoded = TestNetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in encode/decode for ParentsRequest");
        let decoded = decoded.unwrap();
        assert!(
            decoded.included_data().is_empty(),
            "data returned from a parent request"
        );
        if let Units(ParentsRequest(dni, dh)) = decoded.0 {
            assert!(ni == dni && h == dh, "decoded should equal encoded");
        } else {
            panic!("Decoded ParentsRequest as something else");
        }
    }

    #[test]
    fn decoding_network_data_units_response_parents() {
        use UnitMessage::ParentsResponse;

        let h = 43.using_encoded(Hasher64::hash);
        let p1 = test_unchecked_unit(5.into(), 43, 1729);
        let p2 = test_unchecked_unit(13.into(), 43, 1729);
        let p3 = test_unchecked_unit(17.into(), 43, 1729);
        let included_data: Vec<Data> = p1
            .as_signable()
            .included_data()
            .into_iter()
            .chain(p2.as_signable().included_data())
            .chain(p3.as_signable().included_data())
            .collect();
        let parents = vec![p1, p2, p3];

        let nd = TestNetworkData::new(Units(ParentsResponse(h, parents.clone())));
        let decoded = TestNetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in encode/decode for ParentsResponse");
        let decoded = decoded.unwrap();
        assert_eq!(
            decoded.included_data(),
            included_data,
            "data decoded incorrectly"
        );
        if let Units(ParentsResponse(dh, dparents)) = decoded.0 {
            assert_eq!(h, dh, "decoded should equal encoded");
            assert_eq!(
                parents.len(),
                dparents.len(),
                "decoded should equal encoded"
            );
            for (p, dp) in parents.iter().zip(dparents.iter()) {
                assert_eq!(
                    p.as_signable(),
                    dp.as_signable(),
                    "decoded should equal encoded"
                );
            }
        } else {
            panic!("Decoded ParentsResponse as something else");
        }
    }

    #[test]
    fn decoding_network_data_alert_fork_alert() {
        use AlertMessage::ForkAlert;

        let forker = 9.into();
        let f1 = test_unchecked_unit(forker, 10, 0);
        let f2 = test_unchecked_unit(forker, 10, 1);
        let lu1 = test_unchecked_unit(forker, 11, 0);
        let lu2 = test_unchecked_unit(forker, 12, 0);
        let mut included_data = lu1.as_signable().included_data();
        included_data.extend(lu2.as_signable().included_data());
        let sender: NodeIndex = 7.into();
        let alert = crate::alerts::Alert::new(sender, (f1, f2), vec![lu1, lu2]);

        let nd = TestNetworkData::new(Alert(ForkAlert(
            Signed::sign(alert.clone(), &Keychain::new(0.into(), sender)).into_unchecked(),
        )));
        let decoded = TestNetworkData::decode(&mut &nd.encode()[..]);
        assert!(decoded.is_ok(), "Bug in encode/decode for ForkAlert");
        let decoded = decoded.unwrap();
        assert_eq!(
            decoded.included_data(),
            included_data,
            "data decoded incorrectly"
        );
        if let Alert(ForkAlert(unchecked_alert)) = decoded.0 {
            assert_eq!(
                &alert,
                unchecked_alert.as_signable(),
                "decoded should equal encoded"
            )
        } else {
            panic!("Decoded ForkAlert as something else");
        }
    }
}
