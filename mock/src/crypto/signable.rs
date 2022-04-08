use aleph_bft_types::Signable as SignableT;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Signable(String);

impl SignableT for Signable {
    type Hash = Vec<u8>;
    fn hash(&self) -> Self::Hash {
        self.0.clone().into()
    }
}

impl<T: Into<String>> From<T> for Signable {
    fn from(x: T) -> Self {
        Self(x.into())
    }
}
