#![no_main]
use aleph_bft::testing::fuzz::{fuzz, NetworkData, NetworkDataEncoding};
use codec::Decode;
use libfuzzer_sys::{
    arbitrary::{Arbitrary, Error, Result, Unstructured},
    fuzz_target,
};
use log::error;
use std::io::{Read, Result as IOResult};

#[derive(Debug, Decode)]
struct StoredNetworkData(NetworkData);

struct IteratorToRead<I: Iterator<Item = u8>>(I);

impl<I: Iterator<Item = u8>> IteratorToRead<I> {
    fn new(iter: I) -> Self {
        IteratorToRead(iter)
    }
}

impl<I> Read for IteratorToRead<I>
where
    I: Iterator<Item = u8>,
{
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        Ok(buf.iter_mut().zip(&mut self.0).fold(0, |count, (b, v)| {
            *b = v;
            count + 1
        }))
    }
}

impl<'a> Arbitrary<'a> for StoredNetworkData {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let decoder = NetworkDataEncoding::default();
        let all_data = u.arbitrary_iter().expect("no data available").flatten();
        let mut all_data = IteratorToRead::new(all_data);
        let data = decoder.decode_from(&mut all_data);
        match data {
            Ok(v) => Ok(StoredNetworkData(v)),
            Err(_) => Err(Error::IncorrectFormat),
        }
    }
}

#[derive(Debug)]
struct VecOfStoredNetworkData(Vec<StoredNetworkData>);

impl<'a> Arbitrary<'a> for VecOfStoredNetworkData {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let mut result = vec![];
        for nd in u.arbitrary_iter::<StoredNetworkData>()? {
            match nd {
                Err(_) => {
                    error!(target: "Arbitrary for VecOfStoredNetworkData", "Error while generating an instance of the StoredNetworkData type.");
                    continue;
                }
                Ok(v) => result.push(v),
            }
        }
        Ok(VecOfStoredNetworkData(result))
    }
}

fuzz_target!(|data: VecOfStoredNetworkData| {
    let remapped = data.0.into_iter().map(|v| v.0).collect();
    fuzz(remapped, 4, None);
});
