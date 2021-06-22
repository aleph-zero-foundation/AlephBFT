#![no_main]
use alephbft_fuzz::fuzz::{fuzz, NetworkData, ReadToNetworkDataIterator};
use libfuzzer_sys::{
    arbitrary::{Arbitrary, Result, Unstructured},
    fuzz_target,
};
use std::io::{Read, Result as IOResult};

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

#[derive(Debug)]
struct StoredNetworkData(Vec<NetworkData>);

impl<'a> Arbitrary<'a> for StoredNetworkData {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let all_data = u.arbitrary_iter::<u8>()?;
        let all_data = IteratorToRead::new(all_data.flatten());
        let all_data = ReadToNetworkDataIterator::new(all_data);
        Ok(StoredNetworkData(all_data.collect()))
    }
}

fuzz_target!(|data: StoredNetworkData| {
    fuzz(data.0, 4, None);
});
