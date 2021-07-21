#![no_main]
use alephbft_fuzz::fuzz::{fuzz, FuzzNetworkData, ReadToNetworkDataIterator};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let data: Vec<FuzzNetworkData> = ReadToNetworkDataIterator::new(data).collect();
    fuzz(data, 4, None);
});
