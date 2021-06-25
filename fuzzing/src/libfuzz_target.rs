#![no_main]
use alephbft_fuzz::fuzz::{fuzz, NetworkData, ReadToNetworkDataIterator};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let data: Vec<NetworkData> = ReadToNetworkDataIterator::new(data).collect();
    fuzz(data, 4, None);
});
