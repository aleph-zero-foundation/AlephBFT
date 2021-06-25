use afl::fuzz;
use fuzz::{fuzz as fuzz_helper, NetworkData, ReadToNetworkDataIterator};

mod fuzz;

fn main() {
    fuzz!(|data: &[u8]| {
        let data: Vec<NetworkData> = ReadToNetworkDataIterator::new(data).collect();
        fuzz_helper(data, 4, None);
    });
}
