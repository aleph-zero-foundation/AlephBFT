use afl::fuzz;
use aleph_bft_fuzz::{fuzz as fuzz_helper, FuzzNetworkData, ReadToNetworkDataIterator};

fn main() {
    fuzz!(|data: &[u8]| {
        let data: Vec<FuzzNetworkData> = ReadToNetworkDataIterator::new(data).collect();
        fuzz_helper(data, 4, None);
    });
}
