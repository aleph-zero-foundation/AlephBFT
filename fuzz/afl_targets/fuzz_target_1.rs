use afl::fuzz;
use aleph_bft::testing::fuzz::{fuzz as fuzz_helper, NetworkData, ReadToNetworkDataIterator};

fn main() {
    fuzz!(|data: &[u8]| {
        let data: Vec<NetworkData> = ReadToNetworkDataIterator::new(data).collect();
        fuzz_helper(data, 4, None);
    });
}
