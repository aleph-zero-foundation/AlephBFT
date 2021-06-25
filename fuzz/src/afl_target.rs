use afl::fuzz;
use fuzz::{fuzz as fuzz_helper, FuzzNetworkData, ReadToNetworkDataIterator};

mod fuzz;

fn main() {
    fuzz!(|data: &[u8]| {
        let data: Vec<FuzzNetworkData> = ReadToNetworkDataIterator::new(data).collect();
        fuzz_helper(data, 4, None);
    });
}
