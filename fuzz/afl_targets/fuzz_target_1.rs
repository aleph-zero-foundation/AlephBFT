#[macro_use]
extern crate afl;

use aleph_bft::testing::fuzz::{fuzz, NetworkData, ReadToNetworkDataIterator};

fn main() {
    fuzz!(|data: &[u8]| {
        let data: Vec<NetworkData> = ReadToNetworkDataIterator::new(data).collect();
        fuzz(data, 4, None);
    });
}
