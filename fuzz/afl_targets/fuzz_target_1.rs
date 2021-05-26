#[macro_use]
extern crate afl;

use aleph_bft::testing::fuzz::{fuzz, NetworkData, NetworkDataEncoding};
use log::error;
use std::io::{BufReader, Read};

struct ReadToNetworkDataIterator<R> {
    read: BufReader<R>,
    decoder: NetworkDataEncoding,
}

impl<R: Read> ReadToNetworkDataIterator<R> {
    fn new(read: R) -> Self {
        ReadToNetworkDataIterator {
            read: BufReader::new(read),
            decoder: NetworkDataEncoding::new(),
        }
    }
}

impl<R: Read> Iterator for ReadToNetworkDataIterator<R> {
    type Item = NetworkData;

    fn next(&mut self) -> Option<Self::Item> {
        use std::io::BufRead;
        if let Ok(buf) = self.read.fill_buf() {
            if buf.is_empty() {
                return None;
            }
        }
        match self.decoder.decode_from(&mut self.read) {
            Ok(v) => Some(v),
            // otherwise try to read until you reach END-OF-FILE
            Err(e) => {
                error!(target: "fuzz_target_1", "Unable to parse NetworkData: {:?}.", e);
                self.next()
            }
        }
    }
}

fn main() {
    fuzz!(|data: &[u8]| {
        let data: Vec<NetworkData> = ReadToNetworkDataIterator::new(data).collect();
        fuzz(data, 4, None);
    });
}
