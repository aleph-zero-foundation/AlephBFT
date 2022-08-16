use aleph_bft_types::Hasher;
use std::{collections::hash_map::DefaultHasher, hash::Hasher as StdHasher};

// A hasher from the standard library that hashes to u64, should be enough to
// avoid collisions in testing.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct Hasher64;

impl Hasher for Hasher64 {
    type Hash = [u8; 8];

    fn hash(x: &[u8]) -> Self::Hash {
        let mut hasher = DefaultHasher::new();
        hasher.write(x);
        hasher.finish().to_ne_bytes()
    }
}

pub type Hash64 = <Hasher64 as Hasher>::Hash;
