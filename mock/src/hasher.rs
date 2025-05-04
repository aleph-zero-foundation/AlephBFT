use aleph_bft_types::Hasher;
use std::hash::{Hash, Hasher as StdHasher};

// A hasher from the standard library that hashes to u64, should be enough to
// avoid collisions in testing.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct Hasher64;

impl Hasher for Hasher64 {
    type Hash = [u8; 8];

    fn hash(x: &[u8]) -> Self::Hash {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        hasher.write(x);
        let hash_value = hasher.finish();
        let hash_bytes = hash_value.to_ne_bytes();
        let mut hash: [u8; 8] = [0; 8];
        hash.copy_from_slice(&hash_bytes);
        hash
    }
}

pub type Hash64 = <Hasher64 as Hasher>::Hash;
