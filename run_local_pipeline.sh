#!/bin/bash

set -e

cargo clippy --all-targets --all-features -- -D warnings
cargo +nightly fmt --all
cargo test --lib -- --skip medium
