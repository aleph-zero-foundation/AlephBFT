#!/bin/bash

set -e

cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all
cargo test --lib -- --skip medium
