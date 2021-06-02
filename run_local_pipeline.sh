#!/bin/bash

set -e

cargo +nightly clippy --all-targets --all-features -- -D warnings -A clippy::type_complexity
cargo +nightly fmt --all
cargo test --lib -- --skip medium
