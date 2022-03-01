#!/bin/bash

set -e

cargo clippy --all-targets --all-features -- -D warnings -A clippy::type_complexity
cargo fmt --all
cargo test --lib -- --skip medium
