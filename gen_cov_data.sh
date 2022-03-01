#!/bin/bash

set -e

RUSTFLAGS="-Z instrument-coverage" \
    LLVM_PROFILE_FILE="aleph_bft-%m.profraw" \
    cargo test --tests $1 2> covtest.out

version=$(grep Running covtest.out | sed -e "s/.*aleph_bft-\(.*\))/\1/")
rm covtest.out
cp target/debug/deps/aleph_bft-"$version" target/debug/deps/aleph_bft-coverage

cargo profdata -- merge -sparse aleph_bft-*.profraw -o aleph_bft.profdata
rm aleph_bft-*.profraw

cargo cov -- report \
    --use-color \
    --ignore-filename-regex='/rustc' \
    --ignore-filename-regex='/.cargo/registry' \
    --instr-profile=aleph_bft.profdata \
    --object target/debug/deps/aleph_bft-coverage
