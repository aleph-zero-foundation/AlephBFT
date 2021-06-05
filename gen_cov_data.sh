#!/bin/bash

set -e

RUSTFLAGS="-Z instrument-coverage" \
    LLVM_PROFILE_FILE="aleph_bft-%m.profraw" \
    cargo +nightly test --tests $1 2> covtest.out

cargo profdata -- merge -sparse aleph_bft-*.profraw -o aleph_bft.profdata

rm aleph_bft-*.profraw

version=$(grep Running covtest.out | sed -e "s/.*aleph_bft-\(.*\))/\1/")
rm covtest.out

echo $version > /tmp/cov_bin_version

cargo cov -- report \
    --use-color \
    --ignore-filename-regex='/rustc' \
    --ignore-filename-regex='/.cargo/registry' \
    --instr-profile=aleph_bft.profdata \
    --object target/debug/deps/aleph_bft-"$version"
