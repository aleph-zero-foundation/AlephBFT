#!/bin/bash

set -e

RUSTFLAGS="-Z instrument-coverage" \
    LLVM_PROFILE_FILE="rush-%m.profraw" \
    cargo +nightly test --tests 2> covtest.out

cargo profdata -- merge -sparse rush-*.profraw -o rush.profdata

rm rush-*.profraw

version=$(rg Running covtest.out | sed -e "s/.*rush-\(.*\))/\1/")
rm covtest.out

echo $version > /tmp/cov_bin_version

cargo cov -- report \
    --use-color \
    --ignore-filename-regex='/rustc' \
    --ignore-filename-regex='/.cargo/registry' \
    --instr-profile=rush.profdata \
    --object target/debug/deps/rush-"$version"
