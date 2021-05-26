#!/bin/bash

version=$(cat /tmp/cov_bin_version)

cargo cov -- show \
    --use-color \
    --ignore-filename-regex='/rustc' \
    --ignore-filename-regex='/.cargo/registry' \
    --instr-profile=rush.profdata \
    --object target/debug/deps/rush-"$version" \
    --show-instantiations --show-line-counts-or-regions \
    --Xdemangler=rustfilt | less -R
