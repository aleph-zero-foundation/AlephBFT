#!/bin/bash

set -e

function run_pipeline {
	cargo +nightly clippy --all-targets --all-features -- -D warnings -A clippy::type_complexity
	cargo +nightly fmt --all
	cargo test --lib -- --skip medium
}


run_pipeline
pushd fuzz
run_pipeline
popd
pushd mock
run_pipeline
popd
