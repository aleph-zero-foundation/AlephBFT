#!/bin/bash

set -e

cargo build --release --example blockchain

clear

n_members="$1"

for i in $(seq 0 $(expr $n_members - 1)); do
    cargo run --release --example blockchain -- --my-id $i --n-members $n_members --n-finalized 30 2> node$i.log &
done
