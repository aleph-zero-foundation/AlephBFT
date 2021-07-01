#!/bin/bash

killall -p blockchain

set -e

cargo build  --release --example blockchain

clear

n_members="$1"

for i in $(seq 0 $(expr $n_members - 1)); do
   cargo run  --release --example blockchain $i $n_members 30 2> node$i.log &
done
