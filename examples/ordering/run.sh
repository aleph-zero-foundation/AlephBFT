#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: run.sh n_members"
    exit 1
fi

set -e

cargo build --release

clear

n_members="$1"
ports="43000"
port=43000

for i in $(seq 0 $(expr $n_members - 2)); do
    port=$((port+1))
    ports+=",$port"
done

for i in $(seq 0 $(expr $n_members - 1)); do
    cargo run --release -- --id "$i" --ports "$ports" --n-items 50 2> "node$i.log" &
done

echo "Running ordering example... (Ctrl+C to exit)"
trap 'kill $(jobs -p)' SIGINT SIGTERM
wait $(jobs -p)
