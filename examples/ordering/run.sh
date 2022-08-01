#!/bin/bash

usage() {
    echo "Usage: ./run.sh [-n N_NODES] [-m N_MALFUNCTIONING_NODES] [-s N_STALLING_DATA_PROVIDERS] [-c N_CRASHES] [-o N_ORDERED_PER_CRASH] [-d RESTART_DELAY]"
    exit 1
}

N_NODES=2
N_MALFUNCTIONING_NODES=2
N_STALLING_DATA_PROVIDERS=1
N_CRASHES=3
N_ORDERED_PER_CRASH=25
RESTART_DELAY=1

while getopts :n:m:s:c:o:d: flag; do
    case "${flag}" in
        n) N_NODES=${OPTARG};;
        m) N_MALFUNCTIONING_NODES=${OPTARG};;
        s) N_STALLING_DATA_PROVIDERS=${OPTARG};;
        c) N_CRASHES=${OPTARG};;
        o) N_ORDERED_PER_CRASH=${OPTARG};;
        d) RESTART_DELAY=${OPTARG};;
        *) usage;;
    esac
done

n_ordered=$(( (N_CRASHES+1)*N_ORDERED_PER_CRASH ))
stalled=$(seq -s, 0 $((N_STALLING_DATA_PROVIDERS-1)))
port=10000
ports="$port"
for i in $(seq 0 $(expr $N_NODES + $N_MALFUNCTIONING_NODES - 2)); do
    port=$((port+1))
    ports+=",$port"
done

set -e

cargo build --release
binary="../../target/release/aleph-bft-examples-ordering"

clear

run_crash_node () {
    id="$1"
    n_starting=0
    n_data=$N_ORDERED_PER_CRASH
    for (( i = 1; i <= N_CRASHES; i++ )); do
        echo "Starting node $id at $n_starting items ($i/$((N_CRASHES+1)))..."
        ! "$binary" --id "$id" --ports "$ports" --n-data "$n_data" --n-starting "$n_starting" --stalled "$stalled" --crash 2>> "node${id}.log"
        echo "Node $id crashed. Respawning in $RESTART_DELAY seconds..."
        sleep "$RESTART_DELAY"
        n_starting=$n_data
        n_data=$(( n_data + N_ORDERED_PER_CRASH ))
    done
    echo "Starting node $id at $n_starting items ($((N_CRASHES+1))/$((N_CRASHES+1)))..."
    "$binary" --id "$id" --ports "$ports" --n-data "$n_data" --n-starting "$n_starting" --stalled "$stalled" 2>> "node${id}.log"
}

for id in $(seq 0 $(expr $N_NODES + $N_MALFUNCTIONING_NODES - 1)); do
    rm -f "aleph-bft-examples-ordering-backup/${id}.units"
    rm -f "node${id}.log"
done


echo "WARNING
The current implementation of AlephBFT does not strictly guarantee
all input data to be included in the output - a property that will
be added in a future version. This issue occurs when the provider lags
behind other nodes.
Therefore, always check logs to see if there are any unexpected
errors - e.g. connection timeout - or if some crashed nodes are lagging
behind - messages \"Providing None\" are logged, but the total
amount of finalized data does not increase for this particular node.
In such case, try reducing the number of nodes or shortening
the restart delay. Another option is to make more than 1/3 of the nodes
malfunctioning - with that the protocol will stall while the nodes are
restarting so that there won't be a set of nodes that run out ahead.

PARAMETERS
number of nodes: $N_NODES
number of malfunctioning nodes: $N_MALFUNCTIONING_NODES
number of nodes with stalling DataProviders: $N_STALLING_DATA_PROVIDERS
number of forced crashes: $N_CRASHES
number of ordered data per crash: $N_ORDERED_PER_CRASH
restart delay: $RESTART_DELAY second(s)
"

for id in $(seq 0 $(expr $N_NODES - 1)); do
    echo "Starting node ${id}..."
    "$binary" --id "$id" --ports "$ports" --n-data "$n_ordered" --stalled "$stalled" 2>> "node${id}.log" &
done

for i in $(seq $(expr $N_NODES) $(expr $N_NODES + $N_MALFUNCTIONING_NODES - 1)); do
    run_crash_node "$i" &
done

trap 'kill $(jobs -p); wait' SIGINT SIGTERM
wait
