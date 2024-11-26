#!/usr/bin/env bash

set -eou pipefail

function usage() {
  cat << EOF
Usage:
  This script is a demonstration usage of AlephBFT protocol, in which there are N nodes and they want to achieve
  a consensus with regards to provided data. The data sent to AlephBFT from each node is a stream of integers from range
  [0, N * DATA_ITEMS), where DATA_ITEMS is configurable. Each node of index 'i'  sends to the consensus
  integers from range [i * DATA_ITEMS; (i + 1) * DATA_ITEMS). where 0 <= i < N. At the end, each node makes
  sure that it receives all integers from range [0, N * DATA_ITEMS), each integer exactly once.

  N nodes are started on your machine, and they communicate via UDP. Not all nodes behave correctly - some of them crash
  or are stuck while providing data.

  This script is using aleph-bft-examples-ordering and assumes to be available in a relative folder from this script path
    ../../target/release/aleph-bft-examples-ordering

   $0
     [-n|--nodes NODES]
       number of all non-crashing nodes; some of them can have stalled data provider
     [-c|--crashing-nodes CRASHING_NODES]
       number of nodes that crash while providing data
     [-s|--stalling-data-providers STALLING_DATA_PROVIDERS]
       number of nodes that eventually stall while providing data; must be less than --nodes
     [--crashes-count CRASHES_COUNT]
       how many times a crashing node should crash
     [--data-items DATA_ITEMS]
        how many data items each node should order
     [--crash-restart-delay-seconds CRASH_RESTART_DELAY_SECONDS]
        delay (seconds) between subsequent node crashes
     [--unit-creation-delay UNIT_CREATION_DELAY]
        unit creation delay (milliseconds), default 200
EOF
  exit 0
}

NORMAL=$(tput sgr0)
GREEN=$(tput setaf 2; tput bold)
YELLOW=$(tput setaf 3)
RED=$(tput setaf 1)

function get_timestamp() {
  echo "$(date +'%Y-%m-%d %T:%3N')"
}

function error() {
    echo -e "$(get_timestamp) $RED$*$NORMAL"
    exit 1
}

function info() {
    echo -e "$(get_timestamp) $GREEN$*$NORMAL"
}

function warning() {
    echo -e "$(get_timestamp) $YELLOW$*$NORMAL"
}

function run_ordering_binary() {
  local id="$1"
  local starting_data_item="$2"
  local data_items=$3
  local should_stall="${4:-no}"

  local binary_args=(
    --id "$id"
    --ports "${PORTS}"
    --starting-data-item "${starting_data_item}"
    --data-items "${data_items}"
    --required-finalization-value "${EXPECTED_FINALIZED_DATA_ITEMS}"
    --unit-creation-delay "${UNIT_CREATION_DELAY}"
  )
  if [[ "${should_stall}" == "yes-stall" ]]; then
    binary_args+=(--should-stall)
  fi

  info "Starting node ${id} to provide items from ${starting_data_item} to $(( starting_data_item + data_items - 1 )), inclusive"
  "${ordering_binary}" "${binary_args[@]}" 2>> "node${id}.log" > /dev/null &
}

function run_crash_node () {
    id="$1"
    for run_attempt_index in $(seq 0 $(( CRASHES_COUNT - 1 ))); do
        run_ordering_binary "${id}" "${DATA_ITEMS_COUNTER}" "${DATA_ITEMS}"
        pid=$!
        info "Waiting ${CRASH_RESTART_DELAY_SECONDS} seconds..."
        sleep "${CRASH_RESTART_DELAY_SECONDS}"
        info "Killing node with pid ${pid}"
        kill -9 "${pid}" 2> /dev/null
    done
    run_ordering_binary "${id}" "${DATA_ITEMS_COUNTER}" "${DATA_ITEMS}"
}

NODES=2
CRASHING_NODES=2
STALLING_DATA_PROVIDERS=1
CRASHES_COUNT=3
DATA_ITEMS=25
CRASH_RESTART_DELAY_SECONDS=5
DATA_ITEMS_COUNTER=0
UNIT_CREATION_DELAY=200

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--nodes)
      NODES="$2"
      shift;shift
      ;;
    -c|--crashing-nodes)
      CRASHING_NODES="$2"
      shift;shift
      ;;
    -s|--stalling-data-providers)
      STALLING_DATA_PROVIDERS="$2"
      shift;shift
      ;;
    --crashes-count)
      CRASHES_COUNT="$2"
      shift;shift
      ;;
    --data-items)
      DATA_ITEMS="$2"
      shift;shift
      ;;
    --crash-restart-delay-seconds)
      CRASH_RESTART_DELAY_SECONDS="$2"
      shift;shift
      ;;
    --unit-creation-delay)
      UNIT_CREATION_DELAY="$2"
      shift;shift
      ;;
    --help)
      usage
      shift
      ;;
    *)
      error "Unrecognized argument $1!"
      ;;
  esac
done

script_path="${BASH_SOURCE[0]}"
script_dir=$(dirname "${script_path}")
ordering_binary_dir=$(realpath "${script_dir}/../../")
ordering_binary="${ordering_binary_dir}/target/release/aleph-bft-examples-ordering"

if [[ ! -x "${ordering_binary}" ]]; then
  error "${ordering_binary} does not exist or it's not an executable file!"
fi

ALL_NODES=$(( NODES + CRASHING_NODES ))
PORTS=($(seq -s , 10000 $(( 10000 + ALL_NODES - 1 ))))
EXPECTED_FINALIZED_DATA_ITEMS=$(( ALL_NODES * DATA_ITEMS ))

for id in $(seq 0 $(( ALL_NODES - 1 ))); do
    rm -f "aleph-bft-examples-ordering-backup/${id}.units"
    rm -f "node${id}.log"
done

info "Starting $0
PARAMETERS
number of nodes: ${NODES}
number of crashing nodes: ${CRASHING_NODES}
number of nodes with stalling DataProviders: ${STALLING_DATA_PROVIDERS}
number of forced crashes: ${CRASHES_COUNT}
number of ordered data per batch: ${DATA_ITEMS}
restart delay: ${CRASH_RESTART_DELAY_SECONDS} second(s)
"

for id in $(seq 0 $(( NODES - 1 ))); do
  if [[ "${id}" -lt "${STALLING_DATA_PROVIDERS}" ]]; then
    run_ordering_binary "${id}" "${DATA_ITEMS_COUNTER}" "${DATA_ITEMS}" "yes-stall"
  else
    run_ordering_binary "${id}" "${DATA_ITEMS_COUNTER}" "${DATA_ITEMS}"
  fi
  DATA_ITEMS_COUNTER=$(( DATA_ITEMS_COUNTER + DATA_ITEMS ))
done

for id in $(seq $(( NODES )) $(( ALL_NODES - 1 ))); do
    run_crash_node "${id}" &
    DATA_ITEMS_COUNTER=$(( DATA_ITEMS_COUNTER + DATA_ITEMS ))
done

trap 'kill $(jobs -p); wait' SIGINT SIGTERM
wait
