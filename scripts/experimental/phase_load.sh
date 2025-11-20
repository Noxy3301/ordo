#!/bin/bash

# Phase: load (BenchBase --load per port, in parallel)
# Usage: scripts/experimental/phase_load.sh [clients] [start_port]
# Env:
#  GEN_CONFIGS=true|false  Generate configs if missing (default true)
#  MAX_PARALLEL=N          Limit parallel jobs (default: clients)

set -euo pipefail

CLIENTS=${1:-4}
START_PORT=${2:-3307}
GEN_CONFIGS=${GEN_CONFIGS:-true}
MAX_PARALLEL=${MAX_PARALLEL:-$CLIENTS}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../.."
BENCH_DIR="$ROOT_DIR/bench"
CONFIG_MULTI_DIR=${CONFIG_MULTI_DIR:-"$BENCH_DIR/config/multi"}
TERMINALS_PER_INSTANCE=${YCSB_TERMINALS:-}

TS=$(date +%Y%m%d_%H%M%S)
# Prefer sorting by date_time, then phase suffix
SUFFIX="${CLIENTS}c"
if [ -n "$TERMINALS_PER_INSTANCE" ]; then
  SUFFIX="${SUFFIX}_${TERMINALS_PER_INSTANCE}t"
fi
RESULTS_DIR="$BENCH_DIR/results/exp/${TS}_load_${SUFFIX}"
mkdir -p "$RESULTS_DIR"

echo "=== EXP Load Phase ==="
echo "Clients   : $CLIENTS"
if [ -n "$TERMINALS_PER_INSTANCE" ]; then
  echo "Terminals : $TERMINALS_PER_INSTANCE per instance"
fi
echo "StartPort : $START_PORT"
echo "Results   : $RESULTS_DIR"

if [ "$GEN_CONFIGS" = "true" ]; then
  bash "$ROOT_DIR/scripts/experimental/generate_ycsb_configs.sh" "$CLIENTS" "$START_PORT"
fi

sem=0
for i in $(seq 0 $((CLIENTS - 1))); do
  PORT=$((START_PORT + i))
  WORKDIR="$BENCH_DIR/benchbase/port_${PORT}"
  CFG="$CONFIG_MULTI_DIR/ycsb_port_${PORT}.xml"
  mkdir -p "$WORKDIR"
  if [ ! -f "$WORKDIR/benchbase.jar" ]; then
    rsync -a "$BENCH_DIR/benchbase/benchbase-mysql/" "$WORKDIR/"
  fi

  (
    cd "$WORKDIR"
    java -jar benchbase.jar -b ycsb -c "$CFG" --create=false --load=true --execute=false \
      > "$RESULTS_DIR/load_${PORT}.log" 2>&1
  ) &

  sem=$((sem+1))
  if [ "$sem" -ge "$MAX_PARALLEL" ]; then
    wait -n || true
    sem=$((sem-1))
  fi
done

wait || true

echo "=== Load Done ==="
echo "Logs: $RESULTS_DIR"
