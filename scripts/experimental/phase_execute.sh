#!/bin/bash

# Phase: execute (synchronized BenchBase --execute per port, aggregate results)
# Usage: scripts/experimental/phase_execute.sh [clients] [start_port]
# Env:
#  GEN_CONFIGS=true|false   Generate configs if missing (default true)
#  PRESERVE_WORKDIRS=true|false  Keep per-port workdirs (default false)

set -euo pipefail

CLIENTS=${1:-4}
START_PORT=${2:-3307}
GEN_CONFIGS=${GEN_CONFIGS:-true}
PRESERVE_WORKDIRS=${PRESERVE_WORKDIRS:-false}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../.."
BENCH_DIR="$ROOT_DIR/bench"
CONFIG_MULTI_DIR="$BENCH_DIR/config/multi"

TS=$(date +%Y%m%d_%H%M%S)
# Prefer sorting by date_time, then port, then phase suffix
RESULTS_DIR="$BENCH_DIR/results/${TS}_${START_PORT}_exp_execute_${CLIENTS}c"
mkdir -p "$RESULTS_DIR"

echo "=== EXP Execute Phase ==="
echo "Clients   : $CLIENTS"
echo "StartPort : $START_PORT"
echo "Results   : $RESULTS_DIR"

if [ "$GEN_CONFIGS" = "true" ]; then
  bash "$ROOT_DIR/scripts/experimental/generate_ycsb_configs.sh" "$CLIENTS" "$START_PORT"
fi

# Prepare per-port workdirs if missing
for i in $(seq 0 $((CLIENTS - 1))); do
  PORT=$((START_PORT + i))
  WORKDIR="$BENCH_DIR/benchbase/port_${PORT}"
  mkdir -p "$WORKDIR"
  if [ ! -f "$WORKDIR/benchbase.jar" ]; then
    rsync -a "$BENCH_DIR/benchbase/benchbase-mysql/" "$WORKDIR/"
  fi
done

# Synchronization primitives
BARRIER_FILE="/tmp/exp_exec_barrier_$$"
READY_DIR="/tmp/exp_exec_ready_$$"
mkdir -p "$READY_DIR"

declare -a PIDS=()
for i in $(seq 0 $((CLIENTS - 1))); do
  PORT=$((START_PORT + i))
  CFG="$CONFIG_MULTI_DIR/ycsb_port_${PORT}.xml"
  WORKDIR="$BENCH_DIR/benchbase/port_${PORT}"

  (
    cd "$WORKDIR"
    # Signal ready
    touch "$READY_DIR/ready_${PORT}"
    # Wait for barrier
    while [ ! -f "$BARRIER_FILE" ]; do sleep 0.1; done
    # Execute
    java -jar benchbase.jar -b ycsb -c "$CFG" --create=false --load=false --execute=true \
      > "$RESULTS_DIR/execute_${PORT}.log" 2>&1
    # Determine latest run id from summary filename (prefer cwd, then results/)
    latest_summary=""
    if ls -1 ycsb_*summary*.json >/dev/null 2>&1; then
      latest_summary=$(ls -1 ycsb_*summary*.json 2>/dev/null | sort | tail -n1)
    elif ls -1 results/ycsb_*summary*.json >/dev/null 2>&1; then
      latest_summary=$(ls -1 results/ycsb_*summary*.json 2>/dev/null | sort | tail -n1)
    fi
    if [ -n "$latest_summary" ]; then
      sum_base=$(basename "$latest_summary")
      run_prefix=${sum_base%.summary.json}  # e.g., ycsb_2025-08-31_00-15-44
      DEST_DIR="$RESULTS_DIR/port_${PORT}/$run_prefix"
      mkdir -p "$DEST_DIR"
      # Collect matching files from cwd
      if ls -1 ${run_prefix}.* >/dev/null 2>&1; then
        for f in ${run_prefix}.*; do
          mv "$f" "$DEST_DIR/" 2>/dev/null || true
        done
      fi
      # Collect matching files from results/
      if ls -1 results/${run_prefix}.* >/dev/null 2>&1; then
        for f in results/${run_prefix}.*; do
          cp "$f" "$DEST_DIR/" 2>/dev/null || true
        done
      fi
    fi
  ) &
  PIDS+=($!)
done

echo "Waiting for workers to be ready ..."
while [ $(ls "$READY_DIR" | wc -l) -lt "$CLIENTS" ]; do sleep 0.1; done
sleep 2
touch "$BARRIER_FILE"
echo "Barrier released. Executing..."

rc=0
for pid in "${PIDS[@]}"; do
  if ! wait "$pid"; then rc=1; fi
done

rm -f "$BARRIER_FILE"; rm -rf "$READY_DIR"

if [ "$PRESERVE_WORKDIRS" != "true" ]; then
  for i in $(seq 0 $((CLIENTS - 1))); do
    PORT=$((START_PORT + i))
    WORKDIR="$BENCH_DIR/benchbase/port_${PORT}"
    rm -rf "$WORKDIR" || true
  done
fi

echo "Aggregating throughput ..."
total_tps=0; clients_ok=0
{
  echo "Port,Throughput(req/s)"
  for i in $(seq 0 $((CLIENTS - 1))); do
    PORT=$((START_PORT + i))
    PORT_DIR="$RESULTS_DIR/port_${PORT}"
    latest_run=""
    if [ -d "$PORT_DIR" ]; then
      latest_run=$(ls -1 "$PORT_DIR" 2>/dev/null | sort | tail -n1)
    fi
    summary=""
    if [ -n "$latest_run" ] && [ -d "$PORT_DIR/$latest_run" ]; then
      summary=$(ls -1 "$PORT_DIR/$latest_run"/ycsb_*summary*.json 2>/dev/null | sort | tail -n1 || true)
    fi
    tps=""
    if [ -f "$summary" ]; then
      tps=$(grep -o '"Throughput (requests/second)": [0-9.]*' "$summary" | awk '{print $3}')
    fi
    if [ -z "$tps" ]; then
      exelog="$RESULTS_DIR/execute_${PORT}.log"
      if [ -f "$exelog" ]; then
        tps=$(grep -m1 'Rate limited reqs/s: Results' "$exelog" | sed -E 's/.*= ([0-9.]+) requests\/sec.*/\1/')
      fi
    fi
    if [[ "$tps" =~ ^[0-9.]+$ ]]; then
      total_tps=$(awk -v a="$total_tps" -v b="$tps" 'BEGIN{printf "%.3f", a+b}')
      clients_ok=$((clients_ok+1))
      echo "$PORT,$tps"
    else
      echo "$PORT,N/A"
    fi
  done
  echo "Total,$total_tps"
  echo "Clients_OK,$clients_ok/$CLIENTS"
} | tee "$RESULTS_DIR/aggregate.csv"

echo "=== Execute Done ==="
echo "Results: $RESULTS_DIR"

