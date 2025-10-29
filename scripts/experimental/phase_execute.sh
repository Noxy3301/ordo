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
CONFIG_MULTI_DIR=${CONFIG_MULTI_DIR:-"$BENCH_DIR/config/multi"}
TERMINALS_PER_INSTANCE=${YCSB_TERMINALS:-}

TS=$(date +%Y%m%d_%H%M%S)
# Prefer sorting by date_time, then port, then phase suffix
SUFFIX="${CLIENTS}c"
if [ -n "$TERMINALS_PER_INSTANCE" ]; then
  SUFFIX="${SUFFIX}_${TERMINALS_PER_INSTANCE}t"
fi
RESULTS_DIR="$BENCH_DIR/results/${TS}_${START_PORT}_exp_execute_${SUFFIX}"
mkdir -p "$RESULTS_DIR"

echo "=== EXP Execute Phase ==="
echo "Clients   : $CLIENTS"
if [ -n "$TERMINALS_PER_INSTANCE" ]; then
  echo "Terminals : $TERMINALS_PER_INSTANCE per instance"
fi
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

echo "Aggregating throughput/goodput ..."
total_tps=0; total_gps=0; clients_ok=0
{
  echo "Terminals,Throughput(req/s),Goodput(req/s),Avg(us),Median(us),P25(us),P75(us),P90(us),P95(us),P99(us),Min(us),Max(us),TableLocksImmediate,TableLocksWaited"
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
    tps=""; gps=""; avg_lat=""; median_lat=""; p25_lat=""; p75_lat=""; p90_lat=""; p95_lat=""; p99_lat=""; min_lat=""; max_lat=""; locks_i=""; locks_w=""
    metrics=""
    if [ -n "$latest_run" ] && [ -d "$PORT_DIR/$latest_run" ]; then
      metrics=$(ls -1 "$PORT_DIR/$latest_run"/ycsb_*metrics*.json 2>/dev/null | sort | tail -n1 || true)
    fi
    if [ -f "$summary" ]; then
      stats=$(python3 - <<'PY' "$summary" "${metrics:-}"
import json, sys
summary_path = sys.argv[1]
metrics_path = sys.argv[2] if len(sys.argv) > 2 else ""

def num(val, decimals=True):
    if val in (None, "", "null"):
        return ""
    try:
        val = float(str(val).replace(",", ""))
    except Exception:
        return ""
    if decimals:
        return f"{val:.6f}"
    if abs(val - round(val)) < 1e-6:
        return f"{int(round(val))}"
    return f"{val:.0f}"

out = ["", "", "", "", "", "", "", "", "", "", "", "", "", ""]
try:
    with open(summary_path) as f:
        s = json.load(f)
    out[0] = num(s.get("Throughput (requests/second)"))
    out[1] = num(s.get("Goodput (requests/second)"))
    lat = s.get("Latency Distribution", {})
    out[2] = num(lat.get("Average Latency (microseconds)"), decimals=False)
    out[3] = num(lat.get("Median Latency (microseconds)"), decimals=False)
    out[4] = num(lat.get("25th Percentile Latency (microseconds)"), decimals=False)
    out[5] = num(lat.get("75th Percentile Latency (microseconds)"), decimals=False)
    out[6] = num(lat.get("90th Percentile Latency (microseconds)"), decimals=False)
    out[7] = num(lat.get("95th Percentile Latency (microseconds)"), decimals=False)
    out[8] = num(lat.get("99th Percentile Latency (microseconds)"), decimals=False)
    out[9] = num(lat.get("Minimum Latency (microseconds)"), decimals=False)
    out[10] = num(lat.get("Maximum Latency (microseconds)"), decimals=False)
except Exception:
    pass

if metrics_path:
    try:
        with open(metrics_path) as f:
            m = json.load(f)
        out[11] = num(m.get("table_locks_immediate"), decimals=False)
        out[12] = num(m.get("table_locks_waited"), decimals=False)
    except Exception:
        pass

print(",".join(out))
PY
)
      if [ -n "$stats" ]; then
        IFS=',' read -r tps gps avg_lat median_lat p25_lat p75_lat p90_lat p95_lat p99_lat min_lat max_lat locks_i locks_w <<< "$stats"
      fi
    fi
    if [ -z "$tps" ]; then
      exelog="$RESULTS_DIR/execute_${PORT}.log"
      if [ -f "$exelog" ]; then
        tps=$(grep -m1 'Rate limited reqs/s: Results' "$exelog" | sed -E 's/.*= ([0-9.]+) requests\/sec.*/\1/')
      fi
    fi
    if [[ "$tps" =~ ^[0-9.]+$ ]]; then
      total_tps=$(awk -v a="$total_tps" -v b="$tps" 'BEGIN{printf "%.3f", a+b}')
    fi
    if [[ "$gps" =~ ^[0-9.]+$ ]]; then
      total_gps=$(awk -v a="$total_gps" -v b="$gps" 'BEGIN{printf "%.3f", a+b}')
      clients_ok=$((clients_ok+1))
    fi
    printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
      "${TERMINALS_PER_INSTANCE:-N/A}" "${tps:-N/A}" "${gps:-N/A}" \
      "${avg_lat:-N/A}" "${median_lat:-N/A}" "${p25_lat:-N/A}" "${p75_lat:-N/A}" "${p90_lat:-N/A}" "${p95_lat:-N/A}" "${p99_lat:-N/A}" "${min_lat:-N/A}" "${max_lat:-N/A}" \
      "${locks_i:-N/A}" "${locks_w:-N/A}"
  done
  # Totals are tracked above but not written to CSV to keep per-port metrics concise
  echo "Clients_OK,$clients_ok/$CLIENTS"
} | tee "$RESULTS_DIR/aggregate.csv"

echo "=== Execute Done ==="
echo "Results: $RESULTS_DIR"
