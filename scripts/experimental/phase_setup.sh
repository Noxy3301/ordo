#!/bin/bash

# Phase: setup (prepare per-port BenchBase workdirs, optional DB meta, create schema)
# Usage: scripts/experimental/phase_setup.sh [clients] [start_port]
# Env:
#  GEN_CONFIGS=true|false      Generate BenchBase configs if missing (default true)
#  DO_SQL_SETUP=true|false     Run bench/setup.sql on each port (default true)
#  DO_CREATE_SCHEMA=true|false BenchBase --create phase (default true)

set -euo pipefail

CLIENTS=${1:-4}
START_PORT=${2:-3307}

GEN_CONFIGS=${GEN_CONFIGS:-true}
DO_SQL_SETUP=${DO_SQL_SETUP:-true}
DO_CREATE_SCHEMA=${DO_CREATE_SCHEMA:-true}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../.."
BENCH_DIR="$ROOT_DIR/bench"
BENCHBASE_SRC="$BENCH_DIR/benchbase/benchbase-mysql"
JAR="$BENCHBASE_SRC/benchbase.jar"
CONFIG_MULTI_DIR="$BENCH_DIR/config/multi"
MYSQL_BIN="$ROOT_DIR/build/runtime_output_directory/mysql"

TS=$(date +%Y%m%d_%H%M%S)
# Prefer sorting by date_time, then port, then phase suffix
RESULTS_DIR="$BENCH_DIR/results/${TS}_${START_PORT}_exp_setup_${CLIENTS}c"
mkdir -p "$RESULTS_DIR"

echo "=== EXP Setup Phase ==="
echo "Clients   : $CLIENTS"
echo "StartPort : $START_PORT"
echo "Results   : $RESULTS_DIR"

# Ensure BenchBase jar exists
if [ ! -f "$JAR" ]; then
  echo "BenchBase jar not found. Running bench/setup_benchbase.sh ..."
  (cd "$BENCH_DIR" && bash ./setup_benchbase.sh)
fi
if [ ! -f "$JAR" ]; then
  echo "ERROR: benchbase.jar not found at $JAR" >&2
  exit 1
fi

# Generate configs if requested
if [ "$GEN_CONFIGS" = "true" ]; then
  echo "Generating per-port configs ..."
  bash "$ROOT_DIR/scripts/experimental/generate_ycsb_configs.sh" "$CLIENTS" "$START_PORT"
fi

# Prepare workdirs and run optional steps
for i in $(seq 0 $((CLIENTS - 1))); do
  PORT=$((START_PORT + i))
  WORKDIR="$BENCH_DIR/benchbase/port_${PORT}"
  CFG="$CONFIG_MULTI_DIR/ycsb_port_${PORT}.xml"
  mkdir -p "$WORKDIR"
  rsync -a "$BENCHBASE_SRC/" "$WORKDIR/"

  # SQL meta (plugin check, DB create, etc.)
  if [ "$DO_SQL_SETUP" = "true" ] && [ -x "$MYSQL_BIN" ]; then
    echo "[DB/$PORT] Applying bench/setup.sql ..."
    SOCKET="/tmp/mysql_${PORT}.sock"
    "$MYSQL_BIN" -u root --socket="$SOCKET" --port="$PORT" < "$BENCH_DIR/setup.sql" \
      > "$RESULTS_DIR/sql_${PORT}.log" 2>&1 || true
  fi

  if [ "$DO_CREATE_SCHEMA" = "true" ]; then
    echo "[Schema/$PORT] BenchBase --create ..."
    (
      cd "$WORKDIR"
      java -jar benchbase.jar -b ycsb -c "$CFG" --create=true --load=false --execute=false \
        > "$RESULTS_DIR/create_${PORT}.log" 2>&1
    )
  fi
done

echo "=== Setup Done ==="
echo "Logs: $RESULTS_DIR"

