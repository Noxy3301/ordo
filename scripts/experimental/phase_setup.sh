#!/bin/bash

# Phase: setup (prepare BenchBase workdir, optional DB meta, create schema)
# Usage: scripts/experimental/phase_setup.sh [mysqld_port]
# Env:
#  GEN_CONFIGS=true|false      Generate BenchBase configs if missing (default true)
#  DO_SQL_SETUP=true|false     Run bench/setup.sql on each port (default true)
#  DO_CREATE_SCHEMA=true|false BenchBase --create phase (default true)

set -euo pipefail

PORT=${1:-3307}

GEN_CONFIGS=${GEN_CONFIGS:-true}
DO_SQL_SETUP=${DO_SQL_SETUP:-true}
DO_CREATE_SCHEMA=${DO_CREATE_SCHEMA:-true}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../.."
BENCH_DIR="$ROOT_DIR/bench"
BENCHBASE_SRC="$BENCH_DIR/benchbase/benchbase-mysql"
JAR="$BENCHBASE_SRC/benchbase.jar"
CONFIG_FILE=${CONFIG_OUT_FILE:-"$BENCH_DIR/config/generated/ycsb.generated.xml"}
MYSQL_BIN="$ROOT_DIR/build/runtime_output_directory/mysql"
TERMINALS_PER_INSTANCE=${YCSB_TERMINALS:-}
MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}

TS=${RUN_TS:-$(date +%Y%m%d_%H%M%S)}
PHASE_LABEL=${PHASE_LABEL:-setup}
SUFFIX=""
if [ -n "$TERMINALS_PER_INSTANCE" ]; then
  SUFFIX="${TERMINALS_PER_INSTANCE}t"
fi
RESULTS_DIR="$BENCH_DIR/results/exp/${TS}_${PHASE_LABEL}"
if [ -n "$SUFFIX" ]; then
  RESULTS_DIR="${RESULTS_DIR}_${SUFFIX}"
fi
mkdir -p "$RESULTS_DIR"

echo "=== EXP Setup Phase ==="
echo "Port      : $PORT"
if [ -n "$TERMINALS_PER_INSTANCE" ]; then
  echo "Terminals : $TERMINALS_PER_INSTANCE"
fi
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
  echo "Generating config for port $PORT ..."
  CONFIG_OUT_FILE="$CONFIG_FILE" bash "$ROOT_DIR/scripts/experimental/generate_ycsb_configs.sh" "$PORT"
fi

# Prepare workdir and run optional steps
WORKDIR="$BENCH_DIR/benchbase/port_${PORT}"
CFG="$CONFIG_FILE"
mkdir -p "$WORKDIR"
rsync -a "$BENCHBASE_SRC/" "$WORKDIR/"

# SQL meta (plugin check, DB create, etc.)
if [ "$DO_SQL_SETUP" = "true" ] && [ -x "$MYSQL_BIN" ]; then
  echo "[DB/$PORT] Applying bench/setup.sql ..."
  "$MYSQL_BIN" -u root --protocol=TCP -h "$MYSQL_HOST" -P "$PORT" < "$BENCH_DIR/setup.sql" \
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

echo "=== Setup Done ==="
echo "Logs: $RESULTS_DIR"
