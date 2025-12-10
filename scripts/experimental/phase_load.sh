#!/bin/bash

# Phase: load (BenchBase --load)
# Usage: scripts/experimental/phase_load.sh [mysqld_port]
# Env:
#  GEN_CONFIGS=true|false  Generate configs if missing (default true)

set -euo pipefail

PORT=${1:-3307}
GEN_CONFIGS=${GEN_CONFIGS:-true}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../.."
BENCH_DIR="$ROOT_DIR/bench"
CONFIG_FILE=${CONFIG_OUT_FILE:-"$BENCH_DIR/config/generated/ycsb.generated.xml"}
TERMINALS_PER_INSTANCE=${YCSB_TERMINALS:-}

TS=${RUN_TS:-$(date +%Y%m%d_%H%M%S)}
PHASE_LABEL=${PHASE_LABEL:-load}
SUFFIX=""
if [ -n "$TERMINALS_PER_INSTANCE" ]; then
  SUFFIX="${TERMINALS_PER_INSTANCE}t"
fi
RESULTS_DIR="$BENCH_DIR/results/exp/${TS}_${PHASE_LABEL}"
if [ -n "$SUFFIX" ]; then
  RESULTS_DIR="${RESULTS_DIR}_${SUFFIX}"
fi
mkdir -p "$RESULTS_DIR"

echo "=== EXP Load Phase ==="
echo "Port      : $PORT"
if [ -n "$TERMINALS_PER_INSTANCE" ]; then
  echo "Terminals : $TERMINALS_PER_INSTANCE"
fi
echo "Results   : $RESULTS_DIR"

if [ "$GEN_CONFIGS" = "true" ]; then
  CONFIG_OUT_FILE="$CONFIG_FILE" bash "$ROOT_DIR/scripts/experimental/generate_ycsb_config.sh" "$PORT"
fi

WORKDIR="$BENCH_DIR/benchbase/port_${PORT}"
CFG="$CONFIG_FILE"
mkdir -p "$WORKDIR"
if [ ! -f "$WORKDIR/benchbase.jar" ]; then
  rsync -a "$BENCH_DIR/benchbase/benchbase-mysql/" "$WORKDIR/"
fi

(
  cd "$WORKDIR"
  java -jar benchbase.jar -b ycsb -c "$CFG" --create=false --load=true --execute=false \
    > "$RESULTS_DIR/load_${PORT}.log" 2>&1
)

echo "=== Load Done ==="
echo "Logs: $RESULTS_DIR"
