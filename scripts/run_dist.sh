#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'USAGE'
run_dist.sh [options]

Distributed LineairDB/Ordo sweep runner (no local start/stop).
Sweeps terminal counts, runs scripts/experimental/run_ycsb.sh for each,
and aggregates the resulting aggregate.csv files under bench/results/_run_dist.

Options (can also be provided via env vars):
  --profile NAME           (default: a)
  --time SEC               (default: 30)
  --rate OPS               (default: 0 for unlimited)
  --scalefactor N          (default: 1)
  --terminals "LIST"       (space/comma separated, e.g. "--terminals 4,8,16")
  --mysql-host HOST        (default: 127.0.0.1)
  --mysql-port PORT        (default: 3307)
  --ordo-host HOST         (default: 127.0.0.1)
  --ordo-port PORT         (default: 9999)
  --do-sql-setup           Run bench/setup.sql during setup phase (default: false)
  --run-setup              Run setup phase (default: false; execute-only sweep)
  --run-load               Run load phase (default: false; execute-only sweep)
  --preserve-workdirs      Keep BenchBase workdirs after execute (default: false)
  --help                   Show this help
USAGE
}

PROFILE=${PROFILE:-a}
TIME_SEC=${TIME_SEC:-30}
RATE=${RATE:-0}
SCALEFACTOR=${SCALEFACTOR:-1}
TERMINAL_SWEEP=${TERMINAL_SWEEP:-"1 2 4 8 16 24 32 40 48 56"}
MYSQL_HOST=${MYSQL_HOST:-127.0.0.1}
MYSQL_PORT=${MYSQL_PORT:-3307}
ORDO_HOST=${ORDO_HOST:-127.0.0.1}
ORDO_PORT=${ORDO_PORT:-9999}
DO_SQL_SETUP=${DO_SQL_SETUP:-false}
RUN_SETUP=${RUN_SETUP:-false}
RUN_LOAD=${RUN_LOAD:-false}
PRESERVE_WORKDIRS=${PRESERVE_WORKDIRS:-false}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)           PROFILE="$2"; shift 2;;
    --time)              TIME_SEC="$2"; shift 2;;
    --rate)              RATE="$2"; shift 2;;
    --scalefactor)       SCALEFACTOR="$2"; shift 2;;
    --terminals)         TERMINAL_SWEEP="$2"; shift 2;;
    --mysql-host)        MYSQL_HOST="$2"; shift 2;;
    --mysql-port)        MYSQL_PORT="$2"; shift 2;;
    --ordo-host)         ORDO_HOST="$2"; shift 2;;
    --ordo-port)         ORDO_PORT="$2"; shift 2;;
    --do-sql-setup)      DO_SQL_SETUP=true; shift;;
    --run-setup)         RUN_SETUP=true; shift;;
    --run-load)          RUN_LOAD=true; shift;;
    --preserve-workdirs) PRESERVE_WORKDIRS=true; shift;;
    --help|-h)           usage; exit 0;;
    --)                  shift; break;;
    *) echo "Unknown option: $1" >&2; usage; exit 2;;
  esac
done

# Normalize (convert commas to spaces)
TERMINAL_SWEEP="${TERMINAL_SWEEP//,/ }"
read -r -a TERMINAL_COUNTS <<< "$TERMINAL_SWEEP"
if [ "${#TERMINAL_COUNTS[@]}" -eq 0 ]; then
  echo "ERROR: terminal list is empty" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ROOT_DIR="$ROOT_DIR/bench/results/_run_dist"
RUN_ID=$(date +%Y%m%d_%H%M%S)
RUN_SESSION_DIR="$RUN_ROOT_DIR/$RUN_ID"
mkdir -p "$RUN_SESSION_DIR"

log() { printf '[%s] %s\n' "$(date '+%F %T')" "$*"; }

summary_tmp=$(mktemp)
summary_written=false
summary_finalized=false
summary_path=""

cleanup() {
  if ! $summary_finalized && [ -f "$summary_tmp" ]; then
    rm -f "$summary_tmp"
  fi
}
trap cleanup EXIT

collect_results() {
  local terminals="$1"
  local latest_execute_dir
  latest_execute_dir=$(ls -td "$ROOT_DIR/bench/results/exp/"*_execute_* 2>/dev/null | head -n1 || true)
  if [ -z "$latest_execute_dir" ]; then
    log "WARNING: execute directory not found after terminals=${terminals}"
    return
  fi

  local agg_src="$latest_execute_dir/aggregate.csv"
  if [ ! -f "$agg_src" ]; then
    log "WARNING: aggregate.csv missing in $latest_execute_dir"
    return
  fi

  local agg_copy="$RUN_SESSION_DIR/$(basename "$latest_execute_dir").csv"
  cp "$agg_src" "$agg_copy"
  log "Saved aggregate for terminals=${terminals} -> $agg_copy"

  local cleaned
  cleaned=$(mktemp)
  grep -v '^Clients_OK' "$agg_src" > "$cleaned"
  local lines
  lines=$(wc -l < "$cleaned")
  if [ "$lines" -eq 0 ]; then
    rm -f "$cleaned"
    return
  fi

  if ! $summary_written; then
    cat "$cleaned" >> "$summary_tmp"
    summary_written=true
  else
    if [ "$lines" -gt 1 ]; then
      tail -n +2 "$cleaned" >> "$summary_tmp"
    fi
  fi
  rm -f "$cleaned"
}

log "Starting distributed sweep: profile=$PROFILE scalefactor=$SCALEFACTOR time=$TIME_SEC rate=$RATE"
log "Targets: MySQL ${MYSQL_HOST}:${MYSQL_PORT}, Ordo ${ORDO_HOST}:${ORDO_PORT}"
log "Results for this run will be stored under $RUN_SESSION_DIR"
log "Phases: setup=$( [ "$RUN_SETUP" = true ] && echo on || echo off ) load=$( [ "$RUN_LOAD" = true ] && echo on || echo off ) execute=on"

for term in "${TERMINAL_COUNTS[@]}"; do
  log "=== Sweep: ${term} terminals ==="
  DO_SQL_SETUP="$DO_SQL_SETUP" PRESERVE_WORKDIRS="$PRESERVE_WORKDIRS" \
    "$ROOT_DIR/scripts/experimental/run_ycsb.sh" \
      --mysqld-port "$MYSQL_PORT" \
      --mysql-host "$MYSQL_HOST" \
      --profile "$PROFILE" \
      --terminals "$term" \
      --time "$TIME_SEC" \
      --rate "$RATE" \
      --scalefactor "$SCALEFACTOR" \
      --ordo-host "$ORDO_HOST" \
      --ordo-port "$ORDO_PORT" \
      $( [ "$RUN_SETUP" = true ] || echo "--skip-setup" ) \
      $( [ "$RUN_LOAD" = true ] || echo "--skip-load" )
  collect_results "$term"
done

if $summary_written; then
  summary_path="$RUN_SESSION_DIR/summary.csv"
  mv "$summary_tmp" "$summary_path"
  summary_finalized=true
  log "Summary written to $summary_path"
else
  log "WARNING: no summary data collected"
fi

log "Done."
