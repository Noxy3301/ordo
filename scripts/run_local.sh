#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'USAGE'
run_local.sh [options]

End-to-end local LineairDB/Ordo perf sweep runner.
For each terminal count (default: 1 2 4 8 16 24 32 40 48 56) the script:
  1. Stops any running Ordo/MySQL pair and restarts them (ports 9999 / 3307).
  2. Prints the LineairDB sysvars to confirm the Ordo endpoint.
  3. Executes run_ycsb.sh.
  4. Copies aggregate.csv into bench/results/_run_local.

After the sweep it emits a combined summary file at
bench/results/_run_local/run_local_<timestamp>.csv.

Options (can also be provided via env vars):
  --profile NAME           (default: a)
  --time SEC               (default: 30)
  --rate OPS               (default: 0 for unlimited)
  --scalefactor N          (default: 1)
  --terminals "LIST"       (space/comma separated, e.g. "--terminals 4,8,16")
  --help                   Show this help
USAGE
}

PROFILE=${PROFILE:-a}
TIME_SEC=${TIME_SEC:-30}
RATE=${RATE:-0}
SCALEFACTOR=${SCALEFACTOR:-1}
TERMINAL_SWEEP=${TERMINAL_SWEEP:-"1 2"}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)        PROFILE="$2"; shift 2;;
    --time)           TIME_SEC="$2"; shift 2;;
    --rate)           RATE="$2"; shift 2;;
    --scalefactor)    SCALEFACTOR="$2"; shift 2;;
    --terminals)      TERMINAL_SWEEP="$2"; shift 2;;
    --help|-h)        usage; exit 0;;
    --)               shift; break;;
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
ORDO_HOST=${ORDO_HOST:-127.0.0.1}
ORDO_PORT=${ORDO_PORT:-9999}
MYSQLD_PORT=3307
RUN_ROOT_DIR="$ROOT_DIR/bench/results/_run_local"
RUN_ID=$(date +%Y%m%d_%H%M%S)
RUN_SESSION_DIR="$RUN_ROOT_DIR/$RUN_ID"
mkdir -p "$RUN_SESSION_DIR"

log() { printf '[%s] %s\n' "$(date '+%F %T')" "$*"; }

started_mysql=false
started_ordo=false

stop_services() {
  (cd "$ROOT_DIR" && scripts/stop_mysql.sh >/dev/null 2>&1) || true
  started_mysql=false
  (cd "$ROOT_DIR" && scripts/stop_ordo.sh >/dev/null 2>&1) || true
  started_ordo=false
}

start_ordo_service() {
  log "Starting Ordo server (port 9999)"
  (cd "$ROOT_DIR" && scripts/start_ordo.sh)
  started_ordo=true
  sleep 1
}

start_mysql_service() {
  log "Starting MySQL (port ${MYSQLD_PORT}) with LineairDB"
  bash "$ROOT_DIR/scripts/start_mysql.sh" \
    --mysqld-port "$MYSQLD_PORT" \
    --ordo-host "$ORDO_HOST" \
    --ordo-port "$ORDO_PORT"
  started_mysql=true
}

verify_lineairdb_target() {
  log "Verifying LineairDB sysvars"
  "$ROOT_DIR/build/runtime_output_directory/mysql" \
    -u root --protocol=TCP -h 127.0.0.1 -P "$MYSQLD_PORT" \
    -e "SHOW VARIABLES LIKE 'lineairdb_ordo_%';"
}

run_ycsb_workload() {
  local terminals="$1"
  log "Running YCSB (profile=${PROFILE}, terminals=${terminals})"
  "$ROOT_DIR/scripts/experimental/run_ycsb.sh" \
    --mysqld-port "$MYSQLD_PORT" \
    --ordo-host "$ORDO_HOST" \
    --ordo-port "$ORDO_PORT" \
    --profile "$PROFILE" \
    --terminals "$terminals" \
    --time "$TIME_SEC" \
    --rate "$RATE" \
    --scalefactor "$SCALEFACTOR"
}

summary_tmp=$(mktemp)
summary_written=false
summary_finalized=false
summary_path=""

cleanup() {
  stop_services
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

log "Stopping leftover Ordo/MySQL processes"
stop_services

log "Building server + LineairDB SE"
(cd "$ROOT_DIR" && scripts/build_partial.sh)
log "Results for this run will be stored under $RUN_SESSION_DIR"

start_ordo_service
start_mysql_service

for term in "${TERMINAL_COUNTS[@]}"; do
  log "=== Sweep: ${term} terminals ==="
  verify_lineairdb_target
  run_ycsb_workload "$term"
  collect_results "$term"
done

stop_services

if $summary_written; then
  summary_path="$RUN_SESSION_DIR/summary.csv"
  mv "$summary_tmp" "$summary_path"
  summary_finalized=true
  log "Summary written to $summary_path"
else
  log "WARNING: no summary data collected"
fi

log "Done."
