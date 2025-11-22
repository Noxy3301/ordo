#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'USAGE'
run_local.sh [options]

End-to-end local LineairDB/Ordo perf sweep runner.
For each terminal count (default: 1 2 4 8 16 24 32 40 48 56) the script:
  1. Stops any running Ordo/MySQL pair and restarts them (ports 9999 / 3307).
  2. Prints the LineairDB sysvars to confirm the Ordo endpoint.
  3. Executes run_ycsb_multi.sh with --skip-start.
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

PROFILE=${PROFILE:-b}
TIME_SEC=${TIME_SEC:-30}
RATE=${RATE:-0}
SCALEFACTOR=${SCALEFACTOR:-1}
TERMINAL_SWEEP=${TERMINAL_SWEEP:-"1 2 4 6 8 10 12 14 16 18 20 22 24 26 28 30 32 34 36 38 40 42 44 46 48 50 52 54 56 58 60 62 64"}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)        PROFILE="$2"; shift 2;;
    --profile=*)      PROFILE="${1#*=}"; shift;;
    --time)           TIME_SEC="$2"; shift 2;;
    --time=*)         TIME_SEC="${1#*=}"; shift;;
    --rate)           RATE="$2"; shift 2;;
    --rate=*)         RATE="${1#*=}"; shift;;
    --scalefactor)    SCALEFACTOR="$2"; shift 2;;
    --scalefactor=*)  SCALEFACTOR="${1#*=}"; shift;;
    --terminals)      TERMINAL_SWEEP="$2"; shift 2;;
    --terminals=*)    TERMINAL_SWEEP="${1#*=}"; shift;;
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
MYSQL_PORT=3307
INSTANCES=1
RUN_ROOT_DIR="$ROOT_DIR/bench/results/_run_local"
RUN_ID=$(date +%Y%m%d_%H%M%S)
RUN_SESSION_DIR="$RUN_ROOT_DIR/$RUN_ID"
mkdir -p "$RUN_SESSION_DIR"

log() { printf '[%s] %s\n' "$(date '+%F %T')" "$*"; }

PIDSTAT_AVAILABLE=false
if command -v pidstat >/dev/null 2>&1; then
  PIDSTAT_AVAILABLE=true
else
  log "pidstat not found; CPU usage logs will be skipped"
fi
PIDSTAT_DIR="$RUN_SESSION_DIR/pidstat"
PIDSTAT_PIDS=()
ORDO_PID=""
CURRENT_MYSQL_PIDS=()
FREQ_LOG=""

started_mysql=false
started_ordo=false

stop_services() {
  stop_pidstat_monitors
  (cd "$ROOT_DIR" && scripts/stop_mysql.sh >/dev/null 2>&1) || true
  started_mysql=false
  (cd "$ROOT_DIR" && scripts/stop_ordo.sh >/dev/null 2>&1) || true
  started_ordo=false
  ORDO_PID=""
  CURRENT_MYSQL_PIDS=()
}

start_ordo_service() {
  log "Starting Ordo server (port 9999)"
  (cd "$ROOT_DIR" && scripts/start_ordo.sh)
  started_ordo=true
  if [ -f /tmp/ordo_server.pid ]; then
    ORDO_PID=$(cat /tmp/ordo_server.pid 2>/dev/null || true)
    log "Ordo PID: ${ORDO_PID:-unknown}"
  else
    ORDO_PID=""
    log "WARNING: unable to determine Ordo PID"
  fi
  sleep 1
}

start_mysql_service() {
  log "Starting MySQL (port ${MYSQL_PORT}) with LineairDB"
  DETACH=true bash "$ROOT_DIR/scripts/experimental/start_mysql_multi.sh" \
    "$INSTANCES" \
    "$MYSQL_PORT"
  started_mysql=true
  CURRENT_MYSQL_PIDS=()
  for ((i = 0; i < INSTANCES; ++i)); do
    local port=$((MYSQL_PORT + i))
    local pid_file="/tmp/mysql_${port}.pid"
    if [ -f "$pid_file" ]; then
      CURRENT_MYSQL_PIDS+=("$(cat "$pid_file" 2>/dev/null || true)")
    fi
  done
  if [ ${#CURRENT_MYSQL_PIDS[@]} -gt 0 ]; then
    log "MySQL PID(s): ${CURRENT_MYSQL_PIDS[*]}"
  else
    log "WARNING: unable to determine MySQL PID(s)"
  fi
}

verify_lineairdb_target() {
  log "Verifying LineairDB sysvars"
  "$ROOT_DIR/build/runtime_output_directory/mysql" \
    -u root --protocol=TCP -h 127.0.0.1 -P "$MYSQL_PORT" \
    -e "SHOW VARIABLES LIKE 'lineairdb_ordo_%';"
}

start_pidstat_monitors() {
  local terminals="$1"
  $PIDSTAT_AVAILABLE || return
  stop_pidstat_monitors
  mkdir -p "$PIDSTAT_DIR"
  local pid_args=()
  if [ -n "${ORDO_PID:-}" ]; then
    pid_args+=("$ORDO_PID")
  fi
  for pid in "${CURRENT_MYSQL_PIDS[@]}"; do
    if [ -n "$pid" ]; then
      pid_args+=("$pid")
    fi
  done
  if [ ${#pid_args[@]} -gt 0 ]; then
    local joined
    joined=$(IFS=,; echo "${pid_args[*]}")
    local log_file="$PIDSTAT_DIR/term_${terminals}_ordo_mysql.log"
    pidstat -u 1 -p "$joined" > "$log_file" &
    PIDSTAT_PIDS+=($!)
  fi
  local java_log="$PIDSTAT_DIR/term_${terminals}_java.log"
  pidstat -u 1 -C java > "$java_log" &
  PIDSTAT_PIDS+=($!)
  log "pidstat logging enabled for terminals=${terminals} (output: $PIDSTAT_DIR)"
}

stop_pidstat_monitors() {
  if [ ${#PIDSTAT_PIDS[@]} -eq 0 ]; then
    return
  fi
  for pid in "${PIDSTAT_PIDS[@]}"; do
    if [ -n "$pid" ]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  PIDSTAT_PIDS=()
}

start_freq_monitor() {
  local terminals="$1"
  FREQ_LOG="$RUN_SESSION_DIR/freq_log_t${terminals}.txt"
  FREQ_PID_FILE="$RUN_SESSION_DIR/freq_log_t${terminals}.pid"
  "$ROOT_DIR/scripts/freq_watch.sh" start "$FREQ_LOG" "$FREQ_PID_FILE"
}

stop_freq_monitor() {
  if [ -n "${FREQ_PID_FILE:-}" ]; then
    "$ROOT_DIR/scripts/freq_watch.sh" stop "$FREQ_PID_FILE"
    unset FREQ_PID_FILE
  fi
}

run_ycsb_workload() {
  local terminals="$1"
  log "Running YCSB (profile=${PROFILE}, terminals=${terminals})"
  start_pidstat_monitors "$terminals"
  start_freq_monitor "$terminals"
  if ! "$ROOT_DIR/scripts/experimental/run_ycsb_multi.sh" \
    --instances "$INSTANCES" \
    --start-port "$MYSQL_PORT" \
    --profile "$PROFILE" \
    --terminals "$terminals" \
    --time "$TIME_SEC" \
    --rate "$RATE" \
    --scalefactor "$SCALEFACTOR" \
    --skip-start; then
    local rc=$?
    stop_pidstat_monitors
    stop_freq_monitor
    return "$rc"
  fi
  stop_pidstat_monitors
  stop_freq_monitor
}

summary_tmp=$(mktemp)
summary_written=false
summary_finalized=false

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

copy_profiles() {
  local terminals="$1"

  # Server-side RPC profile (ordo-server)
  local latest_server
  latest_server=$(ls -t "$ROOT_DIR"/lineairdb_logs/ordo_rpc_profile_*.csv 2>/dev/null | head -n1 || true)
  if [ -n "$latest_server" ]; then
    cp "$latest_server" "$RUN_SESSION_DIR/ordo_rpc_profile_t${terminals}.csv"
  fi

  # Client-side RPC profile (MySQL plugin; datadir per port)
  local latest_client
  latest_client=$(ls -t "$ROOT_DIR"/build/data_*/lineairdb_logs/ordo_client_profile_*.csv 2>/dev/null | head -n1 || true)
  if [ -n "$latest_client" ]; then
    cp "$latest_client" "$RUN_SESSION_DIR/ordo_client_profile_t${terminals}.csv"
  fi
}

log "Stopping leftover Ordo/MySQL processes"
stop_services

log "Building server + LineairDB SE"
(cd "$ROOT_DIR" && scripts/build_partial.sh)
log "Results for this run will be stored under $RUN_SESSION_DIR"

for term in "${TERMINAL_COUNTS[@]}"; do
  log "=== Sweep: ${term} terminals ==="
  stop_services
  start_ordo_service
  start_mysql_service
  verify_lineairdb_target
  run_ycsb_workload "$term"
  collect_results "$term"
  copy_profiles "$term"
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
