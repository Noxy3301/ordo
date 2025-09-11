#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT_DIR/build/server/ordo-server"
LOG_DIR="$ROOT_DIR/lineairdb_logs"
TS=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/ordo_$TS.log"
PID_FILE="/tmp/ordo_server.pid"

mkdir -p "$LOG_DIR"

if [ ! -x "$BIN" ]; then
  echo "ERROR: binary not found: $BIN" >&2
  echo "Hint: build it via: bash scripts/build.sh (or build_partial.sh)" >&2
  exit 1
fi

if pgrep -f "/build/server/ordo-server" >/dev/null 2>&1; then
  echo "ordo-server already running. Use scripts/stop_ordo.sh to stop it." >&2
  exit 0
fi

echo "Starting ordo-server (port 9999) ..."
nohup "$BIN" > "$LOG_FILE" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"

echo "Started ordo-server with PID $PID"
echo "Logs: $LOG_FILE"
echo "PID file: $PID_FILE"

