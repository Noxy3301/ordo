#!/usr/bin/env bash

# Simple CPU frequency monitor.
# Usage:
#   scripts/freq_watch.sh start <log_path> <pid_path>
#   scripts/freq_watch.sh stop <pid_path>

set -euo pipefail

cmd=${1:-}
case "$cmd" in
  start)
    log_path=${2:-"./freq_log.txt"}
    pid_path=${3:-"/tmp/freq_watch.pid"}
    if [ -f "$pid_path" ] && kill -0 "$(cat "$pid_path")" 2>/dev/null; then
      echo "freq_watch already running (pid=$(cat "$pid_path"))" >&2
      exit 1
    fi
    (
      while true; do
        ts=$(date "+%F %T")
        freqs=$(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq 2>/dev/null | tr '\n' ' ')
        echo "$ts $freqs"
        sleep 1
      done
    ) >>"$log_path" &
    echo $! > "$pid_path"
    ;;
  stop)
    pid_path=${2:-"/tmp/freq_watch.pid"}
    if [ -f "$pid_path" ]; then
      pid=$(cat "$pid_path")
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      rm -f "$pid_path"
    fi
    ;;
  *)
    echo "Usage: $0 {start <log_path> <pid_path>|stop <pid_path>}" >&2
    exit 2
    ;;
esac
