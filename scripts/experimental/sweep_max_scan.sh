#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<USAGE
sweep_max_scan.sh [run_local.sh options]

Override YCSB MAX_SCAN (1,2,4,...,1024), rebuild BenchBase, and run
scripts/run_local.sh with profile B for each value. Any arguments after the
script name are forwarded to run_local.sh (defaults match run_local).
Output from each sweep is copied to bench/results/max_scan_sweep/max_scan_<N>.
USAGE
}

if [[ "${1:-}" =~ ^(--help|-h)$ ]]; then
  usage
  exit 0
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
YCSB_CONSTANTS="$ROOT_DIR/bench/benchbase/src/main/java/com/oltpbenchmark/benchmarks/ycsb/YCSBConstants.java"
BENCHBASE_DIR="$ROOT_DIR/bench/benchbase"
RESULT_ROOT="$ROOT_DIR/bench/results/max_scan_sweep"
RUN_LOCAL="$ROOT_DIR/scripts/run_local.sh"
DEFAULT_TERMINALS=${DEFAULT_TERMINALS:-32}
SUMMARY_OUT="$RESULT_ROOT/summary.csv"

mkdir -p "$RESULT_ROOT"
rm -f "$SUMMARY_OUT"

original_constants=$(mktemp)
trap 'cp "$original_constants" "$YCSB_CONSTANTS"; rm -f "$original_constants"' EXIT
cp "$YCSB_CONSTANTS" "$original_constants"

forward_args=("$@")
has_terminals=false
for arg in "${forward_args[@]}"; do
  case "$arg" in
    --terminals|--terminals=*)
      has_terminals=true
      break
      ;;
  esac
done
if ! $has_terminals; then
  forward_args+=(--terminals "$DEFAULT_TERMINALS")
fi

for exp in $(seq 0 10); do
  max_scan=$((1 << exp))
  python3 - "$YCSB_CONSTANTS" "$max_scan" <<'PY'
import re, sys
path = sys.argv[1]
value = sys.argv[2]
with open(path, "r+", encoding="utf-8") as f:
    data = f.read()
    pattern = r"(public\s+static\s+final\s+int\s+MAX_SCAN\s*=\s*)\d+;"
    def repl(match):
        return f"{match.group(1)}{value};"
    new_data, count = re.subn(pattern, repl, data, count=1)
    if count == 0:
        raise SystemExit(f"Failed to update MAX_SCAN in {path}")
    f.seek(0)
    f.write(new_data)
    f.truncate()
PY

  (
    cd "$BENCHBASE_DIR"
    rm -rf benchbase-mysql
    ./mvnw -q clean package -P mysql -DskipTests
    unzip -oq target/benchbase-mysql.zip
    rm -f target/benchbase-mysql.zip
  )

  bash "$RUN_LOCAL" --profile b "${forward_args[@]}"

  latest_run=$(ls -td "$ROOT_DIR"/bench/results/_run_local/* | head -n 1)
  dest="$RESULT_ROOT/max_scan_${max_scan}"
  rm -rf "$dest"
  cp -r "$latest_run" "$dest"
  echo "[max_scan=${max_scan}] results copied to $dest"

  summary_path="$latest_run/summary.csv"
  if [ -f "$summary_path" ]; then
    if [ ! -f "$SUMMARY_OUT" ]; then
      header=$(head -n1 "$summary_path")
      echo "MaxScan,$header" > "$SUMMARY_OUT"
    fi
    data_line=$(tail -n +2 "$summary_path" | head -n 1)
    if [ -n "$data_line" ]; then
      echo "${max_scan},${data_line}" >> "$SUMMARY_OUT"
    fi
  else
    echo "WARNING: summary.csv not found for max_scan=${max_scan}"
  fi
done

if [ -f "$SUMMARY_OUT" ]; then
  echo "Combined summary written to $SUMMARY_OUT"
else
  echo "WARNING: No summary data collected."
fi
