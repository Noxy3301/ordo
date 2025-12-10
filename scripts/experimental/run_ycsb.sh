#!/bin/bash

# Orchestrate YCSB runs end-to-end (single MySQL instance)
# Usage (flags only):
#   scripts/experimental/run_ycsb.sh \
#     --mysqld-port 3307 --profile a --terminals 4 \
#     --time 120 --rate 0 --scalefactor 100 [--preserve-workdirs]
#
# Profile:
#   a              -> YCSB-A (50% read, 50% update)
#   b              -> YCSB-B (95% read, 5% update)
#   b-scan         -> YCSB-B variant (95% scan, 5% update)
#   c              -> YCSB-C (100% read)
#
# Note: If rate is set to 0, it is converted to "unlimited" (BenchBase requirement).
#   PRESERVE_WORKDIRS     (true|false, default false)
#   GEN_CONFIGS           (true|false, default true)

set -euo pipefail

usage() {
  cat <<USAGE
Usage:
  $0 --mysqld-port P --profile a --terminals T --time SEC --rate R --scalefactor S [--preserve-workdirs] [--ordo-host HOST] [--ordo-port PORT]

Flags:
  --mysqld-port          MySQL port (default 3307)
  --profile              Workload profile: a | b | b-scan | c
  --terminals            Terminals (default 4)
  --time                 Execute duration seconds (default 120)
  --rate                 Requests/sec; 0 means unlimited (default 0)
  --scalefactor          YCSB scalefactor (default 100)
  --preserve-workdirs    Do not delete BenchBase workdirs after execute
  --ordo-host            Ordo server host/IP (default 127.0.0.1)
  --ordo-port            Ordo server port (default 9999)
  --help, -h             Show this help
USAGE
}

MYSQLD_PORT=3307
PROFILE=a
TERMINALS=4
TIME_SEC=120
RATE=0
SCALEFACTOR=100
ORDO_HOST=127.0.0.1
ORDO_PORT=9999

# Parse flags (flags only)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mysqld-port)         MYSQLD_PORT="$2"; shift 2;;
    --profile)             PROFILE="$2"; shift 2;;
    --terminals)           TERMINALS="$2"; shift 2;;
    --time)                TIME_SEC="$2"; shift 2;;
    --rate)                RATE="$2"; shift 2;;
    --scalefactor)         SCALEFACTOR="$2"; shift 2;;
    --preserve-workdirs)   PRESERVE_WORKDIRS=true; shift;;
    --ordo-host)           ORDO_HOST="$2"; shift 2;;
    --ordo-port)           ORDO_PORT="$2"; shift 2;;
    --help|-h)             usage; exit 0;;
    --)                    shift; break;;
    -*)                    echo "Unknown option: $1" >&2; usage; exit 2;;
    *)                     echo "Non-flag argument not supported: $1" >&2; usage; exit 2;;
  esac
done

PRESERVE_WORKDIRS=${PRESERVE_WORKDIRS:-false}
GEN_CONFIGS=${GEN_CONFIGS:-true}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../.."
BENCH_DIR="$ROOT_DIR/bench"
CONFIG_DIR="$BENCH_DIR/config"
GEN_DIR="$CONFIG_DIR/generated"
BASE_TEMPLATE="$CONFIG_DIR/ycsb.xml"
GEN_TEMPLATE="$GEN_DIR/ycsb.generated.xml"
CONFIG_OUT_FILE="$GEN_DIR/ycsb.generated.xml"

banner() { echo; echo "==== $* ===="; }

RUN_TS=$(date +%Y%m%d_%H%M%S)

if [ ! -f "$BASE_TEMPLATE" ]; then
  echo "ERROR: base template not found: $BASE_TEMPLATE" >&2
  exit 1
fi

# Build weights based on profile
WEIGHTS="50,0,0,50,0,0"   # default A
case "$PROFILE" in
  a|A|ycsb-a|ycsba)
    WEIGHTS="50,0,0,50,0,0" ;;
  b|B|ycsb-b|ycsbb)
    WEIGHTS="95,0,0,5,0,0" ;;
  b-scan|b_scan|bscan|ycsb-b-scan|bs|BSCAN)
    WEIGHTS="0,0,95,5,0,0" ;;
  c|C|ycsb-c|ycsbc)
    WEIGHTS="100,0,0,0,0,0" ;;
  *)
    echo "Unknown profile: $PROFILE" >&2
    echo "Supported: a, b, c" >&2
    exit 1 ;;
esac

banner "Preparing generated template ($PROFILE)"
mkdir -p "$GEN_DIR"
cp "$BASE_TEMPLATE" "$GEN_TEMPLATE"

# Replace core knobs
sed -i "s#<terminals>.*</terminals>#<terminals>$TERMINALS</terminals>#" "$GEN_TEMPLATE"
sed -i "s#<scalefactor>.*</scalefactor>#<scalefactor>$SCALEFACTOR</scalefactor>#" "$GEN_TEMPLATE"
sed -i "s#<time>.*</time>#<time>$TIME_SEC</time>#" "$GEN_TEMPLATE"

# Rate: convert 0 to "unlimited" (BenchBase requirement)
if [ "$RATE" = "0" ]; then
  sed -i "s#<rate>.*</rate>#<rate>unlimited</rate>#" "$GEN_TEMPLATE"
else
  sed -i "s#<rate>.*</rate>#<rate>$RATE</rate>#" "$GEN_TEMPLATE"
fi
sed -i "s#<weights>.*</weights>#<weights>$WEIGHTS</weights>#" "$GEN_TEMPLATE"

echo "Config: mysqld_port=$MYSQLD_PORT profile=$PROFILE terminals=$TERMINALS time=$TIME_SEC rate=$RATE scalefactor=$SCALEFACTOR ordo=${ORDO_HOST}:${ORDO_PORT}"

# Reset status counters so metrics start from zero
banner "Resetting MySQL status counters"
"$ROOT_DIR/build/runtime_output_directory/mysql" \
  -u root --protocol=TCP -h 127.0.0.1 -P "$MYSQLD_PORT" \
  -e "FLUSH STATUS"

export TEMPLATE_FILE="$GEN_TEMPLATE"
export GEN_CONFIGS=true
export YCSB_TERMINALS="$TERMINALS"
export CONFIG_OUT_FILE="$CONFIG_OUT_FILE"

mkdir -p "$(dirname "$CONFIG_OUT_FILE")"
banner "Phase: setup"
RUN_TS="$RUN_TS" PHASE_LABEL="1_setup" bash "$ROOT_DIR/scripts/experimental/phase_setup.sh" "$MYSQLD_PORT"

banner "Phase: load"
RUN_TS="$RUN_TS" PHASE_LABEL="2_load" bash "$ROOT_DIR/scripts/experimental/phase_load.sh" "$MYSQLD_PORT"

banner "Phase: execute"
RUN_TS="$RUN_TS" PHASE_LABEL="3_execute" PRESERVE_WORKDIRS="$PRESERVE_WORKDIRS" bash "$ROOT_DIR/scripts/experimental/phase_execute.sh" "$MYSQLD_PORT"

echo
echo "Done. Check bench/results/exp for the latest *_execute_* directory."
