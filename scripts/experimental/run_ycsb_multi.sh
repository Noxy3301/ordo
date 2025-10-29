#!/bin/bash

# Orchestrate multi-instance YCSB runs end-to-end
# Usage (flags only):
#   scripts/experimental/run_ycsb_multi.sh \
#     --instances 4 --start-port 3307 --profile a --terminals 4 \
#     --time 120 --rate 0 --scalefactor 100 [--skip-start] [--preserve-workdirs]
#
# Profile:
#   a              -> YCSB-A (50% read, 50% update)
#
# Note: If rate is set to 0, it is converted to "unlimited" (BenchBase requirement).
#   PRESERVE_WORKDIRS     (true|false, default false)
#   GEN_CONFIGS           (true|false, default true)
#   MAX_PARALLEL          (limit for load phase)

set -euo pipefail

usage() {
  cat <<USAGE
Usage:
  $0 --instances N --start-port P --profile a --terminals T --time SEC --rate R --scalefactor S [--skip-start] [--preserve-workdirs] [--ordo-host HOST] [--ordo-port PORT]

Flags (both --key value and --key=value are supported):
  --instances,    -n     Number of MySQL instances (default 4)
  --start-port,   -p     Starting MySQL port (default 3307)
  --profile,      -w     Workload profile: a
  --terminals,    -t     Terminals per instance (default 4)
  --time,         -d     Execute duration seconds (default 120)
  --rate,         -r     Requests/sec; 0 means unlimited (default 0)
  --scalefactor,  -s     YCSB scalefactor (default 100)
  --skip-start           Skip MySQL start (assume already running)
  --preserve-workdirs    Do not delete BenchBase workdirs after execute
  --ordo-host            Ordo server host/IP (default 127.0.0.1)
  --ordo-port            Ordo server port (default 9999)
  --help, -h             Show this help
USAGE
}

INSTANCES=4
START_PORT=3307
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
    --instances|-n)          INSTANCES="$2"; shift 2;;
    --instances=*)          INSTANCES="${1#*=}"; shift;;
    --start-port|-p)        START_PORT="$2"; shift 2;;
    --start-port=*)         START_PORT="${1#*=}"; shift;;
    --profile|-w)           PROFILE="$2"; shift 2;;
    --profile=*)            PROFILE="${1#*=}"; shift;;
    --terminals|-t)         TERMINALS="$2"; shift 2;;
    --terminals=*)          TERMINALS="${1#*=}"; shift;;
    --time|-d)              TIME_SEC="$2"; shift 2;;
    --time=*)               TIME_SEC="${1#*=}"; shift;;
    --rate|-r)              RATE="$2"; shift 2;;
    --rate=*)               RATE="${1#*=}"; shift;;
    --scalefactor|-s)       SCALEFACTOR="$2"; shift 2;;
    --scalefactor=*)        SCALEFACTOR="${1#*=}"; shift;;
    --skip-start)           SKIP_START=true; shift;;
    --preserve-workdirs)    PRESERVE_WORKDIRS=true; shift;;
    --ordo-host)            ORDO_HOST="$2"; shift 2;;
    --ordo-host=*)          ORDO_HOST="${1#*=}"; shift;;
    --ordo-port)            ORDO_PORT="$2"; shift 2;;
    --ordo-port=*)          ORDO_PORT="${1#*=}"; shift;;
    --help|-h)              usage; exit 0;;
    --)                     shift; break;;
    -*)                     echo "Unknown option: $1" >&2; usage; exit 2;;
    *)                      echo "Non-flag argument not supported: $1" >&2; usage; exit 2;;
  esac
done

PRESERVE_WORKDIRS=${PRESERVE_WORKDIRS:-false}
GEN_CONFIGS=${GEN_CONFIGS:-true}
MAX_PARALLEL=${MAX_PARALLEL:-$INSTANCES}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../.."
BENCH_DIR="$ROOT_DIR/bench"
CONFIG_DIR="$BENCH_DIR/config"
GEN_DIR="$CONFIG_DIR/generated"
BASE_TEMPLATE="$CONFIG_DIR/ycsb.xml"
GEN_TEMPLATE="$GEN_DIR/ycsb.${PROFILE}.generated.xml"

banner() { echo; echo "==== $* ===="; }

if [ ! -f "$BASE_TEMPLATE" ]; then
  echo "ERROR: base template not found: $BASE_TEMPLATE" >&2
  exit 1
fi

# Build weights based on profile
WEIGHTS="50,0,0,50,0,0"   # default A
case "$PROFILE" in
  a|A|ycsb-a|ycsba)
    WEIGHTS="50,0,0,50,0,0" ;;
  *)
    echo "Unknown profile: $PROFILE" >&2
    echo "Supported: a" >&2
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

echo "Config: instances=$INSTANCES start_port=$START_PORT profile=$PROFILE terminals=$TERMINALS time=$TIME_SEC rate=$RATE scalefactor=$SCALEFACTOR ordo=${ORDO_HOST}:${ORDO_PORT}"

if [ "${SKIP_START:-false}" != "true" ]; then
  banner "Starting $INSTANCES MySQL instances from port $START_PORT (Ordo: $ORDO_HOST:$ORDO_PORT)"
  DETACH=true bash "$ROOT_DIR/scripts/experimental/start_mysql_multi.sh" --ordo-host "$ORDO_HOST" --ordo-port "$ORDO_PORT" "$INSTANCES" "$START_PORT"
else
  banner "Skipping MySQL start (SKIP_START=true)"
fi

# Reset status counters so metrics start from zero
banner "Resetting MySQL status counters"
for ((i = 0; i < INSTANCES; ++i)); do
  PORT=$((START_PORT + i))
  "$ROOT_DIR/build/runtime_output_directory/mysql" \
    -u root --protocol=TCP -h 127.0.0.1 -P "$PORT" \
    -e "FLUSH STATUS"
done

export TEMPLATE_FILE="$GEN_TEMPLATE"
export GEN_CONFIGS=true
export OUTPUT_MULTI_DIR="$GEN_DIR/multi"
export CONFIG_MULTI_DIR="$GEN_DIR/multi"

banner "Phase: setup"
bash "$ROOT_DIR/scripts/experimental/phase_setup.sh" "$INSTANCES" "$START_PORT"

banner "Phase: load (MAX_PARALLEL=$MAX_PARALLEL)"
MAX_PARALLEL="$MAX_PARALLEL" bash "$ROOT_DIR/scripts/experimental/phase_load.sh" "$INSTANCES" "$START_PORT"

banner "Phase: execute"
PRESERVE_WORKDIRS="$PRESERVE_WORKDIRS" bash "$ROOT_DIR/scripts/experimental/phase_execute.sh" "$INSTANCES" "$START_PORT"

echo
echo "Done. Check bench/results for the latest *_exp_execute_* directory."
echo "Generated template: $GEN_TEMPLATE (kept for reproducibility)"
