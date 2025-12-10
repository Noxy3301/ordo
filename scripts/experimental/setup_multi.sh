#!/usr/bin

# Run BenchBase setup (bench/setup.sql + --create) against multiple MySQL hosts directly
# Usage:
#   scripts/experimental/setup_multi.sh [--hosts-file FILE] [--default-port PORT]
# Environment overrides:
#   DO_SQL_SETUP=true|false      (default true)  : apply bench/setup.sql
#   DO_CREATE_SCHEMA=true|false  (default true)  : BenchBase --create
#   MYSQL_USER, MYSQL_PASSWORD, MYSQL_EXTRA_OPTS : passed through to phase_setup.sh
#
# hosts-file format: one host per line, optional port
#   172.31.38.125
#   172.31.47.170:3307
#   # comments and blank lines are ignored

set -euo pipefail

usage() {
  cat <<USAGE
setup_multi.sh [--hosts-file FILE] [--default-port PORT]

Run bench/setup.sql + BenchBase --create against multiple MySQL hosts directly
using the local BenchBase jar (no BenchBase on the remote side required).

Options:
  --hosts-file     Path to hosts list (default: bench/config/mysql_hosts.txt)
  --default-port   Port to use when not specified per line (default: 3307)
  --help           Show this help
USAGE
}

HOSTS_FILE="${HOSTS_FILE:-bench/config/mysql_hosts.txt}"
DEFAULT_PORT="${DEFAULT_PORT:-3307}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hosts-file)   HOSTS_FILE="$2"; shift 2;;
    --default-port) DEFAULT_PORT="$2"; shift 2;;
    --help|-h)      usage; exit 0;;
    --)             shift; break;;
    *) echo "Unknown option: $1" >&2; usage; exit 2;;
  esac
done

if [ ! -f "$HOSTS_FILE" ]; then
  echo "Hosts file not found: $HOSTS_FILE" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../.."

DO_SQL_SETUP=${DO_SQL_SETUP:-true}
DO_CREATE_SCHEMA=${DO_CREATE_SCHEMA:-true}

log() { printf '[%s] %s\n' "$(date '+%F %T')" "$*"; }

while IFS= read -r line || [ -n "$line" ]; do
  # Skip comments/blank
  [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue

  entry="${line//[[:space:]]/}"
  host="$entry"
  port="$DEFAULT_PORT"
  if [[ "$entry" == *:* ]]; then
    host="${entry%%:*}"
    port="${entry##*:}"
  fi

  log "Running setup on ${host}:${port} (DO_SQL_SETUP=${DO_SQL_SETUP}, DO_CREATE_SCHEMA=${DO_CREATE_SCHEMA})"
  MYSQL_HOST="$host" \
  DO_SQL_SETUP="$DO_SQL_SETUP" \
  DO_CREATE_SCHEMA="$DO_CREATE_SCHEMA" \
  "$ROOT_DIR/scripts/experimental/phase_setup.sh" "$port"
done < "$HOSTS_FILE"

log "Done."
