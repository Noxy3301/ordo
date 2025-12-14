#!/bin/bash

set -euo pipefail

ORDO_HOST="127.0.0.1"
ORDO_PORT=9999
MYSQLD_PORT=3307

usage() {
  cat <<USAGE
Usage: $0 [--mysqld-port N] [--ordo-host HOST] [--ordo-port PORT]
Defaults: mysqld-port=3307, ordo=127.0.0.1:9999
Data dir / socket are derived from mysqld-port (3307 -> data,/tmp/mysql.sock; others -> data_PORT,/tmp/mysql_PORT.sock)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mysqld-port) MYSQLD_PORT="$2"; shift 2;;
    --ordo-host) ORDO_HOST="$2"; shift 2;;
    --ordo-port) ORDO_PORT="$2"; shift 2;;
    --help|-h) usage; exit 0;;
    --) shift; break;;
    -*) echo "Unknown option: $1" >&2; usage; exit 2;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2;;
  esac
done

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/build"

DATA_DIR="./data"
SOCKET="/tmp/mysql.sock"
PID_FILE="/tmp/mysql.pid"
if [ "$MYSQLD_PORT" != "3307" ]; then
  DATA_DIR="./data_${MYSQLD_PORT}"
  SOCKET="/tmp/mysql_${MYSQLD_PORT}.sock"
  PID_FILE="/tmp/mysql_${MYSQLD_PORT}.pid"
fi

# Step 1: Initialize if data directory doesn't exist
if [ ! -d "$DATA_DIR" ] || [ ! -f "$DATA_DIR/ibdata1" ]; then
  echo "Step 1/5: Initializing MySQL data directory..."
  ./runtime_output_directory/mysqld --initialize-insecure --user="$USER" --datadir="$DATA_DIR"
fi

echo "Step 2/5: Starting MySQL with InnoDB..."
./runtime_output_directory/mysqld --datadir="$DATA_DIR" --socket="$SOCKET" --port="$MYSQLD_PORT" \
  --pid-file="$PID_FILE" \
  --max-connections=16384 \
  --open-files-limit=65535 \
  --table-open-cache=8192 &
BOOT_PID=$!

echo "Step 3/5: Waiting for MySQL to be ready..."
until ./runtime_output_directory/mysqladmin ping -u root --socket="$SOCKET" --port="$MYSQLD_PORT" >/dev/null 2>&1; do
  sleep 1
done

echo "Step 4/5: Installing LineairDB plugin..."
./runtime_output_directory/mysql -u root --socket="$SOCKET" --port="$MYSQLD_PORT" \
  -e "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';" 2>/dev/null || true

echo "Step 5/5: Stopping MySQL and restarting with LineairDB as default..."
kill "$BOOT_PID" 2>/dev/null || true
wait "$BOOT_PID" 2>/dev/null || true
sleep 3

./runtime_output_directory/mysqld --datadir="$DATA_DIR" --socket="$SOCKET" --port="$MYSQLD_PORT" \
  --pid-file="$PID_FILE" --default-storage-engine=lineairdb \
  --max-connections=16384 \
  --open-files-limit=65535 \
  --table-open-cache=8192 &
MYSQL_PID=$!

until ./runtime_output_directory/mysqladmin ping -u root --socket="$SOCKET" --port="$MYSQLD_PORT" >/dev/null 2>&1; do
  sleep 1
done

./runtime_output_directory/mysql -u root --socket="$SOCKET" --port="$MYSQLD_PORT" \
  -e "SET GLOBAL lineairdb_ordo_host='${ORDO_HOST}'; SET GLOBAL lineairdb_ordo_port=${ORDO_PORT};" >/dev/null

echo "MySQL running with LineairDB"
echo "PID       : $MYSQL_PID"
echo "Port      : $MYSQLD_PORT"
echo "Data dir  : $DATA_DIR"
echo "Socket    : $SOCKET"
echo "Ordo      : ${ORDO_HOST}:${ORDO_PORT}"
