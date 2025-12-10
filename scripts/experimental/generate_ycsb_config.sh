#!/bin/bash

# Generate YCSB configuration file
# Usage: ./generate_ycsb_config.sh [mysqld_port]

MYSQLD_PORT=${1:-3307}
MYSQL_HOST=${MYSQL_HOST:-localhost}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/../../bench/config"
# Allow caller to override the template file via env var TEMPLATE_FILE
TEMPLATE_FILE=${TEMPLATE_FILE:-"$CONFIG_DIR/ycsb.xml"}
# Allow caller to override the output config path via CONFIG_OUT_FILE
CONFIG_OUT_FILE=${CONFIG_OUT_FILE:-"$CONFIG_DIR/generated/ycsb.generated.xml"}

echo "Generating YCSB config file for ${MYSQL_HOST}:${MYSQLD_PORT} ..."

mkdir -p "$(dirname "$CONFIG_OUT_FILE")"

tmpfile=$(mktemp)
sed -E "s#jdbc:mysql://[^:/]+:[0-9]+/#jdbc:mysql://${MYSQL_HOST}:${MYSQLD_PORT}/#g" "$TEMPLATE_FILE" > "$tmpfile"
mv "$tmpfile" "$CONFIG_OUT_FILE"
echo "Generated: $CONFIG_OUT_FILE"
