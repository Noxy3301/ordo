#!/bin/bash

# Generate YCSB configuration file
# Usage: ./generate_ycsb_configs.sh [mysqld_port]

MYSQLD_PORT=${1:-3307}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/../../bench/config"
# Allow caller to override the template file via env var TEMPLATE_FILE
TEMPLATE_FILE=${TEMPLATE_FILE:-"$CONFIG_DIR/ycsb.xml"}
# Allow caller to override the output config path via CONFIG_OUT_FILE
CONFIG_OUT_FILE=${CONFIG_OUT_FILE:-"$CONFIG_DIR/generated/ycsb.generated.xml"}

echo "Generating YCSB config file for port $MYSQLD_PORT..."

mkdir -p "$(dirname "$CONFIG_OUT_FILE")"

tmpfile=$(mktemp)
sed "s/:3307/:$MYSQLD_PORT/g" "$TEMPLATE_FILE" > "$tmpfile"
mv "$tmpfile" "$CONFIG_OUT_FILE"
echo "Generated: $CONFIG_OUT_FILE"
