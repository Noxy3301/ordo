#!/bin/bash

# Generate YCSB configuration files for multiple MySQL instances
# Usage: ./generate_ycsb_configs.sh [num_instances] [starting_port]

NUM_INSTANCES=${1:-2}
STARTING_PORT=${2:-3307}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/../../bench/config"
# Allow caller to override the template file via env var TEMPLATE_FILE
TEMPLATE_FILE=${TEMPLATE_FILE:-"$CONFIG_DIR/ycsb.xml"}
# Allow caller to override the output multi-config directory via OUTPUT_MULTI_DIR
MULTI_CONFIG_DIR=${OUTPUT_MULTI_DIR:-"$CONFIG_DIR/multi"}

echo "Generating $NUM_INSTANCES YCSB config files starting from port $STARTING_PORT..."

# Create multi-config directory if it doesn't exist
mkdir -p "$MULTI_CONFIG_DIR"

for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    CONFIG_FILE="$MULTI_CONFIG_DIR/ycsb_port_${PORT}.xml"
    
    echo "Creating config for port $PORT: $CONFIG_FILE"
    
    # Read template and replace port
    sed "s/:3307/:$PORT/g" "$TEMPLATE_FILE" > "$CONFIG_FILE"
    
    echo "  -> Generated: $CONFIG_FILE"
done

echo ""
echo "Generated $NUM_INSTANCES configuration files:"
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    echo "  - $MULTI_CONFIG_DIR/ycsb_port_${PORT}.xml"
done
