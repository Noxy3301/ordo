#!/bin/bash

# Multi-port MySQL Server Startup Script
# Usage: ./start_mysql_multi.sh [--ordo-host HOST] [--ordo-port PORT] [num_instances] [starting_port]

# Defaults
ORDO_HOST="127.0.0.1"
ORDO_PORT=9999

# Parse optional flags first (supports --key value and --key=value)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ordo-host) ORDO_HOST="$2"; shift 2;;
    --ordo-host=*) ORDO_HOST="${1#*=}"; shift;;
    --ordo-port) ORDO_PORT="$2"; shift 2;;
    --ordo-port=*) ORDO_PORT="${1#*=}"; shift;;
    --) shift; break;;
    -*) echo "Unknown option: $1" >&2; exit 2;;
    *) break;;
  esac
done

# Positionals
NUM_INSTANCES=${1:-2}  # Default: 2 instances
STARTING_PORT=${2:-3307}  # Default: starting from port 3307
# If DETACH=true, start instances and return immediately
DETACH=${DETACH:-false}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/../../build"

echo "Starting $NUM_INSTANCES MySQL instances from port $STARTING_PORT..."
echo "LineairDB target: $ORDO_HOST:$ORDO_PORT"

# Array to store MySQL PIDs
declare -a MYSQL_PIDS=()

# Function to cleanup on exit
cleanup() {
    echo "Cleaning up MySQL instances..."
    for pid in "${MYSQL_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            echo "Killed MySQL instance with PID $pid"
        fi
    done
    exit 0
}

# Trap signals for cleanup (only in attached mode)
if [ "$DETACH" != "true" ]; then
  trap cleanup SIGINT SIGTERM
fi

cd "$BUILD_DIR"

# Phase 1: Initialize data directories in parallel
echo "=== Phase 1: Initializing data directories in parallel ==="
declare -a INIT_PIDS=()
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    DATA_DIR="./data_${PORT}"
    
    if [ ! -d "$DATA_DIR" ]; then
        (
            echo "Initializing MySQL data directory for port $PORT..."
            ./runtime_output_directory/mysqld --initialize-insecure --user=$USER --datadir="$DATA_DIR"
            echo "Data directory initialization completed for port $PORT"
        ) &
        INIT_PIDS+=($!)
    fi
done

# Wait for all initializations to complete
if [ ${#INIT_PIDS[@]} -gt 0 ]; then
    echo "Waiting for all data directory initializations to complete..."
    for pid in "${INIT_PIDS[@]}"; do
        wait $pid
    done
    echo "All data directory initializations completed!"
else
    echo "All data directories already exist, skipping initialization."
fi

# Phase 2: Start all MySQL instances in parallel
echo "=== Phase 2: Starting all MySQL instances in parallel ==="
declare -a STARTUP_PIDS=()
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    SOCKET="/tmp/mysql_${PORT}.sock"
    DATA_DIR="./data_${PORT}"
    
    echo "Starting MySQL server on port $PORT..."
    ./runtime_output_directory/mysqld --datadir="$DATA_DIR" --socket="$SOCKET" --port="$PORT" \
        --pid-file="/tmp/mysql_${PORT}.pid" &
    MYSQL_PID=$!
    MYSQL_PIDS+=($MYSQL_PID)
    
    echo "MySQL instance $i started with PID $MYSQL_PID on port $PORT"
done

# Phase 3: Wait for all instances to start
echo "=== Phase 3: Waiting for all instances to be ready ==="
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    SOCKET="/tmp/mysql_${PORT}.sock"
    
    echo "Waiting for MySQL on port $PORT..."
    for j in {1..45}; do  # Longer timeout for parallel startup
        if ./runtime_output_directory/mysqladmin ping -u root --socket="$SOCKET" --port="$PORT" >/dev/null 2>&1; then
            echo "MySQL on port $PORT is ready!"
            break
        fi
        if [ $j -eq 45 ]; then
            echo "MySQL on port $PORT failed to start within 45 seconds"
            cleanup
        fi
        sleep 1
    done &
    STARTUP_PIDS+=($!)
done

# Wait for all startup checks to complete
echo "Waiting for all startup checks to complete..."
for pid in "${STARTUP_PIDS[@]}"; do
    wait $pid
done

# Phase 4: Install LineairDB plugin in parallel
echo "=== Phase 4: Installing LineairDB plugin on all instances ==="
declare -a PLUGIN_PIDS=()
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    SOCKET="/tmp/mysql_${PORT}.sock"
    
    (
        echo "Installing LineairDB plugin on port $PORT..."
        ./runtime_output_directory/mysql -u root --socket="$SOCKET" --port="$PORT" \
            -e "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';" 2>/dev/null || true
        echo "Plugin installation completed for port $PORT"
    ) &
    PLUGIN_PIDS+=($!)
done

# Wait for all plugin installations
echo "Waiting for all plugin installations..."
for pid in "${PLUGIN_PIDS[@]}"; do
    wait $pid
done

# Phase 5: Restart all instances with LineairDB in parallel
echo "=== Phase 5: Restarting all instances with LineairDB ==="
echo "Stopping all instances..."
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    kill ${MYSQL_PIDS[$i]} 2>/dev/null || true
done

# Wait for all to stop
echo "Waiting for all instances to stop..."
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    wait ${MYSQL_PIDS[$i]} 2>/dev/null || true
done
sleep 3

# Start all with LineairDB
echo "Starting all instances with LineairDB as default engine..."
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    SOCKET="/tmp/mysql_${PORT}.sock"
    DATA_DIR="./data_${PORT}"
    
    ./runtime_output_directory/mysqld --datadir="$DATA_DIR" --socket="$SOCKET" --port="$PORT" \
        --pid-file="/tmp/mysql_${PORT}.pid" \
        --default-storage-engine=lineairdb &
    MYSQL_PID=$!
    MYSQL_PIDS[$i]=$MYSQL_PID
    echo "Restarted MySQL instance $i with PID $MYSQL_PID on port $PORT"
done

# Phase 6: Wait for all restarts to complete
echo "=== Phase 6: Waiting for all restarts to complete ==="
declare -a RESTART_PIDS=()
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    SOCKET="/tmp/mysql_${PORT}.sock"
    
    (
        for j in {1..30}; do
            if ./runtime_output_directory/mysqladmin ping -u root --socket="$SOCKET" --port="$PORT" >/dev/null 2>&1; then
                echo "MySQL on port $PORT restarted with LineairDB!"
                break
            fi
            if [ $j -eq 30 ]; then
                echo "MySQL on port $PORT failed to restart within 30 seconds"
                exit 1
            fi
            sleep 1
        done
    ) &
    RESTART_PIDS+=($!)
done

# Wait for all restarts
echo "Waiting for all restarts to complete..."
for pid in "${RESTART_PIDS[@]}"; do
    wait $pid || cleanup
done

echo "=== All instances ready with LineairDB! ==="

# Phase 7: Configure LineairDB client target on all instances
echo "=== Phase 7: Configuring LineairDB target on all instances ==="
declare -a CFG_PIDS=()
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    SOCKET="/tmp/mysql_${PORT}.sock"
    (
      ./runtime_output_directory/mysql -u root --socket="$SOCKET" --port="$PORT" \
        -e "SET GLOBAL lineairdb_ordo_host='${ORDO_HOST}'; SET GLOBAL lineairdb_ordo_port=${ORDO_PORT};" >/dev/null
      echo "Configured LineairDB target for port $PORT -> ${ORDO_HOST}:${ORDO_PORT}"
    ) &
    CFG_PIDS+=($!)
done
for pid in "${CFG_PIDS[@]}"; do
  wait $pid || cleanup
done
echo "All instances configured to use Ordo at ${ORDO_HOST}:${ORDO_PORT}"

echo "All $NUM_INSTANCES MySQL instances are running:"
for i in $(seq 0 $((NUM_INSTANCES - 1))); do
    PORT=$((STARTING_PORT + i))
    echo "  - Port $PORT (PID: ${MYSQL_PIDS[$i]})"
done

echo ""
echo "PIDs: ${MYSQL_PIDS[*]}"
if [ "$DETACH" != "true" ]; then
  echo "Press Ctrl+C to stop all instances"
  # Wait for all background processes
  wait
fi
