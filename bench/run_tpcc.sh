#!/bin/bash

# Run TPC-C benchmark with Benchbase

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHBASE_JAR="$BENCH_DIR/benchbase/benchbase-mysql/benchbase.jar"
CONFIG_FILE="$BENCH_DIR/config/tpcc.xml"
RESULTS_DIR="$BENCH_DIR/results"

# Check if benchbase exists
if [ ! -f "$BENCHBASE_JAR" ]; then
    echo "Benchbase not found. Please run: bash bench/setup_benchbase.sh"
    exit 1
fi

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "=========================================="
echo "Running TPC-C Benchmark with Benchbase"
echo "=========================================="

# Test configurations
ENGINES=("lineairdb" "innodb")
THREADS=(1 2)

for engine in "${ENGINES[@]}"; do
    for thread in "${THREADS[@]}"; do
        echo ""
        echo "Testing $engine with $thread threads..."
        
        # Create engine-specific results directory
        RESULT_DIR="$RESULTS_DIR/tpcc_${engine}_t${thread}"
        mkdir -p "$RESULT_DIR"
        
        # Update configuration
        cp "$CONFIG_FILE" "$CONFIG_FILE.tmp"
        sed -i "s/<terminals>.*<\/terminals>/<terminals>$thread<\/terminals>/" "$CONFIG_FILE.tmp"
        
        # Note: Default storage engine is set at MySQL startup (lineairdb)

        # Run benchmark (create, load, execute)
        echo "  Creating schema..."
        cd "$BENCH_DIR/benchbase/benchbase-mysql"
        java -jar benchbase.jar -b tpcc -c "$CONFIG_FILE.tmp" --create=true --load=false --execute=false
        
        echo "  Loading data..."
        java -jar benchbase.jar -b tpcc -c "$CONFIG_FILE.tmp" --create=false --load=true --execute=false
        
        echo "  Executing benchmark..."
        java -jar benchbase.jar -b tpcc -c "$CONFIG_FILE.tmp" --create=false --load=false --execute=true
        cd "$BENCH_DIR"
        
        # Move results (check both locations)
        if [ -d "results" ]; then
            mv results/* "$RESULT_DIR/" 2>/dev/null || true
            rmdir results 2>/dev/null || true
        fi
        cd "$BENCH_DIR"
        if [ -d "benchbase/benchbase-mysql/results" ]; then
            mv benchbase/benchbase-mysql/results/* "$RESULT_DIR/" 2>/dev/null || true
            rmdir benchbase/benchbase-mysql/results 2>/dev/null || true
        fi
        
        # Clean up temp config
        rm "$CONFIG_FILE.tmp"
        
        echo "  Results saved to: $RESULT_DIR"
    done
done

echo ""
echo "=========================================="
echo "TPC-C Benchmark Complete!"
echo "Results saved in: $RESULTS_DIR"
echo "=========================================="
