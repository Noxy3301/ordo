#!/bin/bash

# Setup Benchbase for Ordo benchmarking

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHBASE_DIR="$BENCH_DIR/benchbase"

echo "Setting up Benchbase..."

# Create benchbase directory
mkdir -p "$BENCHBASE_DIR"
cd "$BENCHBASE_DIR"

# Download Benchbase if not exists
if [ ! -f "benchbase-mysql/benchbase.jar" ]; then
    echo "Downloading Benchbase..."
    wget -O benchbase.tar.gz "https://github.com/cmu-db/benchbase/archive/refs/tags/v2023.tar.gz"
    tar -xzf benchbase.tar.gz --strip-components=1
    rm benchbase.tar.gz
    
    echo "Building Benchbase..."
    ./mvnw clean package -P mysql -DskipTests
    unzip -q target/benchbase-mysql.zip && rm target/benchbase-mysql.zip
    echo "Benchbase downloaded and built."
else
    echo "Benchbase already exists."
fi

java_version=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
echo "Using Java version: $java_version"

echo "Benchbase setup complete!"
echo "JAR location: $BENCHBASE_DIR/benchbase-mysql/benchbase.jar"
