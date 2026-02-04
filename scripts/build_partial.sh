#!/bin/bash

cd $(dirname $0)/..

echo "Building server..."
cd build/server
make -j `nproc`
cd ../..

echo "Building proxy..."

# Sync proxy sources to build/proxy (which is symlinked from third_party/mysql-server/storage/lineairdb)
ROOT_DIR=$(pwd)
BUILD_PROXY_DIR="$ROOT_DIR/build/proxy"

echo "Syncing proxy sources into build/proxy ..."
# Copy all source files
cp -v "$ROOT_DIR/proxy/"*.cc "$BUILD_PROXY_DIR/" 2>/dev/null || true
cp -v "$ROOT_DIR/proxy/"*.hh "$BUILD_PROXY_DIR/" 2>/dev/null || true
cp -v "$ROOT_DIR/proxy/CMakeLists.txt" "$BUILD_PROXY_DIR/" 2>/dev/null || true

# Sync proto file
mkdir -p "$BUILD_PROXY_DIR/proto"
cp -v "$ROOT_DIR/proto/lineairdb.proto" "$BUILD_PROXY_DIR/proto/lineairdb.proto"

cd build
ninja ha_lineairdb_storage_engine.so
