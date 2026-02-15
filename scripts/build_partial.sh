#!/bin/bash

cd $(dirname $0)/..

echo "Building server..."
cd build/server
make -j `nproc`
cd ../..

echo "Building proxy..."

# Sync proxy sources to MySQL SE sources (do not edit third_party directly; this is a build-time copy)
ROOT_DIR=$(pwd)
SE_DIR="$ROOT_DIR/third_party/mysql-server/storage/lineairdb"
echo "Syncing proxy sources into storage/lineairdb ..."
cp -v "$ROOT_DIR"/proxy/*.cc "$ROOT_DIR"/proxy/*.hh "$ROOT_DIR"/proxy/CMakeLists.txt "$SE_DIR/"

cd build
ninja ha_lineairdb_storage_engine.so
