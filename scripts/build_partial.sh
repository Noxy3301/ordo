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
for f in ha_lineairdb.cc ha_lineairdb.hh lineairdb_client.cc lineairdb_client.hh lineairdb_transaction.cc lineairdb_transaction.hh; do
  if [ -f "$ROOT_DIR/proxy/$f" ]; then
    cp -v "$ROOT_DIR/proxy/$f" "$SE_DIR/$f"
  fi
done

cd build
ninja ha_lineairdb_storage_engine.so
