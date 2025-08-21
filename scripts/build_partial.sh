#!/bin/bash

cd $(dirname $0)/..

echo "Building server..."
cd build/server
make -j `nproc`
cd ../..

echo "Building proxy..."
cp -r proxy build/
cd build
ninja ha_lineairdb_storage_engine.so
