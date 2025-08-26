#!/bin/bash

cd build

# Step 1: Initialize if data directory doesn't exist
if [ ! -d "./data/mysql" ]; then
    echo "Step 1/5: Initializing MySQL data directory..."
    ./runtime_output_directory/mysqld --initialize-insecure --user=$USER --datadir=./data
    
    echo "Step 2/5: Starting MySQL with InnoDB..."
    ./runtime_output_directory/mysqld --datadir=./data --socket=/tmp/mysql.sock --port=3307 &
    MYSQL_PID=$!
    
    echo "Step 3/5: Waiting for MySQL to be ready..."
    until ./runtime_output_directory/mysqladmin ping -u root --socket=/tmp/mysql.sock >/dev/null 2>&1; do 
        sleep 1
    done
    
    echo "Step 4/5: Installing LineairDB plugin..."
    ./runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock -e "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';" 2>/dev/null || true
    
    echo "Step 5/5: Stopping MySQL and restarting with LineairDB as default..."
    kill $MYSQL_PID
    sleep 3
fi

# Start MySQL with LineairDB as default storage engine
./runtime_output_directory/mysqld --datadir=./data --socket=/tmp/mysql.sock --port=3307 --default-storage-engine=lineairdb &