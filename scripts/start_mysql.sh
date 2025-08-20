#!/bin/bash

# Initialize and Start MySQL Server
cd build
./runtime_output_directory/mysqld --initialize-insecure --user=$USER --datadir=./data
./runtime_output_directory/mysqld --datadir=./data --socket=/tmp/mysql.sock --port=3307 --default-storage-engine=lineairdb &
