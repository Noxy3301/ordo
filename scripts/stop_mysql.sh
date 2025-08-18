#!/bin/bash

# Get PID of runtime_output_directory/mysqld and kill it
ps aux | grep "runtime_output_directory/mysqld" | grep -v grep | awk '{print $2}' | xargs -r kill -9