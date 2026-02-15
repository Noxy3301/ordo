#!/bin/bash

# Get PID of build/server/lineairdb-server and kill it
ps aux | grep "build/server/lineairdb-server" | grep -v grep | awk '{print $2}' | xargs -r kill -9