#!/bin/bash

# Get PID of build/server/ordo-server and kill it
ps aux | grep "build/server/ordo-server" | grep -v grep | awk '{print $2}' | xargs -r kill -9