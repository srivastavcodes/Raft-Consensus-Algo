#!/bin/bash
set -x
set -e

logfile=/tmp/raftlog

go test -v -race -run "$@" 2>&1 | tee ${logfile}

go run tools/raft-testlog-viz/main.go < ${logfile}
