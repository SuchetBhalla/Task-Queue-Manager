#!/bin/bash

URL="https://localhost:443/trigger"
DURATION="10s"

CONNS=(10 20 50 80 160)

LOG="wrk.log"
> "$LOG"

for c in "${CONNS[@]}"; do
	echo "Running wrk with 1 thread and $c connection(s)..." | tee -a "$LOG"
	wrk -t1 -c"$c" -d"$DURATION" --latency -s trigger.lua "$URL" >> "$LOG" 2>&1
	echo -e "\n---------------------------------------------\n" >> "$LOG"
	sleep 10
done
