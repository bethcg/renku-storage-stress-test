#!/usr/bin/env bash
# Thin wrapper so the suite survives a closed browser tab: it runs under nohup and
# logs to results/run_<timestamp>.log. Follow progress with:  tail -f results/run_*.log
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
LOG="results/run_$(date +%Y%m%dT%H%M%S).log"

if [[ "${STRESS_FOREGROUND:-0}" == "1" ]]; then
  python -u scripts/run_all.py "$@" 2>&1 | tee "$LOG"
  exit "${PIPESTATUS[0]}"
fi

nohup python -u scripts/run_all.py "$@" > "$LOG" 2>&1 &
echo "started PID $! - logging to $LOG"
echo "follow with: tail -f $LOG"
