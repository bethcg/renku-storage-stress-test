#!/usr/bin/env bash
# One-off chain for 2026-10-05: stop run 20261005T1139-5398 after rep 3, then run the follow-up tests.
cd "$(dirname "$0")/.."
LOG=results/run_20261005T113932.log
OUT=results/followup_$(date +%Y%m%dT%H%M%S).log
until grep -q '^\[rep 4/5\]' "$LOG"; do
  kill -0 1105 2>/dev/null || { echo "[chain] main run exited before rep 4 - not chaining" >>"$OUT"; exit 1; }
  sleep 2
done
kill -TERM -1101; sleep 5; kill -KILL -1101 2>/dev/null
echo "[chain] $(date -u +%FT%TZ) stopped main run after rep 3" >>"$OUT"
python -u scripts/07_parallel_sweep.py --tier S >>"$OUT" 2>&1;            echo "[chain] sweep exit=$?" >>"$OUT"
python -u scripts/08_durability.py write --backend azure --mode control >>"$OUT" 2>&1;   echo "[chain] durability azure exit=$?" >>"$OUT"
python -u scripts/08_durability.py write --backend polybox --mode control >>"$OUT" 2>&1; echo "[chain] durability polybox exit=$?" >>"$OUT"
echo "[chain] $(date -u +%FT%TZ) all done" >>"$OUT"
