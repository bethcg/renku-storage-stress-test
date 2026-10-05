#!/usr/bin/env bash
# One-off chain for 2026-10-05, replaces resume_v3.sh at ~17:10 UTC.
# The Azure data connector mount has returned EIO since ~17:00 UTC (API access with our own SAS still works),
# so the rest of today's run continues WITHOUT Azure (decided 17:10 UTC). Azure reps 1-2 (0-based), its sweep and
# its durability control run later in a new session, once the connector works again.
# PolyBox W5 and W8 stay in rep 0 only (decided 13:30 and 15:40 UTC).
#   0. wait for PolyBox rep 1 (PID given as $1, started by resume_v3.sh)
#   1. rep 1: project,local (all workloads)  -- the resume_v3 attempt aborted in preflight on Azure
#   2. rep 2: project,local (all workloads), then polybox (W1-W4, W6, W7, W9)
#   3. follow-ups without Azure: W11 sweep on project,polybox,local; W12 durability control on PolyBox
cd "$(dirname "$0")/.."
WAIT_PID=${1:?usage: resume_v4.sh <pid of running polybox rep-1 run>}
TS=$(date +%Y%m%dT%H%M%S)
OUT=results/followup_$TS.log
step() { echo "[chain] $(date -u +%FT%TZ) start: $*" >>"$OUT"; }
done_() { echo "[chain] $(date -u +%FT%TZ) $1 exit=$2" >>"$OUT"; }

step "waiting for PID $WAIT_PID (polybox rep 1)"
while kill -0 "$WAIT_PID" 2>/dev/null; do sleep 10; done
done_ "polybox rep 1" "?"

step "rep 1: project,local"
python -u scripts/run_all.py --tier S --reps 2 --start-rep 1 --skip-stage \
  --backends project,local >"results/run_${TS}_rep1-others.log" 2>&1
done_ "rep 1 others" $?

step "rep 2: project,local"
python -u scripts/run_all.py --tier S --reps 3 --start-rep 2 --skip-stage \
  --backends project,local >"results/run_${TS}_rep2-others.log" 2>&1
done_ "rep 2 others" $?
step "rep 2: polybox without W5, W8"
python -u scripts/run_all.py --tier S --reps 3 --start-rep 2 --skip-stage \
  --backends polybox --workloads W1,W2,W3,W4,W6,W7,W9 >"results/run_${TS}_rep2-polybox.log" 2>&1
done_ "rep 2 polybox" $?

step "W11 sweep (no azure)"
python -u scripts/07_parallel_sweep.py --tier S --backends project,polybox,local >>"$OUT" 2>&1; done_ "sweep" $?
python -u scripts/08_durability.py write --backend polybox --mode control >>"$OUT" 2>&1;       done_ "durability polybox" $?
echo "[chain] $(date -u +%FT%TZ) all done (azure pending: reps 1-2, sweep, durability control)" >>"$OUT"
