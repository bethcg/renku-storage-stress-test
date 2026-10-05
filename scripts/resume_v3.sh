#!/usr/bin/env bash
# One-off chain for 2026-10-05, replaces resume_v2.sh (still waiting on PolyBox rep 0) at 15:40 UTC.
# PolyBox W5 takes ~1.4 s per file op (~2.5-3 h per rep), so W5 on PolyBox runs in rep 0 only
# (decided 13:30 UTC). PolyBox W8 deletes ~2.5 files/s (~2.5 h per rep), so W8 on PolyBox also runs in rep 0 only
# (decided 15:40 UTC). Reps 1-2 run the other backends with all workloads and PolyBox without W5 and W8.
#   0. wait for PolyBox rep 0 (W5-W9, PID given as $1) to finish
#   1. per rep 1..2: project,azure,local (all workloads), then polybox (W1-W4, W6, W7, W9)
#   2. follow-ups: W11 sweep, then W12 durability controls on Azure and PolyBox
cd "$(dirname "$0")/.."
WAIT_PID=${1:?usage: resume_v3.sh <pid of running polybox rep-0 run>}
TS=$(date +%Y%m%dT%H%M%S)
OUT=results/followup_$TS.log
step() { echo "[chain] $(date -u +%FT%TZ) start: $*" >>"$OUT"; }
done_() { echo "[chain] $(date -u +%FT%TZ) $1 exit=$2" >>"$OUT"; }

step "waiting for PID $WAIT_PID (polybox rep 0)"
while kill -0 "$WAIT_PID" 2>/dev/null; do sleep 10; done
done_ "polybox rep 0" "?"

for rep in 1 2; do
  step "rep $rep: project,azure,local"
  python -u scripts/run_all.py --tier S --reps $((rep + 1)) --start-rep $rep --skip-stage \
    --backends project,azure,local >"results/run_${TS}_rep${rep}-others.log" 2>&1
  done_ "rep $rep others" $?
  step "rep $rep: polybox without W5, W8"
  python -u scripts/run_all.py --tier S --reps $((rep + 1)) --start-rep $rep --skip-stage \
    --backends polybox --workloads W1,W2,W3,W4,W6,W7,W9 >"results/run_${TS}_rep${rep}-polybox.log" 2>&1
  done_ "rep $rep polybox" $?
done

step "W11 sweep"
python -u scripts/07_parallel_sweep.py --tier S >>"$OUT" 2>&1;                           done_ "sweep" $?
python -u scripts/08_durability.py write --backend azure --mode control >>"$OUT" 2>&1;   done_ "durability azure" $?
python -u scripts/08_durability.py write --backend polybox --mode control >>"$OUT" 2>&1; done_ "durability polybox" $?
echo "[chain] $(date -u +%FT%TZ) all done" >>"$OUT"
