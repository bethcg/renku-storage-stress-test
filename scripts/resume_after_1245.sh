#!/usr/bin/env bash
# One-off chain for 2026-10-05, after the session restart at ~12:45 UTC killed run 20261005T1139-5398
# during rep 1/5 on PolyBox (fio W1-W4 done, W5 interrupted). Target is 3 reps in total.
#   1. PolyBox rep 0: W5-W9 only (its fio rows are valid; W10 already done for this run)
#   2. reps 1-2 (0-based) on all backends
#   3. follow-ups: W11 sweep, then W12 durability controls on Azure and PolyBox
# Each step writes its own log; the chain carries on if a step fails.
cd "$(dirname "$0")/.."
TS=$(date +%Y%m%dT%H%M%S)
OUT=results/followup_$TS.log
step() { echo "[chain] $(date -u +%FT%TZ) start: $*" >>"$OUT"; }
done_() { echo "[chain] $(date -u +%FT%TZ) $1 exit=$2" >>"$OUT"; }

step "polybox rep 0, W5-W9"
python -u scripts/run_all.py --tier S --reps 1 --start-rep 0 --skip-stage \
  --backends polybox --workloads W5,W6,W7,W8,W9 >"results/run_${TS}_polybox-rep0.log" 2>&1
done_ "polybox rep 0" $?

step "reps 1-2, all backends"
python -u scripts/run_all.py --tier S --reps 3 --start-rep 1 --skip-stage \
  >"results/run_${TS}_reps1-2.log" 2>&1
done_ "reps 1-2" $?

step "W11 sweep"
python -u scripts/07_parallel_sweep.py --tier S >>"$OUT" 2>&1;                           done_ "sweep" $?
python -u scripts/08_durability.py write --backend azure --mode control >>"$OUT" 2>&1;   done_ "durability azure" $?
python -u scripts/08_durability.py write --backend polybox --mode control >>"$OUT" 2>&1; done_ "durability polybox" $?
echo "[chain] $(date -u +%FT%TZ) all done" >>"$OUT"
