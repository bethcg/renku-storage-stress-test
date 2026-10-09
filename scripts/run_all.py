#!/usr/bin/env python
"""Orchestrate the full suite inside ONE Renku session.

  1. preflight   paths writable, rclone remotes reachable, enough free space
  2. stage       put the tier's dataset on every backend (resumable)
  3. W10         POSIX semantics, once per backend
  4. reps        for each repetition, in a freshly SHUFFLED backend order:
                 W1 W3 W4 W2 (fio) -> W5 (metadata) -> W6 W7 W8 (realistic) -> W9 (ingress)

Usage (via the wrapper):
    bash scripts/run_all.sh --tier S --quick            # smoke test, ~1 % data, 1 rep
    bash scripts/run_all.sh --tier S --reps 5
    bash scripts/run_all.sh --tier L --reps 3 --backends project,azure
    bash scripts/run_all.sh --tier S --start-rep 3      # resume after an interruption

Project storage and session disk serve freshly written data from a cache below the page cache,
so staging and measuring must be >= --min-data-age-h apart (default 6 h): the run refuses to
measure otherwise. Stage first (it stops at the age check), then rerun with --skip-stage.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import random
import shutil
import sys
import traceback
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import (MIN_DATA_AGE_H, ResultWriter, data_root, load_script, load_targets, load_tier,  # noqa: E402
                 rclone, require_aged)

stage_mod = load_script("02_stage_data.py")
fio_mod = load_script("03_run_fio.py")
meta_mod = load_script("04_metadata_bench.py")
real_mod = load_script("05_realistic_bench.py")
sem_mod = load_script("06_semantics_check.py")

ALL = ["W1", "W2", "W3", "W4", "W5", "W6", "W7", "W8", "W9", "W10"]


def footprint_bytes(spec: dict) -> int:
    gen = stage_mod.gen
    return sum(u.nbytes for u in gen.plan_units(spec)) + 2 * spec["write_size"]


def preflight(backends, spec) -> None:
    cfg = load_targets()
    need = footprint_bytes(spec)
    print(f"[preflight] tier {spec['name']} needs ~{need / 1e9:.1f} GB per backend")
    for b in backends:
        t = cfg["targets"][b]
        t["path"].mkdir(parents=True, exist_ok=True)
        probe = t["path"] / ".write-probe"
        probe.write_text("ok")
        probe.unlink()
        if t["kind"] == "rclone":
            rclone("mkdir", t["rclone_remote"])
            rclone("lsf", "--max-depth", "1", t["rclone_remote"])
            print(f"  {b:8s} ok (rclone API reachable; free space not checked for object stores)")
        else:
            free = shutil.disk_usage(t["path"]).free
            flag = "ok" if free > need else "NOT ENOUGH SPACE"
            print(f"  {b:8s} {flag}: {free / 1e9:.1f} GB free")
            if free <= need:
                raise SystemExit(f"{b}: not enough space for tier {spec['name']}")
    cfg["scratch"].mkdir(parents=True, exist_ok=True)


def guarded(writer, backend, workload, rep, fn, *args, **kw):
    try:
        fn(*args, **kw)
    except Exception as e:
        traceback.print_exc()
        writer.write(backend=backend, workload=workload, variant="crashed", rep=rep, ok=False, errors=1,
                     error_msg=f"{type(e).__name__}: {e}"[:300])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="S", choices=["S", "L"])
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--reps", type=int)
    ap.add_argument("--start-rep", type=int, default=0)
    ap.add_argument("--backends", help="comma list; default from targets.yaml")
    ap.add_argument("--workloads", default=",".join(ALL))
    ap.add_argument("--seed", type=int, default=int(dt.date.today().strftime("%Y%m%d")))
    ap.add_argument("--skip-stage", action="store_true")
    ap.add_argument("--min-data-age-h", type=float, default=MIN_DATA_AGE_H,
                    help="refuse to read project/local data staged more recently than this (0 disables)")
    a = ap.parse_args()

    cfg = load_targets()
    spec = load_tier(a.tier, a.quick)
    reps = a.reps or spec["reps"]
    if reps > spec["reps"]:
        raise SystemExit(f"--reps {reps} > {spec['reps']} fresh file sets in tiers.yaml; raise 'reps' there first")
    backends = a.backends.split(",") if a.backends else cfg["default_backends"]
    wl = set(a.workloads.split(","))

    run_id = f"{dt.datetime.now():%Y%m%dT%H%M}-{uuid.uuid4().hex[:4]}"
    os.environ["STRESS_RUN_ID"] = run_id
    writer = ResultWriter(spec["name"], run_id)
    print(f"[run] id={run_id} tier={spec['name']} reps={reps} backends={backends} seed={a.seed}")
    print(f"[run] results -> {writer.path}")

    preflight(backends, spec)
    if not a.skip_stage:
        for b in backends:
            stage_mod.stage(b, spec, writer)

    # Freshly staged data reads too fast on project/local (lib.MIN_DATA_AGE_H), so stage in one
    # invocation and measure in a later one with --skip-stage.
    ages = {}
    for b in backends:
        t = cfg["targets"][b]
        units = [data_root(t, spec["name"]) / u.relpath for u in stage_mod.gen.plan_units(spec)]
        ages[b] = require_aged(t, units, a.min_data_age_h, "staged data")
    writer.write(workload="RUN", variant="data-age", backend="-", data_age_h=ages)
    print(f"[run] hours since staging: {ages}")

    if "W10" in wl and a.start_rep == 0:
        for b in backends:
            guarded(writer, b, "W10", -1, sem_mod.run, b, spec, writer)

    for rep in range(a.start_rep, reps):
        order = backends[:]
        random.Random(a.seed + rep).shuffle(order)
        writer.write(workload="RUN", variant="order", rep=rep, backend="-", order=order, seed=a.seed)
        print(f"\n[rep {rep + 1}/{reps}] order: {' -> '.join(order)}")
        for b in order:
            print(f"  [{b}] fio", flush=True)
            fio_wl = wl & {"W1", "W2", "W3", "W4"}
            if fio_wl:
                guarded(writer, b, "fio", rep, fio_mod.run, b, spec, rep, writer, fio_wl)
            if "W5" in wl:
                print(f"  [{b}] metadata", flush=True)
                guarded(writer, b, "W5", rep, meta_mod.run, b, spec, rep, writer)
            real_wl = wl & {"W6", "W7", "W8"}
            if real_wl:
                print(f"  [{b}] realistic", flush=True)
                guarded(writer, b, "real", rep, real_mod.run, b, spec, rep, writer, real_wl)
            if "W9" in wl:
                print(f"  [{b}] ingress", flush=True)
                guarded(writer, b, "W9", rep, stage_mod.ingress, b, spec, writer, rep)

    print(f"\n[run] done. Analyse with: python analysis/analyze.py {writer.path.parent}")


if __name__ == "__main__":
    main()
