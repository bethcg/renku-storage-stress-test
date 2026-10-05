#!/usr/bin/env python
"""W11 parallelism sweep (roadmap R10): how small-file read speed scales with concurrent readers.

Reads shards of 100 KiB files (ML-dataset-like) with 1, 2, 4, ... threads, as a DataLoader with
num_workers=N or a thread pool would. Every (rep, level) gets its own fresh shard, staged like the
main dataset (connectors via the rclone API, never via the mount), so every measurement is cold.
Backend order and level order are shuffled per repetition.

The data lives in data/<tier>-sweep/ on each backend, separate from the main dataset, because
W6 already consumes every small-files shard of the main tier.

Usage:
    python scripts/07_parallel_sweep.py --tier S --quick                 # smoke test
    python scripts/07_parallel_sweep.py --tier S                         # stage + run all backends
    python scripts/07_parallel_sweep.py --tier S --backends azure,polybox --skip-stage
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import (ResultWriter, data_root, drop_file_cache, load_script, load_targets, load_tier,  # noqa: E402
                 load_yaml, parse_size, percentiles_ms, timer)

stage_mod = load_script("02_stage_data.py")
real_mod = load_script("05_realistic_bench.py")


def sweep_spec(tier: str, quick: bool) -> dict:
    """A stand-alone dataset spec with one 'bin' profile: one shard per (rep, level)."""
    base = load_tier(tier, quick)
    sw = base["sweep"]
    levels = [int(n) for n in sw["levels"]]
    reps = 1 if quick else int(sw["reps"])
    files = int(sw["files"])
    if quick:
        files = max(50, int(files * load_yaml("tiers.yaml").get("quick_factor", 0.01)))
    shards = len(levels) * reps
    return {"name": f"{base['name']}-sweep", "result_tier": base["name"], "reps": reps, "levels": levels,
            "files": files, "profiles": {"sweep-files": {"kind": "bin", "count": files * shards,
                                                          "size": parse_size(sw["size"]), "shards": shards}}}


def run(backend: str, sp: dict, rep: int, writer: ResultWriter, rng: random.Random) -> None:
    t = load_targets()["targets"][backend]
    order = list(enumerate(sp["levels"]))
    rng.shuffle(order)
    for li, n in order:
        d = data_root(t, sp["name"]) / "sweep-files" / f"shard-{rep * len(sp['levels']) + li:03d}"
        with timer() as tl:
            files = sorted(d.iterdir())
        if t["kind"] in ("block", "local"):
            # staged in place, so possibly still in page cache; connectors were uploaded via the API
            for f in files:
                drop_file_cache(f)
        with timer() as tm:
            with ThreadPoolExecutor(n) as ex:
                res = list(ex.map(real_mod._read_file, files))
        nbytes, errors = sum(r[1] for r in res), sum(r[2] for r in res)
        ops = len(files) / tm["s"]
        writer.write(backend=backend, workload="W11", variant=f"sweep-{n:02d}w", profile="sweep-files", rep=rep,
                     threads=n, files=len(files), bytes=nbytes, duration_s=round(tm["s"], 3),
                     throughput_mb_s=round(nbytes / 1e6 / tm["s"], 2), ops_per_s=round(ops, 1),
                     list_s=round(tl["s"], 3), errors=errors, ok=errors == 0,
                     **percentiles_ms([r[0] for r in res]))
        print(f"  [{backend}] {n:2d} readers: {ops:8.1f} files/s  {nbytes / 1e6 / tm['s']:7.1f} MB/s", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="S", choices=["S", "L"])
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--backends", help="comma list; default from targets.yaml")
    ap.add_argument("--start-rep", type=int, default=0)
    ap.add_argument("--seed", type=int, default=int(dt.date.today().strftime("%Y%m%d")))
    ap.add_argument("--skip-stage", action="store_true")
    a = ap.parse_args()

    cfg = load_targets()
    sp = sweep_spec(a.tier, a.quick)
    backends = a.backends.split(",") if a.backends else cfg["default_backends"]
    writer = ResultWriter(sp["result_tier"])
    print(f"[sweep] levels={sp['levels']} reps={sp['reps']} files/measurement={sp['files']} "
          f"data={sp['name']} backends={backends} seed={a.seed}")
    print(f"[sweep] results -> {writer.path}")

    if not a.skip_stage:
        for b in backends:
            stage_mod.stage(b, sp)

    for rep in range(a.start_rep, sp["reps"]):
        order = backends[:]
        random.Random(a.seed + rep).shuffle(order)
        print(f"\n[sweep rep {rep + 1}/{sp['reps']}] order: {' -> '.join(order)}", flush=True)
        for b in order:
            try:
                run(b, sp, rep, writer, random.Random(f"{a.seed}-{rep}-{b}"))
            except Exception as e:
                traceback.print_exc()
                writer.write(backend=b, workload="W11", variant="crashed", rep=rep, ok=False, errors=1,
                             error_msg=f"{type(e).__name__}: {e}"[:300])
    print(f"\n[sweep] done. Analyse with: python analysis/analyze.py {writer.path.parent}")


if __name__ == "__main__":
    main()
