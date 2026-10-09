#!/usr/bin/env python
"""W5 metadata benchmark: create, stat, list, rename, delete many small files.

Metadata latency is where FUSE/object-store mounts differ most from a block volume,
and it is what users feel in `git status`, `ls`, `conda install`, `pip install`,
or Python imports. Runs single-threaded and with 8 threads.

Usage:
    python scripts/04_metadata_bench.py --tier S --backend azure --rep 0
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import ResultWriter, load_targets, load_tier, percentiles_ms, work_root  # noqa: E402

PAYLOAD = os.urandom(1024)  # 1 KiB per file
FILES_PER_DIR = 100


def _timed(fn, arg):
    t0 = time.perf_counter()
    err = 0
    try:
        fn(arg)
    except OSError:
        err = 1
    return time.perf_counter() - t0, err


def _create(p: Path):
    with open(p, "wb") as fh:
        fh.write(PAYLOAD)


def _rename(p: Path):
    os.rename(p, p.with_suffix(".renamed"))


def phase(name, fn, items, threads):
    t0 = time.perf_counter()
    if threads == 1:
        res = [_timed(fn, i) for i in items]
    else:
        with ThreadPoolExecutor(threads) as ex:
            res = list(ex.map(lambda i: _timed(fn, i), items))
    wall = time.perf_counter() - t0
    lats = [r[0] for r in res]
    return wall, lats, sum(r[1] for r in res)


def run(backend: str, spec: dict, rep: int, writer: ResultWriter) -> None:
    t = load_targets()["targets"][backend]
    n = spec["meta_files"]
    for threads in (1, 8):
        root = work_root(t, spec["name"]) / f"meta-r{rep:02d}-t{threads}-{int(time.time())}"
        dirs = [root / f"d{i:04d}" for i in range((n + FILES_PER_DIR - 1) // FILES_PER_DIR)]
        t0 = time.perf_counter()
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)
        mkdir_s = time.perf_counter() - t0
        files = [dirs[i // FILES_PER_DIR] / f"f{i:06d}.dat" for i in range(n)]

        results = {"mkdir": (mkdir_s, [], 0)}
        results["create"] = phase("create", _create, files, threads)
        results["stat"] = phase("stat", os.stat, files, threads)
        results["list"] = phase("list", os.listdir, dirs, threads)
        results["rename"] = phase("rename", _rename, files, threads)
        renamed = [f.with_suffix(".renamed") for f in files]
        results["delete"] = phase("delete", os.remove, renamed, threads)

        for op, (wall, lats, errs) in results.items():
            count = len(dirs) if op in ("mkdir", "list") else n
            # stat and list run right after create, so attribute/directory caches serve them
            writer.write(backend=backend, workload="W5", variant=f"meta-{op}", profile="metadata",
                         cache_state="warm" if op in ("stat", "list") else "cold",
                         rep=rep, threads=threads, files=count, bytes=(n * 1024 if op == "create" else 0),
                         duration_s=round(wall, 3), ops_per_s=round(count / max(wall, 1e-9), 1),
                         errors=errs, ok=errs == 0, **percentiles_ms(lats))
        shutil.rmtree(root, ignore_errors=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="S")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--backend", required=True)
    ap.add_argument("--rep", type=int, default=0)
    a = ap.parse_args()
    spec = load_tier(a.tier, a.quick)
    run(a.backend, spec, a.rep, ResultWriter(spec["name"]))


if __name__ == "__main__":
    main()
