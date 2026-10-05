#!/usr/bin/env python
"""Realistic research workloads W6-W8 on one backend for one repetition.

W6  "ML epoch": read every file of a fresh shard (small-files and medium-files),
    with 1 worker and with 8 parallel workers (like a PyTorch DataLoader).
W7  Columnar formats: Parquet scan of 2 of 8 columns; HDF5 random 1 MiB chunk reads.
W8  Software-environment-like tree: untar N small files, walk + read them all, delete.

Usage:
    python scripts/05_realistic_bench.py --tier S --backend polybox --rep 0
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from lib import (ResultWriter, data_root, load_script, load_targets, load_tier,  # noqa: E402
                 percentiles_ms, timer, work_root)

gen = load_script("01_generate_dataset.py")


def _read_file(p: Path) -> tuple[float, int, int]:
    t0 = time.perf_counter()
    try:
        with open(p, "rb") as fh:
            n = 0
            while chunk := fh.read(1 << 20):
                n += len(chunk)
        return time.perf_counter() - t0, n, 0
    except OSError:
        return time.perf_counter() - t0, 0, 1


def w6(t, spec, rep, writer, backend):
    for profile in ("small-files", "medium-files"):
        for shard, workers in ((2 * rep, 1), (2 * rep + 1, 8)):
            d = data_root(t, spec["name"]) / profile / f"shard-{shard:03d}"
            with timer() as tl:
                files = sorted(d.iterdir())
            with timer() as tm:
                if workers == 1:
                    res = [_read_file(f) for f in files]
                else:
                    with ThreadPoolExecutor(workers) as ex:
                        res = list(ex.map(_read_file, files))
            nbytes = sum(r[1] for r in res)
            writer.write(backend=backend, workload="W6", variant=f"epoch-{workers}w", profile=profile, rep=rep,
                         threads=workers, files=len(files), bytes=nbytes, duration_s=round(tm["s"], 3),
                         throughput_mb_s=round(nbytes / 1e6 / tm["s"], 2), ops_per_s=round(len(files) / tm["s"], 1),
                         list_s=round(tl["s"], 3), errors=sum(r[2] for r in res),
                         ok=sum(r[2] for r in res) == 0, **percentiles_ms([r[0] for r in res]))


def w7(t, spec, rep, writer, backend):
    import h5py
    import pyarrow.parquet as pq

    root = data_root(t, spec["name"]) / "large-files"
    pqf = root / f"table-{rep:03d}.parquet"
    with timer() as tm:
        tbl = pq.read_table(pqf, columns=["c0", "c1"])
        _ = float(tbl.column("c0").to_numpy().sum())
    nbytes = tbl.nbytes
    writer.write(backend=backend, workload="W7", variant="parquet-2of8-cols", profile="large-files", rep=rep,
                 files=1, bytes=nbytes, duration_s=round(tm["s"], 3),
                 throughput_mb_s=round(nbytes / 1e6 / tm["s"], 2), file_bytes=pqf.stat().st_size)

    h5f = root / f"array-{rep:03d}.h5"
    rng = np.random.default_rng(rep)
    lats = []
    with timer() as tm:
        with h5py.File(h5f, "r") as f:
            ds = f["x"]
            nchunks = ds.shape[0] // 256
            for c in rng.integers(0, nchunks, spec["h5_random_reads"]):
                t0 = time.perf_counter()
                _ = ds[int(c) * 256:(int(c) + 1) * 256]
                lats.append(time.perf_counter() - t0)
    n = len(lats)
    writer.write(backend=backend, workload="W7", variant="hdf5-random-chunks", profile="large-files", rep=rep,
                 files=1, bytes=n * 256 * 1024 * 4, duration_s=round(tm["s"], 3),
                 iops=round(n / tm["s"], 1), throughput_mb_s=round(n * 1.048576 / tm["s"], 2), **percentiles_ms(lats))


def w8(t, spec, rep, writer, backend):
    cfg = load_targets()
    tarball = gen.make_tree_tar(spec["tree_files"], cfg["scratch"] / f"tree-{spec['name']}.tar")
    dst = work_root(t, spec["name"]) / f"tree-r{rep:02d}-{int(time.time())}"
    dst.mkdir(parents=True, exist_ok=True)
    n = spec["tree_files"]
    base = dict(backend=backend, workload="W8", profile="tree", rep=rep, files=n)

    err = 0
    with timer() as tm:
        try:
            with tarfile.open(tarball) as tar:
                tar.extractall(dst, filter="data") if sys.version_info >= (3, 12) else tar.extractall(dst)
        except (OSError, tarfile.TarError):
            err = 1
    writer.write(**base, variant="untar", duration_s=round(tm["s"], 3), ops_per_s=round(n / tm["s"], 1),
                 bytes=tarball.stat().st_size, errors=err, ok=err == 0)

    with timer() as tm:
        count = 0
        for dirpath, _, names in os.walk(dst):
            for nm in names:
                _read_file(Path(dirpath) / nm)
                count += 1
    writer.write(**base, variant="walk-read", duration_s=round(tm["s"], 3), ops_per_s=round(count / tm["s"], 1),
                 files_seen=count, ok=count == n)

    with timer() as tm:
        shutil.rmtree(dst, ignore_errors=True)
    writer.write(**base, variant="delete-tree", duration_s=round(tm["s"], 3), ops_per_s=round(n / tm["s"], 1))


def run(backend, spec, rep, writer, only=None):
    t = load_targets()["targets"][backend]
    for name, fn in (("W6", w6), ("W7", w7), ("W8", w8)):
        if not only or name in only:
            try:
                fn(t, spec, rep, writer, backend)
            except Exception as e:  # keep going; record the failure
                writer.write(backend=backend, workload=name, variant="crashed", rep=rep, ok=False, errors=1,
                             error_msg=f"{type(e).__name__}: {e}"[:300])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="S")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--backend", required=True)
    ap.add_argument("--rep", type=int, default=0)
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    spec = load_tier(a.tier, a.quick)
    run(a.backend, spec, a.rep, ResultWriter(spec["name"]), set(filter(None, a.only.split(","))))


if __name__ == "__main__":
    main()
