#!/usr/bin/env python
"""fio workloads W1-W4 on one backend for one repetition.

W1  sequential read, 1 MiB blocks, files 0-7 of the repetition's pool (cold, then warm)
W2  sequential write, 1 MiB blocks, `write_size` bytes, plus remote-visibility lag for rclone
W3  random read, 4 KiB and 64 KiB, time-based, single reader (queue depth 1, psync)
W4  parallel sequential readers: 4 and 16 jobs, one fresh file each

Every repetition reads files no previous repetition touched, so "cold" is honest even
with the rclone VFS cache on the node. psync + buffered I/O is used for every backend
because FUSE mounts do not support O_DIRECT reliably; page cache is dropped per file
with fio's invalidate=1 (posix_fadvise DONTNEED).

Usage:
    python scripts/03_run_fio.py --tier S --backend project --rep 0
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import ResultWriter, data_root, load_targets, load_tier, wait_remote_size, work_root  # noqa: E402

COMMON = ["--ioengine=psync", "--direct=0", "--fallocate=none", "--thread", "--output-format=json"]


def fio(args: list[str]) -> dict:
    p = subprocess.run(["fio", *COMMON, *args], text=True, capture_output=True)
    if p.returncode != 0 and not p.stdout.strip().startswith("{"):
        raise RuntimeError(f"fio failed: {p.stderr.strip()[:500]}")
    out = p.stdout[p.stdout.find("{"):]
    return json.loads(out)


def summarise(res: dict, rw: str) -> dict:
    jobs = res["jobs"]
    io_bytes = sum(j[rw]["io_bytes"] for j in jobs)
    runtime_s = max(j[rw]["runtime"] for j in jobs) / 1000 or 1e-9
    pct = jobs[0][rw].get("clat_ns", {}).get("percentile", {})
    errors = sum(j.get("error", 0) != 0 for j in jobs)
    row = {
        "bytes": io_bytes,
        "duration_s": round(runtime_s, 3),
        "throughput_mb_s": round(io_bytes / 1e6 / runtime_s, 2),
        "iops": round(sum(j[rw]["iops"] for j in jobs), 1),
        "errors": errors,
        "ok": errors == 0,
    }
    for key, name in (("50.000000", "lat_p50_ms"), ("95.000000", "lat_p95_ms"), ("99.000000", "lat_p99_ms")):
        if key in pct:
            row[name] = round(pct[key] / 1e6, 3)
    return row


def pool_files(t: dict, spec: dict, rep: int, slot: str) -> list[Path]:
    lo, hi = spec["pool_layout"][slot]
    base = data_root(t, spec["name"]) / "fio-pool" / f"rep-{rep:02d}"
    files = [base / f"file-{i:02d}.bin" for i in range(lo, hi + 1)]
    missing = [f for f in files if not f.exists()]
    if missing:
        raise FileNotFoundError(f"pool files missing (staged?): {missing[:3]}")
    return files


def run(backend: str, spec: dict, rep: int, writer: ResultWriter, only: set[str] | None = None) -> None:
    t = load_targets()["targets"][backend]
    fsize = spec["profiles"]["fio-pool"]["size"]
    want = (lambda w: True) if not only else (lambda w: w in only)
    base = dict(backend=backend, profile="fio-pool", rep=rep)

    if want("W1"):
        files = pool_files(t, spec, rep, "seq_read")
        args = ["--name=seqread", "--rw=read", "--bs=1M", "--readonly",
                f"--filename={':'.join(map(str, files))}", "--file_service_type=sequential",
                f"--size={fsize * len(files)}"]
        for state, inv in (("cold", 1), ("warm", 0)):
            r = summarise(fio(args + [f"--invalidate={inv}"]), "read")
            writer.write(**base, workload="W1", variant="seq-read-1m", cache_state=state, files=len(files), **r)

    if want("W3"):
        for slot, bs in (("rand_4k", "4k"), ("rand_64k", "64k")):
            f = pool_files(t, spec, rep, slot)[0]
            args = ["--name=randread", "--rw=randread", f"--bs={bs}", "--readonly", f"--filename={f}",
                    f"--size={fsize}", "--time_based", f"--runtime={spec['randread_runtime_s']}", "--invalidate=1"]
            r = summarise(fio(args), "read")
            writer.write(**base, workload="W3", variant=f"rand-read-{bs}", files=1, **r)

    if want("W4"):
        for slot, n in (("conc_4", 4), ("conc_16", 16)):
            files = pool_files(t, spec, rep, slot)
            args = ["--group_reporting", "--rw=read", "--bs=1M", "--readonly", "--invalidate=1", f"--size={fsize}"]
            for i, f in enumerate(files):
                args += [f"--name=r{i}", f"--filename={f}"]
            r = summarise(fio(args), "read")
            writer.write(**base, workload="W4", variant=f"parallel-read-{n}", threads=n, files=n, **r)

    if want("W2"):
        wdir = work_root(t, spec["name"])
        wdir.mkdir(parents=True, exist_ok=True)
        f = wdir / f"fio-write-r{rep:02d}-{int(time.time())}.bin"
        args = ["--name=seqwrite", "--rw=write", "--bs=1M", f"--filename={f}",
                f"--size={spec['write_size']}", "--end_fsync=1"]
        r = summarise(fio(args), "write")
        if t["kind"] == "rclone":
            lag = wait_remote_size(t, str(f.relative_to(t["path"])), spec["write_size"])
            r["remote_visible_s"] = round(lag, 2) if lag is not None else None
            if lag is None:
                r["errors"] += 1
        writer.write(**base, workload="W2", variant="seq-write-1m", files=1, **r)
        try:
            os.remove(f)
        except OSError:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="S")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--backend", required=True)
    ap.add_argument("--rep", type=int, default=0)
    ap.add_argument("--only", default="", help="comma list, e.g. W1,W3")
    a = ap.parse_args()
    spec = load_tier(a.tier, a.quick)
    run(a.backend, spec, a.rep, ResultWriter(spec["name"]), set(filter(None, a.only.split(","))))


if __name__ == "__main__":
    main()
