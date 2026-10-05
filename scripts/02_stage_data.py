#!/usr/bin/env python
"""Put the benchmark dataset on a backend, and time data ingress (W9).

Staging rules (why they matter for fairness):
  * block / local backends: files are generated directly in place.
  * rclone backends: each unit is generated on session scratch and uploaded with the
    rclone API (NOT through the session mount). The node-side rclone VFS cache has
    therefore never seen the data, so reads in the benchmark are real remote reads,
    which is how users experience data uploaded via the Azure portal or PolyBox client.
    Staging is resumable: finished units are recorded in scratch/.staged-<backend>-<tier>.json.

Usage:
    python scripts/02_stage_data.py --tier S --backend azure
    python scripts/02_stage_data.py --tier S --backend azure --ingress --rep 0
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import (ResultWriter, data_root, load_script, load_targets, load_tier, rclone,  # noqa: E402
                 timer, wait_remote_size, wait_visible, work_root)

gen = load_script("01_generate_dataset.py")
RCLONE_FLAGS = ["--transfers", "8", "--checkers", "16", "--retries", "5", "--low-level-retries", "20"]


def _upload(cmd: str, src: Path, dst: str, attempts: int = 4) -> None:
    """Staging upload that outlasts transient throttling (PolyBox rejects bursts of
    parallel uploads for a while). Not used for W9, whose timing must not include waits."""
    for i in range(attempts):
        try:
            rclone(cmd, str(src), dst, *RCLONE_FLAGS)
            return
        except subprocess.CalledProcessError:
            if i == attempts - 1:
                raise
            wait = 30 * 2 ** i
            print(f"[stage] upload of {src.name} failed, retrying in {wait}s ({i + 1}/{attempts - 1})", flush=True)
            time.sleep(wait)


def stage(backend: str, spec: dict, writer: ResultWriter | None = None) -> None:
    cfg = load_targets()
    t = cfg["targets"][backend]
    units = gen.plan_units(spec)
    droot = data_root(t, spec["name"])
    rel_root = f"data/{spec['name']}"
    state_file = cfg["scratch"] / f".staged-{backend}-{spec['name']}.json"
    cfg["scratch"].mkdir(parents=True, exist_ok=True)
    done = set(json.loads(state_file.read_text())) if state_file.exists() else set()
    todo = [u for u in units if u.relpath not in done]
    print(f"[stage] {backend}: {len(units) - len(todo)}/{len(units)} units already staged")
    if not todo:
        return

    upload_s, upload_bytes = 0.0, 0
    for k, u in enumerate(todo, 1):
        if t["kind"] in ("block", "local"):
            gen.generate_unit(u, droot, checksum=False)
        else:
            tmp = cfg["scratch"] / "stage" / backend
            gen.generate_unit(u, tmp, checksum=False)
            src = tmp / u.relpath
            dst = f"{t['rclone_remote'].rstrip('/')}/{rel_root}/{u.relpath}"
            with timer() as tm:
                _upload("copy" if src.is_dir() else "copyto", src, dst)
            upload_s += tm["s"]
            upload_bytes += u.nbytes
            shutil.rmtree(src) if src.is_dir() else src.unlink()
        done.add(u.relpath)
        state_file.write_text(json.dumps(sorted(done)))
        print(f"  [{k}/{len(todo)}] {u.relpath}", flush=True)

    if t["kind"] == "rclone":
        last = droot / todo[-1].relpath
        print(f"[stage] waiting for the mount to show {last} ...")
        lag = wait_visible(last)
        print(f"[stage] visible after {lag:.0f}s")
        if writer:
            writer.write(backend=backend, workload="W9", variant="stage-api", profile="all",
                         bytes=upload_bytes, duration_s=round(upload_s, 3),
                         throughput_mb_s=round(upload_bytes / 1e6 / max(upload_s, 1e-9), 2),
                         dir_cache_lag_s=round(lag, 1))


def ingress(backend: str, spec: dict, writer: ResultWriter, rep: int) -> None:
    """W9: copy a fixed sample (one small-file shard + one large file) from session disk."""
    cfg = load_targets()
    t = cfg["targets"][backend]
    sample = gen.make_ingress_sample(spec, cfg["scratch"] / f"ingress-src-{spec['name']}" / "ingress")
    small, large = sample / "small", sample / "large.bin"
    n_small = len(list(small.iterdir()))
    b_small = sum(p.stat().st_size for p in small.iterdir())
    b_large = large.stat().st_size
    wroot = work_root(t, spec["name"])
    dst = wroot / f"ingress-{rep:02d}-{int(time.time())}"
    dst.mkdir(parents=True, exist_ok=True)

    # (a) through the mount, as a user would with cp / shutil
    with timer() as tm:
        # bytes only, like plain `cp`: copytree's copystat raises on CIFS (no chmod/utime for non-owners)
        (dst / "small").mkdir(parents=True, exist_ok=True)
        for f in small.iterdir():
            shutil.copyfile(f, dst / "small" / f.name)
    writer.write(backend=backend, workload="W9", rep=rep, variant="mount-small", profile="small-files",
                 files=n_small, bytes=b_small, duration_s=round(tm["s"], 3),
                 throughput_mb_s=round(b_small / 1e6 / tm["s"], 2), ops_per_s=round(n_small / tm["s"], 1))
    with timer() as tm:
        shutil.copyfile(large, dst / "large.bin")
    row = dict(backend=backend, workload="W9", rep=rep, variant="mount-large", profile="large-files",
               files=1, bytes=b_large, duration_s=round(tm["s"], 3),
               throughput_mb_s=round(b_large / 1e6 / tm["s"], 2))
    if t["kind"] == "rclone":
        rel = str((dst / "large.bin").relative_to(t["path"]))
        lag = wait_remote_size(t, rel, b_large)
        row["remote_visible_s"] = round(lag, 2) if lag is not None else None
        row["errors"] = 0 if lag is not None else 1
    writer.write(**row)

    # (b) straight to the API with rclone (what an advanced user could do instead)
    if t["kind"] == "rclone":
        rdst = f"{t['rclone_remote'].rstrip('/')}/{dst.relative_to(t['path'])}-api"
        with timer() as tm:
            rclone("copy", str(small), f"{rdst}/small", *RCLONE_FLAGS)
        writer.write(backend=backend, workload="W9", rep=rep, variant="api-small", profile="small-files",
                     files=n_small, bytes=b_small, duration_s=round(tm["s"], 3),
                     throughput_mb_s=round(b_small / 1e6 / tm["s"], 2), ops_per_s=round(n_small / tm["s"], 1))
        with timer() as tm:
            rclone("copyto", str(large), f"{rdst}/large.bin", *RCLONE_FLAGS)
        writer.write(backend=backend, workload="W9", rep=rep, variant="api-large", profile="large-files",
                     files=1, bytes=b_large, duration_s=round(tm["s"], 3),
                     throughput_mb_s=round(b_large / 1e6 / tm["s"], 2))
        rclone("purge", rdst, check=False)

    shutil.rmtree(dst, ignore_errors=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="S")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--backend", required=True)
    ap.add_argument("--ingress", action="store_true", help="run the W9 ingress test instead of staging")
    ap.add_argument("--rep", type=int, default=0)
    a = ap.parse_args()
    spec = load_tier(a.tier, a.quick)
    w = ResultWriter(spec["name"])
    if a.ingress:
        ingress(a.backend, spec, w, a.rep)
    else:
        stage(a.backend, spec, w)


if __name__ == "__main__":
    main()
