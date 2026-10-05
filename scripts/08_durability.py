#!/usr/bin/env python
"""W12 durability (roadmap R5): does data written through a connector mount survive the session
stopping before rclone has finished uploading it?

rclone mounts return from close() as soon as the data is in the node's local VFS cache; the upload
runs afterwards. This test writes files with deterministic content (same generator as the main
dataset) through the mount, then either:

  --mode control   stays in the session and measures, per file, how long after close() the remote
                   API has every byte: the window in which stopping the session could lose data.
  --mode stop      returns right away, so you can stop the session while uploads are still running.
                   Later, from a NEW session, `verify` checks what reached the remote.

Expected content is recomputed from the file names, so `verify` needs no state from the stopped
session (the manifest in results/durability/ is only a convenience).

Usage:
    python scripts/08_durability.py write --backend azure --mode control
    python scripts/08_durability.py write --backend polybox --mode stop --files 4 --size 1GiB
        # -> stop the session in the Renku UI immediately, start a new one, then:
    python scripts/08_durability.py verify --backend polybox --tag <tag printed by write>
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import (MiB, ROOT, ResultWriter, load_script, load_targets, parse_size, rclone,  # noqa: E402
                 remote_path, remote_size, timer)

gen = load_script("01_generate_dataset.py")
TAG_RE = re.compile(r"^\d{8}T\d{6}-(control|stop)-(\d+)x(\d+)$")


def relpath(tag: str, i: int) -> str:
    return f"durability/{tag}/f-{i:02d}.bin"


def expected_sha256(rel: str, size: int) -> str:
    """Replays gen._write_random chunk by chunk, so the digest matches without writing anything."""
    rng, h, left = gen._rng(rel), hashlib.sha256(), size
    while left > 0:
        n = min(gen.CHUNK, left)
        h.update(rng.bytes(n))
        left -= n
    return h.hexdigest()


def actual_sha256(t: dict, rel: str) -> str:
    h = hashlib.sha256()
    if t["kind"] == "rclone":  # read through the API, not the (possibly cached) mount
        conf = os.environ.get("RCLONE_CONFIG", str(ROOT / "config" / "rclone.conf"))
        with subprocess.Popen(["rclone", "--config", conf, "cat", remote_path(t, rel)],
                              stdout=subprocess.PIPE) as p:
            while chunk := p.stdout.read(8 * MiB):
                h.update(chunk)
        if p.returncode:
            raise RuntimeError(f"rclone cat {rel} exited {p.returncode}")
    else:
        with open(t["path"] / rel, "rb") as fh:
            while chunk := fh.read(8 * MiB):
                h.update(chunk)
    return h.hexdigest()


def write(a) -> None:
    t = load_targets()["targets"][a.backend]
    size = parse_size(a.size)
    tag = f"{dt.datetime.now():%Y%m%dT%H%M%S}-{a.mode}-{a.files}x{size}"
    (t["path"] / "durability" / tag).mkdir(parents=True, exist_ok=True)
    writer = ResultWriter("durability")
    rows = []
    for i in range(a.files):
        rel = relpath(tag, i)
        with timer() as tw:
            digest = gen._write_random(t["path"] / rel, size, gen._rng(rel), checksum=True)
        fd = os.open(t["path"] / rel, os.O_RDONLY)
        with timer() as tf:
            try:
                os.fsync(fd)
                fsync_err = ""
            except OSError as e:
                fsync_err = f"{type(e).__name__}: {e}"
        os.close(fd)
        rows.append(dict(rel=rel, bytes=size, sha256=digest, closed_at=time.time(),
                         write_s=round(tw["s"], 3), fsync_s=round(tf["s"], 3), fsync_err=fsync_err))
        print(f"  wrote {rel}: {size / 1e6 / tw['s']:.0f} MB/s apparent, fsync {tf['s']:.2f}s", flush=True)

    manifests = writer.path.parent  # results/durability/, next to the result rows
    manifest = manifests / f"{a.backend}_{tag}.json"
    manifest.write_text(json.dumps(dict(backend=a.backend, tag=tag, mode=a.mode, files=rows,
                                        session_id=writer.info["session_id"]), indent=1))
    last_close = rows[-1]["closed_at"]

    if a.mode == "stop":
        print(f"\n[durability] tag: {tag}\n"
              f"  >>> Stop the session NOW in the Renku UI (pause, or delete for the harsher case). <<<\n"
              f"  Afterwards, in a new session:\n"
              f"    python scripts/08_durability.py verify --backend {a.backend} --tag {tag}\n"
              f"  (manifest: {manifest}; not needed for verify)")
        return

    if t["kind"] != "rclone":
        print("[durability] control mode only measures upload lag for rclone backends; writes here are synchronous")
    for r in rows:
        lag = None
        if t["kind"] == "rclone":
            done_after = None
            t0 = time.time()
            while time.time() - t0 < a.timeout:
                if remote_size(t, r["rel"]) == r["bytes"]:
                    done_after = time.time()
                    break
                time.sleep(1)
            lag = None if done_after is None else round(done_after - r["closed_at"], 1)
        ok = actual_sha256(t, r["rel"]) == r["sha256"] if (lag is not None or t["kind"] != "rclone") else False
        writer.write(backend=a.backend, workload="W12", variant="durability-control", profile=tag,
                     bytes=r["bytes"], duration_s=r["write_s"], fsync_s=r["fsync_s"], fsync_err=r["fsync_err"],
                     remote_complete_after_close_s=lag, ok=ok, errors=0 if ok else 1)
        print(f"  {r['rel']}: remote complete {lag}s after close, content {'OK' if ok else 'MISMATCH/MISSING'}")
    print(f"[durability] all files uploaded {time.time() - last_close:.0f}s after the last close()")
    if a.cleanup:
        cleanup(t, tag)


def verify(a) -> None:
    t = load_targets()["targets"][a.backend]
    m = TAG_RE.match(a.tag)
    if not m:
        raise SystemExit(f"tag {a.tag!r} does not look like <time>-<mode>-<files>x<bytes>")
    mode, n, size = m.group(1), int(m.group(2)), int(m.group(3))
    writer = ResultWriter("durability")
    lost = 0
    for i in range(n):
        rel = relpath(a.tag, i)
        if t["kind"] == "rclone":
            got = remote_size(t, rel)
        else:
            p = t["path"] / rel
            got = p.stat().st_size if p.exists() else None
        if got is None:
            status = "missing"
        elif got != size:
            status = f"truncated ({got} of {size} bytes)"
        else:
            status = "ok" if actual_sha256(t, rel) == expected_sha256(rel, size) else "corrupt"
        lost += status != "ok"
        writer.write(backend=a.backend, workload="W12", variant=f"durability-{mode}-verify", profile=a.tag,
                     bytes=size, remote_bytes=got, status=status, ok=status == "ok", errors=int(status != "ok"))
        print(f"  {rel}: {status}")
    print(f"[durability] {n - lost}/{n} files intact on {a.backend}")
    if a.cleanup:
        cleanup(t, a.tag)


def cleanup(t: dict, tag: str) -> None:
    if t["kind"] == "rclone":
        rclone("purge", remote_path(t, f"durability/{tag}"), check=False)
    else:
        import shutil
        shutil.rmtree(t["path"] / "durability" / tag, ignore_errors=True)
    print(f"[durability] removed durability/{tag}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("write")
    w.add_argument("--backend", required=True)
    w.add_argument("--mode", choices=["control", "stop"], default="control")
    w.add_argument("--files", type=int, default=4)
    w.add_argument("--size", default="256MiB")
    w.add_argument("--timeout", type=float, default=3600, help="control: max seconds to wait per file")
    w.add_argument("--cleanup", action="store_true", help="control: delete the test files afterwards")
    v = sub.add_parser("verify")
    v.add_argument("--backend", required=True)
    v.add_argument("--tag", required=True)
    v.add_argument("--cleanup", action="store_true", help="delete the test files afterwards")
    a = ap.parse_args()
    write(a) if a.cmd == "write" else verify(a)


if __name__ == "__main__":
    main()
