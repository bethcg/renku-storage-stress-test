"""Shared helpers: config loading, dataset layout, result writing.

Every benchmark script imports this module so that all results share one schema
(see the "Metrics to capture" section of the plan).
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import socket
import statistics
import subprocess
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"

MiB = 1024 ** 2
GiB = 1024 ** 3


# --------------------------------------------------------------------------- config
def load_yaml(name: str) -> dict:
    path = Path(name) if os.path.isabs(name) else CONFIG_DIR / name
    with open(path) as fh:
        return yaml.safe_load(fh)


def load_targets() -> dict:
    cfg = load_yaml(os.environ.get("STRESS_TARGETS", "targets.yaml"))
    for name, t in cfg["targets"].items():
        t["name"] = name
        t["path"] = Path(os.path.expandvars(os.path.expanduser(t["path"])))
    cfg["scratch"] = Path(os.path.expandvars(os.path.expanduser(cfg["scratch"])))
    cfg["results_dir"] = ROOT / cfg.get("results_dir", "results")
    return cfg


_UNITS = {"B": 1, "KIB": 1024, "MIB": MiB, "GIB": GiB, "KB": 1000, "MB": 10**6, "GB": 10**9}


def parse_size(v) -> int:
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip().upper()
    for unit in sorted(_UNITS, key=len, reverse=True):
        if s.endswith(unit):
            return int(float(s[: -len(unit)]) * _UNITS[unit])
    return int(s)


def load_tier(tier: str, quick: bool = False) -> dict:
    """Return the tier definition with sizes in bytes.

    --quick shrinks counts and sizes by `quick_factor` and sets reps to 1,
    and uses a separate data folder (<tier>q) so it never mixes with real data.
    """
    tiers = load_yaml("tiers.yaml")
    spec = json.loads(json.dumps(tiers["tiers"][tier]))  # deep copy
    spec["name"] = tier
    spec["pool_layout"] = tiers["pool_layout"]
    for p in spec["profiles"].values():
        p["size"] = parse_size(p["size"])
    spec["write_size"] = parse_size(spec["write_size"])
    if quick:
        f = tiers.get("quick_factor", 0.01)
        spec["name"] = f"{tier}q"
        spec["reps"] = 1
        for p in spec["profiles"].values():
            if "count" in p:
                p["count"] = max(p.get("min_count", p.get("shards", 2)), int(p["count"] * f))
            if p.get("scale_size", True):
                p["size"] = max(1 * MiB, int(p["size"] * f))
            if "shards" in p:
                p["shards"] = 2
        for k in ("meta_files", "tree_files", "h5_random_reads"):
            spec[k] = max(20, int(spec[k] * f))
        spec["write_size"] = max(16 * MiB, int(spec["write_size"] * f))
        spec["randread_runtime_s"] = 5
    return spec


def data_root(target: dict, tier_name: str) -> Path:
    return target["path"] / "data" / tier_name


def work_root(target: dict, tier_name: str) -> Path:
    """Scratch area on the backend for write tests (always deleted afterwards)."""
    return target["path"] / "work" / tier_name


def remote_path(target: dict, *parts: str) -> str:
    """rclone API path that points at the same place as target['path'] (connectors only)."""
    base = target["rclone_remote"].rstrip("/")
    return "/".join([base, *[p.strip("/") for p in parts if p]])


# --------------------------------------------------------------------------- results
def session_info() -> dict:
    return {
        # the pod hostname is unique per session; RENKU_SESSION is just "1" on RenkuLab
        "session_id": os.environ.get("HOSTNAME", socket.gethostname()),
        "node": os.environ.get("KUBERNETES_NODE_NAME", os.environ.get("NODE_NAME", "unknown")),
        "host": socket.gethostname(),
    }


class ResultWriter:
    """Append one JSON object per measurement to results/<tier>/<session>_<date>.jsonl."""

    def __init__(self, tier_name: str, run_id: str | None = None):
        cfg = load_targets()
        self.run_id = run_id or os.environ.get("STRESS_RUN_ID") or uuid.uuid4().hex[:8]
        self.info = session_info()
        out = cfg["results_dir"] / tier_name
        out.mkdir(parents=True, exist_ok=True)
        day = _dt.date.today().isoformat()
        self.path = out / f"{self.info['session_id']}_{day}.jsonl"
        self.tier = tier_name

    def write(self, **row) -> dict:
        base = {
            "run_id": self.run_id,
            "timestamp": _dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            "tier": self.tier,
            **self.info,
            "cache_state": "cold",
            "threads": 1,
            "errors": 0,
            "ok": True,
        }
        base.update(row)
        with open(self.path, "a") as fh:
            fh.write(json.dumps(base) + "\n")
        return base


# --------------------------------------------------------------------------- timing
@contextmanager
def timer():
    box = {}
    t0 = time.perf_counter()
    try:
        yield box
    finally:
        box["s"] = time.perf_counter() - t0


def percentiles_ms(samples_s: list[float]) -> dict:
    if not samples_s:
        return {}
    xs = sorted(s * 1000 for s in samples_s)

    def pct(p):
        k = (len(xs) - 1) * p
        lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
        return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)

    return {
        "lat_p50_ms": round(pct(0.50), 3),
        "lat_p95_ms": round(pct(0.95), 3),
        "lat_p99_ms": round(pct(0.99), 3),
        "lat_mean_ms": round(statistics.fmean(xs), 3),
    }


# --------------------------------------------------------------------------- misc
def load_script(filename: str):
    """Import one of the numbered scripts (e.g. '01_generate_dataset.py') as a module."""
    import importlib.util

    path = Path(__file__).parent / filename
    mod_name = "s_" + path.stem.replace("-", "_")
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    import sys as _sys
    _sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


def wait_visible(path: Path, timeout_s: float = 1200, poll_s: float = 10) -> float:
    """Wait until `path` shows up through a mount (rclone dir cache can lag minutes)."""
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout_s:
        try:
            os.listdir(path.parent)  # nudges the directory cache
            if path.exists():
                return time.perf_counter() - t0
        except OSError:
            pass
        time.sleep(poll_s)
    raise TimeoutError(f"{path} not visible after {timeout_s}s")


def remote_size(target: dict, relpath: str) -> int | None:
    """Size of an object as the remote API sees it, or None if absent."""
    p = rclone("lsjson", "--stat", remote_path(target, relpath), check=False)
    if p.returncode != 0 or not p.stdout.strip():
        return None
    try:
        return int(json.loads(p.stdout)["Size"])
    except (ValueError, KeyError):
        return None


def wait_remote_size(target: dict, relpath: str, size: int, timeout_s: float = 1800, poll_s: float = 2) -> float | None:
    """Seconds until the remote API reports the full object (async upload after close())."""
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout_s:
        if remote_size(target, relpath) == size:
            return time.perf_counter() - t0
        time.sleep(poll_s)
    return None

def sha256_file(path: Path, bufsize: int = 8 * MiB) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(bufsize):
            h.update(chunk)
    return h.hexdigest()


def run(cmd: list[str], check: bool = True, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True, **kw)


def rclone(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    conf = os.environ.get("RCLONE_CONFIG", str(ROOT / "config" / "rclone.conf"))
    try:
        return run(["rclone", "--config", conf, *args], check=check)
    except subprocess.CalledProcessError as e:
        # stderr is captured, so surface its tail or the run log only shows "exit status 1"
        tail = "\n".join((e.stderr or "").strip().splitlines()[-5:])
        print(f"[rclone] {' '.join(args[:3])} failed (exit {e.returncode}):\n{tail}", flush=True)
        raise


# Recently touched data reads far faster than data at rest, even with the page cache dropped,
# because a cache below the page cache serves it. Measured 2026-10-09 with 100 KiB files, 8 readers:
#   project (Azure Files): written 60 s ago 437-528 files/s vs 165-183 at rest. Re-reading is NOT
#     faster, so only writes count. Data read ~1 h after staging was inflated, >= 3 h was not.
#   session disk (Azure disk, likely host cache): written 60 s ago 6,900-9,600 vs ~620 at rest, and
#     re-reading a shard read earlier that day also gives ~9,800, so reads count too. That cache is
#     size-bound, not time-bound, so the age below is a minimum, not a guarantee, for session disk.
#   PolyBox (API-staged): no effect for re-reads.
MIN_DATA_AGE_H = 6.0


def data_age_h(paths: list[Path], include_reads: bool = False, sample: int = 5) -> float | None:
    """Hours since the newest of `paths` was modified (or, with include_reads, also read).

    For a shard folder its mtime is the last file added, so passing unit paths avoids stat'ing every
    file. Reads are judged from the atime of up to `sample` files per folder (relatime updates atime
    on the first read of a day); folder atimes are useless, since listing updates them.
    """
    stamps = []
    for p in paths:
        try:
            st = p.stat()
            stamps.append(st.st_mtime)
            if include_reads:
                files = [p] if p.is_file() else [e for _, e in zip(range(sample), os.scandir(p)) if e.is_file()]
                stamps += [f.stat().st_atime for f in files]
        except OSError:
            pass
    return round((time.time() - max(stamps)) / 3600, 2) if stamps else None


def require_aged(target: dict, paths: list[Path], min_h: float, what: str) -> float | None:
    """Refuse to measure block/local data touched less than `min_h` hours ago (see MIN_DATA_AGE_H).
    Connector data is staged via the API and showed no such effect, so it is only recorded."""
    age = data_age_h(paths, include_reads=target["kind"] == "local")
    if target["kind"] in ("block", "local") and age is not None and age < min_h:
        raise SystemExit(f"{target['name']}: {what} was written {age:.1f} h ago (< {min_h} h). Reads "
                         f"would hit a cache below the page cache and look too fast. Wait, or pass "
                         f"--min-data-age-h 0 to measure anyway (rows record data_age_h).")
    return age


def drop_file_cache(path: Path) -> None:
    """Ask the kernel to drop page cache for one file (works without root).

    Does NOT clear rclone's VFS cache on the node; that is handled by giving
    every repetition fresh files that were uploaded out-of-band. Nor does it clear caches
    below the page cache on block/local backends; that is what require_aged() is for.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
    except OSError:
        pass
