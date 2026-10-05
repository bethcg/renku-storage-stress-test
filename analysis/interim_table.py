"""Interim §2.1 figures for STORAGE_COMPARISON.md while the Tier S run is still going.

Median over the valid reps per cell, with n and the min-max range. `analyze.py` stays the source of truth
for the final numbers (bootstrap CIs, CV flags); this is only a quick look between milestones.

Usage: python analysis/interim_table.py [results/S]
"""
import json
import statistics as st
import sys
from pathlib import Path

DROP_RUNS = {"20261005T0858-0887"}  # partial rep 0 of the first run (see §4 interruption log)
BACKENDS = ["project", "azure", "polybox", "local"]

# (label, workload, variant, threads, cache_state, metric, unit)
CELLS = [
    ("W1 sequential read, 1 MiB blocks", "W1", "seq-read-1m", 1, "cold", "throughput_mb_s", "MB/s"),
    ("W1 sequential read, warm", "W1", "seq-read-1m", 1, "warm", "throughput_mb_s", "MB/s"),
    ("W2 sequential write 1 GiB, incl. upload", "W2", "seq-write-1m", 1, "cold", "visible_mb_s", "MB/s"),
    ("W3 random read 4 KiB", "W3", "rand-read-4k", 1, "cold", "iops", "IOPS"),
    ("W4 16 parallel readers", "W4", "parallel-read-16", 16, "cold", "throughput_mb_s", "MB/s"),
    ("W5 create (1 thread)", "W5", "meta-create", 1, "cold", "ops_per_s", "ops/s"),
    ("W5 rename (1 thread)", "W5", "meta-rename", 1, "cold", "ops_per_s", "ops/s"),
    ("W5 delete (1 thread)", "W5", "meta-delete", 1, "cold", "ops_per_s", "ops/s"),
    ("W5 mkdir (1 thread)", "W5", "meta-mkdir", 1, "cold", "ops_per_s", "ops/s"),
    ("W5 rename (8 threads)", "W5", "meta-rename", 8, "cold", "ops_per_s", "ops/s"),
    ("W6 100 KiB files, 1 worker", "W6", "epoch-1w", 1, "cold", "small:ops_per_s", "files/s"),
    ("W6 100 KiB files, 8 workers", "W6", "epoch-8w", 8, "cold", "small:ops_per_s", "files/s"),
    ("W6 4 MiB files, 8 workers", "W6", "epoch-8w", 8, "cold", "medium:throughput_mb_s", "MB/s"),
    ("W7 Parquet, 2 of 8 columns", "W7", "parquet-2of8-cols", 1, "cold", "throughput_mb_s", "MB/s"),
    ("W7 HDF5, random chunks", "W7", "hdf5-random-chunks", 1, "cold", "throughput_mb_s", "MB/s"),
    ("W8 extract 20,000-file archive", "W8", "untar", 1, "cold", "duration_s", "s"),
    ("W8 delete that tree", "W8", "delete-tree", 1, "cold", "duration_s", "s"),
    ("W9 upload via API, large files", "W9", "api-large", 1, "cold", "throughput_mb_s", "MB/s"),
    ("Lag until 1 GiB write visible", "W2", "seq-write-1m", 1, "cold", "remote_visible_s", "s"),
]


def load(root: Path) -> list[dict]:
    rows = []
    for f in sorted(root.glob("*.jsonl")):
        for line in f.open():
            r = json.loads(line)
            if r.get("run_id") in DROP_RUNS or r.get("workload") == "RUN" or not r.get("ok", True):
                continue
            if r.get("variant") == "crashed":
                continue
            rows.append(r)
    return rows


def value(r: dict, metric: str):
    if metric.startswith(("small:", "medium:")):
        prof, metric = metric.split(":")
        if prof not in str(r.get("profile", "")):
            return None
    if metric == "visible_mb_s":
        if r.get("remote_visible_s") is None:
            return r.get("throughput_mb_s")
        mb = r["throughput_mb_s"] * r["duration_s"]
        return mb / (r["duration_s"] + r["remote_visible_s"])
    return r.get(metric)


def fmt(vals: list[float]) -> str:
    if not vals:
        return "pending"
    m = st.median(vals)
    s = f"{m:,.0f}" if m >= 100 else f"{m:,.1f}" if m >= 1 else f"{m:.2f}"
    if len(vals) > 1:
        cv = st.stdev(vals) / st.mean(vals) * 100 if st.mean(vals) else 0
        s += f" (n={len(vals)}, CV {cv:.0f} %{' ⚠' if cv > 15 else ''})"
    return s


def main() -> None:
    rows = load(Path(sys.argv[1] if len(sys.argv) > 1 else "results/S"))
    reps = sorted({(r.get("backend"), r.get("rep")) for r in rows if r.get("rep") is not None and r["rep"] >= 0})
    print("reps per backend:", {b: sorted({rp for bb, rp in reps if bb == b}) for b in BACKENDS})
    print("| metric | " + " | ".join(BACKENDS) + " |")
    print("|---|" + "---:|" * len(BACKENDS))
    for label, wl, var, thr, cache, metric, unit in CELLS:
        cells = []
        for b in BACKENDS:
            vals = [v for r in rows
                    if r.get("backend") == b and r.get("workload") == wl and r.get("variant") == var
                    and r.get("threads", 1) == thr and r.get("cache_state", "cold") == cache
                    and (v := value(r, metric)) is not None]
            cells.append(fmt(vals))
        print(f"| {label} [{unit}] | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
