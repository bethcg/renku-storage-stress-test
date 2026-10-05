#!/usr/bin/env python
"""Generate the synthetic benchmark dataset deterministically.

The same seed + tier always produces byte-identical files, so every backend holds
exactly the same data. Content is random (incompressible) so no layer can cheat
with compression or deduplication.

Layout under <dest>/ (one "unit" = one shard folder or one file):
    small-files/shard-000/f-00000.bin ...    W6 (ML-epoch style reads)
    medium-files/shard-000/f-00000.bin ...   W6
    large-files/table-000.parquet            W7 Parquet column scan
    large-files/array-000.h5                 W7 HDF5 random chunk reads
    fio-pool/rep-00/file-00.bin ...          W1 W3 W4 (fresh files per repetition)

Usage:
    python scripts/01_generate_dataset.py --tier S --dest /tmp/stress-local/data/S
    python scripts/01_generate_dataset.py --tier S --quick --dest /tmp/x --list
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import tarfile
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from lib import MiB, load_tier  # noqa: E402

SEED = 20261005
CHUNK = 8 * MiB


@dataclass
class Unit:
    relpath: str
    kind: str                      # shard | parquet | h5 | blob
    params: dict = field(default_factory=dict)

    @property
    def nbytes(self) -> int:
        if self.kind == "shard":
            return self.params["files"] * self.params["size"]
        return self.params["size"]


def _rng(relpath: str) -> np.random.Generator:
    h = hashlib.sha256(f"{SEED}:{relpath}".encode()).digest()
    return np.random.default_rng(int.from_bytes(h[:8], "little"))


def plan_units(spec: dict) -> list[Unit]:
    units: list[Unit] = []
    for pname, p in spec["profiles"].items():
        if p["kind"] == "bin":
            per = p["count"] // p["shards"]
            for s in range(p["shards"]):
                units.append(Unit(f"{pname}/shard-{s:03d}", "shard", {"files": per, "size": p["size"]}))
        elif p["kind"] == "columnar":
            half = max(1, p["count"] // 2)
            for i in range(half):
                units.append(Unit(f"{pname}/table-{i:03d}.parquet", "parquet", {"size": p["size"]}))
                units.append(Unit(f"{pname}/array-{i:03d}.h5", "h5", {"size": p["size"]}))
        elif p["kind"] == "pool":
            for r in range(spec["reps"]):
                for j in range(p["files_per_rep"]):
                    units.append(Unit(f"{pname}/rep-{r:02d}/file-{j:02d}.bin", "blob", {"size": p["size"]}))
    return units


# ----------------------------------------------------------------------------- writers
def _write_random(path: Path, size: int, rng: np.random.Generator, checksum: bool) -> str | None:
    h = hashlib.sha256() if checksum else None
    with open(path, "wb") as fh:
        left = size
        while left > 0:
            n = min(CHUNK, left)
            b = rng.bytes(n)
            fh.write(b)
            if h:
                h.update(b)
            left -= n
    return h.hexdigest() if h else None


def _write_parquet(path: Path, size: int, rng: np.random.Generator) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    ncols, row_bytes = 8, 8 * 8
    rows_total = max(1024, size // row_bytes)
    rg = min(1 << 20, rows_total)  # 1 Mi rows = 64 MiB per row group
    schema = pa.schema([(f"c{i}", pa.float64()) for i in range(ncols)])
    with pq.ParquetWriter(path, schema, compression="none", use_dictionary=False) as w:
        done = 0
        while done < rows_total:
            n = min(rg, rows_total - done)
            cols = [pa.array(rng.random(n)) for _ in range(ncols)]
            w.write_table(pa.Table.from_arrays(cols, schema=schema), row_group_size=n)
            done += n


def _write_h5(path: Path, size: int, rng: np.random.Generator) -> None:
    import h5py

    width = 1024  # float32 -> 4 KiB per row, chunk = 256 rows = 1 MiB
    rows_total = max(256, size // (width * 4))
    with h5py.File(path, "w") as f:
        ds = f.create_dataset("x", shape=(rows_total, width), dtype="f4", chunks=(256, width))
        step = 16384
        for start in range(0, rows_total, step):
            n = min(step, rows_total - start)
            ds[start:start + n] = rng.random((n, width), dtype=np.float32)


def generate_unit(unit: Unit, dest_root: Path, checksum: bool = True) -> list[dict]:
    """Write one unit under dest_root. Returns manifest rows."""
    rng = _rng(unit.relpath)
    target = dest_root / unit.relpath
    rows = []
    if unit.kind == "shard":
        target.mkdir(parents=True, exist_ok=True)
        for i in range(unit.params["files"]):
            fp = target / f"f-{i:05d}.bin"
            digest = _write_random(fp, unit.params["size"], rng, checksum)
            rows.append({"path": f"{unit.relpath}/{fp.name}", "bytes": unit.params["size"], "sha256": digest})
        return rows
    target.parent.mkdir(parents=True, exist_ok=True)
    if unit.kind == "blob":
        digest = _write_random(target, unit.params["size"], rng, checksum)
    elif unit.kind == "parquet":
        _write_parquet(target, unit.params["size"], rng)
        digest = None
    elif unit.kind == "h5":
        _write_h5(target, unit.params["size"], rng)
        digest = None
    else:
        raise ValueError(unit.kind)
    if checksum and digest is None:
        from lib import sha256_file
        digest = sha256_file(target)
    return [{"path": unit.relpath, "bytes": target.stat().st_size, "sha256": digest}]


# ----------------------------------------------------------------------------- extras
def make_tree_tar(n_files: int, out: Path) -> Path:
    """W8: a tarball shaped like a conda env / site-packages (many small files, deep tree)."""
    if out.exists():
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    rnd = random.Random(SEED)
    rng = _rng("tree.tar")
    import io
    with tarfile.open(out, "w") as tar:
        for i in range(n_files):
            pkg, sub = i // 400, (i // 40) % 10
            size = int(min(200_000, max(200, rnd.lognormvariate(8.5, 1.2))))  # median ~5 KB
            data = rng.bytes(size)
            ti = tarfile.TarInfo(f"tree/pkg-{pkg:03d}/sub-{sub}/mod-{i:05d}.py")
            ti.size = size
            tar.addfile(ti, io.BytesIO(data))
    return out


def make_ingress_sample(spec: dict, out: Path) -> Path:
    """W9: one small-file shard + one large file, used to time copying data in."""
    marker = out / ".complete"
    if marker.exists():
        return out
    sp = spec["profiles"]["small-files"]
    generate_unit(Unit("ingress/small", "shard", {"files": sp["count"] // sp["shards"], "size": sp["size"]}), out.parent, checksum=False)
    generate_unit(Unit("ingress/large.bin", "blob", {"size": spec["write_size"]}), out.parent, checksum=False)
    marker.touch()
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="S")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--dest", type=Path, required=True)
    ap.add_argument("--only", default="", help="generate only units whose path starts with this prefix")
    ap.add_argument("--no-checksum", action="store_true")
    ap.add_argument("--list", action="store_true", help="print the unit plan and total size, then exit")
    a = ap.parse_args()

    spec = load_tier(a.tier, a.quick)
    units = [u for u in plan_units(spec) if u.relpath.startswith(a.only)]
    total = sum(u.nbytes for u in units)
    print(f"tier {spec['name']}: {len(units)} units, {total / 1e9:.2f} GB")
    if a.list:
        for u in units:
            print(f"  {u.relpath:45s} {u.kind:8s} {u.nbytes / 1e6:10.1f} MB")
        return
    a.dest.mkdir(parents=True, exist_ok=True)
    with open(a.dest / "MANIFEST.jsonl", "a") as man:
        for k, u in enumerate(units, 1):
            for row in generate_unit(u, a.dest, checksum=not a.no_checksum):
                man.write(json.dumps(row) + "\n")
            print(f"  [{k}/{len(units)}] {u.relpath}", flush=True)


if __name__ == "__main__":
    main()
