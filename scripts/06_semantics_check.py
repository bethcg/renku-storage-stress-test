#!/usr/bin/env python
"""W10 POSIX semantics checks: which everyday tools work on each backend.

These are pass/fail eligibility checks, not speed tests. A backend that fails
`sqlite-wal` or `git-commit` should never be recommended for that use, however fast.

Usage:
    python scripts/06_semantics_check.py --tier S --backend azure
"""
from __future__ import annotations

import argparse
import fcntl
import mmap
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import ResultWriter, load_targets, load_tier, work_root  # noqa: E402


def c_append(d: Path):
    p = d / "append.txt"
    for i in range(3):
        with open(p, "a") as fh:
            fh.write(f"line {i}\n")
    assert p.read_text().count("\n") == 3, "append lost data"


def c_overwrite_middle(d: Path):
    p = d / "rw.bin"
    p.write_bytes(b"A" * 4096)
    with open(p, "r+b") as fh:
        fh.seek(1000)
        fh.write(b"B" * 10)
    data = p.read_bytes()
    assert data[1000:1010] == b"B" * 10 and len(data) == 4096, "in-place write failed"


def c_rename_atomic(d: Path):
    a, b = d / "a.txt", d / "b.txt"
    a.write_text("new")
    b.write_text("old")
    os.replace(a, b)
    assert b.read_text() == "new" and not a.exists(), "os.replace not atomic/complete"


def c_rename_dir(d: Path):
    src = d / "dir_src"
    (src / "sub").mkdir(parents=True)
    (src / "sub" / "f.txt").write_text("x")
    os.rename(src, d / "dir_dst")
    assert (d / "dir_dst" / "sub" / "f.txt").read_text() == "x"


def c_flock(d: Path):
    p = d / "lock"
    with open(p, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fh, fcntl.LOCK_UN)


def c_fcntl_lock(d: Path):
    p = d / "lock2"
    with open(p, "w") as fh:
        fcntl.lockf(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.lockf(fh, fcntl.LOCK_UN)


def c_mmap_write(d: Path):
    p = d / "mm.bin"
    p.write_bytes(b"\0" * 8192)
    with open(p, "r+b") as fh:
        m = mmap.mmap(fh.fileno(), 8192)
        m[0:5] = b"hello"
        m.flush()
        m.close()
    assert p.read_bytes()[:5] == b"hello"


def c_sqlite(d: Path):
    con = sqlite3.connect(d / "db.sqlite")
    con.execute("create table t(i integer, s text)")
    con.executemany("insert into t values (?, ?)", [(i, str(i)) for i in range(1000)])
    con.commit()
    assert con.execute("select count(*) from t").fetchone()[0] == 1000
    con.close()


def c_sqlite_wal(d: Path):
    con = sqlite3.connect(d / "wal.sqlite")
    mode = con.execute("pragma journal_mode=wal").fetchone()[0]
    con.execute("create table t(i)")
    con.execute("insert into t values (1)")
    con.commit()
    con.close()
    assert mode == "wal", f"journal_mode={mode}"


def c_symlink(d: Path):
    (d / "target.txt").write_text("t")
    os.symlink("target.txt", d / "link.txt")
    assert (d / "link.txt").read_text() == "t"


def c_hardlink(d: Path):
    (d / "h1.txt").write_text("h")
    os.link(d / "h1.txt", d / "h2.txt")


def c_chmod_exec(d: Path):
    p = d / "run.sh"
    p.write_text("#!/bin/sh\necho ok\n")
    os.chmod(p, 0o755)
    out = subprocess.run([str(p)], capture_output=True, text=True, timeout=30)
    assert out.stdout.strip() == "ok", "cannot execute files"


def c_sparse_seek(d: Path):
    p = d / "sparse.bin"
    with open(p, "wb") as fh:
        fh.seek(100 * 1024 * 1024)
        fh.write(b"end")
    assert p.stat().st_size == 100 * 1024 * 1024 + 3


def c_fsync(d: Path):
    with open(d / "sync.txt", "w") as fh:
        fh.write("x")
        fh.flush()
        os.fsync(fh.fileno())


def c_case_sensitive(d: Path):
    (d / "Case.txt").write_text("upper")
    (d / "case.txt").write_text("lower")
    assert (d / "Case.txt").read_text() == "upper", "filesystem is case-insensitive"


def c_mtime_preserved(d: Path):
    p = d / "mt.txt"
    p.write_text("x")
    os.utime(p, (1_600_000_000, 1_600_000_000))
    assert abs(p.stat().st_mtime - 1_600_000_000) < 2, "mtime not settable"


def _git(d: Path, safe_directory: bool):
    repo = d / "repo"
    repo.mkdir()
    (repo / "a.txt").write_text("x\n")
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
           "GIT_COMMITTER_EMAIL": "t@t"}
    if safe_directory:
        # rclone mounts report files as owned by root, so git refuses them as "dubious ownership";
        # users fix that once with `git config --global --add safe.directory '*'`
        env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="safe.directory", GIT_CONFIG_VALUE_0="*")
    for cmd in (["git", "init", "-q"], ["git", "add", "-A"], ["git", "commit", "-q", "-m", "x"],
                ["git", "status", "--porcelain"]):
        p = subprocess.run(cmd, cwd=repo, env=env, capture_output=True, text=True, timeout=120)
        if p.returncode:
            raise RuntimeError(f"{' '.join(cmd[:2])}: {p.stderr.strip().splitlines()[-1] if p.stderr.strip() else p.returncode}")


def c_git(d: Path):
    """git as users would run it after the one-line safe.directory workaround."""
    _git(d, safe_directory=True)


def c_git_no_safedir(d: Path):
    """git with default config; FAIL here but PASS on `git` means only the workaround is needed."""
    _git(d, safe_directory=False)


CHECKS = [c_append, c_overwrite_middle, c_rename_atomic, c_rename_dir, c_flock, c_fcntl_lock, c_mmap_write,
          c_sqlite, c_sqlite_wal, c_symlink, c_hardlink, c_chmod_exec, c_sparse_seek, c_fsync,
          c_case_sensitive, c_mtime_preserved, c_git, c_git_no_safedir]


def run(backend: str, spec: dict, writer: ResultWriter) -> None:
    t = load_targets()["targets"][backend]
    root = work_root(t, spec["name"]) / f"semantics-{int(time.time())}"
    for check in CHECKS:
        d = root / check.__name__
        d.mkdir(parents=True, exist_ok=True)
        t0 = time.perf_counter()
        ok, msg = True, ""
        try:
            check(d)
        except Exception as e:
            ok, msg = False, f"{type(e).__name__}: {e}"[:300]
        dur = time.perf_counter() - t0
        writer.write(backend=backend, workload="W10", variant=check.__name__[2:].replace("_", "-"),
                     profile="semantics", ok=ok, errors=0 if ok else 1, duration_s=round(dur, 3), error_msg=msg)
        print(f"  {backend:8s} {check.__name__[2:]:18s} {'PASS' if ok else 'FAIL ' + msg}")
    shutil.rmtree(root, ignore_errors=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="S")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--backend", required=True)
    a = ap.parse_args()
    spec = load_tier(a.tier, a.quick)
    run(a.backend, spec, ResultWriter(spec["name"]))


if __name__ == "__main__":
    main()
