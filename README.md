# Renku storage stress test

Compares, inside one RenkuLab session, how the same synthetic data performs on:

| backend   | what it is |
|-----------|------------|
| `project` | Renku project storage (CIFS / Azure Files in the tested session; check `env/` for yours) |
| `azure`   | Azure Blob data connector (rclone FUSE mount) |
| `polybox` | PolyBox data connector (rclone over WebDAV) |
| `local`   | the session's own disk, as a reference |

The full plan, fairness rules and decision tree live in the companion doc
"Renku Storage Stress Test — Plan & Implementation Guide".

## Quick start (in a Renku session)

```bash
conda env create -f environment.yml && conda activate renku-stress
cp config/rclone.conf.template config/rclone.conf     # fill in the SAME credentials as the connectors
$EDITOR config/targets.yaml                           # real mount paths (see `findmnt`)
bash scripts/00_env_capture.sh                        # records mounts, limits, RTT -> env/
bash scripts/run_all.sh --tier S --quick              # smoke test: ~0.1 GB, 1 rep, ~5-15 min
bash scripts/run_all.sh --tier S --reps 5             # full Tier S (~20 GB per backend)
tail -f results/run_*.log
python analysis/analyze.py results/S                  # -> analysis/out/{summary,semantics,breakeven}.csv + figures
```

Tier L (~145 GB per backend): `bash scripts/run_all.sh --tier L --reps 3`.
Resume an interrupted run with `--start-rep N --skip-stage`. Subsets: `--backends project,azure`, `--workloads W1,W6`.

## Files

| file | purpose |
|------|---------|
| `config/targets.yaml` | where each backend is mounted; rclone API path for connectors |
| `config/tiers.yaml` | dataset sizes, repetitions, fio pool layout per tier |
| `config/rclone.conf.template` | rclone remotes for out-of-band staging (copy to `rclone.conf`, git-ignored) |
| `scripts/00_env_capture.sh` | session fingerprint: CPU/memory limits, mounts, fs types, rclone options, RTT |
| `scripts/01_generate_dataset.py` | deterministic synthetic data (fixed seed, incompressible, Parquet/HDF5) |
| `scripts/02_stage_data.py` | stage data per backend (connectors via rclone API, never via the mount); W9 ingress |
| `scripts/03_run_fio.py` | W1 seq read, W2 seq write (+ remote-visibility lag), W3 random read, W4 parallel read |
| `scripts/04_metadata_bench.py` | W5 create / stat / list / rename / delete |
| `scripts/05_realistic_bench.py` | W6 ML-epoch reads, W7 Parquet + HDF5, W8 untar / walk / delete a tree |
| `scripts/06_semantics_check.py` | W10 POSIX checks: append, rename, locks, mmap, SQLite (WAL), git, symlinks... |
| `scripts/07_parallel_sweep.py` | W11 read scaling with 1-32 concurrent readers, own fresh data (run separately) |
| `scripts/08_durability.py` | W12 do connector writes survive a session stop before upload completes? (`write` / `verify`) |
| `scripts/run_all.sh` / `run_all.py` | orchestrator: preflight, staging, shuffled backend order per repetition |
| `analysis/analyze.py` | medians, bootstrap CIs, CV flags, speed vs project storage, break-even passes, figures |

## Fairness rules built in

* All backends are mounted in **one** session -> identical CPU, memory, node.
* Same bytes everywhere (fixed seed); random content defeats compression/dedup.
* Every repetition reads **fresh files**; connector data is uploaded through the rclone API so
  the node's rclone VFS cache has never seen it -> "cold" really is cold.
* Backend order is shuffled per repetition (seed logged).
* Cold and warm are reported separately; cells with CV > 15 % are flagged for rerun.
* Results are written to `results/` in the Git repo, never to a backend under test.

## Data handling

Synthetic data only. Do not put real research or personal data into this test.
Delete the benchmark data afterwards: `rclone --config config/rclone.conf purge <rclone_remote>/data`
for each connector in `config/targets.yaml` (e.g. `azure:renku-stress/data`, `polybox:renku-storage-test/data`).

## Tested

The smoke test (`--tier S --quick`) ran end to end on RenkuLab against all four real backends on
2026-10-05. Project storage in that session was a CIFS share on which git, SQLite, chmod and hardlinks
fail; see `semantics.csv`. With rclone mounts, git needs `git config --global --add safe.directory '*'`.
