#!/usr/bin/env python
"""Turn results/*.jsonl into the tables and figures the decision tree needs.

Outputs (in analysis/out/ unless --out is given):
    summary.csv      one row per (tier, workload, variant, profile, cache, threads, backend):
                     n, median, IQR, CV, bootstrap 95 % CI, speed vs project storage, noisy flag
    semantics.csv    W10 pass/fail matrix (check x backend)
    breakeven.csv    passes over the data after which copying it to project storage pays off
    fig_<W>.png      small multiples per workload, one bar per backend (median, IQR whisker)

Usage:
    python analysis/analyze.py results/S
    python analysis/analyze.py results/ --out analysis/out-all
Works as a plain script or cell-by-cell in Jupyter (# %% markers).
"""
# %%
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Headline metric per workload and whether higher is better
HEADLINE = {
    "W1": ("throughput_mb_s", True), "W2": ("throughput_mb_s", True), "W3": ("iops", True),
    "W4": ("throughput_mb_s", True), "W5": ("ops_per_s", True), "W6": ("ops_per_s", True),
    "W7": ("duration_s", False), "W8": ("duration_s", False), "W9": ("throughput_mb_s", True),
    "W11": ("ops_per_s", True),
}
UNITS = {"throughput_mb_s": "MB/s", "iops": "IOPS", "ops_per_s": "ops/s", "duration_s": "s"}
# Fixed categorical order: colour follows the backend, never its rank.
BACKENDS = ["project", "azure", "polybox", "local"]
COLORS = {"project": "#2a78d6", "azure": "#eb6834", "polybox": "#1baf7a", "local": "#eda100"}
KEYS = ["tier", "workload", "variant", "profile", "cache_state", "threads"]
NOISY_CV = 0.15
BASELINE = "project"


def load(paths: list[Path]) -> pd.DataFrame:
    rows = []
    for p in paths:
        files = sorted(p.rglob("*.jsonl")) if p.is_dir() else [p]
        for f in files:
            with open(f) as fh:
                rows += [json.loads(line) for line in fh if line.strip()]
    df = pd.DataFrame(rows)
    if df.empty:
        sys.exit("no results found")
    df = df[~df["workload"].isin(["RUN"])]
    for k in KEYS:
        if k not in df:
            df[k] = None
    df["profile"] = df["profile"].fillna("-")
    df["threads"] = df["threads"].fillna(1).astype(int)
    # Writes through an rclone mount return as soon as the local VFS cache has the data; the
    # upload happens after close(). Headline write speed = bytes / (write time + time until the
    # remote API shows the object). The cache-only figure is kept as cache_throughput_mb_s.
    if "remote_visible_s" in df:
        lag = df["remote_visible_s"]
        has = df["duration_s"].notna() & lag.notna() & df["bytes"].notna()
        df["cache_throughput_mb_s"] = np.where(has, df["throughput_mb_s"], np.nan)
        df.loc[has, "throughput_mb_s"] = (df.loc[has, "bytes"] / 1e6 /
                                          (df.loc[has, "duration_s"] + lag[has])).round(2)
    return drop_superseded_crashes(df)


# run_all records a crash under the step name, which may cover several workloads
_STEP = {"fio": {"W1", "W2", "W3", "W4"}, "real": {"W6", "W7", "W8"}}


def drop_superseded_crashes(df: pd.DataFrame) -> pd.DataFrame:
    """Drop a crash row when a later rerun of the same step (tier, backend, rep) succeeded."""
    crashed = df["variant"] == "crashed"
    keep = pd.Series(True, index=df.index)
    for i, c in df[crashed].iterrows():
        wls = _STEP.get(c["workload"], {c["workload"]})
        later_ok = df[~crashed & (df["tier"] == c["tier"]) & (df["backend"] == c["backend"]) &
                      # W9 rows written before 2026-10-05 carry no rep
                      ((df["rep"] == c["rep"]) | df["rep"].isna()) &
                      df["workload"].isin(wls) & (df["timestamp"] > c["timestamp"])]
        if len(later_ok):
            keep[i] = False
    return df[keep]


def boot_ci(x: np.ndarray, n: int = 2000, seed: int = 0) -> tuple[float, float]:
    if len(x) < 2:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    meds = np.median(rng.choice(x, size=(n, len(x)), replace=True), axis=1)
    return float(np.percentile(meds, 2.5)), float(np.percentile(meds, 97.5))


def summarise(df: pd.DataFrame) -> pd.DataFrame:
    perf = df[df["workload"].isin(HEADLINE) & (df["variant"] != "crashed")].copy()
    out = []
    for key, g in perf.groupby(KEYS + ["backend"], dropna=False):
        metric, higher = HEADLINE[key[1]]
        x = g[metric].dropna().astype(float).to_numpy()
        if len(x) == 0:
            continue
        lo, hi = boot_ci(x)
        out.append(dict(zip(KEYS + ["backend"], key), metric=metric, unit=UNITS[metric], higher_is_better=higher,
                        n=len(x), median=np.median(x), p25=np.percentile(x, 25), p75=np.percentile(x, 75),
                        cv=float(np.std(x, ddof=1) / np.mean(x)) if len(x) > 1 and np.mean(x) else np.nan,
                        ci95_lo=lo, ci95_hi=hi, errors=int(g["errors"].fillna(0).sum()),
                        p99_ms=float(g["lat_p99_ms"].median()) if "lat_p99_ms" in g else np.nan))
    s = pd.DataFrame(out)
    # speed relative to project storage: > 1 means faster than project storage
    base = s[s["backend"] == BASELINE].set_index(KEYS)["median"]
    def rel(r):
        b = base.get(tuple(r[k] for k in KEYS))
        if b is None or not r["median"]:
            return np.nan
        return r["median"] / b if r["higher_is_better"] else b / r["median"]
    s["speed_vs_project"] = s.apply(rel, axis=1).round(3)
    s["noisy"] = s["cv"] > NOISY_CV
    return s.sort_values(KEYS + ["backend"]).reset_index(drop=True)


def semantics(df: pd.DataFrame) -> pd.DataFrame:
    w = df[df["workload"] == "W10"]
    if w.empty:
        return w
    m = w.groupby(["variant", "backend"])["ok"].agg(lambda v: "PASS" if all(v) else ("FAIL" if not any(v) else "FLAKY"))
    return m.unstack("backend").reindex(columns=[b for b in BACKENDS if b in m.index.get_level_values(1)])


def breakeven(s: pd.DataFrame) -> pd.DataFrame:
    """N* = (r_remote + w_project) / (r_remote - r_project), r and w in seconds per MB.

    After N* full passes over the data, copying it once to project storage and
    reading it there beats reading it from the connector every time.
    """
    rows = []
    def med(backend, workload, variant, profile, cache="cold", threads=1):
        q = s[(s.backend == backend) & (s.workload == workload) & (s.variant == variant) &
              (s.profile == profile) & (s.cache_state == cache) & (s.threads == threads)]
        return float(q["median"].iloc[0]) if len(q) else np.nan
    for tier in s["tier"].unique():
        st = s[s.tier == tier]
        w_proj = med(BASELINE, "W2", "seq-write-1m", "fio-pool")
        cases = [("large sequential files", "W1", "seq-read-1m", "fio-pool", 1, "throughput_mb_s"),
                 ("small files, 1 reader", "W6", "epoch-1w", "small-files", 1, "throughput_mb_s"),
                 ("small files, 8 readers", "W6", "epoch-8w", "small-files", 8, "throughput_mb_s")]
        for label, wl, var, prof, th, _ in cases:
            # W6 headline is files/s; convert to MB/s via the raw rows' throughput column
            r_proj_mbs = _mbs(st, BASELINE, wl, var, prof, th)
            for b in ("azure", "polybox"):
                r_rem_mbs = _mbs(st, b, wl, var, prof, th)
                if np.isnan([r_proj_mbs, r_rem_mbs, w_proj]).any():
                    continue
                r_rem, r_proj, w = 1 / r_rem_mbs, 1 / r_proj_mbs, 1 / w_proj
                n_star = (r_rem + w) / (r_rem - r_proj) if r_rem > r_proj else np.inf
                rows.append(dict(tier=tier, case=label, connector=b, connector_mb_s=round(r_rem_mbs, 1),
                                 project_read_mb_s=round(r_proj_mbs, 1), project_write_mb_s=round(w_proj, 1),
                                 breakeven_passes=round(n_star, 2)))
    return pd.DataFrame(rows)


_RAW: pd.DataFrame | None = None


def _mbs(st, backend, wl, var, prof, threads):
    q = _RAW[(_RAW.tier == st.tier.iloc[0]) & (_RAW.backend == backend) & (_RAW.workload == wl) &
             (_RAW.variant == var) & (_RAW.profile == prof) & (_RAW.threads == threads) &
             (_RAW.cache_state == "cold")]
    return float(q["throughput_mb_s"].median()) if len(q) else np.nan


def _fmt(v: float) -> str:
    for div, suf in ((1e9, "G"), (1e6, "M"), (1e3, "k")):
        if abs(v) >= div:
            return f"{v / div:.3g}{suf}"
    return f"{v:.3g}"


# %%
def figures(s: pd.DataFrame, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ink, quiet, grid = "#1f1f1e", "#6b6a64", "#e6e5df"
    for (tier, wl), g in s.groupby(["tier", "workload"]):
        panels = list(g.groupby(["variant", "profile", "cache_state", "threads"], dropna=False))
        ncol = min(3, len(panels))
        nrow = int(np.ceil(len(panels) / ncol))
        fig, axes = plt.subplots(nrow, ncol, figsize=(4.2 * ncol, 2.0 + 1.6 * nrow), squeeze=False)
        for ax, ((var, prof, cache, th), p) in zip(axes.flat, panels):
            p = p.set_index("backend").reindex([b for b in BACKENDS if b in p["backend"].values])
            y = np.arange(len(p))
            ax.barh(y, p["median"], color=[COLORS[b] for b in p.index], height=0.55, zorder=2)
            ax.errorbar(p["median"], y, xerr=[p["median"] - p["p25"], p["p75"] - p["median"]],
                        fmt="none", ecolor=quiet, elinewidth=1, capsize=2, zorder=3)
            ax.set_yticks(y, p.index, color=ink)
            ax.invert_yaxis()
            ax.set_xscale("log")
            vmin, vmax = float(p["p25"].min()), float(p["p75"].max())
            ax.set_xlim(10 ** np.floor(np.log10(max(vmin, 1e-6)) - 1), vmax * 4)  # bars start a decade below
            ax.grid(axis="x", color=grid, zorder=0)
            for side in ("top", "right"):
                ax.spines[side].set_visible(False)
            ax.spines["left"].set_color(grid)
            ax.spines["bottom"].set_color(grid)
            ax.tick_params(colors=quiet, labelsize=8)
            unit = p["unit"].iloc[0]
            better = "higher is better" if p["higher_is_better"].iloc[0] else "lower is better"
            ax.set_title(f"{var} · {prof} · {cache} · {th}t", fontsize=9, color=ink, loc="left")
            ax.set_xlabel(f"median {unit} ({better}, log scale)", fontsize=8, color=quiet)
            for yi, (b, r) in zip(y, p.iterrows()):
                tag = f"  {_fmt(r['median'])}" + (" (noisy)" if r["noisy"] else "")
                ax.text(r["median"], yi, tag, va="center", fontsize=7.5, color=ink, zorder=4)
        for ax in list(axes.flat)[len(panels):]:
            ax.axis("off")
        fig.suptitle(f"Tier {tier} · {wl}", fontsize=11, color=ink, x=0.01, ha="left")
        fig.tight_layout()
        fig.savefig(out / f"fig_{tier}_{wl}.png", dpi=150)
        plt.close(fig)


# %%
def main() -> None:
    global _RAW
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+", type=Path)
    ap.add_argument("--out", type=Path, default=Path(__file__).parent / "out")
    ap.add_argument("--no-figures", action="store_true")
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)

    df = load(a.paths)
    _RAW = df
    s = summarise(df)
    s.round(3).to_csv(a.out / "summary.csv", index=False)
    sem = semantics(df)
    if not sem.empty:
        sem.to_csv(a.out / "semantics.csv")
    be = breakeven(s)
    be.to_csv(a.out / "breakeven.csv", index=False)
    crashed = df[df["variant"] == "crashed"]
    if not a.no_figures:
        figures(s, a.out)

    pd.set_option("display.width", 160)
    view = s.pivot_table(index=["tier", "workload", "variant", "profile", "cache_state", "threads"],
                         columns="backend", values="speed_vs_project")
    print("\nSpeed relative to project storage (>1 = faster):\n")
    print(view.round(2).to_string())
    if not sem.empty:
        print("\nPOSIX semantics (W10):\n")
        print(sem.to_string())
    if not be.empty:
        print("\nBreak-even passes for copying to project storage first:\n")
        print(be.to_string(index=False))
    sw = s[s["workload"] == "W11"]
    if len(sw):
        print("\nParallelism sweep W11 (median files/s by concurrent readers):\n")
        print(sw.pivot_table(index="threads", columns="backend", values="median").round(1).to_string())
    noisy = s[s["noisy"]]
    if len(noisy):
        print(f"\n{len(noisy)} noisy cells (CV > {NOISY_CV:.0%}) - rerun these:")
        print(noisy[KEYS + ["backend", "n", "cv"]].round(2).to_string(index=False))
    if len(crashed):
        print(f"\n{len(crashed)} crashed workloads:")
        print(crashed[["backend", "workload", "error_msg"]].to_string(index=False))
    print(f"\nwritten to {a.out}")


if __name__ == "__main__":
    main()
