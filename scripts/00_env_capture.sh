#!/usr/bin/env bash
# Record everything about the session that could explain a result.
# Output: env/<session>_<timestamp>/ (commit it together with results/).
set -uo pipefail
cd "$(dirname "$0")/.."

SESSION="${RENKU_SESSION:-${HOSTNAME:-$(hostname)}}"
TS="$(date +%Y-%m-%dT%H%M%S%z)"
OUT="env/${SESSION}_${TS}"
mkdir -p "$OUT"
echo "Writing environment capture to $OUT"

{
  echo "timestamp: $(date -Iseconds)"
  echo "session: $SESSION"
  echo "hostname: $(hostname)"
  echo "node: ${KUBERNETES_NODE_NAME:-${NODE_NAME:-unknown}}"
  echo "kernel: $(uname -r)"
  echo "nproc: $(nproc)"
  echo "cgroup_cpu_max: $(cat /sys/fs/cgroup/cpu.max 2>/dev/null || cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us 2>/dev/null)"
  echo "cgroup_mem_max: $(cat /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null)"
  echo "fio: $(fio --version 2>/dev/null)"
  echo "rclone: $(rclone version 2>/dev/null | head -1)"
  echo "python: $(python --version 2>&1)"
} > "$OUT/summary.txt"

env | grep -E '^(RENKU|JUPYTER|KUBERNETES|NODE_NAME)' | sed -E 's/(TOKEN|SECRET|KEY|PASS)[^=]*=.*/\1=<redacted>/' > "$OUT/env_vars.txt"
cat /proc/meminfo > "$OUT/meminfo.txt"
cat /proc/cpuinfo | grep -m1 "model name" >> "$OUT/summary.txt"
cat /proc/mounts > "$OUT/proc_mounts.txt"
findmnt -J > "$OUT/findmnt.json" 2>/dev/null || true
df -hT > "$OUT/df.txt" 2>/dev/null || true
lsblk -J > "$OUT/lsblk.json" 2>/dev/null || true
cp config/targets.yaml config/tiers.yaml "$OUT/"

# Per-target: filesystem type, mount options, free space
python - "$OUT" <<'PY'
import json, os, sys
sys.path.insert(0, "scripts")
from lib import load_targets
out = sys.argv[1]
cfg = load_targets()
mounts = [l.split() for l in open("/proc/mounts")]
rows = {}
for name, t in cfg["targets"].items():
    p = str(t["path"])
    best = max((m for m in mounts if p.startswith(m[1].rstrip("/") + "/") or p == m[1]),
               key=lambda m: len(m[1]), default=None)
    try:
        st = os.statvfs(os.path.dirname(p) if not os.path.exists(p) else p)
        free_gb = round(st.f_bavail * st.f_frsize / 1e9, 1)
    except OSError as e:
        free_gb = f"error: {e}"
    rows[name] = {
        "path": p,
        "mountpoint": best[1] if best else None,
        "device": best[0] if best else None,
        "fstype": best[2] if best else None,
        "options": best[3] if best else None,
        "free_gb": free_gb,
    }
json.dump(rows, open(f"{out}/targets_mounts.json", "w"), indent=2)
for k, v in rows.items():
    print(f"  {k:8s} {v['fstype']!s:14s} free={v['free_gb']} GB  mount={v['mountpoint']}")
PY

# rclone mount options are set by the csi-rclone driver; the FUSE options are all we can see.
grep -i rclone /proc/mounts > "$OUT/rclone_mounts.txt" 2>/dev/null || echo "no rclone mounts visible" > "$OUT/rclone_mounts.txt"

# Round-trip time to each remote endpoint (10 requests, connect / first byte / total, seconds)
while read -r name url; do
  [ -z "$name" ] && continue
  for i in $(seq 1 10); do
    curl -s -o /dev/null -w "$name %{time_connect} %{time_starttransfer} %{time_total}\n" --max-time 15 "$url" || echo "$name error"
  done
done < <(python - <<'PY'
import sys
sys.path.insert(0, "scripts")
from lib import load_targets
for name, t in load_targets()["targets"].items():
    if t.get("endpoint"):
        print(name, t["endpoint"])
PY
) > "$OUT/rtt.txt"
awk '{c[$1]+=$2; f[$1]+=$3; n[$1]++} END {for (k in n) printf "  rtt %-8s connect=%.1f ms first_byte=%.1f ms\n", k, 1000*c[k]/n[k], 1000*f[k]/n[k]}' "$OUT/rtt.txt"

echo "Done. Also note by hand in $OUT/summary.txt: resource class name, session start-up time (from the RenkuLab UI)."
