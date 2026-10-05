"""Keep the Renku session from looking idle (CPU-based idle check) while the benchmark runs.

Busy-loops ~25% of one core in 1 s cycles. Exits when the watched PID exits or at the deadline.
Usage: python scripts/keepalive.py <pid> <deadline-HH:MM-UTC>
"""
import datetime as dt, os, sys, time

pid, deadline = int(sys.argv[1]), sys.argv[2]
h, m = map(int, deadline.split(":"))
end = dt.datetime.now(dt.timezone.utc).replace(hour=h, minute=m, second=0, microsecond=0)
duty = 0.25
while dt.datetime.now(dt.timezone.utc) < end:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        break
    t = time.monotonic()
    while time.monotonic() - t < duty:
        pass
    time.sleep(1 - duty)
print(f"[keepalive] exit {dt.datetime.now(dt.timezone.utc):%FT%TZ}", flush=True)
