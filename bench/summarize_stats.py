"""Peak CPU / memory per run from monitor.sh CSVs (docker stats samples ~1/s)."""
import csv, glob, os, re
from collections import defaultdict

UNIT = {"B": 2**-30, "KiB": 2**-20, "MiB": 2**-10, "GiB": 1.0,
        "kB": 1e3 / 2**30, "MB": 1e6 / 2**30, "GB": 1e9 / 2**30}

def gib(s):
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", s)
    return float(m.group(1)) * UNIT[m.group(2)]

print(f"{'run':32} {'peak workers CPU%':>17} {'avg workers CPU%':>16} "
      f"{'peak workers mem':>16} {'peak 1-worker mem':>17} {'peak driver mem':>15}")
for f in sorted(glob.glob(os.path.join(os.path.dirname(__file__), "*full*.csv"))):
    cpu_t, mem_t = defaultdict(float), defaultdict(float)     # summed over workers per second
    w_mem, d_mem = 0.0, 0.0
    with open(f) as fh:
        for r in csv.DictReader(fh):
            try:
                cpu = float(r["cpu_pct"].rstrip("%")); mem = gib(r["mem_usage"].split("/")[0])
            except (ValueError, AttributeError, KeyError):
                continue
            if "worker" in r["container"]:
                cpu_t[r["ts"]] += cpu; mem_t[r["ts"]] += mem; w_mem = max(w_mem, mem)
            elif "head" in r["container"] or "master" in r["container"]:
                d_mem = max(d_mem, mem)
    if not cpu_t:
        continue
    busy = [c for c in cpu_t.values() if c > 5]               # ignore idle startup/teardown seconds
    print(f"{os.path.basename(f)[:-4]:32} {max(cpu_t.values()):>16.0f}% "
          f"{(sum(busy)/len(busy) if busy else 0):>15.0f}% "
          f"{max(mem_t.values()):>13.2f} GiB {w_mem:>14.2f} GiB {d_mem:>12.2f} GiB")
