"""make_figs.py — build report figures from bench/ logs and CSVs (all numbers come from committed files)."""
import csv, os, re, statistics
from collections import defaultdict
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

B = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(B, "..", "report", "figs"); os.makedirs(OUT, exist_ok=True)
RUNS, FWS = [1, 2, 3], ["spark", "ray"]
COL = {"spark": "#E25A1C", "ray": "#028CF0"}
UNIT = {"B": 2**-30, "KiB": 2**-20, "MiB": 2**-10, "GiB": 1.0, "kB": 1e3/2**30, "MB": 1e6/2**30, "GB": 1e9/2**30}

def grab(path, pat):
    m = re.search(pat, open(path).read())
    return float(m.group(1)) if m else None

def gib(s):
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", s); return float(m.group(1)) * UNIT[m.group(2)]

def stats(path):
    cpu, wmem = defaultdict(float), 0.0
    for r in csv.DictReader(open(path)):
        if "worker" not in r.get("container", ""):
            continue
        try:
            cpu[int(r["ts"])] += float(r["cpu_pct"].rstrip("%"))
            wmem = max(wmem, gib(r["mem_usage"].split("/")[0]))
        except (ValueError, AttributeError, KeyError):
            pass
    return cpu, wmem

def save(fig, name):
    fig.tight_layout(); fig.savefig(os.path.join(OUT, name), dpi=160); plt.close(fig); print("wrote", name)

# Fig 1: end-to-end
tot = {fw: [grab(f"{B}/{fw}_full_run{n}.log", r"end-to-end wall clock: ([\d.]+)") for n in RUNS] for fw in FWS}
fig, ax = plt.subplots(figsize=(5, 4))
for i, fw in enumerate(FWS):
    v = tot[fw]; med = statistics.median(v)
    ax.bar(i, med, color=COL[fw], alpha=.8, yerr=[[med - min(v)], [max(v) - med]], capsize=6)
    ax.scatter([i] * len(v), v, color="k", zorder=3, s=14)
    ax.text(i, max(v) + 15, f"median {med:.0f} s", ha="center")
ax.set_xticks(range(len(FWS)), [f.capitalize() for f in FWS]); ax.set_ylabel("seconds")
ax.set_title("End-to-end wall clock, 106.8M rows (3 runs)"); ax.set_ylim(0, max(max(v) for v in tot.values()) * 1.15)
save(fig, "fig1_end_to_end.png")

# Fig 2: Ray phases
fig, ax = plt.subplots(figsize=(6, 4)); bottom = [0] * 3; ph = {}
for n in RUNS:
    p = f"{B}/ray_full_run{n}.log"
    a = grab(p, r"phase A[^:]*:\s*([\d.]+)"); b = grab(p, r"phase B[^:]*:\s*([\d.]+)")
    ph[n] = (a, b, tot["ray"][n - 1] - a - b)
for k, (lab, c) in enumerate([("A: ingest->validity (staged)", "#9ecae1"),
                               ("B: 36x dedupe + UDF", "#3182bd"), ("C: hourly + joins + export", "#08519c")]):
    vals = [ph[n][k] for n in RUNS]
    ax.bar([f"run {n}" for n in RUNS], vals, bottom=bottom, color=c, label=lab)
    bottom = [x + y for x, y in zip(bottom, vals)]
ax.set_ylabel("seconds"); ax.set_title("Ray: where the time goes"); ax.legend(fontsize=8)
save(fig, "fig2_ray_phases.png")

# Fig 3: resources
fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 4))
for i, fw in enumerate(FWS):
    cpus, mems = [], []
    for n in RUNS:
        cpu, wmem = stats(f"{B}/{fw}_full_run{n}.csv")
        busy = [c for c in cpu.values() if c > 5]
        cpus.append(sum(busy) / len(busy)); mems.append(wmem)
    a1.bar(i, statistics.median(cpus), color=COL[fw], alpha=.8); a1.scatter([i] * 3, cpus, color="k", s=14, zorder=3)
    a2.bar(i, statistics.median(mems), color=COL[fw], alpha=.8); a2.scatter([i] * 3, mems, color="k", s=14, zorder=3)
a1.axhline(400, ls="--", c="gray"); a1.text(-.4, 405, "4 cores = 400%", fontsize=8)
a1.set_title("Avg CPU while busy (2 workers)"); a1.set_ylabel("%"); a1.set_ylim(0, 450)
a2.axhline(4.0, ls="--", c="gray"); a2.text(-.4, 4.03, "container cap 4 GiB", fontsize=8)
a2.axhline(3.8, ls=":", c="red"); a2.text(-.4, 3.68, "Ray OOM-kill line (95%)", fontsize=8, color="red")
a2.set_title("Peak memory, single worker"); a2.set_ylabel("GiB"); a2.set_ylim(0, 4.4)
for a in (a1, a2): a.set_xticks(range(len(FWS)), [f.capitalize() for f in FWS])
save(fig, "fig3_resources.png")

# Fig 4: CPU over time (run 3)
fig, ax = plt.subplots(figsize=(9, 3.5))
for fw in FWS:
    cpu, _ = stats(f"{B}/{fw}_full_run3.csv"); t0 = min(cpu); xs = sorted(cpu)
    ax.plot([x - t0 for x in xs], [cpu[x] for x in xs], color=COL[fw], lw=1, label=fw.capitalize())
ax.axhline(400, ls="--", c="gray"); ax.set_xlabel("seconds since start"); ax.set_ylabel("workers CPU %")
ax.set_title("Cluster CPU over time (run 3)"); ax.legend()
save(fig, "fig4_cpu_timeline.png")

# Fig 5: UDF micro-benchmark
udf = {}
for fw in FWS:
    for m in re.finditer(r"\[udf\] (\w+)\s+(\w+)\s+median=\s*([\d.]+)s", open(f"{B}/udf_{fw}.log").read()):
        udf[(m.group(1), m.group(2))] = float(m.group(3))
names = {"spark": ["native", "row_udf", "pandas_udf"], "ray": ["native", "row_python", "pandas"]}
labels = ["native (no Python)", "row-at-a-time Python", "vectorized (pandas)"]
fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 4)); w = .38
for j, fw in enumerate(FWS):
    v = [udf[(fw, k)] for k in names[fw]]
    xs = [i + (j - .5) * w for i in range(3)]
    a1.bar(xs, v, w, color=COL[fw], label=fw.capitalize())
    for x, y in zip(xs, v): a1.text(x, y + .4, f"{y:.2f}", ha="center", fontsize=8)
    ov = [v[1] - v[0], v[2] - v[0]]
    xs2 = [i + (j - .5) * w for i in range(2)]
    a2.bar(xs2, ov, w, color=COL[fw], label=fw.capitalize())
    for x, y in zip(xs2, ov): a2.text(x, max(y, 0) + .4, f"{y:.2f}", ha="center", fontsize=8)
a1.set_xticks(range(3), labels, fontsize=8); a1.set_ylabel("seconds (median of 3)")
a1.set_title("UDF stage alone, 20.25M rows"); a1.legend()
a2.set_xticks(range(2), ["row-at-a-time", "vectorized"]); a2.set_ylabel("seconds over native")
a2.set_title("Python overhead = UDF - native"); a2.legend()
save(fig, "fig5_udf.png")
