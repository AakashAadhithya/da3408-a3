"""compare.py — order-independent parity check between Spark and Ray outputs."""
import argparse
import numpy as np, pandas as pd, pyarrow.dataset as pds

TOL_COLS = ["hour_avg_speed", "speed_vs_hour"]   # sum-order dependent -> tolerance
TOL = 1e-9

def fingerprint(path):
    d = pds.dataset(path, format="parquet")       # ignores _SUCCESS / dotfiles
    schema = [(f.name, str(f.type)) for f in d.schema]
    rows, h, svh, hourly = 0, 0, 0.0, {}
    for b in d.to_batches(batch_size=500_000):
        df = b.to_pandas(date_as_object=False)
        rows += len(df)
        rh = pd.util.hash_pandas_object(df.drop(columns=TOL_COLS), index=False).to_numpy()
        h = (h + int(rh.sum(dtype=np.uint64))) % 2**64     # wraps: order-independent
        svh += float(df["speed_vs_hour"].sum())
        hourly.update(df.groupby("pickup_hour")["hour_avg_speed"].first().to_dict())
    return dict(rows=rows, schema=schema, hash=h, svh=svh, hourly=hourly)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("spark"); ap.add_argument("ray")
    a = ap.parse_args()
    s, r = fingerprint(a.spark), fingerprint(a.ray)

    checks = {
        "row count": s["rows"] == r["rows"],
        "schema (names+types+order)": s["schema"] == r["schema"],
        "row multiset hash (exact cols)": s["hash"] == r["hash"],
        "hour keys": set(s["hourly"]) == set(r["hourly"]),
    }
    max_h = max(abs(s["hourly"][k] - r["hourly"][k]) for k in s["hourly"]) if checks["hour keys"] else float("nan")
    checks[f"hourly avg max|diff| <= {TOL} (got {max_h:.2e})"] = max_h <= TOL
    rel = abs(s["svh"] - r["svh"]) / abs(s["svh"])
    checks[f"sum(speed_vs_hour) rel diff <= {TOL} (got {rel:.2e})"] = rel <= TOL

    print(f"rows: spark={s['rows']:,} ray={r['rows']:,}")
    if not checks["schema (names+types+order)"]:
        for (a1, a2) in zip(s["schema"], r["schema"]):
            if a1 != a2: print("  schema diff:", a1, "vs", a2)
    for k, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}  {k}")
    print("PARITY:", "PASS" if all(checks.values()) else "FAIL")
