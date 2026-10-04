"""Interpreter-only speed of trip_features (no Spark/Ray) — isolates the Python 3.8 vs 3.11 factor."""
import glob, sys, time
import pyarrow.parquet as pq

def trip_features(pickup, dropoff, distance):            # IDENTICAL to the pipelines
    dur_s = (dropoff - pickup).total_seconds()
    return (dur_s / 60.0, distance / (dur_s / 3600.0))

t = pq.read_table(sorted(glob.glob("/data/udf_input/*.parquet"))[0])
pu, do, di = (t[c].to_pylist() for c in ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"])
best = min(
    (lambda s: (sum(1 for _ in (trip_features(a, b, c) for a, b, c in zip(pu, do, di))), time.perf_counter() - s)[1])(time.perf_counter())
    for _ in range(3))
print(f"python {sys.version.split()[0]}: {len(pu)/best/1e6:.2f} M rows/s ({len(pu):,} rows, best of 3)")
