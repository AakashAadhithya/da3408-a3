"""udf_bench.py (Ray): isolate the cost of the trip_features transformation."""
import glob, statistics, time
import pandas as pd, pyarrow as pa, pyarrow.compute as pc, ray

PU, DO, DIST = "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"
REPS, BS = 3, 250_000

def trip_features(pickup, dropoff, distance):            # IDENTICAL to the pipelines
    dur_s = (dropoff - pickup).total_seconds()
    return (dur_s / 60.0, distance / (dur_s / 3600.0))

def epoch_s(col):
    return pc.divide(pc.cast(pc.cast(col, pa.int64()), pa.float64()), 1_000_000.0)

def sums(d, s):
    return pa.table({"d": [pc.sum(d).as_py()], "s": [pc.sum(s).as_py()]})

def v_native(t):                                          # vectorized Arrow compute (C++)
    dur = pc.subtract(epoch_s(t[DO]), epoch_s(t[PU]))
    return sums(pc.divide(dur, 60.0), pc.divide(t[DIST], pc.divide(dur, 3600.0)))

def v_row(t):                                             # row-at-a-time Python, no JVM hop
    f = [trip_features(a, b, c) for a, b, c in
         zip(t[PU].to_pylist(), t[DO].to_pylist(), t[DIST].to_pylist())]
    return sums(pa.array([x[0] for x in f]), pa.array([x[1] for x in f]))

def v_pandas(df: pd.DataFrame) -> pd.DataFrame:           # vectorized pandas on the batch
    dur = (df[DO] - df[PU]).dt.total_seconds()
    return pd.DataFrame({"d": [(dur / 60.0).sum()], "s": [(df[DIST] / (dur / 3600.0)).sum()]})

if __name__ == "__main__":
    ray.init(address="auto")
    ray.data.DataContext.get_current().enable_progress_bars = False
    files = sorted(glob.glob("/data/udf_input/*.parquet"))
    ds = ray.data.read_parquet(files, override_num_blocks=len(files)).materialize()   # cache (not timed)
    print(f"[udf] ray input rows: {ds.count():,}", flush=True)
    ds.map_batches(v_native, batch_format="pyarrow", batch_size=BS).take_all()       # warm-up, not timed
    for name, fn, fmt in [("native", v_native, "pyarrow"), ("row_python", v_row, "pyarrow"),
                          ("pandas", v_pandas, "pandas")]:
        times = []
        for _ in range(REPS):
            t = time.perf_counter()
            rows = ds.map_batches(fn, batch_format=fmt, batch_size=BS).take_all()
            times.append(time.perf_counter() - t)
        d, s = sum(r["d"] for r in rows), sum(r["s"] for r in rows)
        print(f"[udf] ray   {name:11s} median={statistics.median(times):7.2f}s "
              f"runs={[round(x, 2) for x in times]} check_d={d:.6e} check_s={s:.6e}", flush=True)
