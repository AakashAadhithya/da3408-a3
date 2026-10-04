"""ray_clean.py — NYC Taxi cleansing pipeline (Ray Data). Same contract as spark_clean.py."""
import argparse, csv, glob, os, shutil, time
from datetime import datetime
from functools import reduce
import pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq
import ray

TARGET = {
    "VendorID": "int", "tpep_pickup_datetime": None, "tpep_dropoff_datetime": None,
    "passenger_count": "bigint", "trip_distance": "double", "RatecodeID": "bigint",
    "store_and_fwd_flag": "string", "PULocationID": "int", "DOLocationID": "int",
    "payment_type": "bigint", "fare_amount": "double", "extra": "double",
    "mta_tax": "double", "tip_amount": "double", "tolls_amount": "double",
    "improvement_surcharge": "double", "total_amount": "double",
    "congestion_surcharge": "double", "airport_fee": "double",
}
PA_TYPES = {"int": pa.int32(), "bigint": pa.int64(), "double": pa.float64(), "string": pa.string()}
PU, DO = "tpep_pickup_datetime", "tpep_dropoff_datetime"
WINDOW = (datetime(2022, 1, 1), datetime(2025, 1, 1))     # [start, end)
OUT_COLS = list(TARGET) + [
    "pickup_date", "pickup_hour", "pickup_dow", "duration_min", "avg_speed_mph",
    "pu_borough", "pu_zone", "do_borough", "do_zone", "hour_avg_speed", "speed_vs_hour",
]
MB = dict(batch_format="pyarrow", batch_size=None)        # one Arrow table per block

def all_of(*conds):
    return reduce(pc.and_, conds)

def between(x, lo, hi):
    return pc.and_(pc.greater_equal(x, lo), pc.less_equal(x, hi))

# ---------- Stage 0: ingest + normalize ----------
def normalize_batch(t):
    by_lower = {c.lower(): c for c in t.column_names}
    cols = []
    for name, typ in TARGET.items():
        col = t[by_lower[name.lower()]]
        cols.append(col if typ is None else pc.cast(col, PA_TYPES[typ]))
    return pa.table(cols, names=list(TARGET))

def ingest(files):
    groups = {}
    for f in files:
        key = tuple((fld.name, str(fld.type)) for fld in pq.read_schema(f))
        groups.setdefault(key, []).append(f)
    parts = [ray.data.read_parquet(fs).map_batches(normalize_batch, **MB) for fs in groups.values()]
    ds = parts[0].union(*parts[1:]) if len(parts) > 1 else parts[0]
    return ds, list(groups.values())

# ---------- Stage 1: cleanse ----------
def epoch_s(col):
    """Same arithmetic as Spark's cast(timestamp -> double): micros / 1e6."""
    return pc.divide(pc.cast(pc.cast(col, pa.int64()), pa.float64()), 1_000_000.0)

def validity_batch(t):
    dur_s = pc.subtract(epoch_s(t[DO]), epoch_s(t[PU]))
    lo = pa.scalar(WINDOW[0], pa.timestamp("us"))
    hi = pa.scalar(WINDOW[1], pa.timestamp("us"))
    mask = all_of(
        pc.greater(t["trip_distance"], 0), pc.greater(t["fare_amount"], 0),
        pc.greater(t["total_amount"], 0),
        between(t["passenger_count"], 1, 6), between(t["RatecodeID"], 1, 6),
        between(dur_s, 60, 3 * 3600),
        pc.greater_equal(t[PU], lo), pc.less(t[PU], hi),
    )
    return t.filter(mask)

def time_features_batch(t):
    t = t.append_column("pickup_date", pc.cast(t[PU], pa.date32()))
    t = t.append_column("pickup_hour", pc.cast(pc.hour(t[PU]), pa.int32()))
    dow = pc.day_of_week(t[PU], count_from_zero=False, week_start=1)       # ISO 1=Mon..7=Sun
    return t.append_column("pickup_dow", pc.cast(dow, pa.int32()))

N_BUCKETS = 64

def add_bucket(t):
    """Bucket = pickup_seconds % N. Exact duplicates share pickup time -> same bucket."""
    sec = pc.divide(pc.cast(t[PU], pa.int64()), 1_000_000)      # int64 // int64
    b = pc.subtract(sec, pc.multiply(pc.divide(sec, N_BUCKETS), N_BUCKETS))
    return t.append_column("_b", b)

def distinct_group(t):
    """Exact distinct rows within one bucket (pyarrow C++ hash group-by)."""
    keys = list(TARGET)
    return t.group_by(keys).aggregate([]).select(keys)

def dedupe(ds):
    return ds.map_batches(add_bucket, **MB).groupby("_b").map_groups(distinct_group, batch_format="pyarrow")

def cleanse(ds, log):
    ds = ds.map_batches(lambda t: t.drop_null(), **MB)
    log("1 null rows dropped", ds)
    ds = ds.map_batches(validity_batch, **MB)
    log("2 validity filters", ds)
    ds = dedupe(ds)                                   # exact full-row dedupe (bucketed)
    log("3 exact duplicates dropped", ds)
    return ds.map_batches(time_features_batch, **MB)

# ---------- Stage 2: transform ----------
def trip_features(pickup, dropoff, distance):
    """Custom Python UDF — IDENTICAL in spark_clean.py. Returns (duration_min, avg_speed_mph)."""
    dur_s = (dropoff - pickup).total_seconds()
    return (dur_s / 60.0, distance / (dur_s / 3600.0))

def udf_batch(t):
    rows = zip(t[PU].to_pylist(), t[DO].to_pylist(), t["trip_distance"].to_pylist())
    feats = [trip_features(a, b, c) for a, b, c in rows]
    t = t.append_column("duration_min", pa.array([f[0] for f in feats], pa.float64()))
    t = t.append_column("avg_speed_mph", pa.array([f[1] for f in feats], pa.float64()))
    return t.filter(pc.less_equal(t["avg_speed_mph"], 100))

def load_zones(path):
    with open(path, newline="") as f:
        return {int(r["LocationID"]): (r["Borough"], r["Zone"]) for r in csv.DictReader(f)}

def lookup(key_col, keys, values):
    """Broadcast hash join on one column: left semantics (no match -> null)."""
    return pc.take(values, pc.index_in(key_col.combine_chunks(), value_set=keys))

def make_zone_join(zones):
    ids = sorted(zones)
    ids_arr = pa.array(ids, pa.int32())
    bor = pa.array([zones[i][0] for i in ids], pa.string())
    zon = pa.array([zones[i][1] for i in ids], pa.string())
    def zone_join_batch(t):
        for p, key in (("pu", "PULocationID"), ("do", "DOLocationID")):
            t = t.append_column(f"{p}_borough", lookup(t[key], ids_arr, bor))
            t = t.append_column(f"{p}_zone", lookup(t[key], ids_arr, zon))
        return t
    return zone_join_batch

def make_hour_join(hourly):
    hrs = sorted(hourly)
    h_arr = pa.array(hrs, pa.int32())
    vals = pa.array([hourly[h] for h in hrs], pa.float64())
    def hour_join_batch(t):
        avg = lookup(t["pickup_hour"], h_arr, vals)
        t = t.append_column("hour_avg_speed", avg)
        t = t.append_column("speed_vs_hour", pc.divide(t["avg_speed_mph"].combine_chunks(), avg))
        return t.select(OUT_COLS)
    return hour_join_batch

def transform(ds, zones, log):
    ds = ds.map_batches(udf_batch, **MB).materialize()                    # == Spark persist()
    log("4 UDF + speed<=100 filter", ds)
    ds = ds.map_batches(make_zone_join(zones), **MB)
    rows = ds.groupby("pickup_hour").mean("avg_speed_mph").take_all()
    hourly = {r["pickup_hour"]: r["mean(avg_speed_mph)"] for r in rows}
    ds = ds.map_batches(make_hour_join(hourly), **MB)
    log("5 zones joined x2 + hourly avg joined", ds)
    return ds

# ---------- main ----------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="/data/raw")
    ap.add_argument("--pattern", default="yellow_tripdata_*.parquet")
    ap.add_argument("--zones", default="/data/taxi_zone_lookup.csv")
    ap.add_argument("--output", default="/results/ray_out")
    ap.add_argument("--debug", action="store_true", help="count rows after each step (slow)")
    args = ap.parse_args()

    ray.init(address="auto")
    ray.data.DataContext.get_current().enable_progress_bars = False

    def log(name, ds):
        if args.debug:
            print(f"[stage] {name}: {ds.count():,} rows", flush=True)

    t0 = time.perf_counter()                       # timer starts after cluster connection
    files = sorted(glob.glob(os.path.join(args.input, args.pattern)))
    ds, groups = ingest(files)
    print(f"{len(files)} files in {len(groups)} schema groups")
    log("0 ingested", ds)
    ds = cleanse(ds, log)
    ds = transform(ds, load_zones(args.zones), log)
    shutil.rmtree(args.output, ignore_errors=True)            # == Spark mode("overwrite")
    ds.write_parquet(args.output)                             # Stage 3: export
    print(f"[timing] end-to-end wall clock: {time.perf_counter() - t0:.2f} s")
