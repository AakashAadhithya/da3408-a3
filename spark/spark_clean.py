"""spark_clean.py — NYC Taxi cleansing pipeline (Spark)."""
import argparse, glob, os
from pyspark.sql import SparkSession, functions as F

TARGET = {
    "VendorID": "int", "tpep_pickup_datetime": None, "tpep_dropoff_datetime": None,
    "passenger_count": "bigint", "trip_distance": "double", "RatecodeID": "bigint",
    "store_and_fwd_flag": "string", "PULocationID": "int", "DOLocationID": "int",
    "payment_type": "bigint", "fare_amount": "double", "extra": "double",
    "mta_tax": "double", "tip_amount": "double", "tolls_amount": "double",
    "improvement_surcharge": "double", "total_amount": "double",
    "congestion_surcharge": "double", "airport_fee": "double",
}
PU, DO = "tpep_pickup_datetime", "tpep_dropoff_datetime"
WINDOW = ("2022-01-01 00:00:00", "2025-01-01 00:00:00")   # [start, end)

# ---------- Stage 0: ingest + normalize ----------
def normalize(df):
    by_lower = {c.lower(): c for c in df.columns}
    cols = []
    for name, typ in TARGET.items():
        src = F.col(f"`{by_lower[name.lower()]}`")
        cols.append((src.cast(typ) if typ else src).alias(name))
    return df.select(cols)

def ingest(spark, files):
    groups = {}
    for f in files:
        groups.setdefault(spark.read.parquet(f).schema.simpleString(), []).append(f)
    parts = [normalize(spark.read.parquet(*fs)) for fs in groups.values()]
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    return df, list(groups.values())

# ---------- Stage 1: cleanse ----------
def epoch_s(c):
    """NTZ wall-clock -> interpreted in session TZ (UTC) -> epoch seconds (double, keeps fractions)."""
    return F.col(c).cast("timestamp").cast("double")

def cleanse(df, log):
    df = df.dropna()
    log("1 null rows dropped", df)

    dur_s = epoch_s(DO) - epoch_s(PU)
    df = df.filter(
        (F.col("trip_distance") > 0) & (F.col("fare_amount") > 0) & (F.col("total_amount") > 0)
        & F.col("passenger_count").between(1, 6)
        & F.col("RatecodeID").between(1, 6)
        & dur_s.between(60, 3 * 3600)
        & (F.col(PU) >= F.expr(f"TIMESTAMP_NTZ '{WINDOW[0]}'"))
        & (F.col(PU) <  F.expr(f"TIMESTAMP_NTZ '{WINDOW[1]}'"))
    )
    log("2 validity filters", df)

    df = df.dropDuplicates()
    log("3 exact duplicates dropped", df)

    df = (df.withColumn("pickup_date", F.to_date(PU))
            .withColumn("pickup_hour", F.hour(PU))
            .withColumn("pickup_dow", F.expr(f"weekday({PU}) + 1")))   # ISO: 1=Mon .. 7=Sun
    return df

# ---------- main ----------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="/data/raw")
    ap.add_argument("--pattern", default="yellow_tripdata_*.parquet")
    ap.add_argument("--debug", action="store_true", help="count rows after each step (slow)")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("spark_clean")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def log(name, df):
        if args.debug:
            print(f"[stage] {name}: {df.count():,} rows", flush=True)

    files = sorted(glob.glob(os.path.join(args.input, args.pattern)))
    df, groups = ingest(spark, files)
    print(f"{len(files)} files in {len(groups)} schema groups")
    log("0 ingested", df)

    df = cleanse(df, log)
    if args.debug:
        df.select(PU, "pickup_date", "pickup_hour", "pickup_dow").show(5, truncate=False)
    spark.stop()
