"""spark_clean.py — NYC Taxi cleansing pipeline (Spark)."""
import argparse, csv, glob, os, shutil, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, DoubleType

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
OUT_COLS = list(TARGET) + [
    "pickup_date", "pickup_hour", "pickup_dow", "duration_min", "avg_speed_mph",
    "pu_borough", "pu_zone", "do_borough", "do_zone", "hour_avg_speed", "speed_vs_hour",
]

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
    return (df.withColumn("pickup_date", F.to_date(PU))
              .withColumn("pickup_hour", F.hour(PU))
              .withColumn("pickup_dow", F.expr(f"weekday({PU}) + 1")))   # ISO 1=Mon..7=Sun

# ---------- Stage 2: transform ----------
def trip_features(pickup, dropoff, distance):
    """Custom Python UDF — IDENTICAL in ray_clean.py. Returns (duration_min, avg_speed_mph)."""
    dur_s = (dropoff - pickup).total_seconds()
    return (dur_s / 60.0, distance / (dur_s / 3600.0))

FEAT_SCHEMA = StructType([StructField("duration_min", DoubleType()),
                          StructField("avg_speed_mph", DoubleType())])
trip_features_udf = F.udf(trip_features, FEAT_SCHEMA).asNondeterministic()

def load_zones(spark, path):
    with open(path, newline="") as f:
        rows = [(int(r["LocationID"]), r["Borough"], r["Zone"], r["service_zone"])
                for r in csv.DictReader(f)]
    return spark.createDataFrame(rows, "LocationID int, Borough string, Zone string, service_zone string")

def transform(spark, df, zones, log, ckpt):
    df = (df.withColumn("_f", trip_features_udf(F.col(PU), F.col(DO), F.col("trip_distance")))
            .withColumn("duration_min", F.col("_f.duration_min"))
            .withColumn("avg_speed_mph", F.col("_f.avg_speed_mph"))
            .drop("_f")
            .filter(F.col("avg_speed_mph") <= 100))
    df.write.mode("overwrite").parquet(ckpt)          # checkpoint: UDF runs exactly once
    df = spark.read.parquet(ckpt)
    log("4 UDF + speed<=100 filter", df)

    z = F.broadcast(zones)
    pu = z.select(F.col("LocationID").alias("PULocationID"),
                  F.col("Borough").alias("pu_borough"), F.col("Zone").alias("pu_zone"))
    do = z.select(F.col("LocationID").alias("DOLocationID"),
                  F.col("Borough").alias("do_borough"), F.col("Zone").alias("do_zone"))
    df = df.join(pu, "PULocationID", "left").join(do, "DOLocationID", "left")

    hourly = df.groupBy("pickup_hour").agg(F.avg("avg_speed_mph").alias("hour_avg_speed"))
    df = (df.join(F.broadcast(hourly), "pickup_hour", "left")
            .withColumn("speed_vs_hour", F.col("avg_speed_mph") / F.col("hour_avg_speed")))
    log("5 zones joined x2 + hourly avg joined", df)
    return df.select(OUT_COLS)

# ---------- main ----------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="/data/raw")
    ap.add_argument("--pattern", default="yellow_tripdata_*.parquet")
    ap.add_argument("--zones", default="/data/taxi_zone_lookup.csv")
    ap.add_argument("--output", default="/results/spark_out")
    ap.add_argument("--debug", action="store_true", help="count rows after each step (slow)")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("spark_clean")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def log(name, df):
        if args.debug:
            print(f"[stage] {name}: {df.count():,} rows", flush=True)

    t0 = time.perf_counter()                       # timer starts after session is up
    files = sorted(glob.glob(os.path.join(args.input, args.pattern)))
    df, groups = ingest(spark, files)
    print(f"{len(files)} files in {len(groups)} schema groups")
    log("0 ingested", df)
    df = cleanse(df, log)
    df = transform(spark, df, load_zones(spark, args.zones), log, args.output + "_ckpt")
    df.write.mode("overwrite").parquet(args.output)          # Stage 3: export (the action)
    print(f"[timing] end-to-end wall clock: {time.perf_counter() - t0:.2f} s")
    shutil.rmtree(args.output + "_ckpt", ignore_errors=True)
    spark.stop()
