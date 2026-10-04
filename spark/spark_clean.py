"""spark_clean.py — NYC Taxi cleansing pipeline (Spark). Stage 0: ingest + normalize."""
import argparse, glob, os
from pyspark.sql import SparkSession, functions as F

# Target schema: 2023-02+ types, airport_fee lowercase. None = keep as read (timestamps).
TARGET = {
    "VendorID": "int", "tpep_pickup_datetime": None, "tpep_dropoff_datetime": None,
    "passenger_count": "bigint", "trip_distance": "double", "RatecodeID": "bigint",
    "store_and_fwd_flag": "string", "PULocationID": "int", "DOLocationID": "int",
    "payment_type": "bigint", "fare_amount": "double", "extra": "double",
    "mta_tax": "double", "tip_amount": "double", "tolls_amount": "double",
    "improvement_surcharge": "double", "total_amount": "double",
    "congestion_surcharge": "double", "airport_fee": "double",
}

def normalize(df):
    """Rename (case-insensitive) and cast every column to TARGET, in TARGET order."""
    by_lower = {c.lower(): c for c in df.columns}
    cols = []
    for name, typ in TARGET.items():
        src = F.col(f"`{by_lower[name.lower()]}`")
        cols.append((src.cast(typ) if typ else src).alias(name))
    return df.select(cols)

def ingest(spark, files):
    """Group files by physical schema, read each group, normalize, union."""
    groups = {}
    for f in files:
        key = spark.read.parquet(f).schema.simpleString()
        groups.setdefault(key, []).append(f)
    parts = [normalize(spark.read.parquet(*fs)) for fs in groups.values()]
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    return df, list(groups.values())

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="/data/raw")
    ap.add_argument("--pattern", default="yellow_tripdata_*.parquet")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("spark_clean")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    files = sorted(glob.glob(os.path.join(args.input, args.pattern)))
    df, groups = ingest(spark, files)
    for i, fs in enumerate(groups):
        print(f"schema group {i}: {len(fs)} files {[os.path.basename(f)[16:23] for f in fs]}")
    df.printSchema()
    print("rows:", df.count())
    spark.stop()
