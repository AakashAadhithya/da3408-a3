"""udf_bench.py (Spark): isolate the cost of the trip_features transformation."""
import statistics, time
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, DoubleType

PU, DO, DIST = "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"
REPS = 3

def trip_features(pickup, dropoff, distance):            # IDENTICAL to the pipelines
    dur_s = (dropoff - pickup).total_seconds()
    return (dur_s / 60.0, distance / (dur_s / 3600.0))

FEAT = StructType([StructField("duration_min", DoubleType()), StructField("avg_speed_mph", DoubleType())])
row_udf = F.udf(trip_features, FEAT).asNondeterministic()

def _pandas_feats(pu: pd.Series, do: pd.Series, dist: pd.Series) -> pd.DataFrame:
    dur_s = (do - pu).dt.total_seconds()
    return pd.DataFrame({"duration_min": dur_s / 60.0, "avg_speed_mph": dist / (dur_s / 3600.0)})
pandas_udf = F.pandas_udf(_pandas_feats, FEAT).asNondeterministic()

def v_native(df):                                         # JVM only, no Python
    dur_s = F.col(DO).cast("timestamp").cast("double") - F.col(PU).cast("timestamp").cast("double")
    return df.select((dur_s / 60.0).alias("d"), (F.col(DIST) / (dur_s / 3600.0)).alias("s"))

def v_row(df):                                            # row-at-a-time, pickled JVM<->Python
    f = row_udf(F.col(PU), F.col(DO), F.col(DIST))
    return df.select(f.alias("f")).select(F.col("f.duration_min").alias("d"), F.col("f.avg_speed_mph").alias("s"))

def v_pandas(df):                                         # vectorized, Arrow batches JVM<->Python
    f = pandas_udf(F.col(PU).cast("timestamp"), F.col(DO).cast("timestamp"), F.col(DIST))
    return df.select(f.alias("f")).select(F.col("f.duration_min").alias("d"), F.col("f.avg_speed_mph").alias("s"))

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("udf_bench")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet("/data/udf_input").cache()
    n = df.count()                                        # fills the cache (not timed)
    print(f"[udf] spark input rows: {n:,}", flush=True)
    v_native(df).agg(F.sum("d")).collect()                # warm-up (JIT, codegen), not timed
    for name, build in [("native", v_native), ("row_udf", v_row), ("pandas_udf", v_pandas)]:
        times = []
        for _ in range(REPS):
            t = time.perf_counter()
            d, s = build(df).agg(F.sum("d"), F.sum("s")).collect()[0]
            times.append(time.perf_counter() - t)
        print(f"[udf] spark {name:11s} median={statistics.median(times):7.2f}s "
              f"runs={[round(x, 2) for x in times]} check_d={d:.6e} check_s={s:.6e}", flush=True)
    spark.stop()
