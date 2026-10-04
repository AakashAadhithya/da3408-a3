"""Show the physical plans of the UDF variants (BatchEvalPython vs ArrowEvalPython); keep UI up for screenshots."""
import time
from pyspark.sql import SparkSession, functions as F
from udf_bench import v_native, v_row, v_pandas

spark = (SparkSession.builder.appName("udf_plans")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
df = spark.read.parquet("/data/udf_input").limit(1_000_000)
for name, build in [("native", v_native), ("row_udf", v_row), ("pandas_udf", v_pandas)]:
    q = build(df).agg(F.sum("d"), F.sum("s"))
    print(f"\n===== {name} =====", flush=True)
    q.explain(mode="formatted")
    q.collect()                       # run it, so the SQL tab shows the executed plan
print("\n>>> UI up for 180 s: http://192.168.64.2:4040 -> SQL / DataFrame", flush=True)
time.sleep(180)
spark.stop()
