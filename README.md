# DA3408 A3 — Spark vs. Ray: The Data Engineering Duel

Identical NYC-Taxi cleansing pipeline in **Apache Spark 3.5.3** and **Ray Data 2.59.0**, each on a
2-worker Docker cluster (2 CPU / 4 GiB per worker), benchmarked on 36 months of Yellow Taxi data (1.9 GB).
Report: [`report/a3_report.pdf`](report/a3_report.pdf) · AI disclosure: [`AI_USAGE.md`](AI_USAGE.md)

| | Spark | Ray |
|---|---|---|
| Median wall clock (3 runs) | **604.65 s** | 659.64 s |
| Output rows (parity PASS) | 106,839,585 | 106,839,585 |
| Row-UDF overhead (20.25M rows) | 25.34 s | 11.79 s |

## Repository layout
| Path | Contents |
|---|---|
| `spark/spark_clean.py` | **Spark pipeline** (required file) |
| `ray/ray_clean.py` | **Ray Data pipeline** (required file) |
| `spark/docker-compose.yml`, `ray/docker-compose.yml` | Clusters: 1 master/head + 2 workers each |
| `spark/Dockerfile` | `da3408/spark-arrow:3.5.3` = stock image + pandas/pyarrow (needed only for `pandas_udf` in the UDF benchmark) |
| `compare.py` | Order-independent parity check (row count, schema, row-multiset hash, float tolerances) |
| `monitor.sh`, `run_bench.sh` | `docker stats` sampler; one monitored benchmark run |
| `spark/udf_bench.py`, `ray/udf_bench.py`, `spark/udf_plans.py`, `pyfactor.py` | UDF deep-dive: native vs row-wise vs vectorized; physical plans; interpreter-only speed |
| `bench/` | Raw evidence: run logs, `docker stats` CSVs, UDF logs, `resource_summary.txt`, plotting/summary scripts |
| `screenshots/` | Spark UI (8080/4040) and Ray Dashboard (8265) evidence |
| `report/` | Report PDF and figures |

## Reproduce
**Data** (not in git): NYC TLC Yellow Taxi Parquet 2022-01 … 2024-12 into `~/data/raw/`, and
`taxi_zone_lookup.csv` into `~/data/` (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Run one cluster at a time; make `results/` writable: `chmod -R a+rwX results`.

```bash
# Spark — full benchmark runs used stock apache/spark:3.5.3-python3; the compose file now points to
# da3408/spark-arrow:3.5.3, so build it first:  docker build -t da3408/spark-arrow:3.5.3 spark/
cd spark && docker compose up -d
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --driver-memory 2g --executor-memory 2500m /workspace/spark/spark_clean.py --output /results/spark_out
docker compose down

# Ray
cd ../ray && docker compose up -d
docker compose exec ray-head python /workspace/ray/ray_clean.py --output /results/ray_out
docker compose down

# Parity, benchmarks, figures
cd .. && python compare.py results/spark_out results/ray_out
./run_bench.sh spark 1        # likewise: ray 1, spark 2, ray 2, ...
python bench/summarize_stats.py && python bench/make_figs.py
```
Dev slice: add `--pattern 'yellow_tripdata_2023-0[12].parquet' --debug` to either pipeline to print row counts per stage.

## Notes
- Ray needed memory-driven redesigns (month-partitioned exact dedupe, bounded batches/files, one file per
  read task) to run on 4 GiB workers — see the *Performance Tuning Note* in the report and `AI_USAGE.md`.
- All numbers come from the logs in `bench/`; nothing was generated synthetically.

## Video walkthrough
Demo and explanation: https://drive.google.com/drive/folders/1dI86TMBp9U-3edFipbzGoFGR6l4mNJOn?usp=share_link
