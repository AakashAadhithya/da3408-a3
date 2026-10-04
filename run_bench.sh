#!/usr/bin/env bash
# usage: ./run_bench.sh ray|spark RUN_NUMBER
set -u
fw=$1; n=$2; root=~/da3408-a3
cd "$root/$fw" && docker compose up -d && sleep 15
"$root/monitor.sh" "$root/bench/${fw}_full_run${n}.csv" & MON=$!
if [ "$fw" = ray ]; then
  docker compose exec ray-head python /workspace/ray/ray_clean.py --output /results/ray_out 2>&1
else
  docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --driver-memory 2g --executor-memory 2500m /workspace/spark/spark_clean.py --output /results/spark_out 2>&1
fi | tee "$root/bench/${fw}_full_run${n}.log" \
   | grep --line-buffered -E "schema groups|months|\[timing\]|failed due to oom|Exception" | cut -c1-140
kill $MON
echo ">>> Run done. Take after-run screenshots now, then press Enter to shut the cluster down."
read -r
docker compose down
