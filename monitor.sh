#!/usr/bin/env bash
# usage: ./monitor.sh out.csv   — samples docker stats ~1/s until killed
out=${1:-stats.csv}
echo "ts,container,cpu_pct,mem_usage" > "$out"
while true; do
  docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" |
    while read -r line; do echo "$(date +%s),$line" >> "$out"; done
done
