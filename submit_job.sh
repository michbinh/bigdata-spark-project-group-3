#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

export PYTHONPATH="$REPO_ROOT/src:${PYTHONPATH:-}"

spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --deploy-mode client \
  --name "LowLevel_FileFormat_Job" \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.sql.shuffle.partitions=10 \
  --py-files "$REPO_ROOT/src/rdd_processing.py,$REPO_ROOT/src/utils.py,$REPO_ROOT/src/generate_logs.py" \
  "$REPO_ROOT/src/main.py" \
  --input-path "$REPO_ROOT/data/raw_logs.txt" \
  --output-path "$REPO_ROOT/output/top_countries.csv"
