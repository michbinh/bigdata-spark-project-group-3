#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

to_file_uri() {
  local windows_path
  windows_path="$(cygpath -m "$1")"
  printf 'file:///%s' "$windows_path"
}

PY_FILES="$(
  to_file_uri "$REPO_ROOT/src/rdd_processing.py"
),$(
  to_file_uri "$REPO_ROOT/src/utils.py"
),$(
  to_file_uri "$REPO_ROOT/src/generate_logs.py"
),$(
  to_file_uri "$REPO_ROOT/src/format_benchmark.py"
)"

ENTRY_POINT="$(to_file_uri "$REPO_ROOT/src/main.py")"
INPUT_PATH="$(cygpath -w "$REPO_ROOT/data/raw_logs.txt")"
OUTPUT_PATH="$(cygpath -w "$REPO_ROOT/output/top_countries.csv")"
PYTHON_EXECUTABLE_POSIX="$(command -v python)"
PYTHON_EXECUTABLE="$(cygpath -w "$PYTHON_EXECUTABLE_POSIX")"
PYTHON_SCRIPTS_WINDOWS="$($PYTHON_EXECUTABLE_POSIX -c 'import sysconfig; print(sysconfig.get_path("scripts"))')"
PYTHON_SCRIPTS_DIR="$(cygpath -u "$PYTHON_SCRIPTS_WINDOWS")"
SPARK_SUBMIT="$PYTHON_SCRIPTS_DIR/spark-submit.cmd"

if [[ ! -f "$SPARK_SUBMIT" ]]; then
  echo "Matching spark-submit.cmd not found: $SPARK_SUBMIT" >&2
  exit 1
fi

export PYTHONPATH="$REPO_ROOT/src:${PYTHONPATH:-}"
export PYSPARK_PYTHON="$PYTHON_EXECUTABLE"
export PYSPARK_DRIVER_PYTHON="$PYTHON_EXECUTABLE"
unset SPARK_HOME

TASK="task1"
FORWARDED_ARGS=("$@")

for ((index = 0; index < ${#FORWARDED_ARGS[@]}; index++)); do
  case "${FORWARDED_ARGS[$index]}" in
    --task=task2)
      TASK="task2"
      ;;
    --task)
      if (( index + 1 < ${#FORWARDED_ARGS[@]} )) && [[ "${FORWARDED_ARGS[$((index + 1))]}" == "task2" ]]; then
        TASK="task2"
      fi
      ;;
  esac
done

if [[ "$TASK" == "task1" ]]; then
  APP_ARGS=(
    --input-path "$INPUT_PATH"
    --output-path "$OUTPUT_PATH"
    "${FORWARDED_ARGS[@]}"
  )
else
  APP_ARGS=("${FORWARDED_ARGS[@]}")
fi

"$SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --deploy-mode client \
  --name "LowLevel_FileFormat_Job" \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.sql.shuffle.partitions=10 \
  --py-files "$PY_FILES" \
  "$ENTRY_POINT" \
  "${APP_ARGS[@]}"
