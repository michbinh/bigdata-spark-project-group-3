from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def resolve_input_path(path_value: Optional[str] = None) -> Path:
    root = repo_root()
    if path_value is None:
        return root / "data" / "raw_logs.txt"
    candidate = Path(path_value)
    if candidate.is_absolute():
        return candidate
    return root / candidate


def create_spark_session(app_name: str = "LowLevel_FileFormat_Job") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description="Run the Spark RDD log-processing pipeline.")
    parser.add_argument("--input-path", default="data/raw_logs.txt", help="Input log file to read.")
    parser.add_argument("--output-path", default=None, help="Optional output file for the top countries summary.")
    parser.add_argument("--top-n", type=int, default=10, help="Number of top countries to display.")
    parser.add_argument("--app-name", default="LowLevel_FileFormat_Job", help="Application name passed to SparkSession.")
    return parser.parse_args(argv)
