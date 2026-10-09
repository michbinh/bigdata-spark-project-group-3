"""
Task 2 - File Format Benchmarking
=================================

Benchmarks the 2015 Flight Delays dataset across CSV, JSON, Parquet, and ORC.

Workflow:
1. Load and canonicalize the Airline dataset.
2. Preserve the full 31-column schema and existing null values.
3. Persist the canonical DataFrame with DISK_ONLY and materialize it once.
4. Run one warm-up round.
5. Run three official write rounds in the fixed order:
   CSV -> JSON -> Parquet -> ORC.
6. Record Write Execution Time and Disk Storage Size.
7. Report the median Write Execution Time for each format.
8. Save detailed and summary benchmark results as CSV files.

The final format directories remain available for ID4's downstream read and
partition experiments.
"""

import argparse
import csv
import os
import statistics
import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.storagelevel import StorageLevel

# ---------------------------------------------------------------------
# Benchmark constants
# ---------------------------------------------------------------------

FORMATS = ["csv", "json", "parquet", "orc"]
WARMUP_RUNS = 1
OFFICIAL_RUNS = 3
SUMMARY_STATISTIC = "median"

EXPECTED_ROW_COUNT = 5_819_079
EXPECTED_COLUMN_COUNT = 31
EXPECTED_YEAR = 2015
EXPECTED_MONTH_MIN = 1
EXPECTED_MONTH_MAX = 12
EXPECTED_MONTH_COUNT = 12
EXPECTED_CARRIER_COUNT = 14
EXPECTED_NULL_ARRDELAY = 105_071

RESULT_COLUMNS = [
    "format", "compression", "row_count", "input_partitions", "run_number",
    "write_time_sec", "part_file_count", "size_bytes", "size_mb", "output_path",
]

SUMMARY_COLUMNS = [
    "format", "compression", "official_runs", "median_write_time_sec",
    "part_file_count", "size_bytes", "size_mb", "output_path",
]

# ---------------------------------------------------------------------
# Spark and canonical dataset preparation
# ---------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    """Create or reuse the SparkSession requested by the benchmark.
    Returns:
        A SparkSession with SparkContext logging set to WARN.
    """
    spark = (
        SparkSession.builder
        .appName("AirlineFileFormatBenchmark")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
def load_canonical_dataframe(spark: SparkSession, input_path: str) -> DataFrame:
    """Read flights.csv and apply the four canonical column renames.
    Args:
        spark: Session used to read the source CSV.
        input_path: Path to the source flights.csv file.
    Returns:
        A DataFrame with inferred types and the four renamed columns.
        This function does not filter rows or impute null values.
    Notes:
        Schema inference remains part of input preparation. In main(), it
        occurs before the timed writes, as does initial materialization.
    """
    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    canonical_df = (
        raw_df
        .withColumnRenamed("YEAR", "Year")
        .withColumnRenamed("MONTH", "Month")
        .withColumnRenamed("AIRLINE", "Carrier")
        .withColumnRenamed("ARRIVAL_DELAY", "ArrDelay")
    )

    return canonical_df
def validate_and_materialize(df: DataFrame) -> tuple[DataFrame, int]:
    """Materialize the input and check the recorded dataset constraints.
    Args:
        df: Canonical DataFrame containing the columns used by the checks.
    Returns:
        The persisted DataFrame and its materialized row count.
    Raises:
        AssertionError: A checked constraint fails when assertions are enabled.
    Notes:
        DISK_ONLY keeps input preparation outside the write timer, but each write still accesses persisted input. OS file cache state is not
        controlled, so this is not an encoding-only measurement. These checks cover selected counts, ranges, and the ArrDelay type;
        they do not establish full schema conformance or full row equality.
    """
    benchmark_df = df.persist(StorageLevel.DISK_ONLY)
    # Force initial input evaluation before any write measurement starts.
    row_count = benchmark_df.count()

    validation = (
        benchmark_df
        .agg(
            F.min("Year").alias("min_year"),
            F.max("Year").alias("max_year"),
            F.countDistinct("Year").alias("distinct_years"),
            F.min("Month").alias("min_month"),
            F.max("Month").alias("max_month"),
            F.countDistinct("Month").alias("distinct_months"),
            F.countDistinct("Carrier").alias("distinct_carriers"),
            F.sum(F.col("ArrDelay").isNull().cast("int")).alias("null_arrdelay"),
        )
        .first()
    )

    assert row_count == EXPECTED_ROW_COUNT
    assert len(benchmark_df.columns) == EXPECTED_COLUMN_COUNT
    assert validation["min_year"] == EXPECTED_YEAR
    assert validation["max_year"] == EXPECTED_YEAR
    assert validation["distinct_years"] == 1
    assert validation["min_month"] == EXPECTED_MONTH_MIN
    assert validation["max_month"] == EXPECTED_MONTH_MAX
    assert validation["distinct_months"] == EXPECTED_MONTH_COUNT
    assert validation["distinct_carriers"] == EXPECTED_CARRIER_COUNT
    assert validation["null_arrdelay"] == EXPECTED_NULL_ARRDELAY
    assert dict(benchmark_df.dtypes)["ArrDelay"] in ["int", "bigint", "float", "double"]

    print("=" * 70)
    print("CANONICAL BENCHMARK INPUT")
    print("=" * 70)
    print(f"Rows              : {row_count:,}")
    print(f"Columns           : {len(benchmark_df.columns)}")
    print(f"Year range        : {validation['min_year']} - {validation['max_year']}")
    print(f"Month range       : {validation['min_month']} - {validation['max_month']}")
    print(f"Distinct carriers : {validation['distinct_carriers']}")
    print(f"Null ArrDelay     : {validation['null_arrdelay']:,}")
    print(f"Storage level     : {benchmark_df.storageLevel}")
    print("\nCanonical benchmark input: READY")

    return benchmark_df, row_count

# ---------------------------------------------------------------------
# Benchmark configuration and helper functions
# ---------------------------------------------------------------------
def get_compression_policy(spark: SparkSession) -> dict[str, str]:
    """Describe the compression settings used by the current session.
    Args:
        spark: Session whose Parquet and ORC codec settings are recorded.
    Returns:
        Policy labels by format. CSV and JSON have no explicit codec option;
        Parquet and ORC values come from the active Spark configuration.
    Notes:
        This function records settings without changing them. The benchmark
        compares these format-and-codec combinations, not codec-free formats.
    """
    return {
        "csv": "default / no explicit compression option",
        "json": "default / no explicit compression option",
        "parquet": spark.conf.get("spark.sql.parquet.compression.codec"),
        "orc": spark.conf.get("spark.sql.orc.compression.codec"),
    }
def get_write_options() -> dict[str, dict[str, str]]:
    """Return the per-format writer options controlled by the benchmark.
    Returns:
        A format-to-options mapping. Only the CSV header option is set;
        compression is not overridden through these writer options.
    """
    return {
        "csv": {"header": "true"},
        "json": {},
        "parquet": {},
        "orc": {},
    }
def timed_write(df: DataFrame, fmt: str, output_path: str, write_options: dict[str, dict[str, str]]) -> float:
    """Write one format in overwrite mode and measure elapsed seconds.
    Args:
        df: Input DataFrame for the write action.
        fmt: Spark file format and key in write_options.
        output_path: Destination directory for this format.
        write_options: Writer options grouped by format.
    Returns:
        Elapsed perf_counter time around writer.format(fmt).save(output_path).
    Notes:
        Writer option setup is outside the timer. The measured action includes input access and Spark's write work; it is not isolated encoding time.
        Output size measurement and caller-side logging happen separately.
    """
    writer = df.write.mode("overwrite")

    for key, value in write_options[fmt].items():
        writer = writer.option(key, value)

    start_time = time.perf_counter()
    writer.format(fmt).save(output_path)
    end_time = time.perf_counter()

    return end_time - start_time
def get_output_size(output_path: str) -> dict[str, int | float]:
    """Count Spark part-* files and sum their file lengths recursively.
    Args:
        output_path: Local filesystem directory to scan with os.walk.
    Returns:
        part_file_count, size_bytes, and the legacy size_mb field. The latter is calculated in MiB using 1024**2 bytes per unit.
    Notes:
        Files such as _SUCCESS and .*.crc are excluded by the part- prefix rule. Metadata embedded inside a part file remains included.
        File lengths measure logical bytes, not allocated filesystem blocks. This helper does not validate path existence or dataset contents.
    """
    total_bytes = 0
    part_file_count = 0

    for root, _, files in os.walk(output_path):
        for filename in files:
            if filename.startswith("part-"):
                file_path = os.path.join(root, filename)
                total_bytes += os.path.getsize(file_path)
                part_file_count += 1

    return {
        "part_file_count": part_file_count,
        "size_bytes": total_bytes,
        "size_mb": total_bytes / (1024 ** 2),
    }

# ---------------------------------------------------------------------
# Main write benchmark function
# ---------------------------------------------------------------------
def run_write_benchmark(df: DataFrame, output_path: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    """Run the four-format write protocol on prepared input.
    Args:
        df: Already validated and materialized canonical Spark DataFrame.
            The caller is responsible for persistence and eventual cleanup.
        output_path: Root directory for CSV, JSON, Parquet, and ORC outputs.
    Returns:
        A tuple containing official run records, per-format summary records,
        and the format-to-output-directory mapping.
    Notes:
        The protocol runs one warm-up round followed by OFFICIAL_RUNS rounds
        in FORMATS order. Each write overwrites its format directory.
        Warm-up timings are discarded. Only write times are aggregated using
        the median; summary sizes and file counts come from the final run.
        Record-count and positive-size checks use assertions. Size consistency
        across runs is printed for inspection, not enforced by an assertion.
    """
    spark = df.sparkSession

    compression_policy = get_compression_policy(spark)
    write_options = get_write_options()

    output_paths = {fmt: os.path.join(output_path, fmt) for fmt in FORMATS}
    os.makedirs(output_path, exist_ok=True)

    # Input metadata collection is outside all timed write calls.
    input_partitions = df.rdd.getNumPartitions()
    row_count = df.count()

    print("\n" + "=" * 70)
    print("BENCHMARK PROTOCOL")
    print("=" * 70)
    print(f"Formats           : {' -> '.join(fmt.upper() for fmt in FORMATS)}")
    print(f"Warm-up rounds    : {WARMUP_RUNS}")
    print(f"Official rounds   : {OFFICIAL_RUNS}")
    print(f"Input rows        : {row_count:,}")
    print(f"Input partitions  : {input_partitions}")

    print("\nCompression policy:")
    for fmt in FORMATS:
        print(f"{fmt.upper():8} -> {compression_policy[fmt]}")

    # Run one complete warm-up round. Warm-up timings are discarded.
    print("\n" + "=" * 70)
    print("WARM-UP ROUND")
    print("=" * 70)

    for fmt in FORMATS:
        print(f"Writing {fmt.upper()}...")
        timed_write(df, fmt, output_paths[fmt], write_options)
        print(f"{fmt.upper()} warm-up completed.")

    # Run three official rounds in a fixed format order.
    benchmark_results = []

    for run_number in range(1, OFFICIAL_RUNS + 1):
        print("\n" + "=" * 70)
        print(f"OFFICIAL RUN {run_number}/{OFFICIAL_RUNS}")
        print("=" * 70)

        for fmt in FORMATS:
            output_dir = output_paths[fmt]
            write_time = timed_write(df, fmt, output_dir, write_options)
            # Scan output files only after the write timer has stopped.
            size_info = get_output_size(output_dir)

            result = {
                "format": fmt,
                "compression": compression_policy[fmt],
                "row_count": row_count,
                "input_partitions": input_partitions,
                "run_number": run_number,
                "write_time_sec": write_time,
                "part_file_count": size_info["part_file_count"],
                "size_bytes": size_info["size_bytes"],
                "size_mb": size_info["size_mb"],
                "output_path": output_dir,
            }

            benchmark_results.append(result)

            print(
                f"{fmt.upper():8} | "
                f"Time: {write_time:8.3f} s | "
                f"Size: {size_info['size_mb']:10.2f} MB | "
                f"Files: {size_info['part_file_count']}"
            )

    # Validate all 12 official measurements before aggregation.
    assert len(benchmark_results) == len(FORMATS) * OFFICIAL_RUNS

    for fmt in FORMATS:
        format_results = [result for result in benchmark_results if result["format"] == fmt]

        assert len(format_results) == OFFICIAL_RUNS
        assert all(r["row_count"] == row_count for r in format_results)
        assert all(r["input_partitions"] == input_partitions for r in format_results)
        assert all(r["part_file_count"] > 0 for r in format_results)
        assert all(r["size_bytes"] > 0 for r in format_results)

        sizes = [r["size_bytes"] for r in format_results]

        print(
            f"{fmt.upper():8} | "
            f"Runs: {len(format_results)} | "
            f"Size consistent: {len(set(sizes)) == 1}"
        )

    # Calculate the final median Write Execution Time for each format.
    benchmark_summary = []

    for fmt in FORMATS:
        format_results = [result for result in benchmark_results if result["format"] == fmt]
        write_times = [result["write_time_sec"] for result in format_results]
        # The final run describes the files left in the overwrite directory.
        final_run = max(format_results, key=lambda result: result["run_number"])

        benchmark_summary.append({
            "format": fmt,
            "compression": compression_policy[fmt],
            "official_runs": OFFICIAL_RUNS,
            "median_write_time_sec": statistics.median(write_times),
            "part_file_count": final_run["part_file_count"],
            "size_bytes": final_run["size_bytes"],
            "size_mb": final_run["size_mb"],
            "output_path": final_run["output_path"],
        })

    return benchmark_results, benchmark_summary, output_paths

# ---------------------------------------------------------------------
# Result export
# ---------------------------------------------------------------------
def save_results(
    benchmark_results: list[dict[str, Any]],
    benchmark_summary: list[dict[str, Any]], output_root: str,
) -> tuple[str, str]:
    """Write official run records and summary records to the existing CSVs.
    Args:
        benchmark_results: Official run records matching RESULT_COLUMNS.
        benchmark_summary: Per-format summary records matching SUMMARY_COLUMNS.
        output_root: Existing destination directory created by the benchmark.
    Returns:
        Paths to write_benchmark_runs.csv and write_benchmark_summary.csv.
    Notes:
        Existing result CSVs are overwritten. This export occurs after the
        benchmark measurements and preserves the existing CSV column names.
    """
    detailed_results_path = os.path.join(output_root, "write_benchmark_runs.csv")

    with open(detailed_results_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=RESULT_COLUMNS)
        writer.writeheader()
        writer.writerows(benchmark_results)

    summary_path = os.path.join(output_root, "write_benchmark_summary.csv")

    with open(summary_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(benchmark_summary)

    return detailed_results_path, summary_path
def print_summary(benchmark_summary: list[dict[str, Any]]) -> None:
    """Print the supplied summary without recomputing benchmark statistics.
    Args:
        benchmark_summary: Per-format records returned by run_write_benchmark.
    Notes:
        The existing MB display label is retained; size_mb values are MiB.
    """
    print("\n" + "=" * 70)
    print("FINAL WRITE BENCHMARK SUMMARY")
    print("=" * 70)

    for result in benchmark_summary:
        print(
            f"{result['format'].upper():8} | "
            f"Compression: {result['compression']:<38} | "
            f"Median Write: {result['median_write_time_sec']:8.3f} s | "
            f"Size: {result['size_mb']:10.2f} MB | "
            f"Files: {result['part_file_count']}"
        )

# ---------------------------------------------------------------------
# Command-line entry point
# ---------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse command-line paths while keeping the project defaults.
    Returns:
        Parsed input_path and output_path arguments.
    """
    parser = argparse.ArgumentParser(description="Benchmark Airline data across CSV, JSON, Parquet, and ORC.")
    parser.add_argument(
        "--input-path",
        default=r"C:\BigDataProject\data\raw\airline\flights.csv",
        help="Path to the source flights.csv file.",
    )
    parser.add_argument(
        "--output-path",
        default=r"C:\BigDataProject\output\format_benchmark",
        help="Root directory for benchmark outputs.",
    )
    return parser.parse_args()
def main() -> None:
    """Execute input preparation, timed writes, and result export from the CLI.
    Raises:
        FileNotFoundError: The requested input path is not a local file.
    Notes:
        The existing finally block releases the assigned benchmark DataFrame
        and stops the SparkSession when leaving the benchmark try block.
    """
    args = parse_args()

    if not os.path.isfile(args.input_path):
        raise FileNotFoundError(f"Input dataset not found: {args.input_path}")

    spark = create_spark_session()
    benchmark_df = None

    try:
        canonical_df = load_canonical_dataframe(spark, args.input_path)
        benchmark_df, _ = validate_and_materialize(canonical_df)
        benchmark_results, benchmark_summary, output_paths = (run_write_benchmark(benchmark_df, args.output_path))
        detailed_results_path, summary_path = save_results(benchmark_results, benchmark_summary, args.output_path)

        print_summary(benchmark_summary)

        print("\n" + "=" * 70)
        print("WRITE BENCHMARK COMPLETED")
        print("=" * 70)
        print(f"Detailed results : {detailed_results_path}")
        print(f"Final summary    : {summary_path}")

        print("\nCanonical outputs for ID4:")
        for fmt in FORMATS:
            print(f"{fmt.upper():8} -> {output_paths[fmt]}")

    finally:
        if benchmark_df is not None:
            benchmark_df.unpersist()

        spark.stop()
        print("\nSparkSession stopped.")

if __name__ == "__main__":
    main()
