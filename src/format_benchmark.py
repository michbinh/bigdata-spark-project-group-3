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
from pathlib import Path
import shutil
import statistics
import tempfile
import time
import urllib.request
import zipfile

from pyspark.sql import SparkSession, functions as F
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
    "format",
    "compression",
    "row_count",
    "input_partitions",
    "run_number",
    "write_time_sec",
    "part_file_count",
    "size_bytes",
    "size_mb",
    "output_path",
]

SUMMARY_COLUMNS = [
    "format",
    "compression",
    "official_runs",
    "median_write_time_sec",
    "part_file_count",
    "size_bytes",
    "size_mb",
    "output_path",
]

READ_RESULT_COLUMNS = [
    "format",
    "run_number",
    "read_time_sec",
    "result_rows",
]

READ_SUMMARY_COLUMNS = [
    "format",
    "official_runs",
    "median_read_time_sec",
]

PARTITION_SUMMARY_COLUMNS = [
    "experiment",
    "output_path",
    "write_time_sec",
    "part_file_count",
    "size_bytes",
    "size_mb",
    "avg_part_size_mb",
]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "airline" / "flights.csv"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "output" / "format_benchmark"
DATASET_URL = "https://www.kaggle.com/api/v1/datasets/download/usdot/flight-delays"

# ---------------------------------------------------------------------
# Dataset download
# ---------------------------------------------------------------------

def download_dataset(input_path):
    """Download and extract only flights.csv when it is not already present."""
    input_path = Path(input_path)

    if input_path.is_file():
        print(f"Input dataset already exists: {input_path}")
        return

    input_path.parent.mkdir(parents=True, exist_ok=True)

    archive_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as archive:
            archive_path = Path(archive.name)
            with urllib.request.urlopen(DATASET_URL) as response:
                shutil.copyfileobj(response, archive)

        with zipfile.ZipFile(archive_path) as zip_file:
            flights_member = next(
                (
                    member
                    for member in zip_file.infolist()
                    if not member.is_dir()
                    and Path(member.filename).name == "flights.csv"
                ),
                None,
            )

            if flights_member is None:
                raise FileNotFoundError(
                    "Downloaded dataset archive does not contain flights.csv."
                )

            with zip_file.open(flights_member) as source, input_path.open("wb") as target:
                shutil.copyfileobj(source, target)

        print(f"Downloaded input dataset: {input_path}")
    finally:
        if archive_path is not None:
            archive_path.unlink(missing_ok=True)

# ---------------------------------------------------------------------
# Spark and canonical dataset preparation
# ---------------------------------------------------------------------

def create_spark_session():
    """Create the SparkSession used for the complete benchmark."""
    spark = (
        SparkSession.builder
        .appName("AirlineFileFormatBenchmark")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_canonical_dataframe(spark, input_path):
    """Read flights.csv and apply the four canonical column renames."""
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

def validate_and_materialize(df):
    """
    Persist the canonical input with DISK_ONLY, materialize it once,
    and verify the audited dataset constraints.
    """
    benchmark_df = df.persist(StorageLevel.DISK_ONLY)
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
            F.sum(
                F.col("ArrDelay").isNull().cast("int")
            ).alias("null_arrdelay"),
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
    assert dict(benchmark_df.dtypes)["ArrDelay"] in [
        "int",
        "bigint",
        "float",
        "double",
    ]

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

def get_compression_policy(spark):
    """Record the active compression configuration used by Spark."""
    return {
        "csv": "default / no explicit compression option",
        "json": "default / no explicit compression option",
        "parquet": spark.conf.get("spark.sql.parquet.compression.codec"),
        "orc": spark.conf.get("spark.sql.orc.compression.codec"),
    }

def get_write_options():
    """Return only the write options intentionally controlled by the benchmark."""
    return {
        "csv": {"header": "true"},
        "json": {},
        "parquet": {},
        "orc": {},
    }

def timed_write(df, fmt, output_path, write_options):
    """Write one format once and return the elapsed Write Execution Time."""
    writer = df.write.mode("overwrite")

    for key, value in write_options[fmt].items():
        writer = writer.option(key, value)

    start_time = time.perf_counter()
    writer.format(fmt).save(output_path)
    end_time = time.perf_counter()

    return end_time - start_time

def get_output_size(output_path):
    """Return the count and total size of Spark part-* data files."""
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

def write_csv_table(path, rows, fieldnames):
    """Write a list of dictionaries as a CSV table."""
    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)

# ---------------------------------------------------------------------
# Main write benchmark function
# ---------------------------------------------------------------------

def run_write_benchmark(df, output_path):
    """
    Run the complete four-format write benchmark.

    Parameters
    ----------
    df:
        The already validated and materialized canonical Spark DataFrame.
    output_path:
        Root directory for CSV, JSON, Parquet, and ORC outputs.

    Returns
    -------
    tuple
        (benchmark_results, benchmark_summary, output_paths)
    """
    spark = df.sparkSession

    compression_policy = get_compression_policy(spark)
    write_options = get_write_options()

    output_paths = {
        fmt: os.path.join(output_path, fmt)
        for fmt in FORMATS
    }
    os.makedirs(output_path, exist_ok=True)

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
        timed_write(
            df,
            fmt,
            output_paths[fmt],
            write_options,
        )
        print(f"{fmt.upper()} warm-up completed.")

    # Run three official rounds in a fixed format order.
    benchmark_results = []

    for run_number in range(1, OFFICIAL_RUNS + 1):
        print("\n" + "=" * 70)
        print(f"OFFICIAL RUN {run_number}/{OFFICIAL_RUNS}")
        print("=" * 70)

        for fmt in FORMATS:
            output_dir = output_paths[fmt]

            write_time = timed_write(
                df,
                fmt,
                output_dir,
                write_options,
            )

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
        format_results = [
            result
            for result in benchmark_results
            if result["format"] == fmt
        ]

        assert len(format_results) == OFFICIAL_RUNS
        assert all(r["row_count"] == row_count for r in format_results)
        assert all(
            r["input_partitions"] == input_partitions
            for r in format_results
        )
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
        format_results = [
            result
            for result in benchmark_results
            if result["format"] == fmt
        ]

        write_times = [
            result["write_time_sec"]
            for result in format_results
        ]

        final_run = max(
            format_results,
            key=lambda result: result["run_number"],
        )

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
# ID4 read query and partition benchmark
# ---------------------------------------------------------------------

def _read_benchmark_dataframe(spark, fmt, input_path, canonical_schema):
    """Read one ID3 format output with a consistent benchmark schema."""
    format_path = os.path.join(input_path, fmt)

    if fmt == "csv":
        return (
            spark.read
            .option("header", True)
            .schema(canonical_schema)
            .csv(format_path)
        )
    if fmt == "json":
        return (
            spark.read
            .schema(canonical_schema)
            .json(format_path)
        )
    if fmt == "parquet":
        return spark.read.parquet(format_path)
    if fmt == "orc":
        return spark.read.orc(format_path)

    raise ValueError(f"Unsupported format: {fmt}")


def _run_fixed_read_query(df):
    """Run the fixed ID4 aggregate query and trigger execution."""
    return (
        df
        .groupBy("Carrier")
        .agg(F.avg("ArrDelay").alias("avg_arr_delay"))
        .collect()
    )


def _partition_write_result(experiment, output_path, write_time_sec):
    """Build one partition experiment result row from actual disk files."""
    size_info = get_output_size(output_path)
    part_file_count = size_info["part_file_count"]
    avg_part_size_mb = (
        size_info["size_mb"] / part_file_count
        if part_file_count
        else 0
    )

    return {
        "experiment": experiment,
        "output_path": output_path,
        "write_time_sec": write_time_sec,
        "part_file_count": part_file_count,
        "size_bytes": size_info["size_bytes"],
        "size_mb": size_info["size_mb"],
        "avg_part_size_mb": avg_part_size_mb,
    }


def run_read_and_partition_benchmark(spark, input_path):
    """
    Run ID4's read query benchmark and partition-control experiments.

    ``input_path`` is the ID3 format benchmark root containing the four output
    directories: csv, json, parquet and orc. Results are written to a sibling
    ``partition_test`` directory so ID3's canonical write outputs stay intact.
    """
    for fmt in FORMATS:
        format_path = os.path.join(input_path, fmt)
        if not os.path.isdir(format_path):
            raise FileNotFoundError(
                f"Missing ID3 {fmt.upper()} output directory: {format_path}"
            )

    output_root = os.path.join(
        os.path.dirname(os.path.abspath(input_path)),
        "partition_test",
    )
    os.makedirs(output_root, exist_ok=True)

    canonical_schema = spark.read.parquet(
        os.path.join(input_path, "parquet")
    ).schema

    print("\n" + "=" * 70)
    print("ID4 READ QUERY BENCHMARK")
    print("=" * 70)
    print("Query: SELECT Carrier, AVG(ArrDelay) FROM data GROUP BY Carrier")
    print(f"Warm-up rounds    : {WARMUP_RUNS}")
    print(f"Official rounds   : {OFFICIAL_RUNS}")
    print(f"Output root       : {output_root}")

    read_results = []
    read_summary = []

    for fmt in FORMATS:
        df = _read_benchmark_dataframe(
            spark,
            fmt,
            input_path,
            canonical_schema,
        )

        _run_fixed_read_query(df)
        print(f"{fmt.upper():8} warm-up completed.")

        read_times = []

        for run_number in range(1, OFFICIAL_RUNS + 1):
            start_time = time.perf_counter()
            query_result = _run_fixed_read_query(df)
            read_time = time.perf_counter() - start_time
            read_times.append(read_time)

            read_results.append({
                "format": fmt,
                "run_number": run_number,
                "read_time_sec": read_time,
                "result_rows": len(query_result),
            })

            print(
                f"{fmt.upper():8} | "
                f"Run: {run_number} | "
                f"Read Query Time: {read_time:8.3f} s | "
                f"Rows: {len(query_result)}"
            )

        read_summary.append({
            "format": fmt,
            "official_runs": OFFICIAL_RUNS,
            "median_read_time_sec": statistics.median(read_times),
        })

    read_runs_path = os.path.join(
        output_root,
        "read_benchmark_runs.csv",
    )
    read_summary_path = os.path.join(
        output_root,
        "read_benchmark_summary.csv",
    )
    write_csv_table(
        read_runs_path,
        read_results,
        READ_RESULT_COLUMNS,
    )
    write_csv_table(
        read_summary_path,
        read_summary,
        READ_SUMMARY_COLUMNS,
    )

    print("\n" + "=" * 70)
    print("ID4 PARTITION CONTROL BENCHMARK")
    print("=" * 70)

    base_df = _read_benchmark_dataframe(
        spark,
        "parquet",
        input_path,
        canonical_schema,
    )

    partition_results = []

    repartition_output = os.path.join(output_root, "repartition_20")
    start_time = time.perf_counter()
    (
        base_df
        .repartition(20)
        .write
        .mode("overwrite")
        .parquet(repartition_output)
    )
    partition_results.append(
        _partition_write_result(
            "repartition_20",
            repartition_output,
            time.perf_counter() - start_time,
        )
    )

    coalesce_output = os.path.join(output_root, "coalesce_2")
    start_time = time.perf_counter()
    (
        base_df
        .coalesce(2)
        .write
        .mode("overwrite")
        .parquet(coalesce_output)
    )
    partition_results.append(
        _partition_write_result(
            "coalesce_2",
            coalesce_output,
            time.perf_counter() - start_time,
        )
    )

    partition_by_output = os.path.join(output_root, "by_year_month")
    start_time = time.perf_counter()
    (
        base_df
        .write
        .mode("overwrite")
        .partitionBy("Year", "Month")
        .parquet(partition_by_output)
    )
    partition_results.append(
        _partition_write_result(
            "partition_by_year_month",
            partition_by_output,
            time.perf_counter() - start_time,
        )
    )

    for result in partition_results:
        print(
            f"{result['experiment']:<24} | "
            f"Time: {result['write_time_sec']:8.3f} s | "
            f"Files: {result['part_file_count']:5} | "
            f"Size: {result['size_mb']:10.2f} MB | "
            f"Avg file: {result['avg_part_size_mb']:8.2f} MB"
        )

    partition_summary_path = os.path.join(
        output_root,
        "partition_benchmark_summary.csv",
    )
    write_csv_table(
        partition_summary_path,
        partition_results,
        PARTITION_SUMMARY_COLUMNS,
    )

    print("\nID4 outputs:")
    print(f"Read runs         : {read_runs_path}")
    print(f"Read summary      : {read_summary_path}")
    print(f"Partition summary : {partition_summary_path}")

    return read_results, read_summary, partition_results

# ---------------------------------------------------------------------
# Result export
# ---------------------------------------------------------------------

def save_results(benchmark_results, benchmark_summary, output_root):
    """Save detailed official runs and the final four-format summary."""
    detailed_results_path = os.path.join(
        output_root,
        "write_benchmark_runs.csv",
    )

    with open(
        detailed_results_path,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=RESULT_COLUMNS,
        )
        writer.writeheader()
        writer.writerows(benchmark_results)

    summary_path = os.path.join(
        output_root,
        "write_benchmark_summary.csv",
    )

    with open(
        summary_path,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=SUMMARY_COLUMNS,
        )
        writer.writeheader()
        writer.writerows(benchmark_summary)

    return detailed_results_path, summary_path


def print_summary(benchmark_summary):
    """Display the final canonical benchmark summary."""
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

def _non_negative_int(value):
    """Parse a non-negative integer for the Spark UI hold duration."""
    try:
        parsed_value = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from exc

    if parsed_value < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed_value


def _keep_spark_ui_alive(spark, seconds):
    """Keep the completed Spark application available for live inspection."""
    if seconds <= 0:
        return

    spark_ui_url = spark.sparkContext.uiWebUrl or "unavailable"
    print(f"Spark UI: {spark_ui_url}")
    print(f"Keeping the Spark application alive for {seconds} seconds.")
    time.sleep(seconds)


def parse_args(argv=None):
    """Parse command-line paths while keeping the project defaults."""
    parser = argparse.ArgumentParser(
        description="Benchmark Airline data across CSV, JSON, Parquet, and ORC."
    )

    parser.add_argument(
        "--input-path",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Path to the source flights.csv file.",
    )

    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Root directory for benchmark outputs.",
    )

    parser.add_argument(
        "--download-data",
        action="store_true",
        help="Download flights.csv when the input file is missing, then run the benchmark.",
    )

    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Download flights.csv when missing, then exit without starting Spark.",
    )

    parser.add_argument(
        "--keep-ui-seconds",
        type=_non_negative_int,
        default=0,
        metavar="N",
        help="Keep the Spark application alive for N seconds after printing results.",
    )

    return parser.parse_args(argv)


def main(argv=None):
    """Execute the complete ID3 write benchmark workflow."""
    args = parse_args(argv)

    if args.download_data or args.download_only:
        download_dataset(args.input_path)

    if not args.input_path.is_file():
        raise FileNotFoundError(
            f"Input dataset not found: {args.input_path}. "
            "Run with --download-data to download it."
        )

    if args.download_only:
        return

    spark = create_spark_session()
    benchmark_df = None

    try:
        canonical_df = load_canonical_dataframe(
            spark,
            str(args.input_path),
        )

        benchmark_df, _ = validate_and_materialize(
            canonical_df
        )

        benchmark_results, benchmark_summary, output_paths = (
            run_write_benchmark(
                benchmark_df,
                str(args.output_path),
            )
        )

        detailed_results_path, summary_path = save_results(
            benchmark_results,
            benchmark_summary,
            args.output_path,
        )

        print_summary(benchmark_summary)

        print("\n" + "=" * 70)
        print("WRITE BENCHMARK COMPLETED")
        print("=" * 70)
        print(f"Detailed results : {detailed_results_path}")
        print(f"Final summary    : {summary_path}")

        print("\nCanonical outputs for ID4:")
        for fmt in FORMATS:
            print(
                f"{fmt.upper():8} -> "
                f"{output_paths[fmt]}"
            )

        run_read_and_partition_benchmark(
            spark,
            args.output_path,
        )

        _keep_spark_ui_alive(spark, args.keep_ui_seconds)

    finally:
        if benchmark_df is not None:
            benchmark_df.unpersist()

        spark.stop()
        print("\nSparkSession stopped.")

if __name__ == "__main__":
    main()
