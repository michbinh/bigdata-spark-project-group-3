"""Run the reproducible end-to-end validation for the Task 1 RDD pipeline."""

import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# Keep the driver and workers on the same installed PySpark runtime. These
# changes are process-local and avoid the known Windows Spark 3.4.1/PySpark
# 3.5.6 mismatch without changing production code or the user's environment.
os.environ.pop("SPARK_HOME", None)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

from src.generate_logs import IP_COUNTRY_MAP
from src.rdd_processing import (
    aggregate_country_access,
    build_country_broadcast,
    build_parsed_rdd,
    enrich_with_country,
    to_top10_dataframe,
)


INPUT_PATH = "data/raw_logs.txt"


def main() -> None:
    spark = None
    country_broadcast = None

    try:
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("Task1 Pipeline Validation")
            .getOrCreate()
        )
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")

        total_input = sc.textFile(INPUT_PATH).count()
        invalid_log_counter = sc.accumulator(0)
        parsed_rdd = build_parsed_rdd(
            sc,
            INPUT_PATH,
            invalid_log_counter=invalid_log_counter,
            cache_result=True,
        )

        counter_before = invalid_log_counter.value
        parsed_count = parsed_rdd.count()
        parsed_rdd.take(5)
        counter_after = invalid_log_counter.value

        country_broadcast = build_country_broadcast(sc, IP_COUNTRY_MAP)
        enriched_rdd = enrich_with_country(parsed_rdd, country_broadcast)
        enriched_count = enriched_rdd.count()
        missing_country_count = enriched_rdd.filter(
            lambda record: not record.get("country")
        ).count()

        country_counts_rdd = aggregate_country_access(enriched_rdd)
        aggregated_total = country_counts_rdd.values().sum()
        country_count = country_counts_rdd.count()

        top10_df = to_top10_dataframe(country_counts_rdd, spark)
        top10_count = top10_df.count()
        top10_rows = [
            (row["country"], row["access_count"])
            for row in top10_df.collect()
        ]

        assert total_input == 10000, f"Expected 10000 input rows, got {total_input}"
        assert parsed_count == 9000, f"Expected 9000 valid rows, got {parsed_count}"
        assert counter_before == 1000, (
            f"Expected 1000 invalid rows, got {counter_before}"
        )
        assert counter_after == counter_before, (
            "Invalid-row accumulator changed after actions on the cached RDD: "
            f"{counter_before} -> {counter_after}"
        )
        assert enriched_count == 9000, (
            f"Expected 9000 enriched rows, got {enriched_count}"
        )
        assert missing_country_count == 0, (
            f"Expected complete country coverage, got {missing_country_count} missing"
        )
        assert aggregated_total == 9000, (
            f"Expected 9000 aggregated accesses, got {aggregated_total}"
        )
        assert top10_count == 10, f"Expected 10 Top 10 rows, got {top10_count}"
        assert top10_df.columns == ["country", "access_count"], (
            f"Unexpected Top 10 schema: {top10_df.columns}"
        )
        assert top10_rows == sorted(
            top10_rows, key=lambda item: (-item[1], item[0])
        ), "Top 10 ordering is incorrect"

        print("Task 1 Pipeline Validation")
        print("==========================")
        print()
        print(f"Total input records: {total_input}")
        print(f"Parsed valid records: {parsed_count}")
        print(f"Invalid records: {counter_after}")
        print(f"Enriched records: {enriched_count}")
        print(f"Missing country: {missing_country_count}")
        print(f"Countries: {country_count}")
        print(f"Aggregated accesses: {aggregated_total}")
        print(f"Top 10 rows: {top10_count}")
        print(f"Accumulator stable: {'YES' if counter_before == counter_after else 'NO'}")
        print()
        print("Top 10 Countries:")
        top10_df.show(truncate=False)
        print("Validation result: PASS")
    finally:
        if country_broadcast is not None:
            country_broadcast.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
