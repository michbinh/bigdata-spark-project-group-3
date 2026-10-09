from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from generate_logs import IP_COUNTRY_MAP
    from rdd_processing import (
        aggregate_country_access,
        build_country_broadcast,
        build_parsed_rdd,
        enrich_with_country,
        to_top10_dataframe,
    )
    from utils import create_spark_session, parse_arguments, resolve_input_path
except ImportError:  # package execution path
    from src.generate_logs import IP_COUNTRY_MAP
    from src.rdd_processing import (
        aggregate_country_access,
        build_country_broadcast,
        build_parsed_rdd,
        enrich_with_country,
        to_top10_dataframe,
    )
    from src.utils import (
        create_spark_session,
        parse_arguments,
        resolve_input_path,
    )


def _print_dispatcher_help() -> None:
    parser = argparse.ArgumentParser(
        description="Run Task 1 (RDD processing) or Task 2 (format benchmarking).",
        epilog=(
            "Use '--task task1 --help' or '--task task2 --help' "
            "for task-specific options."
        ),
    )
    parser.add_argument(
        "--task",
        choices=("task1", "task2"),
        default="task1",
        help="Task to run (default: task1).",
    )
    parser.print_help()


def _select_task(argv=None):
    raw_args = list(sys.argv[1:] if argv is None else argv)
    selector = argparse.ArgumentParser(add_help=False)
    selector.add_argument(
        "--task",
        choices=("task1", "task2"),
        default="task1",
    )
    selected, task_args = selector.parse_known_args(raw_args)

    task_was_explicit = any(
        argument == "--task" or argument.startswith("--task=")
        for argument in raw_args
    )
    if not task_was_explicit and any(
        argument in ("-h", "--help") for argument in raw_args
    ):
        _print_dispatcher_help()
        return None, None

    return selected.task, task_args


def _keep_spark_ui_alive(spark, seconds):
    if seconds <= 0:
        return

    spark_ui_url = spark.sparkContext.uiWebUrl or "unavailable"
    print(f"Spark UI: {spark_ui_url}")
    print(f"Keeping the Spark application alive for {seconds} seconds.")
    time.sleep(seconds)


def _run_task1(argv=None):
    args = parse_arguments(argv)
    input_path = resolve_input_path(args.input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    spark = create_spark_session(app_name=args.app_name)
    country_broadcast = None

    try:
        spark_context = spark.sparkContext
        invalid_log_counter = spark_context.accumulator(0)

        parsed_rdd = build_parsed_rdd(
            spark_context,
            input_path.resolve().as_uri(),
            invalid_log_counter=invalid_log_counter,
            cache_result=True,
        )

        country_broadcast = build_country_broadcast(
            spark_context,
            IP_COUNTRY_MAP,
        )
        enriched_rdd = enrich_with_country(parsed_rdd, country_broadcast)
        country_counts_rdd = aggregate_country_access(enriched_rdd)

        top_rows = to_top10_dataframe(
            country_counts_rdd,
            spark,
        ).collect()[:args.top_n]

        print(f"Top {len(top_rows)} countries:")
        for row in top_rows:
            print(f"  {row['country']}: {row['access_count']}")

        print(f"Invalid records: {invalid_log_counter.value}")

        if args.output_path:
            output_path = Path(args.output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            lines = ["country,count"]
            lines.extend(
                f"{row['country']},{row['access_count']}"
                for row in top_rows
            )
            lines.append(f"invalid_records,{invalid_log_counter.value}")

            output_path.write_text(
                "\n".join(lines) + "\n",
                encoding="utf-8",
            )
            print(f"Saved summary to {output_path}")

        _keep_spark_ui_alive(spark, args.keep_ui_seconds)

    finally:
        if country_broadcast is not None:
            country_broadcast.unpersist(blocking=False)
        spark.stop()


def _run_task2(argv=None):
    if __package__ is None or __package__ == "":
        from format_benchmark import main as run_format_benchmark
    else:
        from src.format_benchmark import main as run_format_benchmark

    return run_format_benchmark(argv)


def main(argv=None):
    task, task_args = _select_task(argv)
    if task is None:
        return None
    if task == "task2":
        return _run_task2(task_args)
    return _run_task1(task_args)


if __name__ == "__main__":
    main()
