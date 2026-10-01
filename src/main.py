from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from rdd_processing import process_log_file
    from utils import create_spark_session, parse_arguments, resolve_input_path
except ImportError:  # pragma: no cover - package execution path
    from src.rdd_processing import process_log_file
    from src.utils import create_spark_session, parse_arguments, resolve_input_path


def main(argv=None):
    args = parse_arguments(argv)
    input_path = resolve_input_path(args.input_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    spark = create_spark_session(app_name=args.app_name)
    try:
        top_countries, invalid_count = process_log_file(spark, str(input_path), top_n=args.top_n)

        print(f"Top {args.top_n} countries:")
        for country, count in top_countries:
            print(f"  {country}: {count}")
        print(f"Invalid records: {invalid_count}")

        if args.output_path:
            output_path = Path(args.output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            lines = ["country,count"]
            for country, count in top_countries:
                lines.append(f"{country},{count}")
            lines.append(f"invalid_records,{invalid_count}")
            output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            print(f"Saved summary to {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
