# Big Data Spark Project — Group 3

## Project Overview

This project demonstrates an end-to-end Apache Spark workflow built around two complementary tasks:

- **Task 1 — Low-level RDD processing:** generate reproducible web access logs, parse and validate them with the RDD API, enrich valid records through a broadcast lookup, count malformed records with an accumulator, aggregate access counts by country, and produce the Top 10 Countries result.
- **Task 2 — File-format and partition benchmarking:** download the USDOT Flight Delays dataset, compare CSV, JSON, Parquet, and ORC for write time, read time, storage size, and output-file count, and demonstrate partition-control operations.

The complete workflow is:

```text
Environment preflight
        |
        v
Generate and self-check web logs
        |
        v
Task 1: RDD parse -> validate -> enrich -> aggregate -> Top 10 Countries
        |
        v
Download and extract the airline dataset
        |
        v
Task 2: format benchmark -> partition experiments -> result artefacts
        |
        v
Tests, report, charts, Spark UI inspection, and live demonstration
```

## Repository Structure

```text
.
├── data/
│   ├── raw_logs.txt                  # Small reproducible input for Task 1; committed
│   └── raw/airline/flights.csv       # Large Task 2 input; generated locally, not committed
├── src/
│   ├── generate_logs.py              # Reproducible web-log generator and self-check
│   ├── rdd_processing.py             # Parser, RDD transformations, and aggregation
│   ├── format_benchmark.py           # Format and partition benchmark workflow
│   ├── main.py                       # Spark application entry point
│   └── utils.py                      # Shared runtime and path helpers
├── tests/                            # Unit and smoke tests
├── docs/                             # Diagrams, benchmark tables, and charts
├── REPORT.md                         # Technical report
├── requirements.txt                  # Python dependencies
├── submit_job.sh                     # Reproducible spark-submit wrapper
├── .gitignore                        # Excludes large and generated artefacts
└── output/                           # Generated Task 1 and Task 2 outputs; not committed
```

Do not commit the airline dataset, generated CSV/JSON/Parquet/ORC directories, Spark warehouse data, runtime logs, checkpoints, credentials, passwords, or API keys.

## Runtime Requirements

The verified development environment uses:

- Python 3.11.x
- PySpark 3.5.6
- Apache Spark 3.5.6
- Java 8 (`1.8.0_502` in the verified Windows environment)
- Git Bash for running `submit_job.sh` on Windows

The `spark-submit` executable and the installed PySpark package must use compatible Spark versions. Do not mix a standalone Spark 3.4.1 installation with PySpark 3.5.6. If `spark-submit --version` reports the wrong release, update `PATH` or `SPARK_HOME` before running the project.

From the repository root, install the Python dependencies:

```powershell
pip install -r requirements.txt
```

Run the following preflight checks in PowerShell:

```powershell
python --version
python -c "import sys, pyspark; from pyspark.sql import SparkSession; print(sys.executable); print('PySpark:', pyspark.__version__); print('SparkSession import: OK')"
spark-submit --version
java -version
$env:JAVA_HOME
Test-Path .\data\raw_logs.txt
& "C:\Program Files\Git\bin\bash.exe" --version
```

Proceed only after Python, PySpark, Spark, Java, the Task 1 input path, and Git Bash are available as expected.

## End-to-End Execution Flow

### Step 1 — Generate the Task 1 input

Run the log generator from the repository root:

```powershell
python .\src\generate_logs.py
```

The generator uses a fixed default seed and writes `data/raw_logs.txt`. It creates 10,000 shuffled records with an exact 90/10 split: 9,000 valid records and 1,000 intentionally malformed records. The malformed records are divided across four mutually exclusive categories: `missing_field`, `invalid_ipv4`, `non_numeric_status`, and `malformed_request`.

Before writing the file, the generator performs a self-check. It verifies the total number of records, the expected count for each category, the valid status-code range, and lookup coverage for all syntactically valid IP prefixes. This makes the input reproducible and validates the assumptions used by the Spark pipeline.

The committed `data/raw_logs.txt` allows Task 1 to run immediately after a clean clone. Regenerate it only when the input must be reproduced or when generator behaviour is being tested.

### Step 2 — Validate the Task 1 pipeline

Run the unit tests before launching Spark:

```powershell
python -m unittest discover -s tests -p "test_top10_pipeline.py" -v
```

The tests validate the parser and Top 10 aggregation behaviour independently of the full deployment wrapper. A failing test should be resolved before the end-to-end job is used for a demonstration.

### Step 3 — Submit Task 1

Run Task 1 through the deployment wrapper:

```powershell
& "C:\Program Files\Git\bin\bash.exe" -lc './submit_job.sh --task task1'
```

Running `./submit_job.sh` without `--task` also uses Task 1 as the default workflow:

```powershell
& "C:\Program Files\Git\bin\bash.exe" -lc './submit_job.sh'
```

The execution sequence is:

1. `submit_job.sh` resolves repository-relative paths and invokes `spark-submit` with `src/main.py` as the application entry point.
2. `main.py` creates the Spark session and selects the Task 1 workflow.
3. `SparkContext.textFile()` loads `data/raw_logs.txt` as an RDD of raw text records.
4. `flatMap()` normalises physical input records into logical log lines.
5. `map()` calls the custom parser for every line. Valid rows become structured records; malformed rows become `None`. The accumulator is incremented when parsing rejects a record.
6. `filter()` removes `None`, leaving only valid parsed records. The parsed RDD is cached and materialised so later actions do not unnecessarily repeat parsing and accumulator updates.
7. A broadcast variable distributes the IP-prefix-to-country lookup map once per executor. Each valid record is enriched with its country through the broadcast value.
8. The enriched RDD is mapped to `(country, 1)` pairs. `reduceByKey()` aggregates access counts and introduces the workflow's shuffle boundary.
9. `takeOrdered(10)` selects the ten countries with the highest access counts, using the country name as a deterministic tie-breaker.
10. The small Top 10 result is converted to a DataFrame for presentation and output. The application reports the malformed-record count and writes the country ranking to the configured Task 1 output location.
11. The Spark session is stopped after the result has been displayed and written.

To keep the application alive for Spark UI inspection after computation finishes, run:

```powershell
& "C:\Program Files\Git\bin\bash.exe" -lc './submit_job.sh --task task1 --keep-ui-seconds 90'
```

Spark UI is normally available at <http://localhost:4040> while the Spark application is alive. The 90-second hold is for inspection only and is not part of benchmark timing.

### Step 4 — Download the Task 2 dataset

Task 2 uses the Kaggle USDOT Flight Delays dataset. Download and extract only the required `flights.csv` file:

```powershell
python .\src\format_benchmark.py --download-only
```

The default input path after extraction is:

```text
data/raw/airline/flights.csv
```

The source dataset is approximately 500 MB, while generated multi-format outputs require substantially more space. Keep at least 10 GB of free disk space before running the full benchmark. The downloaded dataset is local input and must not be committed.

### Step 5 — Run the Task 2 format benchmark

The benchmark can be executed directly:

```powershell
python .\src\format_benchmark.py
```

It can also be launched through the same Spark deployment wrapper used by Task 1:

```powershell
& "C:\Program Files\Git\bin\bash.exe" -lc './submit_job.sh --task task2'
```

The Task 2 workflow performs the following operations:

1. The script resolves the default airline input and output directories relative to the repository root.
2. Spark reads `flights.csv` and prepares the DataFrame used consistently across all format measurements.
3. The DataFrame is written to CSV, JSON, Parquet with Snappy compression, and ORC with Snappy compression.
4. Each write records elapsed time, output file count, total directory size, and average part-file size.
5. Every generated format is read back in a separate measurement, and an action is triggered so the reported duration represents actual Spark execution rather than lazy plan construction.
6. Repeated measurements are summarised using the median to reduce sensitivity to one-off runtime variation.
7. Partition experiments demonstrate how `repartition`, `coalesce`, and partition-aware writes affect parallelism, shuffle cost, file count, and average file size.
8. Benchmark outputs are written with `overwrite` mode to the generated output directories, so rerunning Task 2 replaces prior benchmark artefacts in those locations.

The default output locations are:

```text
output/format_benchmark/     # CSV, JSON, Parquet, and ORC benchmark outputs
output/partition_test/       # Partition-control experiment outputs
```

For Spark UI inspection during Task 2, use:

```powershell
& "C:\Program Files\Git\bin\bash.exe" -lc './submit_job.sh --task task2 --keep-ui-seconds 90'
```

When interpreting the UI, distinguish Spark jobs, stages, and tasks. Actions create jobs; shuffle boundaries separate stages; and each stage launches tasks over its partitions.

### Step 6 — Verify generated results and documentation

After both tasks complete, verify that:

- Task 1 reports exactly 9,000 valid records and 1,000 malformed records for the default generated input.
- The Top 10 Countries output is present and ordered by descending access count.
- Task 2 produced readable CSV, JSON, Parquet, and ORC directories.
- File counts, directory sizes, and benchmark timings were recorded for every format.
- Partition experiment outputs exist under `output/partition_test`.
- The runtime table and chart match the latest benchmark run.

The documented Task 2 results are stored in:

- [`docs/task2_benchmark_results.md`](docs/task2_benchmark_results.md)
- [`docs/benchmark_charts/task2_write_benchmark.svg`](docs/benchmark_charts/task2_write_benchmark.svg)

## Quick Run Reference

From a clean clone on the verified Windows setup:

```powershell
pip install -r requirements.txt
python .\src\generate_logs.py
python -m unittest discover -s tests -p "test_top10_pipeline.py" -v
& "C:\Program Files\Git\bin\bash.exe" -lc './submit_job.sh --task task1'
python .\src\format_benchmark.py --download-only
& "C:\Program Files\Git\bin\bash.exe" -lc './submit_job.sh --task task2'
```

## Data and Artefact Policy

- Commit `data/raw_logs.txt` because it is the small reproducible Task 1 input.
- Do not commit `data/raw/airline/`, `output/`, `benchmark_output/`, runtime `logs/`, or `checkpoints/`.
- Do not stage accidental files such as `tatus`.
- Do not commit Spark warehouse data, generated benchmark formats, credentials, or machine-specific runtime files.

## Git Workflow

Work on a feature branch and do not push directly to `main`. Update the feature branch from `main` before starting, commit a coherent change, push the branch, and open a Pull Request. The Pull Request should identify changed files, execution commands, expected outputs, and known limitations.

Example commit messages:

```text
feat(rdd): add regex log parser
feat(shared-vars): add invalid log accumulator
feat(benchmark): add parquet write measurement
docs(readme): document end-to-end execution flow
fix(deployment): use relative input path
```

## Technical Report

Detailed explanations of RDD lineage, lazy evaluation, fault tolerance, shared variables, Spark deployment, file formats, partition control, measured results, and limitations are available in [`REPORT.md`](REPORT.md).
