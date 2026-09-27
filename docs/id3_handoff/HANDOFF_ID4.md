# ID3 → ID4 Handoff
## Task 2 — Airline File Format Benchmark and Partition Management

**Project:** Big Data Project — Low-Level APIs, Spark Applications, and File Formats  
**Prepared:** 25 September 2026  
**From:** ID3 — Format Lead  
**To:** ID4 — Read Query and Partition Lead  
**Working environment:** Python **3.11.9**, PySpark **3.4.1**, Spark **3.4.1**, Java **11**, local Windows execution  
**Suggested location:** `C:\BigDataProject\HANDOFF_ID4.md`

> **Start here:** ID3 has completed the write-benchmark implementation, and has reported a successful full run of `src/format_benchmark.py`. ID4 should normally consume the existing four output directories, not rerun the write benchmark. The remaining work is output acceptance, Read Query Time, partition experiments, and integration with the report and final Task 2 module.
>
> **Preserve the results:** Both the notebook and script use `overwrite`. Running them against the original output root replaces the datasets and result tables. A script rerun does **not** refresh the notebook-generated `output/id4_handoff/` package automatically.

### How to read this document

**Implemented** describes the supplied script or the executed notebook. **Observed** describes saved notebook output or the owner's terminal logs. **Recommended next step** describes work for ID4; it is not an assertion that this work has already been completed. External documentation is linked where it explains software behavior or installation.

The document uses the final uploaded notebook, `02_airline_format_benchmark(1).ipynb`, the delivered `format_benchmark.py`, the original assignment, the PCCV and Timeline files, and the owner's subsequent runtime confirmations. The notebook's `(1)` suffix is an upload filename suffix; the working project name is `02_airline_format_benchmark.ipynb`.

**Result provenance:** The final standalone execution of `format_benchmark.py`produced the supplied `write_benchmark_runs.csv` and`write_benchmark_summary.csv`.

These files contain 12 official measurements (4 formats × 3 runs) and4 final summary records. They preserve the same canonical dataset, partition count, output sizes, compression configuration, and output paths validated during notebook development, while Write Execution Time differs slightly as expected between independent benchmark executions.

These supplied CSV files are therefore treated as the **canonical frozen write-benchmark result set** for the ID3 → ID4 handoff. ID4, ID6, and later reporting/visualization stages should use this result set consistently unless the team explicitly decides to rerun the complete benchmark under a new protocol or environment.

## Contents

1. [Project scope and completed workflow](#1-project-scope-and-completed-workflow)
2. [Dataset and canonical schema](#2-dataset-and-canonical-schema)
3. [Environment and dependencies](#3-environment-and-dependencies)
4. [Directory structure and artifact ownership](#4-directory-structure-and-artifact-ownership)
5. [Locked write-benchmark protocol](#5-locked-write-benchmark-protocol)
6. [Results and their interpretation](#6-results-and-their-interpretation)
7. [Script interface and integration](#7-script-interface-and-integration)
8. [Execution guide](#8-execution-guide)
9. [Packaging and transfer](#9-packaging-and-transfer)
10. [ID4 continuation plan](#10-id4-continuation-plan)
11. [Troubleshooting and known issues](#11-troubleshooting-and-known-issues)
12. [Status and Definition of Done](#12-status-and-definition-of-done)
13. [Source notes and remaining provenance gaps](#13-source-notes-and-remaining-provenance-gaps)

---

## 1. Project scope and completed workflow

### 1.1 Where this work fits

The original assignment has a theory/report component and three practical tasks. This handoff concerns the **ID3 portion of practical Task 2**, not completion of the entire project.

| Workstream | Main responsibility | Relationship to this handoff |
|---|---|---|
| Task 1 — RDD processing | ID1 and ID2: log parsing, aggregation, Broadcast and Accumulator | Separate pipeline; not required to run the Airline benchmark |
| Task 2 — File formats | ID3: dataset selection, canonical preparation, size and write-time benchmarking | Implemented here |
| Task 2 — Read and partition tests | ID4: Read Query Time, `repartition(20)`, `coalesce(2)`, `partitionBy("Year", "Month")` | Next implementation stage |
| Task 3 — Deployment | ID5: entry point, dependencies and `submit_job.sh` | Final integration must be coordinated |
| Report and repository | ID6 | Receives one consistent result set and theory sections |
| QA, slides and demonstration | ID7 | Receives integrated code, accepted results and screenshots |

Source: original assignment, Task 2; PCCV, ID3–ID7 responsibilities. Status of other members' implementations is not verified by this document.

### 1.2 What ID3 has done

```text
Environment setup and smoke tests
    → Audit Airline 2015 and NYC Yellow Taxi 2024
    → Compare candidates and select Airline 2015
    → Phase A: keep 31 columns, rename four fields, preserve nulls
    → Materialize one canonical DataFrame using DISK_ONLY
    → Phase B: define compression, paths, rounds and measurement rules
    → Phase C: execute four warm-up writes and twelve measured writes
    → Calculate median write times and retain final-round output sizes
    → Phase D: export notebook handoff metadata
    → Package the write implementation into src/format_benchmark.py
    → Owner confirms a successful independent full script run
    → ID4 accepts the outputs, then implements read/partition experiments
```

The notebook is the development and reasoning record. The Python module is the independently runnable implementation. They are complementary artifacts, not two different experiments by design.

### 1.3 Immediate priorities for ID4

**Receive a consistent output snapshot → verify environment and schema → validate the four datasets → agree the read-timing boundary and schema policy → implement read benchmark → run partition experiments → return code, results and screenshots.**

Do not start by downloading another Airline variant, substituting Taxi data, or regenerating the four formats without coordinating with ID3.

---

## 2. Dataset and canonical schema

### 2.1 Dataset identity and selection

The selected dataset is **2015 Flight Delays and Cancellations**, Kaggle identifier `usdot/flight-delays`. The benchmark input is **`flights.csv`**, normally stored at `data/raw/airline/flights.csv`. The `airlines.csv` and `airports.csv` lookup files are not joined into this workload.

The alternative screened was **NYC TLC Yellow Taxi Trip Records, January–December 2024**, comprising twelve monthly Parquet files. It remains screening evidence, not a dependency of the final write script.

The selection rationale was assignment fit: Airline provides meaningful equivalents for `Carrier` and `ArrDelay`, and native year/month fields. Taxi would require different business fields for the aggregate query and derived year/month columns. This is a workflow decision, not a claim that Taxi is unsuitable for format benchmarking in general.

Source locations: [Kaggle Airline dataset](https://www.kaggle.com/datasets/usdot/flight-delays) and [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Neither the packaged script nor the receiving workflow downloads data automatically.

### 2.2 Locked canonical contract

| Property | Contract |
|---|---|
| Rows | **5,819,079** |
| Columns | **31**, in the original order |
| Year | 2015 |
| Month coverage | 1–12; twelve distinct values |
| Carrier categories | 14 |
| `ArrDelay` null count | **105,071**, approximately **1.8056%** |
| Filtering, deduplication, imputation | None |
| Removed columns | None |
| Input persistence | `StorageLevel.DISK_ONLY` |

Source: executed notebook Phase A and `validate_and_materialize()` in the script. The assertions validate the listed conditions; they are not a complete data-quality test of every field.

Only these names change:

| Raw name | Canonical name | Role |
|---|---|---|
| `YEAR` | `Year` | Directory partitioning |
| `MONTH` | `Month` | Directory partitioning |
| `AIRLINE` | `Carrier` | Grouping |
| `ARRIVAL_DELAY` | `ArrDelay` | Numeric aggregation |

The other 27 names and the row population are retained. The CSV reader currently uses `header=True` and `inferSchema=True`; it does not supply an explicit input schema. Inference is outside the write timer.

### 2.3 Why 31 columns and unchanged nulls?

Keeping all columns preserves the chosen workload and leaves a wider table for the later two-column aggregate. The reasoning does not require every format to read the same physical bytes: differences in physical column access are part of the subsequent format comparison.

`ArrDelay` nulls are preserved rather than replaced with zero or dropped. A missing delay is not equivalent to zero delay. Spark SQL `AVG()` excludes null values, so the query can run without imputation. The audit established the null count, not the cause of every missing value. Do not describe every null as a cancelled/diverted flight without an additional check. [Spark NULL semantics](https://archive.apache.org/dist/spark/docs/3.4.1/sql-ref-null-semantics.html)

### 2.4 Full observed canonical schema

All fields were reported as nullable. `nullable=True` is a schema property, not evidence that a column actually contains missing values.

| Position | Name | Spark type |
|---:|---|---|
| 1 | Year | integer |
| 2 | Month | integer |
| 3 | DAY | integer |
| 4 | DAY_OF_WEEK | integer |
| 5 | Carrier | string |
| 6 | FLIGHT_NUMBER | integer |
| 7 | TAIL_NUMBER | string |
| 8 | ORIGIN_AIRPORT | string |
| 9 | DESTINATION_AIRPORT | string |
| 10 | SCHEDULED_DEPARTURE | integer |
| 11 | DEPARTURE_TIME | integer |
| 12 | DEPARTURE_DELAY | integer |
| 13 | TAXI_OUT | integer |
| 14 | WHEELS_OFF | integer |
| 15 | SCHEDULED_TIME | integer |
| 16 | ELAPSED_TIME | integer |
| 17 | AIR_TIME | integer |
| 18 | DISTANCE | integer |
| 19 | WHEELS_ON | integer |
| 20 | TAXI_IN | integer |
| 21 | SCHEDULED_ARRIVAL | integer |
| 22 | ARRIVAL_TIME | integer |
| 23 | ArrDelay | integer |
| 24 | DIVERTED | integer |
| 25 | CANCELLED | integer |
| 26 | CANCELLATION_REASON | string |
| 27 | AIR_SYSTEM_DELAY | integer |
| 28 | SECURITY_DELAY | integer |
| 29 | AIRLINE_DELAY | integer |
| 30 | LATE_AIRCRAFT_DELAY | integer |
| 31 | WEATHER_DELAY | integer |

Source: notebook Phase A1, saved `printSchema()` output. The machine-readable copy is `output/id4_handoff/canonical_schema.json`, exported by notebook Phase D. Preserve field order when supplying this schema to the CSV reader.

---

## 3. Environment and dependencies

### 3.1 Recorded baseline versus unrecorded details

| Component | Recorded or configured baseline | Evidence / qualification |
|---|---|---|
| Operating system | Windows; terminal build string `10.0.26200.9550` | Owner's CMD output; do not infer the Windows product edition from this string |
| Python | **3.11.9** | `.venv` terminal output and notebook metadata |
| PySpark | **3.4.1** | Terminal import output; script dependency |
| Spark JVM runtime | **3.4.1** | Successful terminal smoke test |
| Java | **Temurin 11.0.32.1**, 64-bit | Owner's `java -version` output |
| Java installation path | `C:\Program Files\Eclipse Adoptium\jdk-11.0.32.101-hotspot` | Directory returned by `where java` |
| Project interpreter | `C:\BigDataProject\.venv\Scripts\python.exe` | Explicit interpreter used in successful tests |
| Spark distribution root | `C:\Spark` | Agreed installation layout; verify the actual environment variable on each machine |
| Hadoop helper root | `C:\Hadoop` | Agreed Windows layout; exact helper build/checksum was not captured |
| Editor | VS Code with Python/Jupyter support | Notebook workflow; extension version numbers not captured |
| Execution master | `local[*]` | Explicit in notebook and script |
| SQL shuffle partitions | `8` | Explicit in notebook and script |
| Driver/executor memory overrides | Not explicitly set by this script | Effective heap and any external overrides were not captured |
| Hardware | Not fully recorded | CPU model, RAM capacity, disk model and free space must not be invented |

**Python 3.11 is intentional and accepted for this project. Do not downgrade to 3.9 merely to reproduce an earlier setup example.** Python 3.11 support was introduced in Spark 3.4.0; Spark 3.4.1 also documents Java 11 support. [Spark 3.4.0 release notes](https://spark.apache.org/releases/spark-release-3-4-0.html) · [Spark 3.4.1 overview](https://dlcdn.apache.org/spark/docs/3.4.1/)

### 3.2 Required packages

The delivered script directly imports only **PySpark** outside the Python standard library. `argparse`, `csv`, `os`, `statistics` and `time` require no separate installation. Installing `pyspark==3.4.1` resolves its declared dependencies, including Py4J. Do not independently upgrade Py4J as a response to a gateway connection error.

For notebooks, install `ipykernel` in the selected environment and install Microsoft's **Python** and **Jupyter** VS Code extensions. Pandas, NumPy, PyArrow, Matplotlib, findspark and Kaggle API tooling are not required by this write script. They may be added for separate analysis later. [PySpark installation](https://archive.apache.org/dist/spark/docs/3.4.1/api/python/getting_started/install.html) · [VS Code kernel management](https://code.visualstudio.com/docs/datascience/jupyter-kernel-management)

### 3.3 Fresh-machine installation

Do not reinstall a working environment unnecessarily. These steps are for a receiver who lacks the components.

**Java.** Install an x64 Temurin **JDK 11** from [Adoptium](https://adoptium.net/temurin/releases/?version=11). In the Windows installer, enable the PATH and `JAVA_HOME` features. Match ID3's exact patch for strict reproduction where available; otherwise record the actual JDK 11 patch used. [Windows installer instructions](https://adoptium.net/installation/windows)

**Spark.** Obtain `spark-3.4.1-bin-hadoop3.tgz` and its published checksum from the [Apache archive](https://archive.apache.org/dist/spark/spark-3.4.1/). Extract so that `C:\Spark\bin\spark-submit.cmd` exists directly. Do not leave an extra `spark-3.4.1-bin-hadoop3` directory between `C:\Spark` and `bin`. This baseline uses both the extracted distribution and the matching `pyspark==3.4.1` Python package.

**Windows Hadoop helpers.** The project setup uses `C:\Hadoop\bin\winutils.exe`; a matching `hadoop.dll` may also be present/needed for the selected Windows build. The exact binaries actually installed by ID3 were not supplied. Prefer the team's verified bundle and record its source and hash. Inspect the `hadoop-client-api-*.jar` filename in `C:\Spark\jars` before selecting compatible helpers. Do not infer a helper version solely from the Spark version. The previously referenced [cdarlint/winutils repository](https://github.com/cdarlint/winutils) is a **third-party binary source**, not an Apache Spark release channel. Verify provenance before executing binaries; do not disable security software to install them. A full Hadoop cluster is not part of this local benchmark.

**Python.** Install Python 3.11 x64. The recorded version was 3.11.9; its [official release page](https://www.python.org/downloads/release/python-3119/) provides Windows installers. Do not copy another person's `.venv` directory. Create the receiver's environment from their installed interpreter. [Python venv documentation](https://docs.python.org/3.11/library/venv.html)

**Run in Windows Command Prompt, from the project root, only when `.venv` does not already exist:**

```bat
cd /d C:\BigDataProject
py -3.11 -m venv .venv
.venv\Scripts\python.exe -m pip install pyspark==3.4.1 ipykernel
.venv\Scripts\python.exe -m pip check
.venv\Scripts\python.exe --version
.venv\Scripts\python.exe -c "import pyspark; print('PySpark:', pyspark.__version__)"
```

Expected: Python 3.11.x, PySpark 3.4.1, and no broken dependency report. If `py` is unavailable, invoke the installed Python 3.11 executable by its full path for the environment-creation command. `ipykernel` is only necessary for notebook use; its original installed version was not captured.

### 3.4 Environment variables and PATH

Use Windows search → **Edit the system environment variables** → **Environment Variables**. Set variables at either a consistent user scope or system scope; avoid contradictory copies at both scopes.

| Variable | Baseline value | Meaning |
|---|---|---|
| `JAVA_HOME` | `C:\Program Files\Eclipse Adoptium\jdk-11.0.32.101-hotspot` | JDK root; adapt to the receiver's installed JDK 11 folder |
| `SPARK_HOME` | `C:\Spark` | Spark root, not its `bin` folder |
| `HADOOP_HOME` | `C:\Hadoop` | Hadoop helper root, not its `bin` folder |
| `Path` additions | `%JAVA_HOME%\bin`, `%SPARK_HOME%\bin`, `%HADOOP_HOME%\bin` | Append entries; preserve unrelated existing entries |

Enter root paths without surrounding quote characters in the GUI. Close and reopen CMD and VS Code after changing variables. Do not replace the entire PATH with just these three entries.

`PYTHONPATH` and `PYTHONHOME` are not required by the supplied script. Avoid adding speculative values. `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` can be set **per CMD session** to the project interpreter when using `spark-submit`; the execution example below does this. A notebook still needs its own correct kernel selection.

The recorded `where java` output listed Temurin 11 first and an Oracle Java 8 path second. The recorded global `python` resolved through a WindowsApps alias. This is why examples below use the explicit project interpreter rather than relying on whichever global `python` happens to resolve first. A valid `where java` result alone does not prove `JAVA_HOME` points to the same installation.

### 3.5 Verify the receiver's environment

**Run in a new CMD window. This does not benchmark the Airline data:**

```bat
cd /d C:\BigDataProject
where java
java -version
javac -version
echo JAVA_HOME=%JAVA_HOME%
echo SPARK_HOME=%SPARK_HOME%
echo HADOOP_HOME=%HADOOP_HOME%
where spark-submit
where winutils
dir "%SPARK_HOME%\jars\hadoop-client-api-*.jar"
.venv\Scripts\python.exe --version
.venv\Scripts\python.exe -m pip show pyspark py4j ipykernel
.venv\Scripts\python.exe -u -c "from pyspark.sql import SparkSession; print('BEFORE SPARK', flush=True); s=SparkSession.builder.master('local[2]').appName('ID4EnvironmentCheck').getOrCreate(); print('SPARK:',s.version); print('JVM JAVA:',s.sparkContext._jvm.java.lang.System.getProperty('java.version')); print('COUNT:',s.range(10).count()); s.stop()"
```

Expected decisive lines: Spark **3.4.1**, JVM Java **11.x**, and `COUNT: 10`. `local[2]` is only this small diagnostic; the locked write benchmark still uses `local[*]`. Passing the test verifies startup and a tiny JVM-backed action, not full-workload memory capacity or all filesystem operations.

For VS Code, open the project folder, select `.venv\Scripts\python.exe` using **Python: Select Interpreter**, and independently choose the same environment with the notebook's **Select Kernel** control. Test a simple `print()` cell before loading Spark if notebook startup is uncertain.

---

## 4. Directory structure and artifact ownership

### 4.1 Logical project layout

This is the **documented project layout**, not a live `tree /F` capture of ID3's current disk. Notebook output confirms the generated paths shown below. Ancillary and planned locations are labelled.

```text
C:\BigDataProject\
├── HANDOFF_ID4.md                         This document; version with the code
├── .venv\                                Local environment; do not transfer/commit
├── src\
│   ├── 01_dataset_screening.ipynb         Screening/selection record; working name
│   ├── 02_airline_format_benchmark.ipynb  Phases A–D; reasoning and saved results
│   └── format_benchmark.py               Runnable ID3 write module
├── data\
│   ├── raw\
│   │   ├── airline\
│   │   │   ├── flights.csv               Only raw file required by the script
│   │   │   ├── airlines.csv              Ancillary lookup, if retained
│   │   │   └── airports.csv              Ancillary lookup, if retained
│   │   └── nyc_taxi\                     Screening only; not required by ID4
│   │       └── yellow_tripdata_2024-*.parquet
├── output\
│   ├── format_benchmark\                 Actual full-size generated datasets
│   │   ├── csv\
│   │   │   ├── part-*.csv
│   │   │   └── _SUCCESS
│   │   ├── json\
│   │   │   ├── part-*.json
│   │   │   └── _SUCCESS
│   │   ├── parquet\
│   │   │   ├── part-*.snappy.parquet
│   │   │   └── _SUCCESS
│   │   ├── orc\
│   │   │   ├── part-*.snappy.orc
│   │   │   └── _SUCCESS
│   │   ├── write_benchmark_runs.csv       Twelve official measurements
│   │   └── write_benchmark_summary.csv    Four format summaries
│   ├── id4_handoff\                      Metadata from notebook Phase D
│   │   ├── canonical_schema.json
│   │   ├── benchmark_protocol.json
│   │   ├── output_manifest.csv
│   │   ├── write_benchmark_runs.csv       Copy: may be stale after a script rerun
│   │   ├── write_benchmark_summary.csv    Copy: same freshness warning
│   │   └── README_ID4.txt                 Short notebook-generated handoff
│   ├── read_benchmark\                   Proposed ID4 result location
│   └── partition_test\                   Planned ID4 experiment location
│       ├── repartition_20\
│       ├── coalesce_2\
│       └── by_year_month\
└── logs\                                 Optional runtime evidence
```

Spark and Hadoop installations are outside the project: `C:\Spark\` and `C:\Hadoop\bin\`. Windows `.crc` sidecars and Spark temporary artifacts may also appear; the tree intentionally omits them.

**Raw versus processed:** `data/raw/` is immutable source input. The four directories under `output/format_benchmark/` are processed/serialized versions of the canonical dataset. There is no separate mandatory `data/processed/` directory in this implementation.

**Cache versus deliverable:** `DISK_ONLY` uses Spark-managed temporary storage. It is not the Parquet output, not a durable handoff file, and not reusable across new Spark applications. Transfer the explicit output directories, never a Spark temporary cache folder.

### 4.2 Capture the actual tree

**Run in CMD. These commands list the relevant folders without enumerating the large `.venv` installation:**

```bat
cd /d C:\BigDataProject
tree src /F /A
tree data /F /A
tree output /F /A
```

Save or screenshot the output when documenting the received package. `/F` includes files; `/A` uses plain text branch characters. [Microsoft tree command](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/tree)

### 4.3 What each metadata file contains

| File | Producer | Purpose |
|---|---|---|
| `canonical_schema.json` | Notebook D2 | Spark `StructType` JSON; field order, types and nullability |
| `benchmark_protocol.json` | Notebook D2 | Dataset/mapping, round counts, persistence, codecs and selected Spark settings |
| `output_manifest.csv` | Notebook D3 | One row per format with path, size, file count and median write time |
| `write_benchmark_runs.csv` | Notebook C5 or script `save_results()` | Unrounded individual measurements; primary evidence |
| `write_benchmark_summary.csv` | Notebook C5 or script `save_results()` | Median time and final-run size/file count |
| `README_ID4.txt` | Notebook D4 | Short handoff summary; does not replace this guide |

The script produces the two result CSVs and four datasets. It **does not** export or refresh the other handoff files. Absolute Windows paths inside metadata are origin-machine locations; the receiver must map them to their own project root.

---

## 5. Locked write-benchmark protocol

Source: script functions and executed notebook Phases A–C.

| Item | Implemented rule |
|---|---|
| Input | Same full canonical Airline DataFrame for every write |
| Persistence | `DISK_ONLY`, materialized before timing |
| Master | `local[*]` |
| SQL shuffle setting | `spark.sql.shuffle.partitions=8` |
| Format order | CSV → JSON → Parquet → ORC in every round |
| Warm-up | One round: one full write of each format; timings discarded |
| Official runs | Three rounds; twelve measurement records |
| Total writes | Sixteen, including warm-up |
| Input repartitioning | No `repartition()` or `coalesce()` in the write comparison |
| Mode | `overwrite` for every format and every run |
| Timer | `time.perf_counter()` immediately around `.format(fmt).save(path)` |
| Size calculation | After the timed write; recursive `os.walk()` over `part-*` files |
| Final time | `statistics.median()` of the three official times |
| Final size/files | Values from Official Run 3, whose outputs remain on disk |
| Results export | After all measured runs complete |

### 5.1 Compression and serialization

| Format | Explicit writer options | Observed baseline |
|---|---|---|
| CSV | `header=true` | No explicit compression option; standard writer is uncompressed |
| JSON | None | No explicit compression option; standard writer is uncompressed |
| Parquet | None | Active `spark.sql.parquet.compression.codec`: `snappy` |
| ORC | None | Active `spark.sql.orc.compression.codec`: `snappy` |

The CSV/JSON result label is literally `default / no explicit compression option`. The script reads the active Parquet/ORC codec values from the session; it does not explicitly pin them with per-write options. Therefore record and compare the actual settings rather than assuming every environment has identical defaults. [CSV options](https://archive.apache.org/dist/spark/docs/3.4.1/sql-data-sources-csv.html) · [JSON options](https://archive.apache.org/dist/spark/docs/3.4.1/sql-data-sources-json.html) · [Parquet](https://archive.apache.org/dist/spark/docs/3.4.1/sql-data-sources-parquet.html) · [ORC](https://archive.apache.org/dist/spark/docs/3.4.1/sql-data-sources-orc.html)

This is a **format-plus-compression-configuration comparison**, not an isolated format-only experiment. JSON uses Spark's standard record-oriented JSON output, not one pretty-printed array. Missing fields may be omitted according to the active JSON null-field setting; check the logical nulls after reading rather than requiring literal `null` text everywhere.

### 5.2 What Write Execution Time means

The timer includes Spark's execution of the save action: input-cache access, encoding, applicable compression, task execution, filesystem writes and output commit work. Because `overwrite` is used inside that action, associated overwrite work can also fall inside the interval.

It excludes the initial CSV inference/preparation/materialization, output-size traversal, printing and result export. With `DISK_ONLY`, access to the persisted input is still disk-backed and can interact with the operating-system cache. This is **end-to-end Spark Write Execution Time**, not isolated physical disk bandwidth or a power-loss-durability measurement.

### 5.3 Size units and exclusions

`size_bytes` is the sum of logical file lengths returned by `os.path.getsize()` for filenames beginning `part-`. `_SUCCESS`, external sidecars and other separately named metadata files are not counted. Metadata embedded inside a Parquet/ORC data file remains included in that file's length.

The existing field `size_mb` is computed as `size_bytes / 1024**2`. Its precise unit is **MiB**, despite the legacy field/console label “MB.” Preserve the field name for compatibility, but label report values “MiB” or explicitly state the conversion. This is not Windows' filesystem-allocation “Size on disk.”

### 5.4 Known experimental limits

This is a small, repeated local experiment: three official runs per format, fixed order, shared storage and uncontrolled OS page cache. Warm-up does not make the disk cold or eliminate every source of variance. The saved run had 16 input partitions; **16 is observed, not a hardcoded requirement**, and is distinct from the SQL shuffle setting of 8.

Do not promise the same times or output partition count on different hardware. Capture the receiving machine's effective configuration. Additional randomized-order or cold-cache experiments would be separately labelled extensions, not silent replacements for this baseline.

---

## 6. Results and their interpretation

### 6.1 Frozen final write-benchmark results

The following values come from the final standalone execution of `format_benchmark.py`, as recorded in the supplied `write_benchmark_runs.csv` and `write_benchmark_summary.csv`.

The benchmark contains three official measured runs for each format.The reported Write Execution Time is the median of these three runs.

| Format | Compression | Run 1 (s) | Run 2 (s) | Run 3 (s) | Median (s) | Final bytes | Final MiB | Part files |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| CSV | Default / no explicit compression | 10.556817 | 10.147602 | 10.017568 | **10.147602** | 589,677,593 | 562.360375 | 16 |
| JSON | Default / no explicit compression | 5.067089 | 3.227383 | 2.889796 | **3.227383** | 2,675,232,631 | 2,551.300651 | 16 |
| Parquet | Snappy | 3.036463 | 3.081783 | 2.894705 | **3.036463** | 141,519,434 | 134.963449 | 16 |
| ORC | Snappy | 3.235303 | 3.049728 | 3.357664 | **3.235303** | 169,767,638 | 161.903036 | 16 |

All twelve official measurements used:
- **5,819,079 input rows**
- **31 canonical columns**
- **16 input partitions**
- the same materialized canonical DataFrame
- the same SparkSession and Spark configuration
- the same fixed format order: CSV → JSON → Parquet → ORC

For each format, Disk Storage Size remained identical across all three official runs.

Within this benchmark environment:

- **Parquet** produced the smallest output at approximately **134.96 MiB**.
- **ORC** produced the second-smallest output at approximately **161.90 MiB**.
- **CSV** produced approximately **562.36 MiB**.
- **JSON** produced the largest output at approximately **2,551.30 MiB**.
- **Parquet** recorded the lowest median Write Execution Time at approximately **3.036 s**.

These observations describe this specific benchmark environment and should not be interpreted as universal performance rankings.

The storage comparison also reflects the active compression configuration: Parquet and ORC used Snappy, while CSV and JSON were written without an explicit compression option.

No Read Query Time conclusion is available yet; that measurement belongs to the ID4 stage.

### 6.2 Canonical result set for downstream use

The final standalone benchmark outputs:

- `write_benchmark_runs.csv`
- `write_benchmark_summary.csv`

are the **canonical ID3 write-benchmark result set**.

ID4 should use the four corresponding output directories:

```text
C:\BigDataProject\output\format_benchmark\csv\
C:\BigDataProject\output\format_benchmark\json\
C:\BigDataProject\output\format_benchmark\parquet\
C:\BigDataProject\output\format_benchmark\orc\
```
ID6 should use the same summary values when preparing REPORT.md, and ID7 should use the same values for slides and visualizations.

Do not mix the earlier notebook-development timings with these final standalone script timings. The notebook results remain useful as development evidence, but the standalone script result set is the authoritative benchmark snapshot for downstream work. 

Any future execution of format_benchmark.py should be treated as a new benchmark run rather than an automatic replacement for this frozen result set.
### 6.3 Result CSV schemas

`write_benchmark_runs.csv` has twelve records plus a header:

| Field | Meaning |
|---|---|
| `format` | `csv`, `json`, `parquet`, `orc` |
| `compression` | Runtime/configuration label |
| `row_count` | Canonical **input** row count; not independently counted from each output |
| `input_partitions` | Canonical input partition count |
| `run_number` | Official round 1, 2 or 3 |
| `write_time_sec` | Unrounded elapsed write-action time |
| `part_file_count` | Count measured after that write |
| `size_bytes` | Sum of `part-*` file lengths |
| `size_mb` | Bytes divided by 1,048,576 |
| `output_path` | Format directory on the run's machine |

`write_benchmark_summary.csv` has four records plus a header: `format`, `compression`, `official_runs`, `median_write_time_sec`, `part_file_count`, `size_bytes`, `size_mb`, `output_path`.

**Validation boundary:** ID3 validates input statistics, result-record counts and nonempty outputs. The delivered script does not independently read all four datasets back and prove row-level equivalence. That acceptance check remains a receiver/integration task; “benchmark completed” is not a claim of exhaustive round-trip validation.

---

## 7. Script interface and integration

### 7.1 Public entry point

`run_write_benchmark(df, output_path)` returns:

`(benchmark_results, benchmark_summary, output_paths)`

The first two objects are lists of dictionaries; the third maps format names to directories. The caller supplies an already validated, persisted and materialized canonical DataFrame. The function does not create an independent session, and does not stop the caller's session. It counts the supplied input before timing, then performs the complete write experiment. It does not enforce the persistence precondition itself.

Importing the module does not launch a benchmark because execution is guarded by `if __name__ == "__main__"`.

### 7.2 Function map

| Function | Responsibility |
|---|---|
| `create_spark_session()` | Creates/reuses session with app name, local master and shuffle setting |
| `load_canonical_dataframe(spark, input_path)` | Reads raw CSV and applies four renames |
| `validate_and_materialize(df)` | Persists `DISK_ONLY`, counts and checks the audited contract; returns `(df, row_count)` |
| `get_compression_policy(spark)` | Reads runtime codec labels |
| `get_write_options()` | Supplies CSV header and otherwise empty option dictionaries |
| `timed_write(df, fmt, output_path, write_options)` | Executes one timed overwrite |
| `get_output_size(output_path)` | Counts/sums local `part-*` files recursively |
| `run_write_benchmark(df, output_path)` | Runs warm-up, measurements, validation and median aggregation |
| `save_results(results, summary, output_root)` | Writes the two result CSVs and returns their paths |
| `print_summary(summary)` | Prints format summaries |
| `parse_args()` | Accepts input/output paths |
| `main()` | Orchestrates the standalone run and attempts cleanup in `finally` |

The helper signature `timed_write()` has **four arguments in the script**, versus three in the notebook where options are global. Do not copy notebook calls unchanged into the module.

### 7.3 Integration boundaries

ID4's proposed interface in the PCCV is `run_read_and_partition_benchmark(spark, input_path)`. Agree that `input_path` means the **four-format output root**, not raw `flights.csv`. It is a planned interface, not an existing function in the delivered file.

For read-only continuation, create a receiver session and read the existing outputs. Do **not** call `main()` or `run_write_benchmark()` merely to access them. For a later combined orchestrator, call write and read helpers before shared cleanup; `main()` currently stops Spark at the end.

Implementation limits to preserve/document:

- Only `--input-path` and `--output-path` are exposed as CLI arguments. There are no `--read-only`, `--runs`, `--resume` or `--handoff` options.
- Warm-up currently executes one hardcoded loop. Changing `WARMUP_RUNS` alone would change the displayed value, not implement multiple warm-up rounds.
- The validators are Airline-snapshot-specific despite the generic module filename. Do not disable them to run unrelated data.
- Paths are configurable, but defaults are absolute Windows paths. The size helper and input check target local filesystems, not HDFS/S3 URIs.
- Master and shuffle settings are explicitly set in application code. Do not assume conflicting submit flags override them.
- The script writes result CSVs at completion and has no checkpoint/resume mechanism. An interrupted rerun can leave old CSVs beside partially replaced outputs.
- Assertions are used for validation; do not run this script with Python optimization (`-O`), which removes assertions.

Source: delivered `format_benchmark.py`. These are interface facts, not requests to refactor the working implementation before ID4 starts.

---

## 8. Execution guide

### 8.1 Normal receiver route: do not rerun writes

Obtain the complete four output folders, selected run CSVs, canonical schema and this guide. Verify environment, map paths, and run read-only acceptance. The raw CSV is needed for regeneration, but is not necessary simply to read already-generated outputs.

### 8.2 Check packaging without touching benchmark outputs

**CMD; execute from the project root:**

```bat
cd /d C:\BigDataProject
.venv\Scripts\python.exe -m py_compile src\format_benchmark.py
.venv\Scripts\python.exe src\format_benchmark.py --help
```

The first command checks syntax only. The second checks argument handling/imports and should list the two path arguments. Neither validates the full dataset or proves adequate resources for a full run.

### 8.3 Independent full verification run

Only do this when reproduction is required. Stop other notebook benchmarks first. The example deliberately uses a **different output directory** so it does not replace the selected baseline.

```bat
cd /d C:\BigDataProject
.venv\Scripts\python.exe -u src\format_benchmark.py ^
  --input-path "data\raw\airline\flights.csv" ^
  --output-path "output\format_benchmark_verification"
echo ExitCode=%ERRORLEVEL%
```

These are **CMD commands**, not PowerShell syntax. In VS Code choose **Terminal → Select Default Profile → Command Prompt** before pasting them. Do not add trailing spaces after CMD continuation carets.

Expected milestones: `Canonical benchmark input: READY` → all four warm-ups → Official Runs 1–3 → final summary → `WRITE BENCHMARK COMPLETED` → cleanup. The selected directory should contain four format folders and both result CSVs. This is a full sixteen-write workload, not a quick smoke test.

Preparation may be quiet: the script prints its first canonical summary after preparation/validation. Absence of early output alone is not evidence that the kernel or JVM is dead.

### 8.4 Spark-submit route for integration testing

This is a supported execution route to verify with ID5; it is not a claim that the final Task 3 deployment script has already been tested. The example keeps the write baseline's master and shuffle setting and uses another output root.

```bat
cd /d C:\BigDataProject
set "PYSPARK_PYTHON=C:\BigDataProject\.venv\Scripts\python.exe"
set "PYSPARK_DRIVER_PYTHON=C:\BigDataProject\.venv\Scripts\python.exe"
call "%SPARK_HOME%\bin\spark-submit.cmd" ^
  --master "local[*]" ^
  --conf spark.sql.shuffle.partitions=8 ^
  src\format_benchmark.py ^
  --input-path "data\raw\airline\flights.csv" ^
  --output-path "output\format_benchmark_submit_verification"
```

The assignment's Task 3 sample uses memory settings of 2g and shuffle partitions of 10. Those are not the verified settings of this ID3 benchmark. Coordinate any changed execution configuration and label new measurements accordingly. In client mode, driver heap settings belong in launch configuration rather than a late attempt to change an already-started JVM. [Spark configuration](https://dlcdn.apache.org/spark/docs/3.4.1/configuration.html)

### 8.5 Notebook execution

Notebook 02 remains runnable A → B → C → D in one fresh kernel. Phase D stops Spark. After a restart or stop, recreate the input and session before further Spark operations. Reading old displayed output is not equivalent to having live Python variables.

Do not execute `Run All` solely to read documentation: it repeats the benchmark and overwrites the notebook's fixed output paths. For archival viewing, leave cells unexecuted.

---

## 9. Packaging and transfer

### 9.1 Final standalone benchmark selected

The latest successful standalone execution of `format_benchmark.py` has been selected as the final ID3 write-benchmark snapshot.

The canonical result files are:

```text
output/format_benchmark/write_benchmark_runs.csv
output/format_benchmark/write_benchmark_summary.csv
```

The corresponding generated datasets are:
```text
output/format_benchmark/csv/
output/format_benchmark/json/
output/format_benchmark/parquet/
output/format_benchmark/orc/
```

These artifacts must be treated as one benchmark snapshot. Because format_benchmark.py uses mode("overwrite"), executing the script again against the same output root will replace both the datasets and result tables.
ID4 should therefore consume the existing frozen outputs instead of rerunning
the ID3 write benchmark.

### 9.2 Synchronize metadata after the script rerun

**Recommended receiving/sending procedure; not a new benchmark:** copy the latest selected run CSVs into the handoff package, regenerate the output manifest from that summary, and verify its schema/protocol still describes this run. The following optional standalone Python cell does only the table/manifest synchronization. Run it after selecting the current `format_benchmark` output as final; it does not refresh environment provenance or reconstruct missing schema/protocol files.

```python
from pathlib import Path
import csv
import shutil

# Use the selected completed run; do not rerun Spark to refresh metadata.
root = Path(r"C:\BigDataProject")
source = root / "output" / "format_benchmark"
target = root / "output" / "id4_handoff"
with (source / "write_benchmark_runs.csv").open(encoding="utf-8-sig", newline="") as f:
    runs = list(csv.DictReader(f))
with (source / "write_benchmark_summary.csv").open(encoding="utf-8-sig", newline="") as f:
    summary = list(csv.DictReader(f))
expected = {"csv", "json", "parquet", "orc"}
assert len(runs) == 12 and len(summary) == 4
assert {r["format"] for r in summary} == expected
for fmt in expected:
    selected = [r for r in runs if r["format"] == fmt]
    assert len(selected) == 3 and {int(r["run_number"]) for r in selected} == {1, 2, 3}
assert {int(r["row_count"]) for r in runs} == {5_819_079}
assert len({r["input_partitions"] for r in runs}) == 1
manifest = []
for item in summary:
    matching = next(r for r in runs if r["format"] == item["format"])
    manifest.append({**item, "row_count": matching["row_count"],
                     "input_partitions": matching["input_partitions"]})
target.mkdir(parents=True, exist_ok=True)
for name in ("write_benchmark_runs.csv", "write_benchmark_summary.csv"):
    shutil.copy2(source / name, target / name)
with (target / "output_manifest.csv").open("w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(manifest[0]))
    writer.writeheader()
    writer.writerows(manifest)
print("Current result tables and manifest synchronized; schema/protocol still need a consistency check.")
```

This refresh manifest also retains `official_runs` from the summary. It does not modify the four datasets. Ensure the selected output directories actually match those CSVs; checking row counts copied into metadata is not output-data verification. Do not run old notebook D3 against stale in-memory `benchmark_summary` values after a separate script rerun.

### 9.3 What must actually be transferred

Transfer the full `csv/`, `json/`, `parquet/` and `orc/` directories—not one `part-*` file from each. Include the selected result CSVs, schema/protocol, script, notebook reasoning record and this guide. Include raw `flights.csv` or an agreed exact source snapshot when regeneration is expected. Taxi data is optional screening evidence.

**A manifest containing `C:\BigDataProject\...` paths is not a data transfer.** ID4 needs the actual folders on a shared location or their machine. Compression for transport is acceptable, but benchmark reading must use the extracted output folders, and the transport archive size is not Disk Storage Size.

Do not copy `.venv`, Spark temporary caches or system Java folders as a substitute for environment setup. Preserve small code/documentation/schema/protocol/result artifacts in Git, while managing full datasets and generated outputs outside ordinary source commits according to ID6's repository policy.

### 9.4 Verify identity and preserve evidence

Recommended additions at transfer time are a raw-file SHA-256, a per-output file manifest/checksum list, a timestamp/run label, the runtime package list and machine specifications. These were not comprehensively captured by the delivered script. Generate/check them outside timed operations; do not invent hashes or report hardware details from memory.

The following optional CMD block records lightweight environment and source identity evidence without rewriting benchmark data:

```bat
cd /d C:\BigDataProject
if not exist logs mkdir logs
.venv\Scripts\python.exe -m pip freeze > logs\requirements-observed.txt
certutil -hashfile src\format_benchmark.py SHA256 > logs\format_benchmark-sha256.txt
certutil -hashfile data\raw\airline\flights.csv SHA256 > logs\flights-sha256.txt
```

These files describe the time of capture, not automatically the environment of a historical run. Keep the original package/source snapshot where available.

---

## 10. ID4 continuation plan

### 10.1 Output acceptance first

This step is **not implemented by the delivered write script**. Before timing reads, verify each received format has the full schema, expected row count and null count, and that the grouped results agree. Do this outside timed measurement.

The following is a **proposed read-only acceptance cell** for a fresh notebook or Python process. It uses the existing canonical schema file. It does not generate new format outputs, and does not start the write benchmark.

```python
from pathlib import Path
import json
import math
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType

# Run in a dedicated receiver session, not the active writer session.
root = Path(r"C:\BigDataProject")
with (root / "output/id4_handoff/canonical_schema.json").open(encoding="utf-8") as f:
    schema = StructType.fromJson(json.load(f))
spark = (SparkSession.builder.appName("ID4OutputAcceptance").master("local[*]")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
reference = None
try:
    for fmt in ("csv", "json", "parquet", "orc"):
        path = str(root / "output/format_benchmark" / fmt)
        reader = spark.read.format(fmt)
        if fmt in ("csv", "json"):
            reader = reader.schema(schema)
        if fmt == "csv":
            reader = reader.option("header", "true")
        df = reader.load(path)
        assert df.columns == schema.fieldNames(), f"Column order mismatch: {fmt}"
        assert df.dtypes == [(f.name, f.dataType.simpleString()) for f in schema]
        check = df.agg(F.count("*").alias("rows"),
                       F.sum(F.col("ArrDelay").isNull().cast("long")).alias("nulls")).first()
        assert check["rows"] == 5_819_079 and check["nulls"] == 105_071, fmt
        result = {r["Carrier"]: r["avg_delay"] for r in
                  df.groupBy("Carrier").agg(F.avg("ArrDelay").alias("avg_delay")).collect()}
        assert len(result) == 14, fmt
        if reference is None:
            reference = result
        else:
            assert result.keys() == reference.keys(), fmt
            for carrier, value in result.items():
                expected = reference[carrier]
                assert (value is None and expected is None) or (
                    value is not None and expected is not None and
                    math.isclose(value, expected, rel_tol=1e-9, abs_tol=1e-9)), (fmt, carrier)
        print(fmt.upper(), "PASS", dict(check.asDict()))
finally:
    spark.stop()
```

Expected: four PASS lines with the expected row and null counts. This checks contract-level consistency and the target aggregation, **not byte equality or exhaustive equality of all 31 fields**. It can warm the OS cache; do not call subsequent timings cold-cache results. If `canonical_schema.json` is missing, obtain it from the handoff package rather than inferring CSV/JSON differently.

### 10.2 Read Query Time: agree the remaining design choices

The PCCV requires the same aggregate query and a real action for all formats, with one warm-up and preferably three measured runs summarized by median. The target query is:

```sql
SELECT Carrier, AVG(ArrDelay) AS avg_arrival_delay
FROM data
GROUP BY Carrier;
```

**Recommended baseline to agree with ID3:** load the saved schema outside the timer; supply it explicitly to CSV/JSON; read Parquet/ORC using their stored schema and verify compatibility. Start the timer immediately before constructing the format reader/loading the output directory, include query construction and `.collect()`, and stop when the small grouped result returns. This counts reader setup, planning and execution while excluding an extra text-schema-inference workload.

Do not cache the four read inputs or the aggregate results before measuring file reads. The `DISK_ONLY` preparation policy belongs to the **write** experiment; copying that policy into read timing would risk benchmarking a Spark cache rather than the four formats. Keep warm-up, order and action consistent, and document whether setup is included. A different read boundary is possible, but must be chosen explicitly and applied uniformly.

Collect only the approximately fourteen grouped results, not millions of raw rows. Print/sort for presentation outside the timer unless ordering is deliberately part of the agreed query. Do not time only `spark.read`, a lazy transformation, or a simple count in place of the requested aggregation.

Use `explain()` outside the timer to document the physical read plan. This query has no `WHERE`: its narrow projection relates to **column pruning**, not proof of predicate pushdown. A separately labelled filter query would be needed to study filter pushdown directly.

### 10.3 Partition experiments

Start from the same accepted canonical dataset, normally the baseline Parquet directory. Write experiments under `output/partition_test/`, never over the four baseline outputs.

| Experiment | Destination | What to record |
|---|---|---|
| `repartition(20)` then Parquet write | `repartition_20/` | Actual input/output partitions, actual file count, sizes, shuffle evidence |
| `coalesce(2)` then Parquet write | `coalesce_2/` | Same metrics; inspect imbalance rather than assuming equal-sized files |
| `partitionBy("Year", "Month")` | `by_year_month/` | Directory tree, rows/size per logical partition, files within directories |

The canonical data has twelve Year/Month combinations, but twelve directory combinations do **not** imply exactly twelve data files or the absence of a Small File Problem. Count actual `part-*` files. A new Parquet read may have a different Spark partition count from the original 16-partition writer input.

Read directory-partitioned data from the dataset root so Spark can discover the partition columns. Document actual names such as `Year=2015/Month=1/`; do not require zero-padded months. If demonstrating partition pruning, use an appropriate filter in a separately described query.

These requirements and cautions follow the PCCV ID4 notes. The partition experiments are still pending; no predicted file counts or speed outcomes should be entered as results.

### 10.4 Return package

ID4 should return its module/function, raw read timings, median read summary, query/schema/cache policy, runtime configuration, partition metrics, folder screenshots and acceptance results. Return the theory section 3.2 to ID6 and coordinate the final benchmark set with ID3.

All four read formats must be tested under one controlled receiver environment. If ID4's machine differs from ID3's, label the environments separately; do not portray write and read times from different machines as one same-machine experiment. The final team report, charts and slides must use the same approved numbers and clearly stated provenance.

---

## 11. Troubleshooting and known issues

### 11.1 Py4J errors during full materialization

**Observed incident:** repeated `WinError 10054`, `ConnectionRefusedError`, `Py4JNetworkError`, and reflection/type errors appeared at `benchmark_df.count()` after `persist(MEMORY_AND_DISK)`. Smaller tests had worked. After changing persistence to `DISK_ONLY`, the owner reported successful execution; this is the implemented baseline.

**Interpretation:** the traces show loss of usable communication with the JVM and failure while processing/formatting the original Java exception. They do not by themselves prove an out-of-memory cause. Memory/resource pressure during materialization was a plausible hypothesis, not a confirmed diagnosis from a JVM heap/crash log. Py4J is the communication layer, so these messages do not automatically mean the Py4J package needs reinstalling. [Py4J API and exceptions](https://www.py4j.org/py4j_java_gateway.html)

**Response:** preserve the first error and Java-side logs; stop the failed session; use a fresh kernel/process. Keep the working `DISK_ONLY` baseline. If it fails again, inspect effective heap, available RAM, temporary-disk capacity and the original Java exception before changing several settings. Disk persistence still needs execution memory and is not an OOM guarantee. A new JVM cannot reuse the old session's DataFrame variables.

### 11.2 VS Code kernel restart or execution appears stuck

**Observed incident:** restart waited more than five minutes; a later reboot did not alone resolve the notebook behavior. A terminal Spark smoke test succeeded, helping separate startup/tooling from full-data materialization.

**Response sequence:** save work → stop notebook execution → close the relevant VS Code windows → inspect remaining processes → reopen the project with the intended kernel → test simple Python → test a small Spark action → retry full preparation. In Task Manager inspect the process command line/PID. End only processes identified as belonging to this project. Do not mass-kill all Java/Python processes; they may belong to other applications.

An activity spinner or a momentary 0% CPU reading is not a diagnosis. `jusched.exe` is not the Spark JVM. A terminal smoke test succeeding does not establish that the full dataset or a separate notebook kernel is healthy.

### 11.3 Terminal works but notebook does not

Check the notebook's `sys.executable` and selected kernel against `C:\BigDataProject\.venv\Scripts\python.exe`. Compare `JAVA_HOME`, `SPARK_HOME` and `HADOOP_HOME` between contexts. Close/reopen VS Code after environment edits. Use **Developer: Reload Window** if kernel discovery is stale. Do not switch Python versions or reinstall Spark solely because one notebook is unresponsive. [VS Code kernels](https://code.visualstudio.com/docs/datascience/jupyter-kernel-management)

### 11.4 No output during CSV startup

The first preparation cell creates Spark and calls CSV schema inference before printing the schema. Inference can require another input pass. The script likewise has preparation work before its canonical summary. Use process activity and logs to distinguish work from a stall; do not interrupt solely because there is no immediate printed output. [Spark CSV reader](https://archive.apache.org/dist/spark/docs/3.4.1/sql-data-sources-csv.html)

### 11.5 ShutdownHookManager temporary-directory error

**Observed incident:** after a successful terminal `COUNT: 10`, shutdown logged `NoSuchFileException` while deleting a Spark temporary directory.

This particular error happened during cleanup, after the test action succeeded. It does not invalidate the already-returned count, but the log does not establish exactly why the directory was missing. Do not generalize that all shutdown errors are harmless. Preserve the full log, check process exit/result files, and investigate recurring cleanup problems separately from computation failures.

### 11.6 Wrong interpreter, Java or shell

Use the explicit `.venv` interpreter. Java resolution through PATH can differ from the JDK selected by `JAVA_HOME`; verify both and, when needed, the running JVM property. A `.venv (3.11.9)` kernel is the accepted baseline, not a mismatch requiring downgrade.

CMD uses `call`, `%VARIABLE%` and `^`. PowerShell uses different syntax. An activation/continuation error may be a shell mismatch rather than a Spark failure. The commands in this guide explicitly target CMD.

### 11.7 Reused output paths and stale handoff data

Every benchmark execution can replace the four datasets. A “test” against the original root is not read-only. Use an isolated verification root, and never run concurrent writers/readers against the same directories. After selecting a new final run, synchronize both result CSVs and the manifest. Do not trust a historical notebook display as a live view of disk.

### 11.8 File not found, wrong nesting or missing `part-*`

Check the actual root and full path rather than the notebook's location. `C:\Spark\bin\spark-submit.cmd` must exist directly under the agreed root. Each generated format is a **directory**, not one file named `csv`/`parquet`. Receive all parts. The script does not fetch missing raw data, and `get_output_size()` returning zero is not evidence that a missing directory is valid.

### 11.9 Accidental deletion of `src`

This happened during development. Keep code and documentation under version control and maintain a separate copy before cleanup. Check Recycle Bin and VS Code Local History first. Git restoration only recovers content present in the selected Git state; restoring indiscriminately can discard uncommitted work. Avoid broad directory deletion and do not delete raw inputs while cleaning Spark outputs.

### 11.10 Documentation/implementation differences worth retaining

The final uploaded notebook still contains a Phase A3 Markdown sentence referring to `MEMORY_AND_DISK`; its executed code and the packaged script use `DISK_ONLY`. Treat this as stale prose, not an instruction to revert the fix. Update that sentence when tidying the notebook.

Similarly, the historical `size_mb` field uses binary units, and “frozen” outputs remain writable. These distinctions matter in the report but do not require another benchmark merely to correct wording.

---

## 12. Status and Definition of Done

### 12.1 Current status

| Item | State at handoff | Evidence / remaining qualification |
|---|---|---|
| Candidate screening and Airline selection | Completed in workflow | Selection decision and earlier audits |
| Canonical input and null policy | Completed | Notebook output and script validation |
| Write design and repeated execution | Completed in notebook | Four warm-ups, twelve official measurements |
| Standalone `format_benchmark.py` full execution | **Owner-confirmed complete** | Final standalone result CSVs supplied and verified |
| Notebook Phase D metadata exports | Completed for the notebook run | Saved export paths and final success output |
| Metadata synchronized to later script run | **Check before transfer** | Not done automatically by script |
| Receiver output round-trip acceptance | **Pending** | Not implemented in ID3 write script |
| Read Query Time | **ID4 pending** | Query/timing implementation still required |
| Repartition/coalesce/partitionBy | **ID4 pending** | No measurements claimed yet |
| Joint final Task 2 module | **Pending integration** | Delivered module currently covers writes only |
| ID3 theory section 3.1 | Next workstream | No completed theory submission asserted here |
| Final report, deployment and clean-clone QA | Team responsibilities | Not established by ID3's successful local run |

### 12.2 ID3 code completion

The ID3 write-code milestone is satisfied when the approved full dataset passes preparation, all four formats complete warm-up and three runs, twelve detailed records and four summaries are exported, final output directories are nonempty, and the standalone script runs to completion. This has been demonstrated in the notebook and reported for the script.

This milestone is narrower than “all of Task 2 is done.” The assignment also requires read and partition work.

### 12.3 Handoff acceptance checklist

- [ ] The receiving environment reports Python 3.11, Spark/PySpark 3.4.1 and the agreed Java 11 setup.
- [ ] One approved run is identified; its datasets and result tables match.
- [ ] All four complete output directories are received, not just a path manifest.
- [ ] CSV/JSON are read with the agreed schema and CSV header handling.
- [ ] Contract-level output validation and grouped-result comparison pass.
- [ ] Notebook-derived handoff metadata is checked/refreshed after the script rerun.
- [ ] Source paths are mapped to the receiving machine without editing dataset semantics.
- [ ] ID4's read timer, schema-inference treatment and cache policy are agreed before measurement.
- [ ] Partition experiments use separate directories and actual file counts.
- [ ] Final read/partition results, code, runtime context and screenshots return to ID3/ID6/ID7.

### 12.4 Changes requiring coordination

Coordinate changes to the dataset snapshot, row population, null policy, retained columns, canonical names, compression, persistence, input repartitioning, timing boundary, round counts, result aggregation or output baseline. Moving the project to another root is an operational path change; record it and keep the logical layout. New performance measurements still need their own runtime provenance.

---

## 13. Source notes and remaining provenance gaps

### 13.1 Local sources used

| Source | Contribution |
|---|---|
| `low_level_subject(1).pdf`, Task 2 and deliverables | Original required metrics, partition experiments and module deliverable |
| `Big Data - PCCV.csv`, ID3 and ID4 rows | Ownership, handoff contract, measurement rules and downstream cautions |
| `Big Data - Timeline.csv` | Original task sequencing; its historical status labels are not live status |
| Uploaded `02_airline_format_benchmark(1).ipynb` | Executed Phases A–D, 31-field schema, notebook measurements and metadata paths |
| Delivered `format_benchmark.py` | Actual standalone behavior, function signatures and CLI |
| Owner's CMD logs and subsequent confirmations | Python/Java/Spark versions, observed failure history, successful DISK_ONLY and full script execution |
| `write_benchmark_runs.csv` | Final standalone 12-run benchmark measurements |
| `write_benchmark_summary.csv` | Final standalone four-format median summary |

Reviewed script SHA-256: `81a81ef3f6d0d2bdaf045d5af4d57bcf98ce6fa8c41e8c5035512fb81d63963f`. This identifies the delivered bytes inspected for the guide; line-ending changes or later edits can change the hash. It is not a raw-dataset checksum.

Links throughout the guide are supplementary software documentation, not substitutes for local experiment evidence. Proposed receiver checks and packaging recipes are additions to the guide, not features claimed to exist in the delivered script.

### 13.2 Still worth recording with the selected final run

Raw/output checksums; precise run date/time; actual CPU/RAM/disk; available disk capacity; effective JVM heap; Java/Hadoop helper build and source; relevant external Spark defaults; VS Code/kernel package versions; and any configuration changes since notebook execution.

Do not delay ID4's schema/code preparation solely to perfect every inventory detail, but record enough context before publishing performance comparisons. Preserve the stable implementation and add evidence rather than repeatedly changing settings without a diagnosed need.

**Final continuation:** receive and verify the frozen output snapshot → implement the agreed read benchmark → run separate partition experiments → integrate Task 2 → finalize report evidence.
