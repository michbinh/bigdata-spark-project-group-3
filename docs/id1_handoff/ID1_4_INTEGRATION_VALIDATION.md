# ID1.4 - Broadcast + Pair RDD Integration Validation

## Scope

ID1.4 validates the complete Task 1 integration path:

`Parsed RDD -> Broadcast IP-to-Country enrichment -> Pair RDD -> reduceByKey -> Top 10 -> DataFrame`

The validation uses the existing production functions and does not reimplement the pipeline.

## Production Components

- `src/rdd_processing.py`: parsing, Broadcast enrichment, Pair RDD aggregation, Top 10 selection, and final DataFrame conversion.
- `src/generate_logs.py`: the `/24` IP-to-country mapping used to build the Broadcast variable.
- `data/raw_logs.txt`: the 10,000-record validation dataset.

## Validation Result

| Check | Result |
| --- | ---: |
| Total input | 10,000 |
| Parsed valid | 9,000 |
| Malformed | 1,000 |
| Enriched | 9,000 |
| Missing country | 0 |
| Countries | 14 |
| Aggregated total | 9,000 |
| Top 10 rows | 10 |
| Existing unit tests | 5/5 PASS |
| Accumulator stable | YES |

## Validated Top 10 Countries

| Rank | Country | Access count |
| ---: | --- | ---: |
| 1 | Vietnam | 1,967 |
| 2 | United States | 1,676 |
| 3 | Japan | 1,059 |
| 4 | Singapore | 780 |
| 5 | Germany | 617 |
| 6 | India | 575 |
| 7 | South Korea | 505 |
| 8 | Brazil | 428 |
| 9 | Australia | 357 |
| 10 | France | 322 |

The result is ordered by access count descending, with country ascending as the tie-breaker.

## Pipeline

```text
raw_logs.txt
  -> textFile
  -> flatMap
  -> map(parse)
  -> filter
  -> parsed RDD
  -> Broadcast lookup
  -> enriched RDD
  -> (country, 1)
  -> reduceByKey
  -> Top 10
  -> DataFrame
```

## Environment Note for ID5 / ID7

During ID1.4 integration validation, the initial Spark executions failed because local `SPARK_HOME` referenced Spark 3.4.1 while the installed PySpark package was version 3.5.6. Spark workers also initially attempted to invoke `python3`, which was unavailable in the Windows environment.

The successful validation aligned the Spark runtime, driver Python, and worker Python only for the validation process. This was an environment/configuration issue, not a production-code defect.

Before ID5 performs `spark-submit` packaging and before ID7 performs clean-clone or classroom live-demo validation, verify that these are consistent:

- Spark runtime version
- Installed PySpark version
- `SPARK_HOME`
- Driver Python executable
- Worker Python executable

## Handoff

- The ID1.4 production pipeline is validated.
- No production-code change was required during integration validation.
- `scripts/validate_task1_pipeline.py` provides a quick, reproducible Task 1 validation for ID5 and ID7.
- ID5 can integrate the validated pipeline into `main.py` and the `spark-submit` workflow.
- ID7 can run the validation script during clean-clone acceptance testing.
