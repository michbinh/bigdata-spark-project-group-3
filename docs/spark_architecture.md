# Spark Runtime Architecture

The project submits `src/main.py` with `spark-submit`. The default `local[*]`
mode is the execution mode used for the demo. The cluster diagram below is a
conceptual deployment view; this project has not been tested on YARN,
Kubernetes, or a multi-node Standalone cluster.

## Local Demo Flow

```mermaid
flowchart TD
    Submit["spark-submit --master local[*]"] --> Driver["Driver: src/main.py"]
    Driver --> Session[SparkSession]
    Session --> Context[SparkContext]
    Context --> Local["Local scheduler: local[*]"]
    Local --> Tasks["Tasks run on local cores in the Driver JVM"]
    Tasks --> Result["Top countries and invalid count returned to Driver"]
```

In local mode there is no separate Cluster Manager, Worker Node, or Executor
process. `local[*]` lets Spark run local tasks using the available cores.

## RDD Jobs and Lineage

```mermaid
flowchart TD
    subgraph Job1["Job 1: count() materializes the parsed cache"]
        Text[textFile] --> Parse["map(parse_and_count)"]
        Parse --> Filter["filter(valid records)"]
        Filter --> Cache["cache()"]
        Cache --> Count["count() action"]
    end

    subgraph Job2["Job 2: take(top_n)"]
        Cache --> Pair["map(country, 1)"]
        Pair --> Reduce["reduceByKey()"]
        Reduce --> ReduceShuffle["Shuffle boundary"]
        ReduceShuffle --> Sort["sortBy((-count, country))"]
        Sort --> SortShuffle["Global sort may shuffle"]
        SortShuffle --> Take["take(top_n) action"]
    end

    Parse -. "invalid record" .-> Acc["Accumulator: invalid count"]
    Take --> Output["Top countries"]
```

`count()` and `take()` are separate actions and create separate jobs. The
accumulator is updated while `parse_and_count` handles malformed rows; the
`count()` result is the number of valid parsed rows. The DAG Scheduler divides
work into stages at shuffle boundaries. Exact stage layout depends on Spark's
execution details and partition configuration.

## Conceptual Cluster Deployment

```mermaid
flowchart LR
    Submit["spark-submit --master <cluster-master>"] --> Driver[Driver]
    Driver --> Session[SparkSession / SparkContext]
    Context[SparkContext] --> Manager["Cluster Manager: Standalone, YARN, or Kubernetes"]
    Session --> Context
    Manager --> Worker["Worker Node(s)"]
    Worker --> Executor["Executor process"]
    Executor --> Task["Tasks per partition"]
    Executor -. "concurrent task capacity" .-> Slots["Slots / executor cores"]
    Task --> Result[Results back to Driver]
```

This conceptual view distinguishes a Worker Node (the machine/resource host)
from an Executor (an application process launched on a worker). Slots describe
potential concurrent task capacity, commonly related to executor cores; they
are not separate Spark processes. This cluster topology is explanatory only,
not a claim of successful cluster execution for this project.