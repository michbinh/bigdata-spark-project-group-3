# Spark Application Runtime Architecture

The diagram combines the project's local RDD execution path with a separate,
conceptual cluster-deployment branch. Only the `local[*]` path represents the
project demo; cluster deployment has not been tested.

```mermaid
flowchart TD
    Submit["spark-submit --master local[*]"] --> Application["Spark Application"]
    Application --> Driver["Driver: src/main.py"]
    Driver --> Session["SparkSession"]
    Session --> Context["SparkContext"]
    Context --> Scheduler["DAG Scheduler"]

    subgraph Job1["Job 1 — triggered by count()"]
        Raw["raw_logs"] --> TextFile["textFile(input_path)"]
        TextFile --> Parse["map(parse_and_count)"]
        Parse --> Filter["filter(valid records)"]
        Filter --> Cache["cache parsed RDD"]
        Cache --> Count["count() action"]
        Count -. "action creates Job 1" .-> CountJob["Job 1"]
    end
    CountJob --> Scheduler
    Scheduler --> J1Stage["Job 1 stage: narrow read / parse / filter"]
    J1Stage --> J1Tasks["Tasks per input partition"]

    subgraph Job2["Job 2 — triggered by take(10)"]
        Cache --> Country["map(country, 1)"]
        Country --> ReduceMap["reduceByKey: map-side combine"]
        ReduceMap --> ReduceShuffle["Shuffle boundary: reduceByKey"]
        ReduceShuffle --> ReduceMerge["reduceByKey: merge by country"]
        ReduceMerge --> Sort["sortBy(count desc, country asc)"]
        Sort --> Take["take(10) action"]
        Sort -. "possible shuffle / stage boundary" .-> SortShuffle["sortBy global-sort shuffle (if required)"]
        SortShuffle -. "if required" .-> Take
        Take -. "action creates Job 2" .-> TakeJob["Job 2"]
    end
    TakeJob --> Scheduler
    Scheduler --> J2Before["Job 2 stage(s) before reduceByKey shuffle"]
    J2Before --> J2BeforeTasks["Tasks per partition"]
    Scheduler --> J2After["Job 2 stage(s) after reduceByKey shuffle"]
    J2After --> J2AfterTasks["Tasks per partition"]
    Scheduler -. "only if sortBy creates another shuffle" .-> J2SortStage["Additional sort stage(s)"]
    J2SortStage --> J2SortTasks["Tasks per partition"]
    Take --> TopCountries["Top 10 Countries"]

    J1Tasks --> LocalScheduler["Local scheduler: local[*]"]
    J2BeforeTasks --> LocalScheduler
    J2AfterTasks --> LocalScheduler
    J2SortTasks -. "if present" .-> LocalScheduler
    LocalScheduler --> LocalExecution["Local cores in the Driver JVM / local execution context"]
    LocalExecution --> Driver

    subgraph ClusterConcept["Cluster deployment — conceptual only; not this local E2E"]
        ClusterSubmit["spark-submit --master cluster-master"] --> ClusterApp["Spark Application"]
        ClusterApp --> ClusterDriver["Driver (client or cluster deploy mode)"]
        ClusterDriver --> ClusterManager["Cluster Manager: Standalone / YARN / Kubernetes"]
        ClusterManager --> Worker["Worker Node"]
        Worker --> Executor["Executor process"]
        Executor --> ClusterTasks["Tasks"]
        Executor -. "concurrent task capacity" .-> Slots["Slots / executor cores"]
    end

    classDef conceptual stroke-dasharray: 5 5
    class ClusterSubmit,ClusterApp,ClusterDriver,ClusterManager,Worker,Executor,ClusterTasks,Slots conceptual
```

RDD transformations build lineage/DAG lazily; an action triggers a job. The DAG
Scheduler divides that job into stages according to dependencies and shuffle
boundaries, then stages run as tasks over partitions. In this pipeline,
`reduceByKey` has a shuffle boundary; `sortBy` may introduce another shuffle and
stage boundary depending on runtime behavior. The diagram describes stage
groups, not fixed stage or task counts: exact execution depends on Spark runtime,
input partitions, and configuration.

In `local[*]`, tasks use the local scheduler and local cores in the Driver JVM;
there is no separate Worker Node or Executor process. The dashed cluster branch
shows a conceptual distributed deployment only and is not a tested execution
path for this project.
