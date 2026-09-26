# Big Data Project Report

> **Project:** Low-level RDD Processing, File-format Benchmarking and Spark Deployment  
> **Team:** [Update team name]  
> **Last updated:** [YYYY-MM-DD]

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [RDD Processing and Shared Variables](#2-rdd-processing-and-shared-variables)
3. [Spark Architecture and Deployment](#3-spark-architecture-and-deployment)
4. [File Formats and Partition Benchmark](#4-file-formats-and-partition-benchmark)
5. [Debugging and Performance Optimisation](#5-debugging-and-performance-optimisation)
6. [Results and Conclusion](#6-results-and-conclusion)
7. [References](#7-references)

## 1. Project Overview

### 1.1 Objective

<!-- Team lead: define problem, input, expected output and scope. -->

### 1.2 Repository Structure and Reproducibility

<!-- Describe how to run from a clean clone. -->

## 2. RDD Processing and Shared Variables

### 2.1 RDD Fundamentals

<!-- Owner: ID1. RDD definition, immutability, lineage, lazy evaluation,
transformations vs actions, fault tolerance and RDD vs DataFrame comparison. -->

### 2.2 Core RDD Pipeline

<!-- Owner: ID1. textFile -> parse -> filter -> Pair RDD -> reduceByKey -> Top 10. -->

### 2.3 Broadcast Variables and Accumulators

When Spark executes a transformation, the function passed to it (for example the
lambda inside `map()`) is serialised and shipped to **every task** on the
executors. Any variable the function references is copied along with it. This
creates two problems: a large read-only variable is re-sent many times over, and
any change an executor makes to its copy never travels back to the driver.

**Shared variables** are the mechanism Spark provides for exactly these two
problems. Spark offers two kinds, serving opposite directions of data flow:

| Kind | Direction | Property | Purpose |
|---|---|---|---|
| Broadcast variable | Driver → Executor | Read-only | Distribute shared lookup data |
| Accumulator | Executor → Driver | Add-only | Aggregate statistics during execution |

#### 2.3.1 Broadcast Variable

A **broadcast variable** is a read-only value that the driver sends to each
executor **exactly once** and that the executor then caches in memory, instead
of shipping it with every task.

The driver creates one with `sc.broadcast(value)`. Spark splits the value into
blocks and distributes them peer-to-peer: an executor that already holds a block
can forward it to another executor, so the load on the driver does not grow
linearly with the number of nodes. On the executor side, code reads the value
through the `.value` attribute.

In this project, a broadcast variable carries the `ip_country_map` lookup table,
which maps an IPv4 `/24` prefix to a country name. Every record needs this table
during the enrichment step, so without broadcasting it would be serialised again
for each task:

```python
# Driver
ip_country_broadcast = sc.broadcast(IP_COUNTRY_MAP)

# Executor
def enrich_with_country(record):
    prefix = record["ip"].rsplit(".", 1)[0]
    record["country"] = ip_country_broadcast.value.get(prefix)
    return record

enriched_rdd = parsed_rdd.map(enrich_with_country)
```

A concrete illustration: suppose a job has 200 tasks and the lookup table is
10 MB. Without broadcasting, Spark transfers `200 × 10 MB = 2 GB` over the
network. With broadcasting, it transfers 10 MB to each executor — 100 MB across
10 executors, roughly a twenty-fold reduction.

A broadcast variable is **immutable**. If the source data changes after
broadcasting, the distributed copy does not update itself; the variable must be
released with `unpersist()` or `destroy()` and a new one broadcast in its place.

**When not to use a broadcast variable:**

1. **The data is too large for executor memory.** A broadcast variable must fit
   entirely in the memory of *every* executor. A table of several gigabytes will
   trigger `OutOfMemoryError` or push executors into a garbage-collection loop.
   For large data, a shuffle join lets Spark partition the work instead.
2. **The data is trivially small.** For a single constant or a list of a few
   elements, the bookkeeping cost of a broadcast variable outweighs the benefit;
   letting Spark serialise it with the closure is enough.
3. **The data is used once, or changes constantly.** Broadcasting pays off
   through repeated reuse. If the value is consulted only once, or must be
   destroyed and rebuilt every few operations, the distribution cost is never
   recovered.
4. **The data must be written to from executors.** Broadcast variables are
   read-only; mutating `.value` on an executor changes only that local copy.
   That requirement belongs to an accumulator.

#### 2.3.2 Accumulator

An **accumulator** is an add-only variable that lets executors send figures back
to the driver. The driver creates one with `sc.accumulator(0)`; each task keeps a
local copy and calls `.add()` on it; when a task **completes**, Spark sends its
local contribution to the driver and merges it into the running total.

Only the driver may read `.value`. Reading `.value` on an executor raises an
error, because at that point only the local value of the currently running task
exists, not the global total.

In this project, an accumulator counts malformed log records without a second
pass over the data and without stopping the job:

```python
# Driver
invalid_log_counter = sc.accumulator(0)

# Executor
def parse_with_counter(line):
    record = parse_log_line(line)
    if record is None:
        invalid_log_counter.add(1)
    return record

parsed_rdd = raw_rdd.map(parse_with_counter).filter(lambda r: r is not None)
parsed_rdd.cache()
parsed_rdd.count()                  # action that materialises the whole dataset

print(invalid_log_counter.value)    # driver only, and only after an action
```

Because Spark evaluates lazily, `invalid_log_counter.value` is **meaningful only
after an action has run**. Reading it beforehand always returns 0 — not because
there are no malformed records, but because the lineage has not been executed at
all.

#### 2.3.3 Limitation: accumulators are not exactly-once

The guarantee Spark offers depends on where `.add()` is called:

| Location of `.add()` | Spark's guarantee |
|---|---|
| Inside an **action** (for example `foreach`) | **Exactly-once** — Spark discards updates from retried tasks |
| Inside a **transformation** (for example `map`) | **No guarantee** — the count may be inflated or short |

Within a transformation, three situations distort the count:

1. **A task fails and is retried.** If a task dies partway through, Spark
   reschedules that partition. The contribution of the failed attempt may or may
   not already have been merged, which can double-count.
2. **Speculative execution.** With `spark.speculation=true`, Spark launches a
   duplicate of a slow task. Both copies add to the accumulator even though only
   one result is used.
3. **Lineage recomputation (the most common cause).** An RDD does not retain its
   results. If an uncached RDD feeds two actions, Spark replays the whole lineage
   for the second one, and the `map` containing `.add()` runs again from the
   start — doubling the count.

**The mitigation used in this project** is to cache the parsed and filtered RDD,
then materialise it exactly once with a single action before any downstream
action runs:

```python
parsed_rdd.cache()     # or .persist()
parsed_rdd.count()     # materialises the whole dataset exactly once
```

`count()` is preferred over `collect()`, which pulls the entire dataset to the
driver and risks exhausting driver memory, and over `take(n)`, which reads only
enough elements to satisfy `n` and therefore leaves the remaining partitions
unvisited, producing an undercount.

Even with caching, this remains risk reduction rather than a guarantee: it does
**not** make an accumulator exactly-once. If a cached partition is lost, Spark
rebuilds it from the lineage and the count drifts again. Accumulators are
therefore suited to observability and debugging figures, and should **not** be
treated as the source of truth for a business result. Where an exact number is
required, derive it from a deterministic transformation over the cached RDD —
for example `raw_rdd.count() - parsed_rdd.count()`.

#### 2.3.4 Classification of malformed records

The generator `src/generate_logs.py` produces `data/raw_logs.txt` with 90% valid
records and 10% malformed ones. The malformed 10% is split evenly across **four
error types**, which are precisely the cases where `parse_log_line()` returns
`None` and the accumulator counts.

The four types are checked **in the priority order below, stopping at the first
match**, so every malformed record belongs to exactly one type:

| # | Error type | Condition | Example |
|---|---|---|---|
| 1 | **Missing field** | Fewer fields parsed than the schema requires | `10.10.1.7 - - [22/Sep/2026:08:15:03 +0700] "GET /cart HTTP/1.1" 200` (no `bytes`) |
| 2 | **Malformed IPv4** | All fields present, but the IP field is not valid IPv4 | `999.12.44 - - [...] "GET / HTTP/1.1" 200 1234` |
| 3 | **Non-numeric status code** | All fields present, IP valid, status does not parse as an integer | `10.20.1.9 - - [...] "GET / HTTP/1.1" OK 1234` |
| 4 | **Malformed request** | All fields present, IP and status valid, but the method, endpoint or HTTP version is structurally wrong | `10.30.1.4 - - [...] "GET-only" 200 1234` |

The priority order is essential because one record can violate several
conditions at once. A line that both omits the `bytes` field and carries a bad
IP is classified as type 1, not type 2 — until the fields have been separated,
nothing can be concluded about the contents of any individual field. Because of
this rule, the four type counts always sum to exactly the number of malformed
records, with no record counted twice.

#### 2.3.5 Verification results

Executed on PySpark 3.5.9 (`local[*]`, OpenJDK 17) against a 10,000-line
`data/raw_logs.txt`:

| Measure | Result |
|---|---|
| Valid records | 9,000 (90.00%) |
| `invalid_log_counter.value` | 1,000 (10.00%) |
| Count after three further downstream actions | still 1,000 — no double counting |
| Records resolved to a country by the broadcast lookup | 9,000 of 9,000, across 14 countries |
| Sum of values from `reduceByKey(country)` | 9,000 = the number of valid records |
| Valid plus malformed | 9,000 + 1,000 = 10,000 = total input lines |

### 2.4 Task 1 Result

<!-- Insert final Top 10 Countries result, invalid-record count and screenshot. -->

## 3. Spark Architecture and Deployment

### 3.1 Spark Application Architecture

<!-- Owner: ID5. Driver, Cluster Manager, Worker, Executor, Job, Stage and Task. -->

### 3.2 spark-submit and Deploy Modes

#### 3.2.1 What deploy mode decides

A Spark application always comprises two kinds of process. The **driver** runs
`main()`, builds the `SparkContext`, analyses the lineage, splits the job into
stages and tasks, and receives the results. The **executors** are the worker
processes that run those tasks and hold cached data.

Executors **always** run on cluster nodes. The only thing `--deploy-mode`
decides is: **where the driver runs.**

```text
client mode                              cluster mode
-----------------------------------      -----------------------------------
[Submit machine]                         [Submit machine]
  └── Driver  ◄──── results ────┐          └── spark-submit (exits after submit)
        │                       │                     │
        ▼                       │                     ▼
[Cluster]                       │        [Cluster]
  Executor 1 ───────────────────┤          Driver  ◄── runs INSIDE the cluster
  Executor 2 ───────────────────┤            ├── Executor 1
  Executor 3 ───────────────────┘            ├── Executor 2
                                             └── Executor 3
```

> `--deploy-mode` is unrelated to `--master local[*]`. In `local` mode there is
> no cluster at all — the driver and executors share a single JVM on one
> machine — and `--deploy-mode` is ignored.

#### 3.2.2 `--deploy-mode client`

The driver runs on the machine that issued `spark-submit`, which makes that
machine a live component of the application.

- Driver logs, `print()` output and stack traces appear directly in the
  terminal; results of `collect()`, `take()` and `show()` are displayed
  immediately.
- All scheduling traffic crosses the network between the submit machine and the
  cluster.
- **Closing the terminal or losing the network kills the application** — once
  the driver is gone, the executors are reclaimed.
- The submit machine must accept inbound connections from the executors back to
  the driver, which is often impossible behind NAT, a VPN or a firewall.
- The driver consumes local CPU and memory, so `collect()` on a large dataset
  exhausts the laptop's memory rather than the cluster's.

**Use for:** development, debugging, demonstrations, and interactive notebooks
(`spark-shell`, `pyspark`, Jupyter — these tools run *only* in client mode
because they require an interactive REPL loop).

#### 3.2.3 `--deploy-mode cluster`

The cluster manager allocates a container or node inside the cluster to host the
driver; `spark-submit` merely submits the application and exits.

- The submit machine can be shut down immediately afterwards and the job
  continues.
- The driver shares the cluster's internal network with the executors, so
  scheduling latency is far lower.
- The driver consumes cluster resources, declared with `--driver-memory` and
  `--driver-cores`.
- **Logs are not visible directly** — they must be retrieved through the Spark
  History Server, `yarn logs -applicationId <id>` or `kubectl logs`.
- The cluster manager can restart the driver automatically when it dies
  (`--supervise` on Standalone, `spark.yarn.maxAppAttempts` on YARN).
- Dependencies must live somewhere the cluster can reach (HDFS, S3) or be
  shipped with the job; they **cannot** be referenced by a local path on the
  submit machine.

**Use for:** production runs, long-running jobs, scheduled jobs and jobs
submitted from CI/CD.

#### 3.2.4 Comparison of client and cluster mode

| Criterion | `client` | `cluster` |
|---|---|---|
| Driver location | Submit machine | Node inside the cluster |
| Executor location | Inside the cluster | Inside the cluster |
| Viewing driver logs | Directly in the terminal | History Server / `yarn logs` / `kubectl logs` |
| Shutting down the submit machine | Application dies | Application keeps running |
| Scheduling latency | High (across WAN/VPN) | Low (within the cluster) |
| Driver resources | Local CPU and memory | Cluster resources |
| Automatic driver restart | No | Yes, if enabled |
| Network requirement | Executors must reach the submit machine | None |
| Dependency location | Local paths work | Must be on shared storage (HDFS/S3) |
| Interactive shell | Supported | Not supported |
| Typical use | Development, debugging, demos | Production, scheduled jobs |

#### 3.2.5 The three cluster managers

A **cluster manager** allocates resources — CPU, memory, containers — to a Spark
application. It answers *where the machines come from* to run the driver and
executors, which is a separate question from *where the driver runs*, decided by
the deploy mode.

> Mesos was formerly a fourth cluster manager but was deprecated in Spark 3.2
> and removed in Spark 4.0, so it is not covered here.

**Standalone** ships with the Spark distribution itself and consists of a master
process and worker processes. Nothing beyond Spark and a JVM is required.
`--master spark://<master-host>:7077`

- *Strengths:* the simplest to set up, lightweight, few moving parts. Well
  suited to a cluster dedicated to Spark.
- *Weaknesses:* runs Spark only, so resources cannot be shared with Hive, Flink
  or MapReduce; queueing and access control are rudimentary (FIFO by default);
  the master is a single point of failure unless configured for high
  availability with ZooKeeper.

**YARN** is the resource manager of the Hadoop ecosystem. Spark runs as an
ordinary YARN application in which an ApplicationMaster negotiates containers
with the ResourceManager. `--master yarn`

- *Strengths:* mature in enterprise environments; shares one cluster across
  several frameworks; offers hierarchical queues, quotas and priorities through
  the Capacity and Fair schedulers; integrates with Kerberos; exploits data
  locality with HDFS.
- *Weaknesses:* requires operating a full Hadoop cluster; configuration is
  involved; containers share host-level libraries, making dependency isolation
  between jobs difficult.
- In `cluster` mode the driver runs *inside* the ApplicationMaster. In `client`
  mode the ApplicationMaster only requests containers while the driver stays on
  the submit machine.

**Kubernetes** support has been generally available since Spark 3.1. The driver
and each executor run in their own pod.
`--master k8s://https://<api-server>:<port>`

- *Strengths:* thorough dependency isolation, since each job carries its own
  container image; fits cloud-native infrastructure and CI/CD; shares a cluster
  with other kinds of workload.
- *Weaknesses:* requires Kubernetes operational knowledge; pod start-up time
  adds latency to short jobs; the shuffle service and temporary storage need
  additional configuration.
- Primarily used in `cluster` mode; `client` mode requires the driver to sit in
  a pod with an address the executors can call back to.

| Criterion | Standalone | YARN | Kubernetes |
|---|---|---|---|
| Additional installation | None (ships with Spark) | Hadoop cluster | Kubernetes cluster |
| Shared with other frameworks | No | Yes | Yes |
| Dependency isolation | Weak | Moderate | Strong (container image) |
| Queues and quotas | Rudimentary (FIFO) | Strong (Capacity/Fair) | Namespaces and quotas |
| Security and authentication | Basic | Kerberos | RBAC and secrets |
| Operational complexity | Low | High | High |
| HDFS data locality | Only if co-located | Best | Weak (storage is separate) |
| Deploy modes supported | client and cluster | client and cluster | mainly cluster |

#### 3.2.6 Measured constraint: Python workers do not inherit `sys.path`

This subsection reports figures measured on a real machine rather than theory.

On PySpark 3.5 — the version pinned in `requirements.txt` (`>=3.5,<4`) — Python
workers **do not inherit the driver's `sys.path`**. A worker that cannot import
the module holding a pickled function dies at stage 0: the JVM reports
`Connection reset by peer`, which is in fact the worker failing to `import`
`rdd_processing` while unpickling a function that references it.

Because this repository keeps its modules in `src/`, the workers' working
directory never contains them, so the failure occurs **even when the job is
launched from the repository root**. This was measured against the repository
layout itself:

| Module location | Launched from repository root | PySpark |
|---|---|---|
| Repository root (earlier scaffold) | 3/3 passed | 3.5.9 |
| **`src/` (this repository)** | **fails — worker dies** | 3.5.9 |
| `src/`, with `src` on the worker's path | 11/11 checks passed | 3.5.9 |
| Either location | passes | 4.2.0 |

The cause was established by a controlled experiment: holding the launch
directory fixed and varying only the worker process's `PYTHONPATH`. Without it
the run failed; with it the run passed, reporting exactly 9,000 valid records
and 1,000 malformed ones. The cause is therefore conclusively the failed module
import, not networking, caching or the data itself. The underlying reason is
that Spark pickles module-level functions **by reference** — module name plus
qualified name — so the worker must be able to import that module.

Consequences for deployment. Note that, unlike a layout with modules at the
repository root, the `src/` layout leaves no configuration in which this can be
ignored:

| Situation | Impact |
|---|---|
| `local[*]`, launched from the repository root | **Fails** — `src/` is not on the workers' path |
| `local[*]`, with `src` exported on `PYTHONPATH` or shipped with `--py-files` | Passes |
| `cluster` mode | Fails unless the modules are shipped; the workers are on other machines, so no local path helps |

The standard remedy in the Spark documentation is to ship the modules with the
application:

```bash
spark-submit \
  --master <master> \
  --deploy-mode cluster \
  --py-files src/rdd_processing.py,src/generate_logs.py \
  src/main.py --input <input-path>
```

**This remedy could not be verified on the Windows machine used here.** Both
`--py-files` and `sc.addPyFile()` route through Hadoop's `FileUtil.chmod`, which
requires `winutils.exe`; without `HADOOP_HOME` the call fails while registering
the file, before the job starts. This is a limitation of Windows rather than of
the `--py-files` mechanism, and should be confirmed on the real deployment
environment.

### 3.3 Deployment Command and Reproducibility

<!-- Owner: ID5. Document main.py, utils.py, submit_job.sh and command. -->

## 4. File Formats and Partition Benchmark

### 4.1 Row-based versus Columnar Formats

<!-- Owner: ID3. CSV/JSON versus Parquet/ORC. -->

### 4.2 Write Benchmark: Size and Time

<!-- Owner: ID3. One canonical result table; include compression configuration. -->

### 4.3 Read and Partition Benchmark

<!-- Owner: ID4. Query time, repartition, coalesce, partitionBy and small-file problem. -->

## 5. Debugging and Performance Optimisation

### 5.1 Data Skew and Slow Joins

<!-- Owner: ID6. Hot keys, salting, repartitioning and when broadcast join is valid. -->

### 5.2 Garbage Collection and Tungsten

<!-- Owner: ID6. Reducing GC pressure, not eliminating GC. -->

### 5.3 Driver OOM and Executor OOM

<!-- Owner: ID6. Separate causes and mitigations. -->

## 6. Results and Conclusion

<!-- Owner: ID6. Summarise validated results and limitations. -->

## 7. References

<!-- Use consistent citation format. -->
