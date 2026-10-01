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

<!-- Owner: ID2. ip_country_map broadcast, invalid_log_counter and retry caveat. -->

### 2.4 Task 1 Result

<!-- Insert final Top 10 Countries result, invalid-record count and screenshot. -->

## 3. Spark Architecture and Deployment

### 3.1 Spark Application Architecture

Trong project này, `spark-submit` khởi chạy ứng dụng và chọn deployment master; `Driver` tạo `SparkSession`/`SparkContext` rồi điều phối RDD pipeline. Luồng thực tế là `textFile -> map(parse_and_count) -> filter -> cache -> count() -> map(country, 1) -> reduceByKey -> sortBy -> take(top_n)`. `count()` và `take()` là hai action riêng, vì vậy chúng tạo hai job riêng. `sortBy` sắp xếp count giảm dần, rồi country tăng dần khi count bằng nhau.

#### 3.1.1 Driver, SparkSession / SparkContext và ứng dụng này

`main.py` là điểm vào của ứng dụng. Khi chạy, ứng dụng tạo một `SparkSession` (và gián tiếp `SparkContext`) bằng `utils.py`, sau đó gọi `process_log_file()` trong `src/rdd_processing.py`. `SparkSession` là đối tượng điều phối ứng dụng Spark ở mức high-level; bên dưới nó là `SparkContext` — đối tượng cấp thấp nhất dùng để tạo RDD, đăng ký job và điều phối task. Driver là tiến trình chịu trách nhiệm xây dựng DAG, chia stage, gửi task và thu kết quả về.

Về mặt project này, Driver thực hiện các công việc sau:

- khởi tạo `SparkSession`; deployment master được chọn ở `spark-submit --master`, không bị application code ghi đè;
- đọc `data/raw_logs.txt` thành `RDD[String]` qua `sc.textFile(...)`;
- parse từng dòng bằng `map(parse_and_count)`;
- lọc các record hợp lệ bằng `filter(lambda r: r is not None)`;
- cache RDD đã parse/lọc rồi chạy `count()` để materialize trước action downstream;
- chuyển record hợp lệ thành cặp `(country, 1)`, gom bằng `reduceByKey`, rồi `sortBy(lambda item: (-item[1], item[0]))`;
- lấy `take(top_n)` để hiển thị Top N.

Các phép biến đổi RDD là lazy. `count()` là action thứ nhất: nó duyệt parsed RDD và materialize cache. `take(top_n)` là action thứ hai: nó đọc parsed RDD đã cache rồi kích hoạt aggregation và sort. `parse_and_count` tăng accumulator khi parser trả về record không hợp lệ; `count()` chỉ đếm record hợp lệ, không phải nguồn của invalid count.

#### 3.1.2 Cluster Manager, Worker Node, Executor

Trong mô hình Spark chuẩn:

- `Cluster Manager` là layer cấp phát tài nguyên: Standalone, YARN, Kubernetes.
- `Worker Node` là máy/Container được Cluster Manager cấp cho ứng dụng.
- `Executor` là tiến trình chạy trên Worker Node, chịu trách nhiệm thực thi task thực tế và lưu cache dữ liệu có thể tái sử dụng.

Trong project local demo, `spark-submit --master local[*]` chạy local scheduler trong JVM của Driver; không có Cluster Manager, Worker Node hay Executor process tách rời như cluster deployment. Các vai trò Worker/Executor được mô tả theo kiến trúc cluster tổng quát. YARN/Standalone/Kubernetes ở đây là kiến thức deployment chung, không phải xác nhận project đã chạy trên các cluster đó.

#### 3.1.3 Job, Stage, Task, DAG và RDD lineage

RDD là kiểu dữ liệu bất biến, phân tán và được xây dựng theo lineage. Mỗi transformation không tạo kết quả ngay. Trong code, job của `count()` chạy `textFile -> map/parse -> filter invalid -> cache`; sau đó job của `take(top_n)` tiếp tục từ parsed RDD đã cache: `map(country, 1) -> reduceByKey -> sortBy -> take(top_n)`. Driver biểu diễn các phụ thuộc này thành DAG (Directed Acyclic Graph).

- `Job` là đơn vị thực thi được tạo bởi một action. Trong repo, `count()` và `take()` đều tạo job. Job được phân thành nhiều `Stage`.
- `Stage` là tập hợp các task có thể chạy song song khi không cần shuffle.
- `Task` là đơn vị xử lý nhỏ nhất cho một partition của RDD, và mỗi task chạy trên một Executor.
- `Slot` là cách mô tả năng lực chạy task đồng thời, thường gắn với số core được cấp cho Executor; đây là khái niệm sức chứa, không phải một tiến trình Spark riêng.

Một điều rất quan trọng cần nhấn mạnh: không phải mọi transformation đều tạo stage mới. `map` và `filter` là narrow dependency. `reduceByKey` là wide transformation và tạo shuffle boundary để gom cùng country. `sortBy` sắp xếp theo khóa `(-count, country)` và cũng có thể cần shuffle để sắp xếp toàn cục. DAG Scheduler chia stage tại các ranh giới shuffle; không nên suy ra một số stage cố định chỉ từ sơ đồ khái niệm.

#### 3.1.4 Quan hệ Job → Stage → Task → Executor trong project thực tế

Dòng dữ liệu của repo có thể mô tả như sau:

- `sc.textFile(input_path)` tạo RDD dòng log.
- `map(parse_and_count)` và `filter(...)` xử lý dữ liệu từng partition. Đây là phần transform narrow.
- `count()` là action thứ nhất và tạo Job 1. Job này chạy `textFile -> map(parse_and_count) -> filter -> cache`; `count()` đếm record hợp lệ để materialize cache. Invalid count được cập nhật riêng bởi accumulator bên trong `parse_and_count`.
- Sau Job 1, `take(top_n)` là action thứ hai và tạo Job 2. Job này bắt đầu từ cached parsed RDD, rồi chạy `map(country, 1) -> reduceByKey -> sortBy -> take`.
- `reduceByKey` shuffle theo khóa `country`, tạo ranh giới giữa các stage. `sortBy` dùng thứ tự count giảm dần/country tăng dần và có thể tạo thêm shuffle cho global sort. Tasks được tạo theo partitions và thực thi theo scheduler/mode đang dùng.

Nói cách khác, `Job` được gắn với action; `Stage` được chia dựa trên dependency và shuffle; `Task` là phần thực thi trên từng partition; `Executor` là nơi task chạy.

#### 3.1.5 Driver điều phối như thế nào

Driver làm 3 việc chính:

1. Dựng plan: từ RDD lineage và action, Driver tạo DAG thực thi.
2. Chia stage: dựa trên narrow vs wide dependency; các partition trong cùng stage có thể chạy song song.
3. Gửi task: mỗi task được phân phối tới Executor trên Worker phù hợp, rồi Driver thu kết quả và trả về output.

Trong cluster mode, Driver gửi closures và tasks tới Executors trên Worker Nodes. Trong `local[*]`, tasks được local scheduler chạy trong cùng JVM với Driver, dùng các core local; không có Executors trên các Worker Node độc lập.

#### 3.1.6 Runtime architecture của local demo

```mermaid
flowchart TD
  A[spark-submit --master local[*]] --> B[Driver: src/main.py]
  B --> C[SparkSession]
  C --> D[SparkContext]
  D --> E[Local scheduler: local[*]]
  E --> F[Tasks on local cores]
  F --> G[Results returned to Driver]
```

Đây là luồng thực tế được cấu hình cho demo local. Không có hai Worker Node hay Executor process riêng được giả định hoặc tuyên bố đã kiểm thử. Diagram kiến trúc cluster tổng quát, có Cluster Manager/Worker/Executor, nằm tại `docs/spark_architecture.md`.

#### 3.1.7 Diagram luồng thực thi / DAG

```mermaid
flowchart TD
  subgraph J1[Job 1: count()]
    A[textFile] --> B[map parse_and_count]
    B --> C[filter valid]
    C --> D[cache]
    D --> E[count action]
  end
  subgraph J2[Job 2: take(top_n)]
    D --> F[map country, 1]
    F --> G[reduceByKey]
    G --> H[Shuffle boundary]
    H --> I[sortBy count desc, country asc]
    I --> K[take action]
  end
  B -. invalid record .-> L[Accumulator invalid_count]
  K --> M[Top countries]
```

`count()` và `take()` là hai action, không phải một Job duy nhất. `reduceByKey` cần shuffle để gộp cùng country; `sortBy` thực hiện sắp xếp sau bước tổng hợp. Accumulator được cập nhật lúc parse record lỗi, độc lập với giá trị do `count()` trả về.

#### 3.1.8 DataFrame/SQL plan và native RDD

DataFrame/SQL đi qua các bước logical plan, optimized logical plan và physical plan. Catalyst phân tích và tối ưu logical plan trước khi Spark thực thi physical plan. Native RDD transformations trong project này dùng RDD lineage và các scheduler của Spark; chúng không đi qua Catalyst optimizer như DataFrame/SQL.

### 3.2 spark-submit and Deploy Modes

#### 3.2.1 Deploy mode quyết định điều gì

Một ứng dụng Spark luôn gồm hai loại tiến trình. **Driver** chạy hàm `main()`,
dựng `SparkContext`, phân tích lineage, chia job thành stage/task và nhận kết
quả trả về. **Executor** là tiến trình worker thực thi task và giữ dữ liệu đã
cache.

Trong cluster deployment, Executors là process thực thi tasks trên Worker Nodes
được Cluster Manager cấp. Riêng `local[*]`, không có Cluster Manager, Worker
hay Executor process tách rời; local scheduler chạy tasks trong JVM của Driver
trên các core local. Với deployment dùng cluster manager, `--deploy-mode` quyết
định **Driver chạy ở đâu**.

```text
client mode                              cluster mode
-----------------------------------      -----------------------------------
[Máy submit]                             [Máy submit]
  └── Driver  ◄──── kết quả ────┐          └── spark-submit (thoát sau khi gửi)
        │                       │                     │
        ▼                       │                     ▼
[Cluster]                       │        [Cluster]
  Executor 1 ───────────────────┤          Driver  ◄── nằm TRONG cluster
  Executor 2 ───────────────────┤            ├── Executor 1
  Executor 3 ───────────────────┘            ├── Executor 2
                                             └── Executor 3
```

> `--deploy-mode` **không** liên quan tới `--master local[*]`. Ở chế độ `local`
> không có cluster nào cả — Driver và Executor cùng nằm trong một JVM trên một
> máy, và `--deploy-mode` bị bỏ qua.

#### 3.2.2 `--deploy-mode client`

Driver chạy ngay trên máy gõ lệnh `spark-submit`; máy submit trở thành một
thành phần đang chạy của ứng dụng.

- Log của Driver, `print()` và stack trace hiện thẳng ra terminal; kết quả của
  `collect()`, `take()`, `show()` hiển thị trực tiếp.
- Toàn bộ lưu lượng điều phối đi qua đường mạng giữa máy submit và cluster.
- **Tắt terminal hoặc mất mạng thì ứng dụng chết** — Driver không còn thì
  Executor cũng bị thu hồi.
- Máy submit phải cho phép Executor kết nối ngược về Driver; thường không khả
  thi nếu máy submit nằm sau NAT/VPN hoặc firewall chặn inbound.
- Driver dùng CPU/RAM của máy cá nhân, nên `collect()` trên tập lớn gây OOM ở
  chính laptop chứ không phải ở cluster.

**Dùng khi:** phát triển, gỡ lỗi, demo, và notebook tương tác (`spark-shell`,
`pyspark`, Jupyter — các công cụ này *chỉ* chạy được ở client mode vì cần vòng
lặp REPL).

#### 3.2.3 `--deploy-mode cluster`

Cluster Manager cấp phát một container/node trong cluster để chạy Driver;
`spark-submit` chỉ gửi đơn rồi thoát.

- Máy submit có thể tắt ngay sau khi submit, job vẫn chạy tiếp.
- Driver cùng mạng nội bộ với Executor nên độ trễ điều phối thấp hơn nhiều.
- Driver dùng tài nguyên cluster, khai báo qua `--driver-memory` /
  `--driver-cores`.
- **Không thấy log trực tiếp** — phải lấy qua Spark History Server,
  `yarn logs -applicationId <id>` hoặc `kubectl logs`.
- Cluster Manager có thể tự khởi động lại Driver khi nó chết (`--supervise` ở
  Standalone, `spark.yarn.maxAppAttempts` ở YARN).
- File phụ thuộc phải nằm ở nơi cluster truy cập được (HDFS, S3) hoặc được gửi
  kèm — **không** thể trỏ tới đường dẫn cục bộ trên máy submit.

**Dùng khi:** chạy chính thức, job dài, job theo lịch, job submit từ CI/CD.

#### 3.2.4 So sánh client và cluster

| Tiêu chí | `client` | `cluster` |
|---|---|---|
| Vị trí Driver | Máy submit | Node trong cluster |
| Vị trí Executor | Trong cluster | Trong cluster |
| Xem log Driver | Trực tiếp trên terminal | History Server / `yarn logs` / `kubectl logs` |
| Tắt máy submit | Ứng dụng chết | Ứng dụng vẫn chạy |
| Độ trễ mạng điều phối | Cao (qua WAN/VPN) | Thấp (trong cùng cluster) |
| Tài nguyên Driver | CPU/RAM máy cá nhân | Tài nguyên cluster |
| Tự khởi động lại Driver | Không | Có (nếu bật) |
| Yêu cầu mạng | Executor phải kết nối ngược về máy submit | Không cần |
| Vị trí file phụ thuộc | Đường dẫn cục bộ dùng được | Phải ở nơi chia sẻ được (HDFS/S3) |
| Shell tương tác | Có | Không |
| Trường hợp dùng | Dev, debug, demo | Chạy chính thức, job theo lịch |

#### 3.2.5 Ba Cluster Manager

**Cluster Manager** là thành phần cấp phát tài nguyên (CPU, RAM, container).
Nó trả lời câu hỏi *lấy máy ở đâu để chạy Driver và Executor* — tách biệt với
câu hỏi *Driver chạy ở đâu* của deploy mode.

Các master Standalone, YARN và Kubernetes dưới đây là ví dụ cấu hình deployment,
không phải kết quả chạy hoặc benchmark của project. Script demo hiện mặc định
truyền `local[*]` cho `spark-submit`.

> Mesos từng là Cluster Manager thứ tư nhưng đã bị deprecated từ Spark 3.2 và
> gỡ bỏ ở Spark 4.0, nên không trình bày ở đây.

**Standalone** — đi kèm sẵn trong bản phân phối Spark, gồm tiến trình Master và
các tiến trình Worker; không cần cài gì ngoài Spark và JVM.
`--master spark://<master-host>:7077`

- *Ưu:* cài đặt đơn giản nhất, gọn nhẹ, phù hợp cluster chỉ chạy Spark.
- *Nhược:* chỉ chạy được Spark, không chia sẻ tài nguyên với Hive/Flink/
  MapReduce; hàng đợi và phân quyền sơ khai (mặc định FIFO); Master là điểm
  chết đơn lẻ nếu không cấu hình HA bằng ZooKeeper.

**YARN** — bộ quản lý tài nguyên của hệ sinh thái Hadoop. Spark chạy như một
ứng dụng YARN, trong đó ApplicationMaster đàm phán container với
ResourceManager. `--master yarn`

- *Ưu:* chín muồi trong doanh nghiệp; chia sẻ cluster giữa nhiều framework; có
  hàng đợi phân cấp, quota và ưu tiên (Capacity/Fair Scheduler); tích hợp sẵn
  Kerberos; tận dụng được data locality với HDFS.
- *Nhược:* phải vận hành cả cụm Hadoop; cấu hình phức tạp; container dùng chung
  thư viện ở tầng host nên khó cô lập phụ thuộc giữa các job.
- Ở `cluster` mode Driver chạy *bên trong* ApplicationMaster; ở `client` mode
  ApplicationMaster chỉ xin container còn Driver ở máy submit.

**Kubernetes** — từ Spark 3.1 đã generally available. Driver và mỗi Executor
chạy trong một Pod riêng.
`--master k8s://https://<api-server>:<port>`

- *Ưu:* cô lập phụ thuộc triệt để nhờ container image riêng cho từng job; hợp
  hạ tầng cloud-native và CI/CD; dùng chung cluster với các workload khác.
- *Nhược:* cần biết vận hành Kubernetes; thời gian khởi động Pod làm tăng độ
  trễ với job ngắn; shuffle service và lưu trữ tạm phải cấu hình thêm.
- Chủ yếu dùng `cluster` mode; `client` mode yêu cầu Driver nằm trong Pod có
  địa chỉ mà Executor gọi ngược về được.

| Tiêu chí | Standalone | YARN | Kubernetes |
|---|---|---|---|
| Cần cài thêm | Không (kèm Spark) | Cụm Hadoop | Cụm Kubernetes |
| Chia sẻ với framework khác | Không | Có | Có |
| Cô lập phụ thuộc | Yếu | Trung bình | Mạnh (container image) |
| Hàng đợi / quota | Sơ khai (FIFO) | Mạnh (Capacity/Fair) | Qua namespace + quota |
| Bảo mật / xác thực | Cơ bản | Kerberos | RBAC + Secret |
| Độ phức tạp vận hành | Thấp | Cao | Cao |
| Data locality với HDFS | Nếu cùng node | Tốt nhất | Yếu (lưu trữ tách rời) |
| Deploy mode hỗ trợ | client + cluster | client + cluster | chủ yếu cluster |

#### 3.2.6 Ràng buộc đo được: Python worker không kế thừa `sys.path`

Phần này là **số liệu đo trên máy thật**, không phải lý thuyết.

Trên PySpark 3.5 — đúng phiên bản `requirements.txt` đang pin (`>=3.5,<4`) —
Python worker **không kế thừa `sys.path` của Driver**. Nếu phát lệnh từ thư mục
khác gốc repo, worker chết ngay ở stage 0: phía JVM báo `Connection reset by
peer`, thực chất là worker không `import` được module `rdd_processing` khi
unpickle hàm tham chiếu tới nó.

| Phiên bản | Chạy từ gốc repo | Chạy từ thư mục khác |
|---|---|---|
| PySpark 4.2.0 | 5/5 PASS | 5/5 PASS |
| **PySpark 3.5.9** | **3/3 PASS** | **0/3 — worker chết** |

Nguyên nhân được xác định bằng thí nghiệm có đối chứng: giữ nguyên thư mục phát
lệnh, chỉ thay đổi đúng một biến là `PYTHONPATH` của tiến trình worker — không
đặt thì FAIL, có đặt thì PASS với đúng 9.000 bản ghi hợp lệ và 1.000 lỗi. Vậy
nguyên nhân chắc chắn là worker không import được module, không phải do mạng,
cache hay dữ liệu. Gốc rễ là Spark pickle hàm cấp module **theo tham chiếu**
(tên module + qualname), nên worker bắt buộc phải import được module đó.

Hệ quả cho việc triển khai:

| Tình huống | Ảnh hưởng |
|---|---|
| `local[*]`, phát lệnh từ gốc repo | Không ảnh hưởng — đây là cấu hình demo |
| `local[*]`, phát lệnh từ thư mục khác | Job chết; khắc phục bằng cách cho `submit_job.sh` tự `cd` về gốc repo |
| `cluster` mode | Không né được bằng `cd` vì worker ở máy khác |

Cách xử lý tiêu chuẩn theo tài liệu Spark là gửi kèm module khi submit:

```bash
spark-submit \
  --master <master> \
  --deploy-mode cluster \
  --py-files src/rdd_processing.py,src/utils.py,src/generate_logs.py \
  src/main.py --input-path <input-path> --output-path <output-path>
```

**Chưa kiểm chứng được cách này trên máy Windows đang dùng:** `--py-files` và
`sc.addPyFile()` đều đi qua `FileUtil.chmod` của Hadoop, mà hàm này cần
`winutils.exe`; thiếu `HADOOP_HOME` thì lệnh hỏng ngay ở bước đăng ký file.
Đây là giới hạn của Windows chứ không phải của cơ chế `--py-files`. ID5 cần xác
nhận lại khi dựng môi trường triển khai thật.

### 3.3 Deployment Command and Reproducibility

#### 3.3.1 Project entry point

Khi chạy bằng `python main.py`, file ở root chuyển tiếp tới `src/main.py`. Với `spark-submit`, entry point là `src/main.py`. Luồng ứng dụng:

```text
main.py (root wrapper) -> src/main.py
  ↓
parse CLI arguments
  ↓
resolve input path
  ↓
create SparkSession / SparkContext via utils.py
  ↓
call process_log_file() in src/rdd_processing.py
  ↓
print Top 10 countries and invalid count
  ↓
optional save summary to CSV
  ↓
stop Spark session
```

Luồng submit là `submit_job.sh -> spark-submit --master "$SPARK_MASTER" -> src/main.py`. Script mặc định `SPARK_MASTER=local[*]`; có thể đặt biến này để chọn master khác. `src/main.py` không truyền master vào `SparkSession.builder`, vì vậy master từ `spark-submit` được giữ nguyên. Khi chạy Python trực tiếp để development, PySpark dùng local mode mặc định nếu không có cấu hình submit.

Cấu trúc thực tế:

- `main.py` ở root repo đóng vai trò launcher để chạy ứng dụng từ mọi thư mục.
- `src/main.py` giữ logic chính thực thi và có fallback import cho cả trường hợp chạy như script và chạy như package.
- `src/utils.py` chứa các helper thực sự hữu ích: `repo_root()`, `resolve_input_path()`, `create_spark_session()`, `parse_arguments()`.
- `submit_job.sh` ở root repo truyền `SPARK_MASTER` (mặc định `local[*]`) vào `spark-submit --master` và chạy ở `--deploy-mode client`.

#### 3.3.2 Reproducibility flow

Để chạy lại trên máy local Linux/macOS, thao tác chuẩn là:

```bash
cd <repo-root>
pip install -r requirements.txt
python src/generate_logs.py
bash submit_job.sh
```

Trên Windows, vì Spark worker cần Python interpreter rõ ràng và `python` alias có thể bị app execution alias chặn, cách an toàn hơn là chạy Python trực tiếp trước khi gọi Spark hoặc đặt biến môi trường `PYSPARK_PYTHON` tới file `python.exe` thực tế:

```powershell
cd "C:\path\to\bigdata-spark-project-group-3"
$env:PYSPARK_PYTHON = "C:\Users\Admin\AppData\Local\Programs\Python\Python312\python.exe"
python src/generate_logs.py
spark-submit --master local[*] --deploy-mode client --name LowLevel_FileFormat_Job --driver-memory 2g --executor-memory 2g --conf spark.sql.shuffle.partitions=10 --py-files src/rdd_processing.py,src/utils.py,src/generate_logs.py src/main.py --input-path data/raw_logs.txt --output-path output/top_countries.csv
```

Lưu ý: câu lệnh trên là cách dùng thực tế trong môi trường Windows lúc kiểm thử. Nó không phải là một “mô hình triển khai cluster” đầy đủ, mà là cách chạy demo local phù hợp với môi trường này.

#### 3.3.3 `utils.py` và chức năng hữu ích

`utils.py` không cần xây dựng nhiều abstraction. Trong repo, các function hữu ích đã được giữ lại và tối giản theo đúng nhu cầu:

- `repo_root()` trả về đường dẫn gốc của repo;
- `resolve_input_path()` chuyển `--input-path data/raw_logs.txt` thành đường dẫn tuyệt đối dựa theo repo root;
- `create_spark_session()` tạo `SparkSession` với app name mà không đặt master;
- `parse_arguments()` xử lý `--input-path`, `--output-path`, `--top-n`, `--app-name`.

Điều này giúp `main.py` dễ tái sử dụng mà không cần mang theo code xử lý path và config rời rạc trong nhiều file.

#### 3.3.4 `submit_job.sh`

File `submit_job.sh` ở repo root thể hiện lệnh deploy chuẩn của ứng dụng:

```bash
#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

export PYTHONPATH="$REPO_ROOT/src:${PYTHONPATH:-}"

spark-submit \
  --master "${SPARK_MASTER:-local[*]}" \
  --deploy-mode client \
  --name "LowLevel_FileFormat_Job" \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.sql.shuffle.partitions=10 \
  --py-files "$REPO_ROOT/src/rdd_processing.py,$REPO_ROOT/src/utils.py,$REPO_ROOT/src/generate_logs.py" \
  "$REPO_ROOT/src/main.py" \
  --input-path "$REPO_ROOT/data/raw_logs.txt" \
  --output-path "$REPO_ROOT/output/top_countries.csv"
```

Lệnh này bao gồm các thành phần bắt buộc:

- `--master "$SPARK_MASTER"`: chọn deployment master ở `spark-submit`; giá trị mặc định của script là `local[*]`.
- `--deploy-mode client`: Driver chạy trên máy submit (đúng với local demo), không phải trên cluster manager.
- `src/main.py`: entry point của ứng dụng.
- `--py-files`: gửi kèm các module Python cần import trên worker để tránh lỗi `No module named rdd_processing` hoặc `No module named utils` khi Spark tạo Python worker.
- `--input-path` và `--output-path`: đường dẫn dữ liệu đầu vào và file lưu kết quả tiểu kết.

#### 3.3.5 Ghi chú thực tế về Windows và `--py-files`

Trong repo hiện tại, việc sử dụng `--py-files` là hợp lý vì Spark Python worker phải import được module được tham chiếu trong closure. Tuy nhiên, ở môi trường Windows đang kiểm thử, việc chạy `spark-submit` vẫn bị chặn bởi vấn đề hệ thống: Python worker không tìm thấy interpreter Python và Spark báo `Python was not found`; đồng thời còn có cảnh báo `HADOOP_HOME` / `winutils.exe` chưa được cấu hình. Đây là giới hạn của hệ thống local hiện tại, không phải là lỗi logic của project.

Do đó, kết luận đáng tin cậy nhất là:

- Python unit tests và import logic đã được kiểm tra thành công;
- `main.py` và `src/main.py` có thể chạy đúng ở mức Python đơn thuần nếu môi trường Spark local hợp lệ;
- `spark-submit` local trên Windows cần cấu hình thêm `PYSPARK_PYTHON` và có thể cần cài `winutils`/`HADOOP_HOME` nếu chạy trên môi trường cluster hoặc multi-node.

Vì vậy, các bước dùng để tái tạo quá trình chạy Spark cần được ghi rõ là “được kiểm thử ở mức source code và local Python smoke test”, còn phần `spark-submit` cluster/Windows runtime cần xác nhận lại trong môi trường triển khai thực.

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
