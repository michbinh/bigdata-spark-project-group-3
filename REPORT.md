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

Khi Spark thực thi một transformation, hàm được truyền vào (ví dụ hàm lambda
trong `map()`) sẽ được serialize và gửi kèm tới **từng task** trên các Executor.
Mọi biến mà hàm đó tham chiếu cũng được sao chép theo. Hệ quả là hai vấn đề:
biến chỉ-đọc có kích thước lớn bị gửi lặp lại rất nhiều lần, và mọi thay đổi mà
Executor thực hiện trên bản sao đều không quay ngược về Driver.

**Shared Variables** là cơ chế Spark cung cấp để giải quyết đúng hai vấn đề
này. Spark có hai loại, phục vụ hai chiều dữ liệu ngược nhau:

| Loại | Chiều dữ liệu | Tính chất | Dùng để |
|---|---|---|---|
| Broadcast Variable | Driver → Executor | Chỉ đọc (read-only) | Phát tán dữ liệu tra cứu dùng chung |
| Accumulator | Executor → Driver | Chỉ ghi thêm (add-only) | Gom số liệu thống kê trong lúc chạy |

#### 2.3.1 Broadcast Variable

**Broadcast Variable** là biến chỉ đọc được Driver gửi tới mỗi Executor **đúng
một lần** và cache lại trong bộ nhớ của Executor đó, thay vì gửi kèm theo từng
task.

Driver tạo biến bằng `sc.broadcast(value)`. Spark chia biến thành các block và
phân phối theo cơ chế ngang hàng (peer-to-peer): Executor đã nhận được block có
thể chuyển tiếp cho Executor khác, nên tải trên Driver không tăng tuyến tính
theo số node. Phía Executor, code đọc dữ liệu qua thuộc tính `.value`.

Trong đồ án này, Broadcast được dùng cho bảng tra cứu `ip_country_map` (ánh xạ
tiền tố IP `/24` sang tên quốc gia). Bảng này cần thiết ở mọi bản ghi trong
bước làm giàu dữ liệu, nên nếu không broadcast thì nó sẽ bị serialize lại cho
từng task:

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

Lợi ích cụ thể: giả sử job có 200 task và bảng tra cứu nặng 10 MB. Không dùng
Broadcast, Spark phải truyền `200 × 10 MB = 2 GB` qua mạng. Dùng Broadcast, chỉ
truyền 10 MB tới mỗi Executor — với 10 Executor là 100 MB, giảm khoảng 20 lần.

Broadcast là **bất biến**. Nếu dữ liệu nguồn thay đổi sau khi đã broadcast,
biến đã phát tán không tự cập nhật; phải giải phóng bằng `unpersist()` hoặc
`destroy()` rồi broadcast lại một biến mới.

**Khi nào KHÔNG nên dùng Broadcast:**

1. **Dữ liệu quá lớn so với bộ nhớ Executor.** Broadcast Variable phải nằm trọn
   trong bộ nhớ của *mỗi* Executor. Một bảng vài GB sẽ gây `OutOfMemoryError`
   hoặc đẩy Executor vào vòng lặp GC. Với dữ liệu lớn, nên dùng shuffle join để
   Spark chia nhỏ theo partition.
2. **Dữ liệu quá nhỏ.** Với một hằng số hay list vài phần tử, chi phí quản lý
   Broadcast lớn hơn lợi ích; cứ để Spark serialize kèm closure.
3. **Dữ liệu chỉ dùng một lần hoặc thay đổi liên tục.** Broadcast có lợi nhờ
   được tái sử dụng nhiều lần; nếu phải `destroy()` và tạo lại liên tục thì chi
   phí phát tán không được bù lại.
4. **Dữ liệu cần ghi/cập nhật từ Executor.** Broadcast là read-only. Sửa
   `.value` trên Executor chỉ sửa bản sao cục bộ. Nhu cầu này thuộc về
   Accumulator.

#### 2.3.2 Accumulator

**Accumulator** là biến chỉ được cộng dồn, cho phép Executor gửi số liệu ngược
về Driver. Driver tạo biến bằng `sc.accumulator(0)`; mỗi task giữ một bản cục
bộ và gọi `.add()` lên bản đó; khi task **hoàn thành**, Spark gửi phần đóng góp
cục bộ về Driver và cộng vào giá trị tổng.

Chỉ Driver được phép đọc `.value`. Executor đọc `.value` sẽ báo lỗi, vì tại đó
chỉ tồn tại giá trị cục bộ của riêng task đang chạy, không phải tổng toàn cục.

Trong đồ án này, Accumulator đếm số bản ghi log hỏng mà không cần duyệt dữ liệu
thêm một lượt và không làm job dừng lại:

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
parsed_rdd.count()                  # action materialize toàn bộ dữ liệu

print(invalid_log_counter.value)    # chỉ đọc ở Driver, sau action
```

Vì Spark đánh giá lười, `invalid_log_counter.value` **chỉ có ý nghĩa sau khi
một action đã chạy**. Đọc trước action luôn trả về 0 — không phải vì không có
bản ghi hỏng, mà vì lineage chưa hề được thực thi.

#### 2.3.3 Giới hạn: Accumulator không bảo đảm exactly-once

Bảo đảm mà Spark đưa ra khác nhau tùy vị trí đặt lệnh `.add()`:

| Vị trí `.add()` | Bảo đảm của Spark |
|---|---|
| Trong một **action** (ví dụ `foreach`) | **Exactly-once** — Spark bỏ qua cập nhật từ task bị lặp |
| Trong một **transformation** (ví dụ `map`) | **Không bảo đảm** — có thể đếm nhiều lần hoặc thiếu |

Với transformation, có ba tình huống làm sai số đếm:

1. **Task thất bại và chạy lại.** Nếu một task lỗi giữa chừng, Spark lên lịch
   chạy lại partition đó; phần đã cộng của lần chạy hỏng có thể đã hoặc chưa
   được gộp, dẫn tới đếm trùng.
2. **Speculative execution.** Khi `spark.speculation=true`, Spark chạy song song
   một bản sao của task chậm. Cả hai bản đều cộng vào Accumulator dù chỉ một
   bản được dùng kết quả.
3. **Lineage bị tính lại (phổ biến nhất).** RDD không tự lưu kết quả; nếu một
   RDD không được cache mà bị hai action cùng dùng, Spark chạy lại toàn bộ
   lineage cho action thứ hai, và hàm `map` chứa `.add()` chạy lại từ đầu — số
   đếm bị nhân đôi.

**Cách phòng tránh trong đồ án này:** cache RDD đã parse và filter, rồi
materialize đúng một lần bằng một action trước mọi action downstream khác:

```python
parsed_rdd.cache()     # hoặc .persist()
parsed_rdd.count()     # materialize toàn bộ dữ liệu đúng một lần
```

Ưu tiên `count()` thay vì `collect()` (kéo toàn bộ dữ liệu về Driver, nguy cơ
tràn bộ nhớ Driver) hoặc `take(n)` (chỉ đọc đủ `n` phần tử rồi dừng, nên các
partition còn lại không được duyệt và Accumulator đếm thiếu).

Ngay cả khi đã cache, đây vẫn chỉ là biện pháp giảm rủi ro chứ **không** biến
Accumulator thành exactly-once: nếu một partition đã cache bị mất, Spark dựng
lại partition đó từ lineage và số đếm vẫn sai. Vì vậy Accumulator phù hợp cho
số liệu quan trắc/gỡ lỗi, **không** nên dùng làm nguồn chân lý cho kết quả
nghiệp vụ. Nếu cần con số chính xác tuyệt đối, hãy đếm bằng một phép biến đổi
xác định trên RDD đã cache — ví dụ `raw_rdd.count() - parsed_rdd.count()`.

#### 2.3.4 Phân loại bản ghi log hỏng

Bộ sinh dữ liệu `src/generate_logs.py` tạo ra `data/raw_logs.txt` theo tỉ lệ
90% bản ghi hợp lệ và 10% bản ghi hỏng. Phần 10% hỏng chia đều cho **4 loại
lỗi**, và cũng chính là các trường hợp `parse_log_line()` trả về `None` để
Accumulator đếm.

Bốn loại được kiểm tra **theo đúng thứ tự ưu tiên dưới đây, dừng ở loại đầu
tiên khớp**, nên mỗi bản ghi hỏng thuộc về đúng một loại duy nhất:

| # | Loại lỗi | Điều kiện | Ví dụ |
|---|---|---|---|
| 1 | **Thiếu trường** | Số trường tách được ít hơn schema yêu cầu | `10.10.1.7 - - [22/Sep/2026:08:15:03 +0700] "GET /cart HTTP/1.1" 200` (thiếu `bytes`) |
| 2 | **IPv4 sai định dạng** | Đủ trường, nhưng trường IP không khớp định dạng IPv4 | `999.12.44 - - [...] "GET / HTTP/1.1" 200 1234` |
| 3 | **Status code không phải số** | Đủ trường, IP hợp lệ, status không parse được thành số nguyên | `10.20.1.9 - - [...] "GET / HTTP/1.1" OK 1234` |
| 4 | **Request malformed** | Đủ trường, IP hợp lệ, status hợp lệ, nhưng method/endpoint sai cấu trúc | `10.30.1.4 - - [...] "GET-only" 200 1234` |

Thứ tự ưu tiên là bắt buộc vì một bản ghi có thể vi phạm nhiều điều kiện cùng
lúc. Ví dụ dòng vừa thiếu trường `bytes` vừa có IP sai sẽ được xếp vào loại 1,
không phải loại 2 — vì khi chưa tách đủ trường thì chưa thể kết luận gì về nội
dung từng trường. Nhờ quy tắc này, tổng số bản ghi của 4 loại luôn đúng bằng
tổng số bản ghi hỏng, không có bản ghi nào bị đếm hai lần.

#### 2.3.5 Kết quả kiểm chứng

Chạy trên PySpark 3.5.9 (`local[*]`, OpenJDK 17) với `data/raw_logs.txt`
gồm 10.000 dòng:

| Hạng mục | Kết quả |
|---|---|
| Bản ghi hợp lệ | 9.000 (90,00%) |
| `invalid_log_counter.value` | 1.000 (10,00%) |
| Số đếm sau 3 action downstream nữa | vẫn 1.000 — không đếm trùng |
| Bản ghi được Broadcast phân giải ra quốc gia | 9.000/9.000, đủ 14 quốc gia |
| Tổng value của `reduceByKey(country)` | 9.000 = số bản ghi hợp lệ |
| Hợp lệ + hỏng | 9.000 + 1.000 = 10.000 = tổng số dòng đầu vào |

> **Lưu ý triển khai (gửi ID5):** đo được trên PySpark 3.5 — Python worker
> **không** kế thừa `sys.path` của Driver, nên job chỉ chạy khi lệnh được phát
> từ thư mục gốc của repo (phát từ chỗ khác: 0/3 lần chạy thành công). Nguyên
> nhân là Spark pickle hàm cấp module theo tham chiếu, buộc worker phải
> `import` được module. Khi chuyển sang `--deploy-mode cluster`, không thể né
> bằng cách đổi thư mục vì worker nằm trên máy khác; cách xử lý tiêu chuẩn
> theo tài liệu Spark là gửi kèm module bằng `--py-files`. Chi tiết và mức độ
> cấp thiết của từng tình huống xem `docs/2.3_deploy_mode_DRAFT.md` mục C.3.

### 2.4 Task 1 Result

<!-- Insert final Top 10 Countries result, invalid-record count and screenshot. -->

## 3. Spark Architecture and Deployment

### 3.1 Spark Application Architecture

<!-- Owner: ID5. Driver, Cluster Manager, Worker, Executor, Job, Stage and Task. -->

### 3.2 spark-submit and Deploy Modes

<!-- Owner: ID2 + ID5. client vs cluster, Standalone/YARN/Kubernetes. -->

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
