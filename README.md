# Big Data Spark Project — Group 3

## Mục tiêu project

Nhóm xây dựng một mini Spark project theo toàn bộ quy trình:

```text
Tạo log mẫu → xử lý log bằng RDD → Top 10 Countries
→ đóng gói spark-submit → benchmark định dạng file/partition
→ report → QA → slide và live demo
```

## Cấu trúc repo: up file vào đâu?

```text
.
├── data/                       # Input mẫu nhỏ, CÓ commit
│   └── raw_logs.txt             # ID2 tạo để Task 1 chạy lại được
├── src/                        # Toàn bộ source code Python
│   ├── generate_logs.py         # ID2
│   ├── rdd_processing.py        # ID1 + ID2
│   ├── format_benchmark.py      # ID3 + ID4
│   ├── main.py                  # ID5
│   └── utils.py                 # ID5 / helper dùng chung
├── tests/                       # Test nhỏ hoặc smoke test có thể chạy lại
├── docs/                        # Sơ đồ kiến trúc và kết quả bảng/chart
│   └── spark_architecture.md    # ID5: runtime architecture và RDD DAG
├── REPORT.md                    # Báo cáo chung; mỗi người chỉ sửa section của mình
├── requirements.txt             # Danh sách thư viện cần cài
├── submit_job.sh                # Lệnh chạy Spark của ID5
├── .gitignore                   # Không up output nặng / log Spark
├── output/                      # Không commit
├── benchmark_output/            # Không commit
├── logs/                        # Không commit
└── checkpoints/                 # Không commit
```

> Không commit: dataset benchmark lớn, CSV/JSON/Parquet/ORC sinh ra, Spark warehouse, logs, checkpoints hoặc file chứa mật khẩu/API key.

## Phân công, output và nơi up
## Phân công, output và nơi up

| ID | Công việc | Output bắt buộc | Vị trí upload | Branch |
| --- | --- | --- | --- | --- |
| **ID1** | Parser RDD: đọc log, parse, filter, Pair RDD, `reduceByKey` | `rdd_processing.py`; lý thuyết RDD + RDD vs DataFrame | `src/`; mục 2.1-2.2 trong `REPORT.md`; sơ đồ vào `docs/` | `feature/id1-rdd-core` |
| **ID2** | Tạo log; Broadcast IP -> Country; Accumulator log lỗi | `generate_logs.py`, `raw_logs.txt`; phần Shared Variables | `src/`, `data/`, mục 2.3 trong `REPORT.md` | `feature/id2-shared-vars` |
| **ID3** | Ghi CSV/JSON/Parquet/ORC, đo size và write time | `format_benchmark.py`; bảng/biểu đồ write benchmark | `src/`; bảng/chart vào `docs/`; mục 4.1-4.2 trong `REPORT.md` | `feature/id3-format-write` |
| **ID4** | Read benchmark; `repartition`, `coalesce`, `partitionBy` | Phần đọc/partition trong `format_benchmark.py`; bảng/chart | `src/`; bảng/chart vào `docs/`; mục 4.3 trong `REPORT.md` | `feature/id4-partition-benchmark` |
| **ID5** | Architecture + triển khai Spark | `main.py`, `utils.py`, `submit_job.sh`; sơ đồ kiến trúc | `src/`, root repo, `docs/`; mục 3 trong `REPORT.md` | `feature/id5-deployment` |
| **ID6 - Gia Minh** | Git Master, report editor, Debugging/Performance | Repo sạch, report ghép hoàn chỉnh; phần Skew/GC/OOM | `README.md`, `.gitignore`, `REPORT.md`, `docs/` | `docs/id6-report-git-governance` |
| **ID7** | QA clean-clone, slide và live demo | `Presentation_Slides.pptx`, demo script, ảnh/video backup | `docs/`; báo lỗi qua Issue/PR comment | `feature/id7-demo-qa` |
## Quy tắc Git

1. Chỉ làm trên branch của mình; **không push trực tiếp vào `main`**.
2. Trước khi làm, cập nhật branch từ `main`.
3. Khi xong một phần: `git add .` → `git commit -m "..."` → `git push`.
4. Tạo Pull Request từ branch của mình vào `main`; mô tả file đổi, lệnh chạy, output mong đợi và limitation.
5. ID6 review/merge. Nếu có conflict hoặc thay đổi input/schema, báo owner liên quan trước khi merge.

## Commit message mẫu

```text
feat(rdd): add regex log parser
feat(shared-vars): add invalid log accumulator
feat(benchmark): add parquet write measurement
docs(report): add RDD lineage explanation
fix(deployment): use relative input path
```

## Contract Phase 1 — cần thống nhất trước khi ghép

1. **ID2** tạo và commit `src/generate_logs.py` cùng `data/raw_logs.txt` (90% valid, 10% malformed).
2. ID2 ghi rõ format mỗi dòng log và các loại lỗi được tạo.
3. **ID1** parse ra schema thống nhất: `ip`, `timestamp`, `method`, `endpoint`, `status`.
4. ID1 + ID2 chỉ ghép Broadcast/Accumulator sau khi thống nhất schema.
5. Output Task 1 cuối cùng phải in được: **Top 10 Countries** và **invalid log count**.

## Cách chạy sau này

```bash
pip install -r requirements.txt
python src/generate_logs.py
bash submit_job.sh
```

To invoke the application directly, use `python main.py --input-path data/raw_logs.txt --output-path output/top_countries.csv`.

The submit script defaults to `local[*]`; `spark-submit --master` selects the deployment master. Set `SPARK_MASTER` to pass a different master to `spark-submit` (for example, `SPARK_MASTER=yarn bash submit_job.sh` is only a deployment example and requires a configured YARN cluster).

`SparkSession` nhận master từ `spark-submit`; application không đặt master riêng. The CLI options are `--input-path` and `--output-path`.
