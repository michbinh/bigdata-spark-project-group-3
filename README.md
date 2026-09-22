# Big Data Spark Project

## Repository structure

```text
.
├── data/                  # Version small sample inputs only
│   └── raw_logs.txt
├── src/                   # Python source modules
├── tests/                 # Reproducible tests
├── docs/                  # Diagrams, screenshots and supporting notes
├── REPORT.md              # Shared final report
├── requirements.txt
├── submit_job.sh
└── .gitignore
```

Generated data and Spark runtime outputs belong in `output/`,
`benchmark_output/`, `logs/` or `checkpoints/`; they are intentionally not committed.

## Branch convention

| Owner | Branch | Scope |
|---|---|---|
| ID1 | `feature/rdd-core` | RDD parser, core transformations and aggregation |
| ID2 | `feature/shared-vars` | Log generation, Broadcast and Accumulator |
| ID3 | `feature/format-write` | Format write benchmark |
| ID4 | `feature/partition-benchmark` | Read and partition benchmark |
| ID5 | `feature/deployment` | `main.py`, `utils.py`, `submit_job.sh` |
| Gia Minh | `docs/report-git-governance` | Report integration, repository governance and optimisation section |

## Pull request rule

1. Create a branch from the latest `main`.
2. Keep each PR focused on one task.
3. Describe: changed files, run command, expected output, known limitation.
4. Request review from Gia Minh; only merge after code/report section is checked.
5. Do not push directly to `main`.

## Commit format

Use concise, task-based messages:

```text
feat(rdd): add regex log parser
feat(shared-vars): add invalid log accumulator
docs(report): add RDD lineage explanation
fix(benchmark): measure recursive output size
```

## Phase 1 integration contract

- `data/raw_logs.txt` is the shared test input from ID2.
- ID1 must document the parsed record schema before ID2 merges shared variables.
- The Task 1 final pipeline must produce Top 10 Countries and the invalid-record count.
- Any change to log format or parsed schema must be communicated in the related PR.
