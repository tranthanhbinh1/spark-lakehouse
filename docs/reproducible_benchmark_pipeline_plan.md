# Reproducible Benchmark Pipeline Plan

## Summary

Build a benchmark layer around the existing lakehouse MVP before any cloud implementation. The first slice targets the **on-premises baseline only**, stores results in **Iceberg plus JSON artifacts**, and uses **Airflow-only execution for Spark pipeline tasks**.

The primary goal is **research-grade benchmarking** to produce performance, resource utilization, and cost measurements for a 2-month comparative study: *Comparative Analysis of Lakehouse Workloads On-Premises and Hybrid Cloud*.

The current simulated-arrival DAGs must remain unchanged for historical ingestion. Benchmarking gets a separate pipeline so tests do not mutate `yellow_trips_next_partition` or `green_trips_next_partition`.

## Design Decisions

The following decisions were resolved during planning and must be respected during implementation:

| # | Decision | Resolution |
|---|---|---|
| 1 | Primary goal | Research-grade benchmarking for a 2-month comparative study (on-prem vs hybrid cloud) |
| 2 | Driver model | **Airflow-only for Spark pipeline tasks** — stage, quality, and gold runs go through Airflow `SparkSubmitOperator`, matching production |
| 3 | CLI scope | Single invocation: trigger DAG → poll → run Trino queries → collect metrics → write artifacts → load Iceberg metrics |
| 4 | Airflow version | 3.x with REST API v2 (`/api/v2/`) |
| 5 | Airflow auth | Basic Auth via `AIRFLOW_BENCHMARK_USER` / `AIRFLOW_BENCHMARK_PASSWORD` env vars |
| 6 | Trino dependency | Yes, add `trino` Python client for query timing and query IDs |
| 7 | Benchmark DAG | Single DAG (`benchmark_taxi_pipeline`), `dataset` passed via `dag_run.conf` |
| 8 | Quality step | Included — full pipeline fidelity (stage → quality → gold) |
| 9 | Spark metrics depth | Task-level timing + Spark app ID only; deep-dive via Spark History Server manually |
| 10 | Spark app ID capture | Parse from Airflow task logs via REST API |
| 11 | I/O columns | Keep `records_read`/`records_written` (nullable); dropped `input_bytes`/`output_bytes` |
| 12 | Artifact storage | Local filesystem only (`benchmarks/artifacts/{run_id}/`) |
| 13 | Run ID format | `bench-{timestamp}-{workload}-{profile}` — one per workload execution |
| 14 | Execution order | Sequential: all reps of partition 1, then partition 2, etc. |
| 15 | Failure handling | Fail-fast — stop entire workload on first failure, record what happened |
| 16 | Query suite timing | After each partition's pipeline completes |
| 17 | Trino execution path | From CLI via `trino` Python client, not through Airflow |
| 18 | Idempotency scope | Minimal — verify rerun succeeds and row counts match, no deep data comparison |
| 19 | Iceberg loading | Via Trino `INSERT` from the CLI; avoid local PySpark/Iceberg runtime mismatch |
| 20 | Report format | JSON `summary.json` + human-readable console table |
| 21 | Polling strategy | Fixed 30-second interval when waiting for Airflow DAG runs |
| 22 | Code location | `src/etl/benchmark/` |
| 23 | DDL location | `src/etl/sql/05_benchmark_run_metrics.sql` |
| 24 | Preflight checks | All mandatory; dirty git worktree is warning-only (records `git_dirty = true`) |
| 25 | Logging | Use existing `src/utils/logger.py` |
| 26 | Spark config | Same as production DAGs — no overrides in first slice |

## Key Changes

- Add a benchmark package under `src/etl/benchmark/` with:
  - A CLI runner:
    ```bash
    uv run python -m etl.benchmark.runner \
      --profile conf/environments/onprem.toml \
      --workload benchmarks/workloads/onprem_smoke.toml
    ```
    No `--driver` flag needed — Spark pipeline tasks always run through Airflow in this slice.
  - Workload parsing from TOML using Python stdlib `tomllib`.
  - Run manifest generation with `run_id`, `git_sha`, dirty-worktree flag, config hash, workload hash, profile name, start/end timestamps, and artifact path.
  - JSON artifact writing as the canonical raw evidence for every benchmark run.
  - Iceberg metrics loading as a post-run step via Trino `INSERT`.

- Add benchmark assets:
  ```text
  benchmarks/
    README.md
    workloads/
      onprem_smoke.toml
      onprem_monthly_baseline.toml
      onprem_idempotency.toml
    queries/
      silver_partition_count.sql
      gold_revenue_monthly.sql
      silver_revenue_scan.sql
    artifacts/        # ignored; runtime JSON outputs only
  ```

- Add environment profiles:
  ```text
  conf/environments/
    onprem.toml
  ```
  The profile defines logical endpoints for Airflow, Spark, Trino, and artifact output. Do not put secrets in this file; read secrets from existing runtime environment variables (`AIRFLOW_BENCHMARK_USER`, `AIRFLOW_BENCHMARK_PASSWORD`).

- Add Iceberg benchmark namespace/table DDL in `src/etl/sql/05_benchmark_run_metrics.sql`:
  ```sql
  create namespace if not exists lakehouse.benchmark;

  create table if not exists lakehouse.benchmark.run_metrics (
      run_id string,
      architecture string,
      profile string,
      workload_name string,
      task_name string,
      engine string,
      dataset string,
      year int,
      month int,
      repetition int,
      status string,
      started_at timestamp,
      finished_at timestamp,
      duration_seconds double,
      records_read bigint,
      records_written bigint,
      query_name string,
      query_id string,
      spark_app_id string,
      airflow_dag_id string,
      airflow_dag_run_id string,
      artifact_path string,
      git_sha string,
      git_dirty boolean,
      config_hash string,
      workload_hash string,
      error_class string,
      error_message string,
      processed_at timestamp
  )
  using iceberg
  partitioned by (architecture, profile, workload_name);
  ```

  Changes from original draft:
  - Removed `driver` column because execution paths are fixed in this slice: Spark pipeline tasks use Airflow, Trino queries and metric inserts use the CLI.
  - Removed `input_bytes` and `output_bytes` columns (unreliable from Airflow task-level metrics).
  - Kept `records_read` and `records_written` as nullable columns with task-specific semantics.

- Add a dedicated Airflow DAG:
  ```text
  benchmark_taxi_pipeline
  ```
  Single DAG handles both datasets. It accepts `dag_run.conf` instead of Airflow Variables:
  ```json
  {
    "run_id": "bench-20260611T120000Z-onprem_smoke-onprem",
    "profile": "onprem",
    "workload_name": "onprem_smoke",
    "dataset": "yellow",
    "year": 2011,
    "month": 1,
    "repetition": 1,
    "input_base": "s3a://raw/data"
  }
  ```
  DAG flow:
  ```text
  validate_conf
    -> stage_trip_data
    -> check_silver_quality
    -> build_gold_revenue
  ```
  No `advance_partition` task. No mutation of existing monthly-progress variables.
  Uses the same Spark configuration as the production simulated-arrival DAGs.

  Each partition/repetition trigger uses a deterministic Airflow DAG run ID:
  ```text
  {run_id}__{dataset}_{year}_{month:02d}__r{repetition}
  ```
  Example:
  ```text
  bench-20260611T120000Z-onprem_smoke-onprem__yellow_2011_01__r1
  ```

- Airflow CLI/API behavior:
  - CLI uses Airflow REST API v2 (Airflow 3.x) with Basic Auth.
  - Trigger endpoint: `POST /api/v2/dags/{dag_id}/dagRuns`.
  - Poll endpoint: `GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}` at fixed 30-second intervals.
  - Task metrics endpoint: `GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances`.
  - Task logs endpoint: `GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}?full_content=true` (for Spark app ID parsing).
  - Store task instance start/end/state in JSON artifacts and load them into Iceberg.

- Benchmark task types:
  - `preflight`: validate profile, workload file, raw partition paths (S3-compatible object store), Airflow connectivity (REST API reachable, DAG exists), Trino connectivity (connection + benchmark schema), Iceberg `benchmark.run_metrics` table existence, git dirty check (warning only).
  - `pipeline`: run stage → quality → gold for a dataset/month/repetition via Airflow DAG trigger.
  - `query_suite`: run fixed Trino SQL queries from the CLI via `trino` Python client, after each partition's pipeline completes.
  - `idempotency`: rerun the same partition and verify the run succeeds and row counts match (minimal check, no deep data comparison).
  - `report`: summarize metrics to `summary.json` and print human-readable table to console.

## Implementation Details

- Workload TOML format:
  ```toml
  name = "onprem_smoke"
  architecture = "onprem"
  repetitions = 3
  input_base = "s3a://raw/data"

  [[partitions]]
  dataset = "yellow"
  year = 2011
  month = 1

  [[partitions]]
  dataset = "green"
  year = 2014
  month = 1

  [query_suite]
  enabled = true
  repetitions = 5
  queries = [
    "silver_partition_count",
    "gold_revenue_monthly",
    "silver_revenue_scan",
  ]
  ```

  Note: removed `driver = "airflow"` field because the workload runner has fixed execution paths in this slice.

- Profile TOML format:
  ```toml
  name = "onprem"
  architecture = "onprem"
  artifact_base = "benchmarks/artifacts"

  [airflow]
  base_url = "http://localhost:8080"
  dag_id = "benchmark_taxi_pipeline"
  auth_env_user = "AIRFLOW_BENCHMARK_USER"
  auth_env_password = "AIRFLOW_BENCHMARK_PASSWORD"
  poll_interval_seconds = 30

  [trino]
  host = "localhost"
  port = 8081
  user = "benchmark"
  catalog = "lakehouse"
  schema = "silver"

  [object_store]
  endpoint_url = "http://localhost:9000"
  input_bucket = "raw"
  input_prefix = "data"
  access_key_env = "AWS_ACCESS_KEY_ID"
  secret_key_env = "AWS_SECRET_ACCESS_KEY"
  region_env = "AWS_REGION"

  [spark]
  input_base = "s3a://raw/data"
  event_log_dir = "/opt/spark/spark-events"
  ```

- Add Python dependency:
  - Add `trino` Python client to `pyproject.toml` and update `uv.lock`.
  - Reason: query timing and query IDs should come from a proper Trino client, not shell parsing.

- Run ID and DAG run ID format:
  - Pattern: `bench-{ISO8601_timestamp}-{workload_name}-{profile_name}`
  - Example: `bench-20260611T120000Z-onprem_smoke-onprem`
  - One `run_id` per workload execution; individual tasks are differentiated by `task_name`, `dataset`, `year`, `month`, and `repetition` columns.
  - Each Airflow DAG run uses `{run_id}__{dataset}_{year}_{month:02d}__r{repetition}`.
  - The CLI must pass this deterministic value when triggering `POST /api/v2/dags/{dag_id}/dagRuns`.

- Execution order:
  - Sequential processing: iterate partitions in order, and for each partition iterate repetitions in order.
  - Example for `onprem_smoke` with 3 reps: `yellow/2011/01 × r1, r2, r3` → `green/2014/01 × r1, r2, r3`.
  - Query suite runs after each partition's pipeline completes (after all repetitions of that partition).

- Failure handling:
  - Fail-fast: stop the entire workload on the first failure.
  - Record the failure in JSON artifacts and write a failed metric row to `metrics_rows.jsonl`.
  - Do NOT proceed to subsequent partitions or repetitions.
  - If Spark app ID extraction fails, do not fail the workload. Record `spark_app_id = null` and add a warning to the relevant JSON artifact.

- JSON artifact shape:
  ```text
  benchmarks/artifacts/{run_id}/
    manifest.json
    preflight.json
    airflow_task_instances.json
    trino_queries.json
    metrics_rows.jsonl
    summary.json
  ```

- Spark app ID capture:
  - Parse from Airflow task logs retrieved via REST API.
  - The `SparkSubmitOperator` logs lines like `Connected to Spark cluster with app id: ...`.
  - Use regex to extract the app ID from task log output for `stage_trip_data`, `check_silver_quality`, and `build_gold_revenue` tasks.
  - Store as `spark_app_id` in the metrics row; enables manual deep-dive via Spark History Server.
  - Use the task instance `try_number` returned by Airflow when requesting logs.
  - Fallback: if no app ID is found, metrics still load with `spark_app_id = null`.

- Records count semantics:
  - `stage_trip_data`: `records_read` is raw parquet row count when available; `records_written` is normalized silver partition row count.
  - `check_silver_quality`: `records_read` is silver partition row count checked; `records_written` is number of quality-result rows appended.
  - `build_gold_revenue`: `records_read` is silver partition row count aggregated; `records_written` is gold aggregate row count.
  - `query_suite`: `records_read` is null; `records_written` is row count returned to the CLI.
  - If a value cannot be measured without adding extra Spark actions beyond the existing job behavior, leave it null for the first slice rather than adding benchmark-only workload overhead.

- Metrics loading:
  - The CLI writes `metrics_rows.jsonl` first.
  - The CLI then inserts those rows into `lakehouse.benchmark.run_metrics` through the Trino Python client.
  - Trino is already required for query benchmarking, so this avoids depending on a local PySpark runtime that may not match the Airflow/Spark/Iceberg runtime.
  - If Trino insert fails, JSON artifacts remain the source of truth and the CLI exits non-zero after writing `summary.json`.

- Query execution rules:
  - Query files are parameterized only with `{dataset}`, `{year}`, and `{month}`.
  - Each query run records wall-clock duration, status, query name, Trino query ID, row count returned, and error details.
  - Queries execute from the CLI via the `trino` Python client, not through Airflow.
  - Query timing is a separate benchmark phase from pipeline timing. Queries run after all repetitions for a partition complete, so query metrics are tied to the partition, not to an individual Spark pipeline repetition.
  - First slice labels query repetitions by order only; do not claim true cold-cache measurement unless services are explicitly restarted.

- Existing Spark jobs remain the source of truth:
  - `nyc_tlc_stg_trip_data.py`
  - `nyc_tlc_silver_quality.py`
  - `nyc_tlc_gold_revenue.py`
  Benchmarking wraps them; it does not duplicate their business logic.

- Logging:
  - Use the existing `src/utils/logger.py` for all benchmark CLI logging.
  - Include structured messages with timestamp, task name, and status.

## Proposed Module Structure

```text
src/etl/benchmark/
  __init__.py
  runner.py            # CLI entry point (python -m etl.benchmark.runner)
  __main__.py          # Module runner shim
  config.py            # Profile + workload TOML parsing and validation
  manifest.py          # Run manifest generation (run_id, git_sha, hashes)
  preflight.py         # All preflight checks
  airflow_client.py    # Airflow REST API v2 client (trigger, poll, logs)
  trino_runner.py      # Trino query execution via trino client
  metrics.py           # Metrics collection, JSONL writing, Trino-backed Iceberg inserts
  report.py            # Summary generation (JSON + console table)
  artifacts.py         # JSON artifact directory management
```

## Test Plan

- Static checks:
  ```bash
  uv run ruff check .
  python3 -m py_compile src/etl/benchmark/runner.py
  python3 -m py_compile src/etl/benchmark/config.py
  python3 -m py_compile src/etl/benchmark/airflow_client.py
  ```

- CLI validation:
  - Invalid profile path fails before running Airflow.
  - Invalid workload TOML fails with a clear error.
  - Missing required partition fields fail before triggering Airflow.
  - `--dry-run` prints planned benchmark tasks and writes no Iceberg metrics.

- Preflight validation:
  - All 7 checks run before any pipeline task.
  - Dirty git worktree produces a warning but does not block execution.
  - Unreachable Airflow, Trino, or missing Iceberg table blocks execution.

- Airflow benchmark DAG validation:
  - Manual DAG run with valid `dag_run.conf` succeeds for `yellow/2011/1`.
  - Manual DAG run with valid `dag_run.conf` succeeds for `green/2014/1`.
  - Missing `dataset/year/month/run_id` in `dag_run.conf` fails in `validate_conf`.
  - Existing simulated-arrival DAG variables are unchanged after benchmark runs.

- Metrics validation:
  - Each benchmark run creates a unique artifact directory under `benchmarks/artifacts/{run_id}/`.
  - `manifest.json`, `metrics_rows.jsonl`, and `summary.json` are written.
  - `lakehouse.benchmark.run_metrics` receives rows for preflight, Airflow tasks, and Trino queries.
  - Failed runs still write JSON artifacts and failed metric rows (fail-fast records failure before stopping).

- Reproducibility scenarios:
  - Run `onprem_smoke.toml` twice and confirm both runs produce separate `run_id`s.
  - Run `onprem_idempotency.toml` against the same partition and confirm stage/gold overwrite succeeds and row counts match.
  - Compare repeated Trino query durations across repetitions from Iceberg metrics.

## Assumptions And Defaults

- First implementation slice is **on-premises baseline only**.
- Benchmark driver is **Airflow-only for Spark pipeline tasks** — stage, quality, and gold runs go through Airflow `SparkSubmitOperator`, matching the production execution path.
- Result storage is **Iceberg + local JSON artifacts**.
- Benchmark DAG must not reuse or modify the existing simulated-arrival DAG state variables.
- Secrets stay in environment variables or existing local `.env` files; benchmark config files must not contain secrets.
- Add one new dependency, `trino`, because SQL benchmark measurement should not rely on parsing CLI text.
- True cold-cache benchmarking is out of scope for the first slice unless a later workload explicitly restarts Trino/Spark services.
- Cloud/hybrid profiles are deferred until the on-premises benchmark harness produces trustworthy baseline measurements.
- Spark configuration in the benchmark DAG matches production DAGs — no performance tuning overrides in the first slice.
