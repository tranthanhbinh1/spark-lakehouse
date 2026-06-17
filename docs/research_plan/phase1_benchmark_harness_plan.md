# Phase 1 Benchmark Harness And On-Premises Baseline Plan

## Relationship To The Canonical Research Plan

This document is the implementation plan for Phase 1 of the canonical research plan:

- Parent plan: `docs/research_plan/hybrid_storage_tradeoff_research_plan.md`
- Parent phase: `Phase 1: Benchmark Harness And On-Premises Baseline`
- Goal: make the current local MVP measurable before any AWS S3 or Glue Catalog work

This file must stay subordinate to the parent plan. Changes to thesis direction, research questions, hypotheses, architecture scope, or benchmark objectives belong in the parent plan first. This file should only describe how to implement the Phase 1 benchmark harness.

## Summary

Implement a reproducible benchmark harness for the existing local lakehouse MVP. The harness will use a local benchmark CLI to trigger a dedicated Airflow benchmark DAG, run fixed NYC TLC partitions through the existing stage -> quality -> gold Spark jobs, execute a fixed Trino query suite, write JSON benchmark artifacts, and persist normalized metrics to an Iceberg table.

Initial workload:

- `yellow`, `2011-01`
- `green`, `2014-01`

Hard boundaries:

- Do not implement AWS S3, Glue Catalog, or hybrid-storage infrastructure in this phase.
- Do not change the canonical hybrid-storage plan unless the research scope changes.
- Do not mutate simulated-arrival Airflow Variables from benchmark runs.
- Do not duplicate existing ETL business logic inside benchmark code.

## Implementation Changes

### Benchmark Assets

Add benchmark-controlled inputs:

- `benchmarks/workloads/smoke.toml`
  - Workload name: `smoke`
  - Partitions: `yellow 2011-01`, `green 2014-01`
  - Defaults: `pipeline_repetitions = 3`, `query_repetitions = 5`
- `benchmarks/queries/*.sql`
  - Fixed Trino queries using placeholders for `{dataset}`, `{year}`, and `{month}`
  - Include partition row-count, quality-result, and gold-revenue checks
- `conf/environments/onprem.toml`
  - `architecture = "onprem"`
  - local Airflow API endpoint
  - local Trino endpoint
  - Spark input base: `s3a://raw/data`
  - secret source: environment variables
  - local network/runtime assumptions
- Generated artifacts under `benchmarks/artifacts/`
  - Add this path to `.gitignore`
  - Store one artifact directory per benchmark run

### Airflow Benchmark DAG

Add a dedicated benchmark DAG:

- DAG ID: `taxi_benchmark_pipeline`
- `schedule=None`
- `catchup=False`
- `max_active_runs=1`

The DAG must read all workload input from `dag_run.conf`:

- `benchmark_run_id`
- `dataset`
- `year`
- `month`
- `repetition`
- `input_base`

The DAG must run the existing Spark jobs in this order:

1. `nyc_tlc_stg_trip_data.py`
2. `nyc_tlc_silver_quality.py`
3. `nyc_tlc_gold_revenue.py`

The DAG must not read or write:

- `yellow_trips_next_partition`
- `green_trips_next_partition`
- `yellow_trips_end_partition`
- `green_trips_end_partition`

Spark submit configuration should match the current simulated-arrival DAGs unless runtime validation proves a benchmark-specific override is required.

### Benchmark Runner

Add a local runner:

- Script: `scripts/benchmarks/run_benchmark.py`
- Config parsing: stdlib `tomllib`
- Primary mode: trigger Airflow DAG runs through the Airflow REST API
- Dry-run mode: validate configs, render DAG run IDs and query SQL, compute config hash, and exit without triggering Airflow or Trino

Run identifiers:

- `benchmark_run_id = bench_{profile}_{workload}_{utc_timestamp}_{git_short_sha}`
- Airflow DAG run ID: `{benchmark_run_id}__{dataset}_{year}_{month:02d}__r{repetition:02d}`

Runner responsibilities:

- Load workload and environment profile files
- Compute a deterministic config hash from workload, profile, and query files
- Capture the current git SHA
- Trigger one Airflow DAG run per dataset/month/repetition
- Poll each DAG run to terminal state
- Collect task instance state, start time, end time, duration, try number, and log evidence
- Execute each Trino query for each configured query repetition
- Write raw JSON artifacts before inserting metrics
- Insert normalized metrics into the benchmark Iceberg table through Trino

Airflow API behavior:

- Use Airflow 3 API paths for DAG-run creation, DAG-run lookup, and task-instance listing.
- Implement log fetching behind one client method.
- Try the local Airflow 3 log endpoint first.
- Fall back to the documented v1 task-log endpoint only when the Airflow 3 path returns 404.

Trino behavior:

- Execute SQL through Trino `/v1/statement`.
- Follow `nextUri` until query completion.
- Capture query ID, final state, elapsed time, rows, bytes, and errors when available.

### Metrics Table

Add a benchmark metrics table DDL:

- File: `src/etl/sql/05_benchmark_run_metrics.sql`
- Table: `lakehouse.benchmark.run_metrics`
- Format: Iceberg v2
- Grain: one row per measured unit

Allowed `metric_type` values:

- `pipeline`
- `airflow_task`
- `trino_query`

Required columns:

- `benchmark_run_id`
- `metric_id`
- `metric_type`
- `architecture`
- `environment`
- `workload_name`
- `dag_id`
- `dag_run_id`
- `task_id`
- `query_name`
- `query_id`
- `dataset`
- `year`
- `month`
- `repetition`
- `status`
- `started_at`
- `finished_at`
- `duration_seconds`
- `records_read`
- `records_written`
- `input_bytes`
- `output_bytes`
- `error_class`
- `error_message`
- `git_sha`
- `config_hash`
- `artifact_path`
- `processed_at`

Metrics inserts must use explicit column lists. The first implementation should insert metrics through Trino, not local PySpark, to avoid mismatch between local Python and container runtime.

## Test And Acceptance Plan

Static checks:

- `python3 -m py_compile orchestration/dags/benchmark_pipeline.py scripts/benchmarks/run_benchmark.py`
- `uv run ruff check .`

Config and artifact checks:

- Runner `--dry-run` validates workload/profile files.
- Runner `--dry-run` renders DAG run IDs.
- Runner `--dry-run` renders Trino SQL.
- Runner `--dry-run` computes git SHA and config hash.
- Generated JSON artifacts validate with `python3 -m json.tool`.

Airflow checks:

- `airflow tasks list taxi_benchmark_pipeline` succeeds inside the Airflow runtime.
- One manual benchmark DAG run for `yellow 2011-01` reaches success.
- The manual benchmark run executes stage -> quality -> gold.
- Simulated-arrival Variables remain unchanged after the benchmark run.

Smoke acceptance:

- `yellow 2011-01` completes 3 successful pipeline repetitions.
- `green 2014-01` completes 3 successful pipeline repetitions.
- Each configured Trino query completes 5 successful repetitions per dataset/month.
- `lakehouse.benchmark.run_metrics` contains `pipeline`, `airflow_task`, and `trino_query` rows for the same `benchmark_run_id`.
- JSON artifacts include workload path, profile path, rendered query names, git SHA, config hash, Airflow DAG run IDs, task states, query IDs, and errors when present.

## Assumptions And Defaults

- The first implementation targets minimum defensible metrics only: timings, statuses, git SHA, config hash, query latency, query IDs, row/byte stats when available, and JSON artifacts.
- Deep Spark event-log parsing is deferred until the benchmark smoke path is stable.
- Object-store request statistics are deferred until after smoke validation.
- The existing Spark job CLIs remain stable.
- Existing Iceberg table identifiers remain stable.
- Benchmark code wraps the existing pipeline jobs rather than reimplementing pipeline logic.
- The same benchmark harness will later be reused for the hybrid-storage profile.
