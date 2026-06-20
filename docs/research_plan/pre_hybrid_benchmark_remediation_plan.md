# Pre-Hybrid Benchmark Remediation Plan

## Relationship To The Research Plans

This document defines the remediation work required after the Phase 1 benchmark
smoke run and before Phase 2 hybrid-storage implementation begins.

- Canonical plan:
  `docs/research_plan/hybrid_storage_tradeoff_research_plan.md`
- Phase 1 implementation plan:
  `docs/research_plan/phase1_benchmark_harness_plan.md`
- Exit condition: produce a trusted on-premises baseline that replaces the
  current smoke result

This plan does not change the research questions, hypotheses, or locked hybrid
architecture. The hybrid target remains local compute with AWS S3, Apache
Iceberg, and AWS Glue Data Catalog.

## Summary

Repair the local Spark runtime, make benchmark data deterministic, isolate local
and hybrid catalogs, and collect storage-sensitive metrics before comparative
benchmarking. Local tables remain under `lakehouse`; future hybrid tables use
`lakehouse_hybrid`. Benchmark metrics remain centralized in
`lakehouse.benchmark.run_metrics`.

Phase 2 must not begin until the acceptance criteria in this document pass.

## Implementation Changes

### 1. Repair Spark Runtime Packaging

- Mount the complete `lakehouse/src` tree at `/opt/lakehouse/src` in Airflow and
  Spark containers.
- Set `PYTHONPATH=/opt/lakehouse/src` and submit jobs from
  `/opt/lakehouse/src/etl/jobs`.
- Replace relative quality-schema imports with absolute `etl.*` imports.
- Update benchmark and simulated-arrival DAGs to use the new application paths.
- Add group `0` to `spark-history` so it can read event logs written by the
  Airflow worker.
- Recreate affected containers and verify newly completed applications appear
  through the Spark History Server REST API.

### 2. Add Execution Identity And Catalog Isolation

- Add these optional Spark job CLI arguments:
  - `--catalog`, defaulting to `lakehouse`
  - `--benchmark-run-id`
  - `--dag-run-id`
  - `--repetition`
  - `--application-name`
- Build all Silver, Quality, and Gold identifiers from `--catalog`.
- Extend benchmark DAG configuration with `catalog`.
- Pass deterministic Spark application names containing the benchmark run,
  partition, repetition, and task.
- Keep the local profile on `lakehouse`; reserve `lakehouse_hybrid` for
  Glue/S3.
- Render benchmark SQL using `{catalog}` and `{benchmark_run_id}` placeholders.
- Configure the metrics destination separately as
  `lakehouse.benchmark.run_metrics`.

### 3. Make Quality And Metric Writes Deterministic

- Extend Quality results with nullable `benchmark_run_id`, required
  `dag_run_id`, and nullable `repetition`.
- Pass the Airflow run ID from benchmark and simulated-arrival DAGs.
- Filter the quality-summary benchmark query by the current
  `benchmark_run_id`.
- Preserve Quality history rather than overwriting it.
- Apply the existing-table migration through Spark SQL and update the
  create-table DDL for fresh environments.
- Make benchmark metric loading retry-safe by deleting rows for the current
  `benchmark_run_id` before reinserting them.
- Assert that every generated `metric_id` is unique before insertion.

### 4. Strengthen Measurement

- Add a `SparkHistoryClient` configured by `history_server_base_url`.
- Match completed Spark applications using deterministic application names and
  Airflow task time windows.
- Aggregate completed stage attempts into task metrics:
  - input and output bytes
  - input and output records
  - executor runtime
  - spill and shuffle metrics retained in raw artifacts
- Add `spark_application_id` to normalized metrics.
- Query Iceberg `$partitions` after each workload and capture `record_count`,
  `file_count`, and `total_size`.
- Add metric type `iceberg_partition` and columns `table_name`, `file_count`,
  and `data_size_bytes`.
- Replace the Trino metric misuse of `records_written` with a dedicated
  `result_rows` column.
- Add `cache_state` with allowed values `uncontrolled`, `warm`, and `cold`.
- Store host, Docker image, resource-limit, Spark, and Trino configuration in
  `environment_snapshot.json`.

### 5. Improve The Query Suite

Retain metadata-count and correctness queries, but do not use them as
storage-performance evidence. Add:

- A partition-filtered Silver aggregation over monetary, distance, and duration
  columns.
- A pickup-location aggregation over the selected partition.
- A broad Silver scan for the partition-pruning comparison.
- A matching filtered scan using the same aggregate logic.

Require local and hybrid catalogs to contain identical benchmark partitions
before comparing broad scans.

Warm measurements use one unrecorded warm-up followed by five recorded
repetitions. Cold measurements require a Trino restart before each recorded
first execution. Measurements without that protocol must use
`cache_state = 'uncontrolled'`.

## Interface And Schema Changes

- Environment profiles gain:
  - `data_catalog`
  - `metrics_table`
  - `history_server_base_url`
  - runtime container names used by the cache and environment collectors
- DAG run configuration gains `catalog`.
- Quality results gain execution identity fields.
- Benchmark metrics gain Spark application, query result, file-layout, and
  cache-state fields.
- SQL templates gain `{catalog}` and `{benchmark_run_id}` substitutions.

## Test And Acceptance Plan

### Static And Runtime Checks

- Run `python3 -m py_compile` for changed clients, jobs, and DAGs.
- Run `uv run ruff check .`.
- Run `uv run pyrefly check` for the benchmark clients and runner.
- Import the quality job successfully inside the Airflow worker.
- Confirm simulated-arrival and benchmark DAGs parse and use
  `/opt/lakehouse/src`.
- Run Spark DDL migrations and verify historical Quality rows remain readable.
- Execute one Spark task and confirm the History Server returns its application,
  stages, and environment.

### Benchmark Acceptance

- Run the smoke workload with three pipeline repetitions and five recorded query
  repetitions.
- Require every DAG run, Airflow task, and Trino query to succeed.
- Require unique `metric_id` values for the benchmark run.
- Require every Airflow task metric to have a `spark_application_id`.
- Require Spark counters to be present while preserving legitimate zero values.
- Require storage-sensitive queries to report materially more processed bytes
  than metadata-only count queries.
- Verify Quality summaries include only the current benchmark run.
- Verify repeated partition writes leave Silver and Gold row counts and file
  metrics consistent.
- Verify benchmark metric insertion can be retried without duplicate rows.
- Generate the final on-premises aggregate report only after all checks pass.

## Assumptions And Defaults

- Spark `3.5.6` and Trino `480` remain fixed during baseline collection.
- The benchmark metrics table stays local while measuring either data catalog.
- Historical Quality rows are retained and separated by execution identity.
- Cold-cache results are excluded until the restart protocol is automated and
  verified.
- Required runtime changes in the adjacent `infra-homelab` repository are part
  of this remediation.
- Phase 2 starts only after a new accepted on-premises baseline replaces the
  current smoke result.
