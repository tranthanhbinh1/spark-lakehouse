# Pandera Silver DQ and Gold Revenue Plan

## Summary

Use the already-added Pandera dependency to validate Silver data inside PySpark jobs, then build PySpark Gold revenue marts. Pandera owns schema and row-level Silver checks; Spark handles partition existence, row counts, and aggregate warning metrics.

## Task 1: Add Quality Namespace and Audit Table

- Add `lakehouse.quality`.
- Add Iceberg table `lakehouse.quality.silver_trip_quality_results`.
- Store: dataset, year, month, check_name, severity, status, observed_value, threshold, message, processed_at.

## Task 2: Define Pandera Silver Schemas

- Add reusable Pandera `DataFrameModel` schemas for yellow and green Silver tables.
- Validate required column types and row-level constraints: `pickup_ts/dropoff_ts` present, `trip_duration_min > 0`, `trip_distance >= 0`, `passenger_count between 1 and 6`, valid `year/month`, non-null key fields.
- Keep dataset-specific fields explicit: yellow `airport_fee`; green `ehail_fee` and `trip_type`.

## Task 3: Build Pandera DQ Spark Job

- Add `nyc_tlc_silver_quality.py`.
- CLI: `--dataset yellow|green --year YYYY --month M --dry-run`.
- Read only the requested Silver partition.
- Run partition and row-count checks before Pandera validation.
- Run Pandera validation and immediately extract `df_out.pandera.errors`.
- Convert Pandera errors into audit rows.
- Exit non-zero only when hard checks fail.

## Task 4: Wire DQ Into Airflow

- Update yellow and green DAGs: `choose_partition -> stage_*_trips -> check_silver_quality -> advance_partition`.
- Failed hard Pandera validation blocks `advance_partition`.
- Warning checks are written to the audit table but do not block progression.

## Task 5: Add Gold Revenue Table DDL

- Add `lakehouse.gold.trip_revenue_monthly`.
- Partition by `dataset`, `year`, `month`.
- Include monthly metrics for trip count, valid trips, revenue trips, fare, tips, tolls, total amount, average fare, average tip, average distance, and average duration.

## Task 6: Build Gold Revenue Spark Job

- Add `nyc_tlc_gold_revenue.py`.
- CLI: `--dataset yellow|green --year YYYY --month M --dry-run`.
- Read only the DQ-passed Silver partition.
- Aggregate valid revenue trips.
- Write with partition overwrite so reruns are safe.

## Task 7: Wire Gold Build Into Airflow

- Final DAG flow: `choose_partition -> stage_*_trips -> check_silver_quality -> build_gold_revenue -> advance_partition`.
- Partition advances only after Silver load, Pandera DQ, and Gold build all succeed.

## Task 8: Verify

- Run `uv run ruff check .`.
- Confirm `pandera.pyspark` imports locally.
- Run Silver dry run for one yellow and one green partition.
- Run Pandera DQ dry run for a valid loaded partition.
- Run DQ against a missing partition and confirm failure.
- Run DQ against intentionally bad sample data if practical.
- Run Gold dry run for one partition.
- Trigger one DAG run and verify task ordering.

## Assumptions

- Pandera dependency is already present in both project and Airflow Spark driver runtime.
- Pandera is used for schema and row-level DQ checks.
- Spark remains responsible for partition existence, row count, and aggregate warning metrics.
- Hard DQ failures block Airflow progression.
- First Gold mart remains revenue/trip-volume only.
- No taxi-zone/location modelling in this slice.
