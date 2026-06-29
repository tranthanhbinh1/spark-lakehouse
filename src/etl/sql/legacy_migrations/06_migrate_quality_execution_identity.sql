-- Legacy one-time migration only. Fresh environments use src/etl/sql/03_silver_trips_quality_results.sql.
-- Do not run this after the current create-table DDL; these columns already exist.

alter table lakehouse.quality.silver_trip_quality_results
add columns (
    benchmark_run_id string,
    dag_run_id string,
    repetition int
);
