alter table lakehouse.quality.silver_trip_quality_results
add columns (
    benchmark_run_id string,
    dag_run_id string,
    repetition int
); 
