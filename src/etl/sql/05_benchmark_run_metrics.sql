create namespace if not exists lakehouse.benchmark;

create table if not exists lakehouse.benchmark.run_metrics (
    benchmark_run_id string,
    metric_id string,
    metric_type string,
    architecture string,
    environment string,
    workload_name string,
    dag_id string,
    dag_run_id string,
    task_id string,
    query_name string,
    query_id string,
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
    result_rows bigint,
    input_bytes bigint,
    output_bytes bigint,
    executor_run_time_ms bigint,
    memory_bytes_spilled bigint,
    disk_bytes_spilled bigint,
    shuffle_read_bytes bigint,
    shuffle_read_records bigint,
    shuffle_write_bytes bigint,
    shuffle_write_records bigint,
    spark_application_id string,
    table_name string,
    file_count bigint,
    data_size_bytes bigint,
    cache_state string,
    error_class string,
    error_message string,
    git_sha string,
    config_hash string,
    artifact_path string,
    processed_at timestamp
)
using iceberg
partitioned by (architecture, environment, workload_name, metric_type)
tblproperties (
    'format-version'='2',
    'write.format.default'='parquet'
);
