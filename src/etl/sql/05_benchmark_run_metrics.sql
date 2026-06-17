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
    input_bytes bigint,
    output_bytes bigint,
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
