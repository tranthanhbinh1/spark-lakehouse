alter table lakehouse.benchmark.run_metrics
add columns (
    result_rows bigint,
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
    cache_state string
);
