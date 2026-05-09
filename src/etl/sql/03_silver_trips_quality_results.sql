create table if not exists lakehouse.quality.silver_trip_quality_results (
    dataset string,
    year int,
    month int, 
    check_name string, 
    severity string,
    status string,
    observed_value string, 
    threshold string, 
    extra string, 
    processed_at timestamp
)

using iceberg
partitioned by (dataset, year, month)
tblproperties (
    'format-version'='2',
    'write.format.default'='parquet'
);
