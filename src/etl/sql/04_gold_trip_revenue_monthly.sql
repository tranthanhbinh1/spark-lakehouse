create table if not exists lakehouse.gold.trip_revenue_monthly (
    dataset string,
    year int,
    month int,
    trip_count bigint,
    valid_trip_count bigint,
    revenue_trip_count bigint,
    fare_amount_sum double,
    tip_amount_sum double,
    tolls_amount_sum double,
    total_amount_sum double,
    avg_fare_amount double,
    avg_tip_amount double,
    avg_trip_distance double,
    avg_trip_duration_min double,
    processed_at timestamp
)
using iceberg
partitioned by (dataset, year, month)
tblproperties (
    'format-version'='2',
    'write.format.default'='parquet'
);
