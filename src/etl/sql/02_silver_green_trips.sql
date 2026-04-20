create table if not exists lakehouse.silver.green_trips (
    vendor_id bigint,
    pickup_ts timestamp,
    dropoff_ts timestamp,
    passenger_count bigint,
    trip_distance double,
    rate_code_id bigint,
    store_and_fwd_flag string,
    pickup_location_id bigint,
    dropoff_location_id bigint,
    payment_type bigint,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    ehail_fee double,
    improvement_surcharge float,
    total_amount float,
    congestion_surcharge double,
    trip_type int,
    trip_duration_min bigint,
    year int,
    month int,
    is_valid_trip boolean,
    has_tip boolean,
    tip_ratio double
)
using iceberg
partitioned by (year, month)
tblproperties (
    'format-version'='2',
    'write.format.default'='parquet'
);
