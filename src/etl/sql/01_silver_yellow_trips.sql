create table if not exists lakehouse.silver.yellow_trips (
    vendor_id int,
    pickup_ts timestamp,
    dropoff_ts timestamp,
    passenger_count int,
    trip_distance float,
    ratecode_id int,
    store_and_fwd_flag string,
    pu_location_id int,
    do_location_id int,
    payment_type int,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    improvement_surcharge float,
    total_amount float,
    congestion_surcharge float,
    airport_fee float
)
using iceberg
partitioned by (month(pickup_ts))
tblproperties (
    'format-version'='2',
    'write.format.default'='parquet'
);
