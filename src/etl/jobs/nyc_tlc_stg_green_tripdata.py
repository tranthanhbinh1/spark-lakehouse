from itertools import product

from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.types import (
    DoubleType,
)


def build_spark(app_name: str = "nyc-tlc-staging-green-tripdata") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark/spark-events")
        .config("spark.executor.memory", "6g")
        .getOrCreate()
    )


def normalize(df: DataFrame) -> DataFrame:
    rename_map = {
        "VendorID": "vendor_id",
        "vendorID": "vendor_id",
        "lpep_pickup_datetime": "pickup_ts",
        "Lpep_pickup_datetime": "pickup_ts",
        "lpep_dropoff_datetime": "dropoff_ts",
        "Lpep_dropoff_datetime": "dropoff_ts",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "RatecodeID": "rate_code_id",
        "RateCodeID": "rate_code_id",
    }

    _df: DataFrame = (
        df.select(*[f.col(c).alias(rename_map.get(c, c)) for c in df.columns])
        .withColumn("pickup_ts", f.to_timestamp("pickup_ts"))
        .withColumn("dropoff_ts", f.to_timestamp("dropoff_ts"))
        .withColumn(
            "trip_duration_min",
            (f.col("dropoff_ts").cast("long") - f.col("pickup_ts").cast("long")) / 60.0,
        )
        .withColumn("trip_duration_min", f.col("trip_duration_min").cast("long"))
        .withColumn("year", f.year("pickup_ts"))
        .withColumn("month", f.month("pickup_ts"))
        .withColumn("is_valid_trip", f.col("trip_duration_min") > 0)
        .withColumn(
            "congestion_surcharge",
            f.col("congestion_surcharge").cast(DoubleType()),
        )
    )

    if "trip_type" in _df.columns:
        _df = _df.withColumn("trip_type", f.col("trip_type").cast("int"))
    else:
        _df = _df.withColumn("trip_type", f.lit(None).cast("int"))

    if "ehail_fee" in _df.columns:
        _df = _df.withColumn("ehail_fee", f.col("ehail_fee").cast(DoubleType()))
    else:
        _df = _df.withColumn("ehail_fee", f.lit(None).cast(DoubleType()))

    money_cols = [
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
    ]
    for col in money_cols:
        if col in _df.columns:
            _df = _df.withColumn(col, f.col(col).cast("float"))
        else:
            _df = _df.withColumn(col, f.lit(None).cast("float"))

    normalized_df = (
        _df.withColumn("has_tip", f.col("tip_amount") > 0)
        .withColumn(
            "tip_ratio",
            f.when(
                f.col("fare_amount") > 0,
                f.round(f.col("tip_amount") / f.col("fare_amount"), 2),
            ).otherwise(0.0),
        )
        .filter((f.col("passenger_count") > 0) & (f.col("passenger_count") <= 6))
    )

    target_columns = [
        "vendor_id",
        "pickup_ts",
        "dropoff_ts",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "ehail_fee",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "trip_type",
        "trip_duration_min",
        "year",
        "month",
        "is_valid_trip",
        "has_tip",
        "tip_ratio",
    ]

    select_exprs = [
        f.col(c) if c in normalized_df.columns else f.lit(None).alias(c)
        for c in target_columns
    ]
    return normalized_df.select(*select_exprs)


def main():
    spark = build_spark()

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set(
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        str(256 * 1024 * 1024),
    )

    _year = [i for i in range(2014, 2025 + 1, 1)]
    _month = [i for i in range(1, 12 + 1, 1)]
    partitions = product(_year, _month)

    for year, month in partitions:
        try:
            df = spark.read.option("basePath", "s3a://raw/data/").parquet(
                f"s3a://raw/data/{year}/green_tripdata_{year}-{month:02d}.parquet"
            )
        # NOTE: Bad practice, did it here to by pass path not found error
        except Exception as e:
            print(repr(e))
            continue
        stg = normalize(df).filter((f.col("year") == year) & (f.col("month") == month))
        stg.writeTo("lakehouse.silver.green_trips").overwritePartitions()

    spark.stop()


if __name__ == "__main__":
    main()
