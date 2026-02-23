from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.types import (
    DoubleType,
)


def build_spark(app_name: str = "nyc-tlc-staging-yellow-tripdata") -> SparkSession:
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
        "tpep_pickup_datetime": "pickup_ts",
        "tpep_dropoff_datetime": "dropoff_ts",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "Airport_fee": "airport_fee",
        "RatecodeID": "rate_code_id",
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

    money_cols = [
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "airport_fee",
    ]
    for col in money_cols:
        if col in _df.columns:
            _df = _df.withColumn(col, f.col(col).cast("decimal(10,2)"))
        else:
            _df = _df.withColumn(col, f.lit(None).cast("decimal(10,2)"))

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

    return normalized_df


def main():
    spark = build_spark()

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set(
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        str(256 * 1024 * 1024),
    )

    _year = [i for i in range(2011, 2025 + 1, 1)]
    _month = [i for i in range(1, 12 + 1, 1)]
    partitions = zip(_year, _month)

    for year, month in partitions:
        df = spark.read.option("basePath", "s3a://raw/data/").parquet(
            f"s3a://raw/data/{year}/yellow_tripdata_{year}-{month:02d}.parquet"
        )
        stg = normalize(df)
        stg.writeTo("lakehouse.silver.yellow_trips").append()

    spark.stop()


if __name__ == "__main__":
    main()
