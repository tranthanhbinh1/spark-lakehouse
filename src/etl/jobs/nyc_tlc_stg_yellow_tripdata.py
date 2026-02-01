from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    DecimalType,
    DoubleType,
)


def build_spark(app_name: str = "nyc-tlc-staging-yellow-tripdata") -> SparkSession:
    # Don't hardcode master here; let spark-submit decide.
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark/spark-events")
        .config("spark.executor.memory", "6g")
        .getOrCreate()
    )


def main():
    spark = build_spark()

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set(
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        str(256 * 1024 * 1024),
    )

    schema = StructType(
        [
            StructField("VendorID", LongType(), True),
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", LongType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("RatecodeID", LongType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("payment_type", LongType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("Airport_fee", DoubleType(), True),
        ]
    )

    raw = (
        spark.read.schema(schema)
        .option("basePath", "s3a://raw/data/")
        .parquet("s3a://raw/data/*/yellow_tripdata_*.parquet")
    )

    rename_map = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_ts",
        "tpep_dropoff_datetime": "dropoff_ts",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "Airport_fee": "airport_fee",
        "RatecodeID": "rate_code_id",
    }

    stg = (
        raw.select(*[f.col(c).alias(rename_map.get(c, c)) for c in raw.columns])
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
    )

    # TODO: needs proper handling based on data dictionary
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
        if col in stg.columns:
            stg = stg.withColumn(col, f.col(col).cast("decimal(10,2)"))
        else:
            stg = stg.withColumn(col, f.lit(None).cast("decimal(10,2)"))

    stg = (
        stg.withColumn("has_tip", f.col("tip_amount") > 0)
        .withColumn(
            "tip_ratio",
            f.when(
                f.col("fare_amount") > 0,
                f.round(f.col("tip_amount") / f.col("fare_amount"), 2),
            ).otherwise(0.0),
        )
        .filter((f.col("passenger_count") > 0) & (f.col("passenger_count") <= 6))
    )

    (
        stg.repartition("year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet("s3a://staging/data/yellow_tripdata")
    )

    spark.stop()


if __name__ == "__main__":
    main()
