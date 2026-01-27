from pyspark.sql import SparkSession, functions as F
from utils.logger import log_jvm_heap


def build_spark(app_name: str = "nyc-tlc-staging-yellow-tripdata") -> SparkSession:
    # Don't hardcode master here; let spark-submit decide.
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.executor.memory", "6g")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    log_jvm_heap(spark)

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set(
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        str(256 * 1024 * 1024),
    )

    raw = spark.read.option("basePath", "s3a://raw/data/").parquet(
        "s3a://raw/data/*/yellow_tripdata_*.parquet"
    )

    rename_map = {
        "VendorID": "vendor_id",
        "vendor_name": "vendor_id",
        "pickup_datetime": "pickup_ts",
        "tpep_pickup_datetime": "pickup_ts",
        "Trip_Pickup_DateTime": "pickup_ts",
        "dropoff_datetime": "dropoff_ts",
        "tpep_dropoff_datetime": "dropoff_ts",
        "Trip_Dropoff_DateTime": "dropoff_ts",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "Airport_fee": "airport_fee",
        "RatecodeID": "rate_code_id",
        "store_and_forward": "store_and_forward_flag",
        "store_and_fwd_flag": "store_and_forward_flag",
        "Passenger_Count": "passenger_count",
        "Trip_Distance": "trip_distance",
        "Fare_Amt": "fare_amount",
        "Tip_Amt": "tip_amount",
        "Tolls_Amt": "tolls_amount",
        "Total_Amt": "total_amount",
    }
    excluded_cols = {"rate_code", "Rate_Code", "payment_type", "Payment_Type"}
    stg = (
        raw.select(
            [
                F.col(c).alias(rename_map.get(c, c))
                for c in raw.columns
                if c not in excluded_cols
            ]
        )
        .withColumn("pickup_ts", F.to_timestamp("pickup_ts"))
        .withColumn("dropoff_ts", F.to_timestamp("dropoff_ts"))
        .withColumn(
            "trip_duration_min",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0,
        )
        .withColumn("trip_duration_min", F.col("trip_duration_min").cast("long"))
        .withColumn("year", F.year("pickup_ts"))
        .withColumn("month", F.month("pickup_ts"))
        .withColumn("is_valid_trip", F.col("trip_duration_min") > 0)
    )

    # TODO: needs proper handling based on data dictionary
    if "rate_code_id" not in stg.columns:
        stg = stg.withColumn("rate_code_id", F.lit(None).cast("int"))
    if "payment_type" not in stg.columns:
        stg = stg.withColumn("payment_type", F.lit(None).cast("string"))
    if "extra" not in stg.columns and "surcharge" in stg.columns:
        stg = stg.withColumn("extra", F.col("surcharge"))
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
            stg = stg.withColumn(col, F.col(col).cast("decimal(10,2)"))
        else:
            stg = stg.withColumn(col, F.lit(None).cast("decimal(10,2)"))

    stg = (
        stg.withColumn("has_tip", F.col("tip_amount") > 0)
        .withColumn(
            "tip_ratio",
            F.when(
                F.col("fare_amount") > 0,
                F.round(F.col("tip_amount") / F.col("fare_amount"), 2),
            ).otherwise(0.0),
        )
        .filter((F.col("passenger_count") > 0) & (F.col("passenger_count") <= 6))
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
