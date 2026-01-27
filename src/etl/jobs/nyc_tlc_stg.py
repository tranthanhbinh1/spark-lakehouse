from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str = "nyc-tlc-staging") -> SparkSession:
    # Don't hardcode master here; let spark-submit decide.
    return SparkSession.builder.appName(app_name).getOrCreate()


def main():
    spark = build_spark()

    raw = spark.read.parquet("s3a://raw/data/2023/yellow_tripdata_*.parquet")

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
        raw.select([F.col(c).alias(rename_map.get(c, c)) for c in raw.columns])
        .withColumn(
            "trip_duration_min",
            (
                F.col("dropoff_ts").cast("long")
                - F.col("pickup_ts").cast("long")
            )
            / 60.0,
        )
        .withColumn("trip_duration_min", F.col("trip_duration_min").cast("long"))
        .withColumn("trip_year", F.year("pickup_ts"))
        .withColumn("trip_month", F.month("pickup_ts"))
        .withColumn("is_valid_trip", F.col("trip_duration_min") > 0)
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
        stg = stg.withColumn(col, F.col(col).cast("decimal(10,2)"))

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
        stg.repartition("trip_year", "trip_month")
        .write.mode("overwrite")
        .partitionBy("trip_year", "trip_month")
        .parquet("s3a://staging/data/yellow")
    )

    spark.stop()


if __name__ == "__main__":
    main()
