from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str = "nyc-tlc-staging") -> SparkSession:
    # Don't hardcode master here; let spark-submit decide.
    return SparkSession.builder.appName(app_name).getOrCreate()


def main():
    spark = build_spark()

    raw = spark.read.parquet("s3a://nyc_tlc/source/yellow/2023/*.parquet")

    stg = (
        raw.withColumnRenamed("tpep_pickup_datetime", "pickup_ts")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_ts")
        .withColumn(
            "trip_duration_min",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0,
        )
        .withColumn("trip_year", F.year("pickup_ts"))
        .withColumn("trip_month", F.month("pickup_ts"))
    )

    (
        stg.repartition("trip_year", "trip_month")
        .write.mode("overwrite")
        .partitionBy("trip_year", "trip_month")
        .parquet("s3a://nyc_tlc/staging/yellow")
    )

    spark.stop()


if __name__ == "__main__":
    main()
