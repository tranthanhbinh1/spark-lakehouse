import argparse
import pandera.pyspark as pa

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark/spark-events")
        .config("spark.executor.memory", "6g")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["yellow", "green"], required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--input-base", default="s3a://raw/data")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def rename_columns(df: DataFrame, rename_map: dict[str, str]) -> DataFrame:
    return df.select(*[f.col(c).alias(rename_map.get(c, c)) for c in df.columns])


def with_optional_column(df: DataFrame, column: str, data_type: str) -> DataFrame:
    if column in df.columns:
        return df.withColumn(column, f.col(column).cast(data_type))
    return df.withColumn(column, f.lit(None).cast(data_type))


def add_trip_derivations(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("pickup_ts", f.to_timestamp("pickup_ts"))
        .withColumn("dropoff_ts", f.to_timestamp("dropoff_ts"))
        .withColumn(
            "trip_duration_min",
            (f.col("dropoff_ts").cast("long") - f.col("pickup_ts").cast("long")) / 60.0,
        )
        .withColumn("year", f.year("pickup_ts"))
        .withColumn("month", f.month("pickup_ts"))
        .withColumn("is_valid_trip", f.col("trip_duration_min") > 0)
        .withColumn("has_tip", f.col("tip_amount") > 0)
        .withColumn(
            "tip_ratio",
            f.when(
                f.col("fare_amount") > 0,
                f.round(f.col("tip_amount") / f.col("fare_amount"), 2),
            ).otherwise(0.0),
        )
    )


def normalize_yellow(df: DataFrame) -> DataFrame:
    rename_map = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_ts",
        "tpep_dropoff_datetime": "dropoff_ts",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "Airport_fee": "airport_fee",
        "RatecodeID": "rate_code_id",
    }

    normalized_df = rename_columns(df, rename_map)

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
        normalized_df = with_optional_column(normalized_df, col, "float")

    normalized_df = with_optional_column(normalized_df, "congestion_surcharge", "float")
    normalized_df = add_trip_derivations(normalized_df)

    return (
        normalized_df.withColumn(
            "trip_duration_min", f.col("trip_duration_min").cast("float")
        )
        .withColumn("is_valid_trip", f.col("trip_duration_min") > 0)
        .withColumn("has_tip", f.col("tip_amount") > 0)
        .withColumn(
            "tip_ratio",
            f.when(
                f.col("fare_amount") > 0,
                f.round(f.col("tip_amount") / f.col("fare_amount"), 2),
            )
            .otherwise(0.0)
            .cast("float"),
        )
        .filter((f.col("passenger_count") > 0) & (f.col("passenger_count") <= 6))
        .select(
            f.col("vendor_id").cast("int"),
            "pickup_ts",
            "dropoff_ts",
            f.col("passenger_count").cast("int"),
            f.col("trip_distance").cast("float"),
            f.col("rate_code_id").cast("int"),
            "store_and_fwd_flag",
            f.col("pickup_location_id").cast("int"),
            f.col("dropoff_location_id").cast("int"),
            f.col("payment_type").cast("int"),
            f.col("fare_amount").cast("float"),
            f.col("extra").cast("float"),
            f.col("mta_tax").cast("float"),
            f.col("tip_amount").cast("float"),
            f.col("tolls_amount").cast("float"),
            f.col("improvement_surcharge").cast("float"),
            f.col("total_amount").cast("float"),
            f.col("congestion_surcharge").cast("float"),
            f.col("airport_fee").cast("float"),
            "trip_duration_min",
            "year",
            "month",
            "is_valid_trip",
            "has_tip",
            "tip_ratio",
        )
    )


def normalize_green(df: DataFrame) -> DataFrame:
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

    normalized_df = rename_columns(df, rename_map)

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
        normalized_df = with_optional_column(normalized_df, col, "float")

    normalized_df = with_optional_column(
        normalized_df, "congestion_surcharge", "double"
    )
    normalized_df = with_optional_column(normalized_df, "ehail_fee", "double")
    normalized_df = with_optional_column(normalized_df, "trip_type", "int")
    normalized_df = add_trip_derivations(normalized_df)

    return (
        normalized_df.withColumn(
            "trip_duration_min", f.col("trip_duration_min").cast("bigint")
        )
        .withColumn("is_valid_trip", f.col("trip_duration_min") > 0)
        .withColumn("has_tip", f.col("tip_amount") > 0)
        .withColumn(
            "tip_ratio",
            f.when(
                f.col("fare_amount") > 0,
                f.round(f.col("tip_amount") / f.col("fare_amount"), 2),
            ).otherwise(0.0),
        )
        .filter((f.col("passenger_count") > 0) & (f.col("passenger_count") <= 6))
        .select(
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
            f.col("fare_amount").cast("float"),
            f.col("extra").cast("float"),
            f.col("mta_tax").cast("float"),
            f.col("tip_amount").cast("float"),
            f.col("tolls_amount").cast("float"),
            "ehail_fee",
            f.col("improvement_surcharge").cast("float"),
            f.col("total_amount").cast("float"),
            "congestion_surcharge",
            "trip_type",
            "trip_duration_min",
            "year",
            "month",
            "is_valid_trip",
            "has_tip",
            "tip_ratio",
        )
    )


def normalize(df: DataFrame, dataset: str) -> DataFrame:
    if dataset == "yellow":
        return normalize_yellow(df)
    if dataset == "green":
        return normalize_green(df)
    raise ValueError(f"Unsupported dataset: {dataset}")


def main() -> None:
    args = parse_args()

    spark = build_spark(f"nyc-tlc-stg-{args.dataset}-{args.year}-{args.month:02d}")

    input_path = (
        f"{args.input_base}/{args.year}/"
        f"{args.dataset}_tripdata_{args.year}-{args.month:02d}.parquet"
    )

    target_table = f"lakehouse.silver.{args.dataset}_trips"

    df = spark.read.parquet(input_path)

    stg = normalize(df, args.dataset).filter(
        (f.col("year") == args.year) & (f.col("month") == args.month)
    )

    if args.dry_run:
        print(f"Validated {input_path} -> {target_table}")
        stg.printSchema()
        spark.stop()
        return

    stg.writeTo(target_table).overwritePartitions()
    spark.stop()


if __name__ == "__main__":
    main()
