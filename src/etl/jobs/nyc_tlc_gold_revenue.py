import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


GOLD_REVENUE_TABLE = "lakehouse.gold.trip_revenue_monthly"

GOLD_COLUMNS = [
    "dataset",
    "year",
    "month",
    "trip_count",
    "valid_trip_count",
    "revenue_trip_count",
    "fare_amount_sum",
    "tip_amount_sum",
    "tolls_amount_sum",
    "total_amount_sum",
    "avg_fare_amount",
    "avg_tip_amount",
    "avg_trip_distance",
    "avg_trip_duration_min",
    "processed_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["yellow", "green"], required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark/spark-events")
        .config("spark.executor.memory", "6g")
        .getOrCreate()
    )


def read_silver_partition(
    spark: SparkSession, dataset: str, year: int, month: int
) -> DataFrame:
    table = f"lakehouse.silver.{dataset}_trips"
    return spark.table(table).where((F.col("year") == year) & (F.col("month") == month))


def aggregate_revenue(df: DataFrame, dataset: str, year: int, month: int) -> DataFrame:
    revenue_trip = (
        (F.col("is_valid_trip") == F.lit(True))
        & (F.col("fare_amount") > F.lit(0))
        & (F.col("total_amount") > F.lit(0))
    )

    revenue_fare = F.when(revenue_trip, F.col("fare_amount").cast("double"))
    revenue_tip = F.when(revenue_trip, F.col("tip_amount").cast("double"))
    revenue_tolls = F.when(revenue_trip, F.col("tolls_amount").cast("double"))
    revenue_total = F.when(revenue_trip, F.col("total_amount").cast("double"))
    revenue_distance = F.when(revenue_trip, F.col("trip_distance").cast("double"))
    revenue_duration = F.when(revenue_trip, F.col("trip_duration_min").cast("double"))

    aggregated = df.agg(
        F.count(F.lit(1)).cast("long").alias("trip_count"),
        F.sum(F.when(F.col("is_valid_trip") == F.lit(True), 1).otherwise(0))
        .cast("long")
        .alias("valid_trip_count"),
        F.sum(F.when(revenue_trip, 1).otherwise(0))
        .cast("long")
        .alias("revenue_trip_count"),
        F.coalesce(F.sum(revenue_fare), F.lit(0.0)).alias("fare_amount_sum"),
        F.coalesce(F.sum(revenue_tip), F.lit(0.0)).alias("tip_amount_sum"),
        F.coalesce(F.sum(revenue_tolls), F.lit(0.0)).alias("tolls_amount_sum"),
        F.coalesce(F.sum(revenue_total), F.lit(0.0)).alias("total_amount_sum"),
        F.avg(revenue_fare).alias("avg_fare_amount"),
        F.avg(revenue_tip).alias("avg_tip_amount"),
        F.avg(revenue_distance).alias("avg_trip_distance"),
        F.avg(revenue_duration).alias("avg_trip_duration_min"),
    )

    return aggregated.select(
        F.lit(dataset).cast("string").alias("dataset"),
        F.lit(year).cast("int").alias("year"),
        F.lit(month).cast("int").alias("month"),
        *[F.col(column) for column in GOLD_COLUMNS[3:-1]],
        F.current_timestamp().alias("processed_at"),
    )


def main() -> None:
    args = parse_args()
    spark = build_spark(
        f"nyc-tlc-gold-revenue-{args.dataset}-{args.year}-{args.month:02d}"
    )

    try:
        silver_partition = read_silver_partition(
            spark, args.dataset, args.year, args.month
        )
        source_count = silver_partition.count()
        if source_count == 0:
            raise RuntimeError(
                "No silver rows found for "
                f"dataset={args.dataset}, year={args.year}, month={args.month}"
            )

        revenue = aggregate_revenue(
            silver_partition, args.dataset, args.year, args.month
        )

        if args.dry_run:
            print(f"Gold target table: {GOLD_REVENUE_TABLE}")
            print(f"Silver source table: lakehouse.silver.{args.dataset}_trips")
            print(
                "Silver partition: "
                f"dataset={args.dataset}, year={args.year}, month={args.month}"
            )
            print(f"Source rows: {source_count}")
            revenue.printSchema()
            revenue.show(truncate=False)
            return

        revenue.writeTo(GOLD_REVENUE_TABLE).overwritePartitions()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
