import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


QUALITY_RESULTS_TABLE = "lakehouse.quality.silver_trip_quality_results"

QUALITY_RESULTS_SCHEMA = T.StructType(
    [
        T.StructField("dataset", T.StringType(), nullable=False),
        T.StructField("year", T.IntegerType(), nullable=False),
        T.StructField("month", T.IntegerType(), nullable=False),
        T.StructField("check_name", T.StringType(), nullable=False),
        T.StructField("severity", T.StringType(), nullable=False),
        T.StructField("status", T.StringType(), nullable=False),
        T.StructField("observed_value", T.StringType(), nullable=True),
        T.StructField("threshold", T.StringType(), nullable=True),
        T.StructField("extra", T.StringType(), nullable=True),
        T.StructField("processed_at", T.TimestampType(), nullable=False),
    ]
)


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


def run_pre_checks(df: DataFrame) -> list[dict]:
    row_count = df.count()

    return [
        {
            "check_name": "partition_row_count",
            "severity": "hard",
            "status": "pass" if row_count > 0 else "fail",
            "observed_value": str(row_count),
            "threshold": "> 0",
            "extra": None,
        }
    ]


def failed_read_check(error: Exception) -> list[dict]:
    return [
        {
            "check_name": "silver_partition_read",
            "severity": "hard",
            "status": "fail",
            "observed_value": type(error).__name__,
            "threshold": "readable silver table partition",
            "extra": str(error),
        }
    ]


def schema_for_dataset(dataset: str):
    from nyc_tlc_silver_quality_schemas import (
        GreenTripsPanderaSchema,
        YellowTripsPanderaSchema,
    )

    if dataset == "yellow":
        return YellowTripsPanderaSchema
    if dataset == "green":
        return GreenTripsPanderaSchema

    raise ValueError(f"Unsupported dataset: {dataset}")


def run_pandera_checks(df: DataFrame, dataset: str) -> list[dict]:
    try:
        schema = schema_for_dataset(dataset)
        validated_df = schema.validate(check_obj=df)
        errors = dict(validated_df.pandera.errors)
    except Exception as error:
        return [
            {
                "check_name": "pandera_validation",
                "severity": "hard",
                "status": "fail",
                "observed_value": type(error).__name__,
                "threshold": "0 errors",
                "extra": str(error),
            }
        ]

    if not errors:
        return [
            {
                "check_name": "pandera_validation",
                "severity": "hard",
                "status": "pass",
                "observed_value": "0",
                "threshold": "0 errors",
                "extra": None,
            }
        ]

    return [
        {
            "check_name": "pandera_validation",
            "severity": "hard",
            "status": "fail",
            "observed_value": str(len(errors)),
            "threshold": "0 errors",
            "extra": json.dumps(errors, default=str),
        }
    ]


def with_audit_fields(
    results: list[dict], dataset: str, year: int, month: int
) -> list[dict]:
    processed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    return [
        {
            "dataset": dataset,
            "year": year,
            "month": month,
            "check_name": result["check_name"],
            "severity": result["severity"],
            "status": result["status"],
            "observed_value": result["observed_value"],
            "threshold": result["threshold"],
            "extra": result["extra"],
            "processed_at": processed_at,
        }
        for result in results
    ]


def write_results(spark: SparkSession, rows: list[dict]) -> None:
    results_df = spark.createDataFrame(rows, schema=QUALITY_RESULTS_SCHEMA)
    results_df.writeTo(QUALITY_RESULTS_TABLE).append()


def has_hard_failures(results: list[dict]) -> bool:
    return any(
        result["severity"] == "hard" and result["status"] == "fail"
        for result in results
    )


def print_dry_run(
    df: DataFrame | None,
    rows: list[dict],
    dataset: str,
    year: int,
    month: int,
) -> None:
    print(f"Quality target table: {QUALITY_RESULTS_TABLE}")
    print(f"Silver source table: lakehouse.silver.{dataset}_trips")
    print(f"Silver partition: dataset={dataset}, year={year}, month={month}")
    if df is not None:
        df.printSchema()
    print(json.dumps(rows, default=str, indent=2))


def main() -> int:
    args = parse_args()
    spark = build_spark(
        f"nyc-tlc-silver-quality-{args.dataset}-{args.year}-{args.month:02d}"
    )

    silver_partition = None
    try:
        try:
            silver_partition = read_silver_partition(
                spark, args.dataset, args.year, args.month
            )
            results = run_pre_checks(silver_partition)
            if not has_hard_failures(results):
                results.extend(run_pandera_checks(silver_partition, args.dataset))
        except Exception as error:
            results = failed_read_check(error)

        audit_rows = with_audit_fields(results, args.dataset, args.year, args.month)

        if args.dry_run:
            print_dry_run(
                silver_partition,
                audit_rows,
                args.dataset,
                args.year,
                args.month,
            )
        else:
            write_results(spark, audit_rows)

        return 1 if has_hard_failures(results) else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
