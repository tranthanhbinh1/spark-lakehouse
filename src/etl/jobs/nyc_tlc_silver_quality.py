import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from etl.schemas.nyc_tlc_silver_quality_schemas import (
    GreenTripsPanderaSchema,
    YellowTripsPanderaSchema,
)

QUALITY_RESULTS_SCHEMA = T.StructType(
    [
        T.StructField("benchmark_run_id", T.StringType(), nullable=True),
        T.StructField("dag_run_id", T.StringType(), nullable=False),
        T.StructField("repetition", T.IntegerType(), nullable=True),
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
    parser.add_argument("--catalog", default="lakehouse")
    parser.add_argument("--benchmark-run-id")
    parser.add_argument("--dag-run-id", required=True)
    parser.add_argument("--repetition", type=int)
    parser.add_argument("--application-name")
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
    spark: SparkSession, catalog: str, dataset: str, year: int, month: int
) -> DataFrame:
    table = f"{catalog}.silver.{dataset}_trips"
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


def run_validity_flag_checks(df: DataFrame) -> list[dict]:
    expected_is_valid_trip = F.col("trip_duration_min") > 0
    mismatch_count = df.where(
        F.col("is_valid_trip").isNull()
        | (F.col("is_valid_trip") != expected_is_valid_trip)
    ).count()

    return [
        {
            "check_name": "is_valid_trip_derivation",
            "severity": "hard",
            "status": "pass" if mismatch_count == 0 else "fail",
            "observed_value": str(mismatch_count),
            "threshold": "0 mismatches against trip_duration_min > 0",
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
    results: list[dict],
    dataset: str,
    year: int,
    month: int,
    benchmark_run_id: str | None,
    dag_run_id: str,
    repetition: int | None,
) -> list[dict]:
    processed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    return [
        {
            "benchmark_run_id": benchmark_run_id,
            "dag_run_id": dag_run_id,
            "repetition": repetition,
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


def write_results(spark: SparkSession, table: str, rows: list[dict]) -> None:
    results_df = spark.createDataFrame(rows, schema=QUALITY_RESULTS_SCHEMA)
    results_df.writeTo(table).append()


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
    catalog: str,
    quality_results_table: str,
) -> None:
    print(f"Quality target table: {quality_results_table}")
    print(f"Silver source table: {catalog}.silver.{dataset}_trips")
    print(f"Silver partition: dataset={dataset}, year={year}, month={month}")
    if df is not None:
        df.printSchema()
    print(json.dumps(rows, default=str, indent=2))


def main() -> int:
    args = parse_args()
    default_app_name = (
        f"nyc-tlc-silver-quality-{args.dataset}-{args.year}-{args.month:02d}"
    )
    spark = build_spark(args.application_name or default_app_name)
    quality_results_table = f"{args.catalog}.quality.silver_trip_quality_results"

    silver_partition = None
    try:
        try:
            silver_partition = read_silver_partition(
                spark, args.catalog, args.dataset, args.year, args.month
            )
            results = run_pre_checks(silver_partition)
            if not has_hard_failures(results):
                results.extend(run_validity_flag_checks(silver_partition))
            if not has_hard_failures(results):
                results.extend(run_pandera_checks(silver_partition, args.dataset))
        except Exception as error:
            results = failed_read_check(error)

        audit_rows = with_audit_fields(
            results,
            args.dataset,
            args.year,
            args.month,
            args.benchmark_run_id,
            args.dag_run_id,
            args.repetition,
        )

        if args.dry_run:
            print_dry_run(
                silver_partition,
                audit_rows,
                args.dataset,
                args.year,
                args.month,
                args.catalog,
                quality_results_table,
            )
        else:
            write_results(spark, quality_results_table, audit_rows)

        return 1 if has_hard_failures(results) else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
