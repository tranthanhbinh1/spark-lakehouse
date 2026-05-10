from etl.quality import GreenTripsPanderaSchema, YellowTripsPanderaSchema

import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["yellow", "green"], required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--input-base", default="s3a://raw/data")
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


def run_pre_checks(df: DataFrame, dataset: str, year: int, month: int) -> list[dict]:
    row_count = df.count()

    results = [
        {
            "check_name": "partition_row_count",
            "severity": "hard",
            "status": "pass" if row_count > 0 else "fail",
            "observed_value": str(row_count),
            "threshold": "> 0",
            "extra": None,
        }
    ]

    return results


def schema_for_dataset(dataset: str):
    if dataset == "yellow":
        return YellowTripsPanderaSchema
    elif dataset == "green":
        return GreenTripsPanderaSchema

    raise ValueError(f"Unsupported dataset: {dataset}")


def run_pandera_checks(df: DataFrame, dataset: str) -> list[dict]:
    schema = schema_for_dataset(dataset)
    validated_df = schema.validate(check_obj=df)

    errors = dict(validated_df.pandera.errors)

    if not errors:
        return [
            {
                "check_name": "pandera_schema",
                "severity": "hard",
                "status": "pass",
                "observed_value": "0",
                "threshold": "0 errors",
                "extra": None,
            }
        ]

    return [
        {
            "check_name": "pandera_schema",
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
    processed_at = datetime.now(timezone.tzname("Asia/Ho_Chi_Minh"))

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
    results_df = spark.createDataFrame(rows)

    results_df.writeTo("lakehouse.quality.silver_trips_quality_results").append()


def has_hard_failures(results: list[dict]) -> bool:
    return any(
        results["severity"] == "hard" and results["status"] == "fail"
        for result in results
    )
