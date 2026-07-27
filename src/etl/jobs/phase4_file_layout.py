import argparse
import json
import time
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create one controlled Phase 4 Iceberg layout partition."
    )
    parser.add_argument("--dataset", choices=["yellow", "green"], required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--source-namespace", required=True)
    parser.add_argument("--target-namespace", required=True)
    parser.add_argument("--layout", choices=["fragmented", "compact"], required=True)
    parser.add_argument("--fragmented-file-count", type=int, default=16)
    parser.add_argument("--application-name")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/spark/spark-events")
        .config("spark.executor.memory", "6g")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .getOrCreate()
    )


def table_name(catalog: str, namespace: str, dataset: str) -> str:
    return f"{catalog}.{namespace}.{dataset}_trips"


def result_payload(
    args: argparse.Namespace,
    source_table: str,
    target_table: str,
    source_rows: int,
    elapsed_seconds: float | None,
) -> dict[str, Any]:
    return {
        "dataset": args.dataset,
        "year": args.year,
        "month": args.month,
        "layout": args.layout,
        "source_table": source_table,
        "target_table": target_table,
        "source_rows": source_rows,
        "requested_fragmented_file_count": args.fragmented_file_count,
        "elapsed_seconds": elapsed_seconds,
        "dry_run": args.dry_run,
    }


def main() -> None:
    args = parse_args()
    if args.fragmented_file_count < 2:
        raise ValueError("--fragmented-file-count must be at least 2")
    if args.source_namespace == args.target_namespace:
        raise ValueError("Source and target namespaces must be different")

    app_name = args.application_name or (
        f"phase4-layout-{args.layout}-{args.dataset}-{args.year}-{args.month:02d}"
    )
    spark = build_spark(app_name)
    source_table = table_name(args.catalog, args.source_namespace, args.dataset)
    target_table = table_name(args.catalog, args.target_namespace, args.dataset)

    try:
        source = spark.table(source_table).where(
            (F.col("year") == args.year) & (F.col("month") == args.month)
        )
        source_rows = source.count()
        if source_rows == 0:
            raise RuntimeError(
                f"No source rows in {source_table} for {args.year}-{args.month:02d}"
            )
        if args.layout == "fragmented" and source_rows < args.fragmented_file_count:
            raise RuntimeError(
                f"Cannot create {args.fragmented_file_count} non-empty files "
                f"from only {source_rows} rows"
            )

        if args.dry_run:
            print(
                json.dumps(
                    result_payload(
                        args,
                        source_table,
                        target_table,
                        source_rows,
                        None,
                    ),
                    sort_keys=True,
                )
            )
            return

        if args.layout == "fragmented":
            arranged = source.repartition(args.fragmented_file_count)
        else:
            arranged = source.coalesce(1)

        started_at = time.monotonic()
        (
            arranged.writeTo(target_table)
            .option("fanout-enabled", "true")
            .overwritePartitions()
        )
        elapsed_seconds = time.monotonic() - started_at
        print(
            json.dumps(
                result_payload(
                    args,
                    source_table,
                    target_table,
                    source_rows,
                    elapsed_seconds,
                ),
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
