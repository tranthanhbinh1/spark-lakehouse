import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import boto3
import tomllib

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from clients.trino_client import TrinoClient  # noqa: E402

DDL_FILES = [
    "01_silver_yellow_trips.sql",
    "02_silver_green_trips.sql",
    "03_silver_trips_quality_results.sql",
    "04_gold_trip_revenue_monthly.sql",
]
PHASE3_METRIC_COLUMNS = {
    "comparison_id": "string",
    "trial_id": "string",
    "sequence_position": "int",
    "measurement_protocol": "string",
    "retry_count": "int",
    "queued_time_ms": "bigint",
    "planning_time_ms": "bigint",
    "cpu_time_ms": "bigint",
    "physical_input_bytes": "bigint",
    "peak_memory_bytes": "bigint",
    "metric_name": "string",
    "metric_value": "double",
    "metric_unit": "string",
}


def load(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def checksum_stream(handle: Any) -> str:
    digest = hashlib.sha256()
    for block in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(block)
    return digest.hexdigest()


def checksum(path: Path) -> str:
    with path.open("rb") as handle:
        return checksum_stream(handle)


def client(profile: dict[str, Any]) -> Any:
    config = profile["object_store"]
    if config.get("profile"):
        session = boto3.Session(
            profile_name=config["profile"], region_name=config.get("region")
        )
        return session.client("s3")
    return boto3.client(
        "s3",
        endpoint_url=config.get("endpoint_url"),
        aws_access_key_id=os.environ.get(config.get("access_key_env", "")),
        aws_secret_access_key=os.environ.get(config.get("secret_key_env", "")),
    )


def source_path(root: Path, partition: dict[str, Any]) -> Path:
    year = int(partition["year"])
    month = int(partition["month"])
    dataset = str(partition["dataset"])
    return root / str(year) / f"{dataset}_tripdata_{year}-{month:02d}.parquet"


def spark_sql(profile: dict[str, Any], sql: str, capture: bool = False) -> str:
    command = [
        "docker",
        "exec",
        "-i",
        profile["runtime"]["spark_master_container"],
        "/opt/spark/bin/spark-sql",
        "-e",
        sql,
    ]
    result = subprocess.run(
        command,
        check=True,
        capture_output=capture,
        text=True,
    )
    return result.stdout if capture else ""


def namespace_locations(profile: dict[str, Any]) -> dict[str, str | None]:
    namespaces = profile["namespaces"]
    warehouse = profile.get("warehouse")
    if not warehouse:
        return {str(namespace): None for namespace in namespaces.values()}
    base = f"s3://{warehouse['bucket']}/{str(warehouse['prefix']).strip('/')}"
    return {str(namespace): f"{base}/{namespace}" for namespace in namespaces.values()}


def render_ddl(profile: dict[str, Any]) -> list[str]:
    catalog = str(profile["data_catalog"])
    namespaces = profile["namespaces"]
    source_dir = (
        ROOT / "src/etl/sql/hybrid_aws"
        if catalog == "lakehouse_hybrid"
        else ROOT / "src/etl/sql"
    )
    statements = []
    for namespace, location in namespace_locations(profile).items():
        location_clause = f" location '{location}'" if location else ""
        statements.append(
            f"create namespace if not exists {catalog}.{namespace}{location_clause}"
        )
    replacements = {
        f"{catalog}.silver": f"{catalog}.{namespaces['silver']}",
        f"{catalog}.quality": f"{catalog}.{namespaces['quality']}",
        f"{catalog}.gold": f"{catalog}.{namespaces['gold']}",
    }
    for name in DDL_FILES:
        text = (source_dir / name).read_text()
        for old, new in replacements.items():
            text = text.replace(old, new)
        statements.append(text)
    return statements


def verify_namespace_locations(profile: dict[str, Any]) -> list[dict[str, Any]]:
    catalog = str(profile["data_catalog"])
    results = []
    for namespace, expected in namespace_locations(profile).items():
        output = spark_sql(
            profile,
            f"describe namespace extended {catalog}.{namespace}",
            capture=True,
        )
        if expected and expected.rstrip("/") not in output:
            raise ValueError(
                f"Namespace {catalog}.{namespace} is outside {expected}: {output}"
            )
        results.append(
            {
                "namespace": f"{catalog}.{namespace}",
                "expected_location": expected,
                "describe_output": output,
            }
        )
    return results


def migrate_metrics_table(profile: dict[str, Any], dry_run: bool) -> list[str]:
    table = str(profile["metrics_table"])
    ddl = (ROOT / "src/etl/sql/05_benchmark_run_metrics.sql").read_text()
    if dry_run:
        return [
            ddl,
            *[
                f"alter table {table} add columns ({name} {column_type})"
                for name, column_type in PHASE3_METRIC_COLUMNS.items()
            ],
        ]
    spark_sql(profile, ddl)
    result = TrinoClient(profile["trino"]).execute(f"describe {table}")
    if result.get("state") != "FINISHED" or result.get("error"):
        raise RuntimeError(f"Could not describe metrics table: {result}")
    existing = {str(row[0]).lower() for row in result.get("rows", [])}
    migrations = []
    for name, column_type in PHASE3_METRIC_COLUMNS.items():
        if name in existing:
            continue
        statement = f"alter table {table} add columns ({name} {column_type})"
        spark_sql(profile, statement)
        migrations.append(statement)
    verified = TrinoClient(profile["trino"]).execute(f"describe {table}")
    final_columns = {str(row[0]).lower() for row in verified.get("rows", [])}
    missing = sorted(set(PHASE3_METRIC_COLUMNS) - final_columns)
    if missing:
        raise ValueError(f"Metrics migration incomplete; missing columns: {missing}")
    return migrations


def remote_record(
    s3: Any,
    bucket: str,
    key: str,
    local_sha256: str,
    size_bytes: int,
) -> dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    try:
        remote_sha256 = checksum_stream(response["Body"])
    finally:
        response["Body"].close()
    head = s3.head_object(Bucket=bucket, Key=key)
    if int(head["ContentLength"]) != size_bytes:
        raise ValueError(f"Size mismatch for s3://{bucket}/{key}")
    if remote_sha256 != local_sha256:
        raise ValueError(f"SHA-256 mismatch for s3://{bucket}/{key}")
    return {
        "remote_sha256": remote_sha256,
        "etag": str(head.get("ETag", "")).strip('"') or None,
        "version_id": head.get("VersionId"),
        "last_modified": (
            head["LastModified"].isoformat() if head.get("LastModified") else None
        ),
        "object_metadata": head.get("Metadata", {}),
    }


def capture_cleanup_scope(manifest_path: Path) -> dict[str, Any]:
    payload = json.loads(manifest_path.read_text())
    cleanup_objects = []
    for item in payload["architectures"]:
        profile = load(Path(item["profile"]))
        warehouse = profile.get("warehouse")
        if not warehouse:
            continue
        s3 = client(profile)
        bucket = str(warehouse["bucket"])
        prefix = str(warehouse["prefix"]).strip("/") + "/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = str(obj["Key"])
                head = s3.head_object(Bucket=bucket, Key=key)
                cleanup_objects.append(
                    {
                        "architecture": item["architecture"],
                        "bucket": bucket,
                        "key": key,
                        "size_bytes": int(obj["Size"]),
                        "etag": str(obj.get("ETag", "")).strip('"') or None,
                        "version_id": head.get("VersionId"),
                        "last_modified": obj["LastModified"].isoformat(),
                        "scope": "warehouse",
                    }
                )
    payload["cleanup_objects"] = cleanup_objects
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return payload


def cleanup_from_manifest(
    manifest_path: Path,
    accepted_marker: Path,
    dry_run: bool,
) -> None:
    if not accepted_marker.exists():
        raise ValueError(f"Cleanup requires accepted report marker: {accepted_marker}")
    payload = json.loads(manifest_path.read_text())
    comparison_id = str(payload["comparison_id"])
    if accepted_marker.read_text().strip() != comparison_id:
        raise ValueError("Accepted report marker does not match manifest comparison_id")
    profiles = {
        item["architecture"]: load(Path(item["profile"]))
        for item in payload["architectures"]
    }
    for record in [*payload["objects"], *payload.get("cleanup_objects", [])]:
        profile = profiles[record["architecture"]]
        print(f"delete s3://{record['bucket']}/{record['key']}")
        if not dry_run:
            client(profile).delete_object(
                Bucket=record["bucket"],
                Key=record["key"],
                **(
                    {"VersionId": record["version_id"]}
                    if record.get("version_id")
                    else {}
                ),
            )
    for item in payload["architectures"]:
        profile = profiles[item["architecture"]]
        catalog = str(profile["data_catalog"])
        for namespace in reversed(list(profile["namespaces"].values())):
            statement = f"drop namespace if exists {catalog}.{namespace} cascade"
            print(statement)
            if not dry_run:
                spark_sql(profile, statement)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare isolated Phase 3 inputs and tables."
    )
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("benchmarks/comparisons/phase3_baseline.toml"),
    )
    parser.add_argument("--comparison-id", default="phase3_preparation")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=Path("benchmarks/artifacts/phase3_preparation"),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--cleanup-manifest", type=Path)
    parser.add_argument("--capture-cleanup-scope", type=Path)
    parser.add_argument("--accepted-report-marker", type=Path)
    args = parser.parse_args()

    if args.capture_cleanup_scope:
        capture_cleanup_scope(args.capture_cleanup_scope)
        return 0

    if args.cleanup_manifest:
        if args.accepted_report_marker is None:
            raise ValueError("--cleanup-manifest requires --accepted-report-marker")
        cleanup_from_manifest(
            args.cleanup_manifest,
            args.accepted_report_marker,
            args.dry_run,
        )
        return 0

    spec = load(args.comparison)
    workload = load(Path(spec["workload"]))
    objects: list[dict[str, Any]] = []
    architecture_records = []
    metrics_migrations: list[str] = []
    for index, architecture in enumerate(spec["architectures"]):
        profile_path = Path(architecture["profile"])
        profile = load(profile_path)
        store = profile["object_store"]
        s3 = None if args.dry_run else client(profile)
        for partition in workload["partitions"]:
            source = source_path(args.data_root, partition)
            if not source.exists():
                raise FileNotFoundError(source)
            key = f"{str(store['prefix']).strip('/')}/{partition['year']}/{source.name}"
            local_sha256 = checksum(source)
            record = {
                "architecture": architecture["name"],
                "profile": str(profile_path),
                "source_path": str(source),
                "object_uri": f"s3://{store['bucket']}/{key}",
                "bucket": store["bucket"],
                "key": key,
                "size_bytes": source.stat().st_size,
                "local_sha256": local_sha256,
            }
            if not args.validate_only:
                print(f"seed {source} -> {record['object_uri']}")
                if not args.dry_run:
                    assert s3 is not None
                    s3.upload_file(
                        str(source),
                        store["bucket"],
                        key,
                        ExtraArgs={"Metadata": {"sha256": local_sha256}},
                    )
            if not args.dry_run:
                assert s3 is not None
                record.update(
                    remote_record(
                        s3,
                        str(store["bucket"]),
                        key,
                        local_sha256,
                        source.stat().st_size,
                    )
                )
            objects.append(record)

        namespace_evidence = []
        if not args.validate_only:
            for ddl in render_ddl(profile):
                print(ddl)
                if not args.dry_run:
                    spark_sql(profile, ddl)
        if not args.dry_run:
            namespace_evidence = verify_namespace_locations(profile)
        if index == 0:
            metrics_migrations = migrate_metrics_table(profile, args.dry_run)
        architecture_records.append(
            {
                "architecture": architecture["name"],
                "profile": str(profile_path),
                "catalog": profile["data_catalog"],
                "namespaces": profile["namespaces"],
                "namespace_evidence": namespace_evidence,
            }
        )

    manifest = {
        "comparison_id": args.comparison_id,
        "comparison_path": str(args.comparison),
        "architectures": architecture_records,
        "objects": objects,
        "metrics_migrations": metrics_migrations,
    }
    args.artifact_root.mkdir(parents=True, exist_ok=True)
    manifest_path = args.artifact_root / f"{args.comparison_id}_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
