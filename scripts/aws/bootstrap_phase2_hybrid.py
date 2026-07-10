import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
import tomllib
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class AwsHybridConfig:
    profile: str
    region: str
    account_id: str
    raw_bucket: str
    raw_prefix: str
    warehouse_bucket: str
    warehouse_prefix: str
    glue_databases: list[str]


@dataclass(frozen=True)
class Partition:
    dataset: str
    year: int
    month: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap AWS resources for the Phase 2 hybrid-storage baseline."
    )
    parser.add_argument(
        "--profile-path",
        type=Path,
        default=Path("conf/environments/hybrid_aws.toml"),
    )
    parser.add_argument(
        "--workload-path",
        type=Path,
        default=Path("benchmarks/workloads/smoke.toml"),
    )
    parser.add_argument(
        "--local-data-root",
        type=Path,
        default=Path("data"),
        help="Local raw parquet root containing data/{year}/*.parquet.",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Create buckets and Glue databases but do not upload raw parquet files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned AWS actions without mutating AWS.",
    )
    return parser.parse_args()


def load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as file:
        return tomllib.load(file)


def load_config(path: Path) -> AwsHybridConfig:
    raw = load_toml(path)["aws"]
    return AwsHybridConfig(
        profile=str(raw["profile"]),
        region=str(raw["region"]),
        account_id=str(raw["account_id"]),
        raw_bucket=str(raw["raw_bucket"]),
        raw_prefix=str(raw["raw_prefix"]).strip("/"),
        warehouse_bucket=str(raw["warehouse_bucket"]),
        warehouse_prefix=str(raw["warehouse_prefix"]).strip("/"),
        glue_databases=[str(name) for name in raw["glue_databases"]],
    )


def load_partitions(path: Path) -> list[Partition]:
    raw = load_toml(path)
    return [
        Partition(
            dataset=str(item["dataset"]),
            year=int(item["year"]),
            month=int(item["month"]),
        )
        for item in raw.get("partitions", [])
    ]


def session_for(config: AwsHybridConfig) -> boto3.Session:
    return boto3.Session(profile_name=config.profile, region_name=config.region)


def ensure_bucket(s3, config: AwsHybridConfig, bucket: str, dry_run: bool) -> None:
    print(f"ensure bucket: s3://{bucket}")
    if dry_run:
        return

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as error:
        response_metadata = error.response.get("ResponseMetadata", {})
        status_raw = response_metadata.get("HTTPStatusCode", 0)
        status = int(status_raw) if status_raw is not None else 0
        if status not in {301, 403, 404}:
            raise
        kwargs: dict[str, Any] = {"Bucket": bucket}
        if config.region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": config.region}
        s3.create_bucket(**kwargs)

    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    s3.put_bucket_encryption(
        Bucket=bucket,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256",
                    },
                }
            ],
        },
    )


def ensure_prefix(s3, bucket: str, prefix: str, dry_run: bool) -> None:
    key = f"{prefix.rstrip('/')}/.keep"
    print(f"ensure prefix marker: s3://{bucket}/{key}")
    if not dry_run:
        s3.put_object(Bucket=bucket, Key=key, Body=b"")


def ensure_glue_database(
    glue, config: AwsHybridConfig, database: str, dry_run: bool
) -> None:
    location = (
        f"s3://{config.warehouse_bucket}/"
        f"{config.warehouse_prefix.rstrip('/')}/{database}.db"
    )
    print(f"ensure Glue database: {database} -> {location}")
    if dry_run:
        return

    try:
        glue.get_database(Name=database)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                "Name": database,
                "Description": (
                    "Phase 2 hybrid-storage Iceberg namespace for local compute."
                ),
                "LocationUri": location,
            }
        )


def raw_file_path(root: Path, partition: Partition) -> Path:
    return (
        root
        / str(partition.year)
        / f"{partition.dataset}_tripdata_{partition.year}-{partition.month:02d}.parquet"
    )


def upload_raw_file(
    s3,
    config: AwsHybridConfig,
    root: Path,
    partition: Partition,
    dry_run: bool,
) -> None:
    source = raw_file_path(root, partition)
    key = (
        f"{config.raw_prefix}/{partition.year}/"
        f"{partition.dataset}_tripdata_{partition.year}-{partition.month:02d}.parquet"
    )
    if not source.exists():
        if dry_run:
            print(f"missing local raw file: {source}")
            return
        raise FileNotFoundError(
            f"Missing local raw file for {partition}: {source}. "
            "Run bootstrap/initial_load.py for the benchmark year or pass --skip-upload."
        )

    print(f"upload raw file: {source} -> s3://{config.raw_bucket}/{key}")
    if not dry_run:
        s3.upload_file(str(source), config.raw_bucket, key)


def main() -> int:
    args = parse_args()
    config = load_config(args.profile_path)
    partitions = load_partitions(args.workload_path)
    session = session_for(config)
    s3 = session.client("s3")
    glue = session.client("glue")

    ensure_bucket(s3, config, config.raw_bucket, args.dry_run)
    ensure_bucket(s3, config, config.warehouse_bucket, args.dry_run)
    ensure_prefix(s3, config.raw_bucket, config.raw_prefix, args.dry_run)
    ensure_prefix(s3, config.warehouse_bucket, config.warehouse_prefix, args.dry_run)

    for database in config.glue_databases:
        ensure_glue_database(glue, config, database, args.dry_run)

    if not args.skip_upload:
        for partition in partitions:
            upload_raw_file(s3, config, args.local_data_root, partition, args.dry_run)

    return 0


if __name__ == "__main__":
    sys.exit(main())
