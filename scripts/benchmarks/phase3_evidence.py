import argparse
import json
import re
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import tomllib

REQUEST_METRICS = (
    "AllRequests",
    "GetRequests",
    "PutRequests",
    "4xxErrors",
    "5xxErrors",
    "BytesDownloaded",
    "BytesUploaded",
)
STORAGE_METRICS = (
    ("BucketSizeBytes", "StandardStorage", "Bytes"),
    ("NumberOfObjects", "AllStorageTypes", "Count"),
)


def load(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def aws_session(profile: dict[str, Any]) -> boto3.Session:
    store = profile["object_store"]
    return boto3.Session(
        profile_name=store.get("profile"),
        region_name=store.get("region", "us-east-1"),
    )


def filter_id(comparison_id: str, label: str) -> str:
    digest = comparison_id.replace("_", "-").lower()[-40:]
    return f"phase3-{digest}-{label}"[:64]


def request_metric_targets(
    comparison_id: str, profile: dict[str, Any]
) -> list[dict[str, str]]:
    store = profile["object_store"]
    warehouse = profile["warehouse"]
    return [
        {
            "label": "raw",
            "bucket": str(store["bucket"]),
            "prefix": str(store["prefix"]).strip("/") + "/",
            "filter_id": filter_id(comparison_id, "raw"),
        },
        {
            "label": "warehouse",
            "bucket": str(warehouse["bucket"]),
            "prefix": str(warehouse["prefix"]).strip("/") + "/",
            "filter_id": filter_id(comparison_id, "warehouse"),
        },
    ]


def set_request_metrics(
    comparison_id: str, profile: dict[str, Any], enabled: bool
) -> list[dict[str, str]]:
    s3 = aws_session(profile).client("s3")
    targets = request_metric_targets(comparison_id, profile)
    for target in targets:
        if enabled:
            s3.put_bucket_metrics_configuration(
                Bucket=target["bucket"],
                Id=target["filter_id"],
                MetricsConfiguration={
                    "Id": target["filter_id"],
                    "Filter": {"Prefix": target["prefix"]},
                },
            )
        else:
            s3.delete_bucket_metrics_configuration(
                Bucket=target["bucket"],
                Id=target["filter_id"],
            )
    return targets


def metric_datapoints(
    cloudwatch: Any,
    bucket: str,
    metric_name: str,
    start: datetime,
    end: datetime,
    metric_filter_id: str | None = None,
    storage_type: str | None = None,
    statistic: str = "Sum",
) -> list[dict[str, Any]]:
    dimensions = [{"Name": "BucketName", "Value": bucket}]
    if metric_filter_id:
        dimensions.append({"Name": "FilterId", "Value": metric_filter_id})
    if storage_type:
        dimensions.append({"Name": "StorageType", "Value": storage_type})
    response = cloudwatch.get_metric_statistics(
        Namespace="AWS/S3",
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start,
        EndTime=end,
        Period=60 if metric_filter_id else 86400,
        Statistics=[statistic],
    )
    points = sorted(response.get("Datapoints", []), key=lambda item: item["Timestamp"])
    return [
        {
            **point,
            "Timestamp": point["Timestamp"].isoformat(),
        }
        for point in points
    ]


def collect_request_window(
    comparison_id: str,
    profile: dict[str, Any],
    start: datetime,
    end: datetime,
    poll_attempts: int = 5,
    poll_interval_seconds: int = 60,
) -> dict[str, Any]:
    cloudwatch = aws_session(profile).client("cloudwatch")
    targets = request_metric_targets(comparison_id, profile)
    payload: dict[str, Any] = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "period_seconds": 60,
        "targets": [],
    }
    for attempt in range(1, poll_attempts + 1):
        records = []
        found = False
        for target in targets:
            metrics = {}
            for metric_name in REQUEST_METRICS:
                points = metric_datapoints(
                    cloudwatch,
                    target["bucket"],
                    metric_name,
                    start - timedelta(minutes=1),
                    end + timedelta(minutes=2),
                    metric_filter_id=target["filter_id"],
                )
                metrics[metric_name] = points
                found = found or bool(points)
            records.append({**target, "metrics": metrics})
        payload["targets"] = records
        payload["poll_attempts"] = attempt
        if found or attempt == poll_attempts:
            payload["complete"] = found
            return payload
        time.sleep(poll_interval_seconds)
    return payload


def prefix_inventory(s3: Any, bucket: str, prefix: str) -> dict[str, Any]:
    paginator = s3.get_paginator("list_objects_v2")
    object_count = 0
    size_bytes = 0
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            object_count += 1
            size_bytes += int(item["Size"])
            objects.append(
                {
                    "key": item["Key"],
                    "size_bytes": int(item["Size"]),
                    "etag": str(item.get("ETag", "")).strip('"') or None,
                    "last_modified": item["LastModified"].isoformat(),
                }
            )
    return {
        "bucket": bucket,
        "prefix": prefix,
        "object_count": object_count,
        "size_bytes": size_bytes,
        "objects": objects,
    }


def static_snapshot(comparison_id: str, profile: dict[str, Any]) -> dict[str, Any]:
    session = aws_session(profile)
    s3 = session.client("s3")
    cloudwatch = session.client("cloudwatch")
    now = datetime.now(timezone.utc)
    targets = request_metric_targets(comparison_id, profile)
    inventories = [
        prefix_inventory(s3, target["bucket"], target["prefix"]) for target in targets
    ]
    storage = []
    for bucket in sorted({target["bucket"] for target in targets}):
        metrics = {}
        for metric_name, storage_type, unit in STORAGE_METRICS:
            metrics[metric_name] = {
                "unit": unit,
                "datapoints": metric_datapoints(
                    cloudwatch,
                    bucket,
                    metric_name,
                    now - timedelta(days=3),
                    now,
                    storage_type=storage_type,
                    statistic="Average",
                ),
            }
        storage.append({"bucket": bucket, "metrics": metrics})
    pricing_client = session.client("pricing", region_name="us-east-1")
    price_list = []
    paginator = pricing_client.get_paginator("get_products")
    for page in paginator.paginate(
        ServiceCode="AmazonS3",
        Filters=[
            {
                "Type": "TERM_MATCH",
                "Field": "location",
                "Value": "US East (N. Virginia)",
            },
        ],
        PaginationConfig={"PageSize": 100},
    ):
        price_list.extend(page.get("PriceList", []))
    data_transfer_price_list = []
    for page in paginator.paginate(
        ServiceCode="AWSDataTransfer",
        Filters=[
            {
                "Type": "TERM_MATCH",
                "Field": "fromRegionCode",
                "Value": "us-east-1",
            },
            {
                "Type": "TERM_MATCH",
                "Field": "transferType",
                "Value": "AWS Outbound",
            },
            {
                "Type": "TERM_MATCH",
                "Field": "toLocation",
                "Value": "External",
            },
        ],
        PaginationConfig={"PageSize": 100},
    ):
        data_transfer_price_list.extend(page.get("PriceList", []))
    return {
        "captured_at": now.isoformat(),
        "prefix_inventories": inventories,
        "bucket_storage_metrics": storage,
        "storage_metric_limitation": (
            "CloudWatch storage metrics are daily bucket/storage-class snapshots, "
            "not precise per-prefix or per-execution measurements."
        ),
        "pricing": {
            "service_code": "AmazonS3",
            "region": "us-east-1",
            "price_list": price_list,
            "data_transfer_service_code": "AWSDataTransfer",
            "data_transfer_price_list": data_transfer_price_list,
        },
    }


def parse_size(value: str) -> float:
    units = {
        "B": 1,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
    }
    match = re.fullmatch(r"([0-9.]+)\s*([A-Za-z]+)", value.strip())
    if match is None:
        raise ValueError(f"Unsupported Docker size: {value}")
    return float(match.group(1)) * units[match.group(2)]


class DockerSampler:
    def __init__(self, containers: list[str], interval_seconds: int = 5) -> None:
        self.containers = containers
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _sample(self) -> None:
        while not self._stop.is_set():
            try:
                result = subprocess.run(
                    [
                        "docker",
                        "stats",
                        "--no-stream",
                        "--format",
                        "{{json .}}",
                        *self.containers,
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                captured_at = datetime.now(timezone.utc).isoformat()
                for line in result.stdout.splitlines():
                    row = json.loads(line)
                    memory_usage = str(row.get("MemUsage", "")).split("/", 1)[0].strip()
                    self.samples.append(
                        {
                            "captured_at": captured_at,
                            "container": row.get("Name") or row.get("Container"),
                            "cpu_percent": float(str(row["CPUPerc"]).rstrip("%")),
                            "memory_bytes": parse_size(memory_usage),
                        }
                    )
            except (
                OSError,
                ValueError,
                KeyError,
                subprocess.CalledProcessError,
            ) as error:
                self.errors.append(str(error))
            self._stop.wait(self.interval_seconds)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_seconds + 2)
        return {
            "interval_seconds": self.interval_seconds,
            "samples": self.samples,
            "errors": self.errors,
        }


def runtime_containers(profile: dict[str, Any]) -> list[str]:
    runtime = profile["runtime"]
    values = []
    for key, value in runtime.items():
        if key.endswith("_container"):
            values.append(str(value))
        elif key.endswith("_containers"):
            values.extend(str(item) for item in value)
    return sorted(set(values))


def refresh_state_evidence(state_path: Path, profile: dict[str, Any]) -> dict[str, Any]:
    state = json.loads(state_path.read_text())
    comparison_id = str(state["comparison_id"])
    windows = state.get("evidence", {}).get("windows", {})
    for window in windows.values():
        request_metrics = window.get("request_metrics", {})
        if request_metrics.get("complete"):
            continue
        window["request_metrics"] = collect_request_window(
            comparison_id,
            profile,
            datetime.fromisoformat(window["start"]),
            datetime.fromisoformat(window["end"]),
        )
        state_path.write_text(json.dumps(state, indent=2, sort_keys=True))
    return state


def cost_explorer_totals(
    profile: dict[str, Any], start: str, end: str
) -> dict[str, Any]:
    client = aws_session(profile).client("ce", region_name="us-east-1")
    return client.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="DAILY",
        Metrics=["UnblendedCost", "UsageQuantity"],
        Filter={
            "Dimensions": {
                "Key": "SERVICE",
                "Values": ["Amazon Simple Storage Service"],
            }
        },
        GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage Phase 3 AWS evidence.")
    parser.add_argument("--comparison-id", required=True)
    parser.add_argument(
        "--profile", type=Path, default=Path("conf/environments/phase3_hybrid_aws.toml")
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("benchmarks/artifacts/phase3_evidence"),
    )
    parser.add_argument(
        "action",
        choices=[
            "enable",
            "disable",
            "snapshot",
            "collect-window",
            "refresh-state",
            "cost-explorer",
        ],
    )
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--state", type=Path)
    args = parser.parse_args()
    profile = load(args.profile)
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    if args.action in {"enable", "disable"}:
        payload: Any = set_request_metrics(
            args.comparison_id, profile, args.action == "enable"
        )
    elif args.action == "snapshot":
        payload = static_snapshot(args.comparison_id, profile)
    elif args.action == "refresh-state":
        if args.state is None:
            raise ValueError("refresh-state requires --state")
        payload = refresh_state_evidence(args.state, profile)
    elif args.action == "cost-explorer":
        if not args.start or not args.end:
            raise ValueError("cost-explorer requires YYYY-MM-DD --start and --end")
        payload = cost_explorer_totals(profile, args.start, args.end)
    else:
        if not args.start or not args.end:
            raise ValueError("collect-window requires --start and --end")
        payload = collect_request_window(
            args.comparison_id,
            profile,
            datetime.fromisoformat(args.start),
            datetime.fromisoformat(args.end),
        )
    output = args.artifact_dir / f"{args.action}.json"
    output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
