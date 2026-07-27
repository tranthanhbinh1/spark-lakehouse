import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from clients.trino_client import TrinoClient  # noqa: E402
from scripts.benchmarks.prepare_phase3 import (  # noqa: E402
    migrate_metrics_table,
    spark_sql,
)
from scripts.benchmarks.run_benchmark import environment_snapshot  # noqa: E402

SILVER_DDL_FILES = {
    "yellow": "01_silver_yellow_trips.sql",
    "green": "02_silver_green_trips.sql",
}
TARGET_FILE_SIZE_BYTES = 10 * 1024 * 1024 * 1024


def load(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def git_commit_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def clean_worktree() -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    )
    return not result.stdout.strip()


def require_success(result: dict[str, Any], label: str) -> dict[str, Any]:
    if result.get("state") == "FINISHED" and not result.get("error"):
        return result
    raise RuntimeError(
        f"Trino operation failed for {label}: "
        f"query_id={result.get('query_id')} error={result.get('error')}"
    )


def namespace_location(profile: dict[str, Any], namespace: str) -> str | None:
    warehouse = profile.get("warehouse")
    if not warehouse:
        return None
    base = f"s3://{warehouse['bucket']}/{str(warehouse['prefix']).strip('/')}"
    return f"{base}/{namespace}"


def render_layout_ddl(profile: dict[str, Any]) -> list[str]:
    catalog = str(profile["data_catalog"])
    namespace = str(profile["namespaces"]["silver"])
    location = namespace_location(profile, namespace)
    location_clause = f" location '{location}'" if location else ""
    source_dir = (
        ROOT / "src/etl/sql/hybrid_aws"
        if catalog == "lakehouse_hybrid"
        else ROOT / "src/etl/sql"
    )
    statements = [
        f"create namespace if not exists {catalog}.{namespace}{location_clause}"
    ]
    for dataset, filename in SILVER_DDL_FILES.items():
        text = (source_dir / filename).read_text()
        text = text.replace(
            f"{catalog}.silver.{dataset}_trips",
            f"{catalog}.{namespace}.{dataset}_trips",
        )
        text = text.replace(
            "'write.format.default'='parquet'",
            "'write.format.default'='parquet',\n"
            "    'write.distribution-mode'='none',\n"
            f"    'write.target-file-size-bytes'='{TARGET_FILE_SIZE_BYTES}'",
        )
        statements.append(text)
    return statements


def spark_submit_command(
    cell: dict[str, Any],
    profile: dict[str, Any],
    partition: dict[str, Any],
    fragmented_file_count: int,
    dry_run: bool,
) -> list[str]:
    layout = str(cell["layout"])
    target_namespace = str(profile["namespaces"]["silver"])
    application_name = (
        f"phase4-preflight-{cell['name']}-{partition['dataset']}-"
        f"{partition['year']}-{int(partition['month']):02d}"
    )
    command = [
        "docker",
        "exec",
        str(profile["runtime"]["spark_master_container"]),
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--conf",
        "spark.driver.host=spark-master",
        "--conf",
        "spark.driver.bindAddress=0.0.0.0",
    ]
    if str(cell["architecture"]) == "hybrid_aws":
        command.extend(
            [
                "--conf",
                "spark.executorEnv.AWS_PROFILE=lakehouse-aws",
                "--conf",
                (
                    "spark.executorEnv.AWS_CREDENTIAL_PROFILES_FILE="
                    "/home/spark/.aws/credentials"
                ),
            ]
        )
    command.extend(
        [
            "/opt/lakehouse/src/etl/jobs/phase4_file_layout.py",
            "--dataset",
            str(partition["dataset"]),
            "--year",
            str(partition["year"]),
            "--month",
            str(partition["month"]),
            "--catalog",
            str(profile["data_catalog"]),
            "--source-namespace",
            str(cell["source_namespace"]),
            "--target-namespace",
            target_namespace,
            "--layout",
            layout,
            "--fragmented-file-count",
            str(fragmented_file_count),
            "--application-name",
            application_name,
        ]
    )
    if dry_run:
        command.append("--dry-run")
    return command


def parse_job_output(output: str) -> dict[str, Any]:
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "target_table" in payload:
            return payload
    raise ValueError("Phase 4 Spark job did not emit its JSON result")


def table_schema(client: TrinoClient, table: str) -> list[list[Any]]:
    result = require_success(client.execute(f"describe {table}"), f"schema {table}")
    return [[row[0], row[1]] for row in result["rows"]]


def quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def partition_content(
    client: TrinoClient,
    table: str,
    schema: list[list[Any]],
    year: int,
    month: int,
) -> dict[str, Any]:
    columns = [str(row[0]) for row in schema]
    row_value = ", ".join(quoted(column) for column in columns)
    null_counts = ", ".join(
        f"count_if({quoted(column)} is null) as {quoted('null_' + column)}"
        for column in columns
    )
    sql = (
        "select count(*) as row_count, "
        f"to_hex(checksum(row({row_value}))) as content_checksum, "
        f"{null_counts} from {table} "
        f"where year = {year} and month = {month}"
    )
    result = require_success(client.execute(sql), f"content {table} {year}-{month:02d}")
    rows = result.get("row_dicts", [])
    if len(rows) != 1:
        raise ValueError(f"Expected one content row for {table}: {rows}")
    return rows[0]


def partition_files(
    client: TrinoClient,
    table: str,
    year: int,
    month: int,
) -> dict[str, Any]:
    catalog, namespace, table_name = table.split(".", maxsplit=2)
    files_table = (
        f"{quoted(catalog)}.{quoted(namespace)}.{quoted(table_name + '$files')}"
    )
    sql = (
        "select count(*) as file_count, "
        "count_if(record_count <= 0) as empty_file_count, "
        "sum(record_count) as record_count, "
        "sum(file_size_in_bytes) as data_size_bytes "
        f"from {files_table} "
        f"where partition.year = {year} and partition.month = {month}"
    )
    result = require_success(client.execute(sql), f"files {table} {year}-{month:02d}")
    rows = result.get("row_dicts", [])
    if len(rows) != 1:
        raise ValueError(f"Expected one file row for {table}: {rows}")
    return rows[0]


def deterministic_results(
    client: TrinoClient,
    table: str,
    year: int,
    month: int,
) -> dict[str, Any]:
    aggregate_sql = (
        "select count(*) as row_count, "
        "count_if(is_valid_trip) as valid_trip_count, "
        "count(distinct pickup_location_id) as pickup_location_count, "
        "cast(round(sum(cast(total_amount as decimal(38, 6))), 4) "
        "as varchar) as total_amount "
        f"from {table} where year = {year} and month = {month}"
    )
    pickup_sql = (
        "select pickup_location_id, count(*) as trip_count, "
        "cast(round(sum(cast(total_amount as decimal(38, 6))), 4) "
        "as varchar) as total_amount "
        f"from {table} where year = {year} and month = {month} "
        "group by pickup_location_id order by pickup_location_id"
    )
    aggregate = require_success(
        client.execute(aggregate_sql),
        f"deterministic aggregate {table} {year}-{month:02d}",
    )
    pickup = require_success(
        client.execute(pickup_sql),
        f"deterministic pickup {table} {year}-{month:02d}",
    )
    return {
        "aggregate": aggregate["rows"],
        "pickup": pickup["rows"],
    }


def preflight(
    spec: dict[str, Any],
    profiles: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    workload = load(ROOT / str(spec["workload"]))
    expected_fragmented = int(spec["fragmented_file_count"])
    records: list[dict[str, Any]] = []
    errors: list[str] = []

    for cell in spec["cells"]:
        profile = profiles[str(cell["name"])]
        layout = str(cell["layout"])
        expected_files = expected_fragmented if layout == "fragmented" else 1
        client = TrinoClient(profile["trino"])
        namespace = str(profile["namespaces"]["silver"])
        for partition in workload["partitions"]:
            dataset = str(partition["dataset"])
            year = int(partition["year"])
            month = int(partition["month"])
            table = f"{profile['data_catalog']}.{namespace}.{dataset}_trips"
            schema = table_schema(client, table)
            content = partition_content(client, table, schema, year, month)
            files = partition_files(client, table, year, month)
            results = deterministic_results(client, table, year, month)
            record = {
                "cell": cell["name"],
                "architecture": cell["architecture"],
                "layout": layout,
                "table": table,
                "dataset": dataset,
                "year": year,
                "month": month,
                "schema": schema,
                "content": content,
                "files": files,
                "deterministic_results": results,
            }
            records.append(record)
            if int(files["file_count"] or 0) != expected_files:
                errors.append(
                    f"{cell['name']} {dataset} {year}-{month:02d}: "
                    f"expected {expected_files} files, got {files['file_count']}"
                )
            if int(files["empty_file_count"] or 0) != 0:
                errors.append(
                    f"{cell['name']} {dataset} {year}-{month:02d}: "
                    f"found {files['empty_file_count']} empty files"
                )
            if int(files["record_count"] or 0) != int(content["row_count"] or 0):
                errors.append(
                    f"{cell['name']} {dataset} {year}-{month:02d}: "
                    "file metadata record count differs from table row count"
                )

    expected_cells = len(spec["cells"]) * len(workload["partitions"])
    if len(records) != expected_cells:
        errors.append(f"Expected {expected_cells} cells, collected {len(records)}")

    grouped: dict[tuple[str, int, int], list[dict[str, Any]]] = {}
    for record in records:
        key = (record["dataset"], record["year"], record["month"])
        grouped.setdefault(key, []).append(record)
    for key, group in grouped.items():
        reference = group[0]
        for record in group[1:]:
            for field in ("schema", "content", "deterministic_results"):
                if record[field] != reference[field]:
                    errors.append(
                        f"Logical-equivalence mismatch for {key} field={field}: "
                        f"{reference['cell']} vs {record['cell']}"
                    )

    return {
        "status": "passed" if not errors else "failed",
        "expected_cell_count": expected_cells,
        "observed_cell_count": len(records),
        "records": records,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare and validate the Phase 4 2x2 file-layout experiment."
    )
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("benchmarks/comparisons/phase4_file_layout.toml"),
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=Path("benchmarks/artifacts/phase4_preflight"),
    )
    parser.add_argument("--preflight-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--skip-preflight", action="store_true")
    args = parser.parse_args()

    spec = load(args.comparison)
    preflight_id = args.preflight_id or (
        "phase4_preflight_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    artifact_dir = args.artifact_root / preflight_id
    artifact_path = artifact_dir / "preflight.json"
    profiles = {
        str(cell["name"]): load(ROOT / str(cell["profile"])) for cell in spec["cells"]
    }
    payload: dict[str, Any] = {
        "preflight_id": preflight_id,
        "comparison_path": str(args.comparison),
        "comparison_spec": spec,
        "resolved_profiles": profiles,
        "git_commit_sha": git_commit_sha(),
        "worktree_clean": clean_worktree(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "preparation_jobs": [],
        "artifact_path": str(artifact_path),
    }
    if not args.dry_run:
        payload["infrastructure_snapshots"] = {
            name: environment_snapshot(profile) for name, profile in profiles.items()
        }

    if not args.preflight_only:
        for index, cell in enumerate(spec["cells"]):
            profile = profiles[str(cell["name"])]
            for statement in render_layout_ddl(profile):
                if args.dry_run:
                    payload.setdefault("ddl", []).append(statement)
                else:
                    spark_sql(profile, statement)
            if index == 0:
                payload["metrics_migrations"] = migrate_metrics_table(
                    profile, args.dry_run
                )
            workload = load(ROOT / str(spec["workload"]))
            for partition in workload["partitions"]:
                command = spark_submit_command(
                    cell,
                    profile,
                    partition,
                    int(spec["fragmented_file_count"]),
                    args.dry_run,
                )
                job_record: dict[str, Any] = {
                    "cell": cell["name"],
                    "partition": partition,
                    "command": command,
                }
                if not args.dry_run:
                    result = subprocess.run(
                        command,
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    job_record["result"] = parse_job_output(result.stdout)
                payload["preparation_jobs"].append(job_record)

    if not args.skip_preflight and not args.dry_run:
        payload["preflight"] = preflight(spec, profiles)

    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    if payload.get("preflight", {}).get("status") == "failed":
        raise RuntimeError(f"Phase 4 preflight failed; inspect {artifact_path}")
    print(artifact_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
