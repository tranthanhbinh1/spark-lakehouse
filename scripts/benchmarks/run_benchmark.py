import argparse
import hashlib
import json
import platform
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import tomllib

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from clients.airflow_client import AirflowClient  # noqa: E402
from clients.spark_history_client import SparkHistoryClient  # noqa: E402
from clients.trino_client import TrinoClient  # noqa: E402
from clients.utils import utc_now_iso, utc_timestamp  # noqa: E402


@dataclass(frozen=True)
class Partition:
    dataset: str
    year: int
    month: int


@dataclass(frozen=True)
class Workload:
    path: Path
    name: str
    pipeline_repetitions: int
    query_repetitions: int
    partitions: list[Partition]


@dataclass(frozen=True)
class Query:
    name: str
    path: Path
    template: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the local lakehouse benchmark harness."
    )
    parser.add_argument(
        "--workload",
        type=Path,
        default=Path("benchmarks/workloads/smoke.toml"),
    )
    parser.add_argument(
        "--profile",
        type=Path,
        default=Path("conf/environments/onprem.toml"),
    )
    parser.add_argument("--queries-dir", type=Path, default=Path("benchmarks/queries"))
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("benchmarks/artifacts")
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--benchmark-run-id",
        help="Override the generated benchmark_run_id for a new benchmark execution.",
    )
    parser.add_argument(
        "--insert-metrics-from-artifact",
        type=Path,
        help="Reload metrics from an existing benchmark_run.json without rerunning the benchmark.",
    )
    parser.add_argument(
        "--skip-metrics-insert",
        action="store_true",
        help="Run Airflow and Trino but do not insert normalized metrics.",
    )
    return parser.parse_args()


def load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as file:
        return tomllib.load(file)


def load_workload(path: Path) -> Workload:
    raw = load_toml(path)
    partitions = [
        Partition(
            dataset=str(item["dataset"]),
            year=int(item["year"]),
            month=int(item["month"]),
        )
        for item in raw.get("partitions", [])
    ]
    if not partitions:
        raise ValueError(f"Workload has no partitions: {path}")

    for partition in partitions:
        if partition.dataset not in {"yellow", "green"}:
            raise ValueError(f"Unsupported dataset: {partition.dataset}")
        if not 1 <= partition.month <= 12:
            raise ValueError(f"Invalid month for {partition}: {partition.month}")

    return Workload(
        path=path,
        name=str(raw["name"]),
        pipeline_repetitions=int(raw.get("pipeline_repetitions", 1)),
        query_repetitions=int(raw.get("query_repetitions", 1)),
        partitions=partitions,
    )


def load_queries(queries_dir: Path) -> list[Query]:
    queries = [
        Query(path.stem, path, path.read_text())
        for path in sorted(queries_dir.glob("*.sql"))
    ]
    if not queries:
        raise ValueError(f"No SQL queries found in {queries_dir}")
    return queries


def git_short_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return result.stdout.strip()


def canonical_config_hash(
    workload: Workload,
    profile: dict[str, Any],
    queries: list[Query],
) -> str:
    payload = {
        "workload": load_toml(workload.path),
        "profile": profile,
        "queries": [
            {"name": query.name, "path": str(query.path), "sql": query.template}
            for query in queries
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def render_benchmark_run_id(
    profile: dict[str, Any], workload: Workload, sha: str
) -> str:
    profile_name = str(profile["name"])
    return f"bench_{profile_name}_{workload.name}_{utc_timestamp()}_{sha}"


def render_dag_run_id(
    benchmark_run_id: str, partition: Partition, repetition: int
) -> str:
    return (
        f"{benchmark_run_id}__{partition.dataset}_{partition.year}_"
        f"{partition.month:02d}__r{repetition:02d}"
    )


def render_query_sql(
    query: Query, partition: Partition, catalog: str, benchmark_run_id: str
) -> str:
    return query.template.format(
        catalog=catalog,
        benchmark_run_id=benchmark_run_id,
        dataset=partition.dataset,
        year=partition.year,
        month=partition.month,
    )


def json_default(value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object is not JSON serializable: {type(value)}")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_default))


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def timestamp_literal(value: Any) -> str:
    if value in {None, ""}:
        return "NULL"
    return f"cast(from_iso8601_timestamp({sql_literal(value)}) as timestamp)"


def metric_insert_sql(metrics_table: str, metrics: list[dict[str, Any]]) -> str:
    columns = [
        "benchmark_run_id",
        "metric_id",
        "metric_type",
        "architecture",
        "environment",
        "workload_name",
        "dag_id",
        "dag_run_id",
        "task_id",
        "query_name",
        "query_id",
        "dataset",
        "year",
        "month",
        "repetition",
        "status",
        "started_at",
        "finished_at",
        "duration_seconds",
        "records_read",
        "records_written",
        "result_rows",
        "input_bytes",
        "output_bytes",
        "executor_run_time_ms",
        "memory_bytes_spilled",
        "disk_bytes_spilled",
        "shuffle_read_bytes",
        "shuffle_read_records",
        "shuffle_write_bytes",
        "shuffle_write_records",
        "spark_application_id",
        "table_name",
        "file_count",
        "data_size_bytes",
        "cache_state",
        "error_class",
        "error_message",
        "git_sha",
        "config_hash",
        "artifact_path",
        "processed_at",
    ]
    timestamp_columns = {"started_at", "finished_at", "processed_at"}
    rows = []
    for metric in metrics:
        values = []
        for column in columns:
            value = metric.get(column)
            if column in timestamp_columns:
                values.append(timestamp_literal(value))
            else:
                values.append(sql_literal(value))
        rows.append("(" + ", ".join(values) + ")")
    return (
        f"insert into {metrics_table} ("
        + ", ".join(columns)
        + ") values\n"
        + ",\n".join(rows)
    )


def require_trino_success(result: dict[str, Any], label: str) -> dict[str, Any]:
    state = str(result.get("state", "")).upper()
    error = result.get("error")
    if state == "FINISHED" and not error:
        return result

    message = None
    if isinstance(error, dict):
        message = error.get("message") or error.get("errorName")
    raise RuntimeError(
        f"Trino operation failed: {label}; "
        f"query_id={result.get('query_id')}; state={state}; error={message or error}"
    )


def seconds_between(start: Any, end: Any) -> float | None:
    if not start or not end:
        return None
    try:
        start_dt = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (end_dt - start_dt).total_seconds()


def task_metric(
    base: dict[str, Any],
    task: dict[str, Any],
    artifact_path: Path,
) -> dict[str, Any]:
    task_id = task.get("task_id") or task.get("taskId")
    started_at = task.get("start_date") or task.get("startDate")
    finished_at = task.get("end_date") or task.get("endDate")
    return {
        **base,
        "metric_id": f"{base['dag_run_id']}__task__{task_id}",
        "metric_type": "airflow_task",
        "task_id": task_id,
        "query_name": None,
        "query_id": None,
        "status": task.get("state"),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": task.get("duration")
        or seconds_between(started_at, finished_at),
        "records_read": None,
        "records_written": None,
        "input_bytes": None,
        "output_bytes": None,
        "error_class": None,
        "error_message": None,
        "artifact_path": str(artifact_path),
    }


def pipeline_metric(
    base: dict[str, Any],
    dag_run: dict[str, Any],
    artifact_path: Path,
) -> dict[str, Any]:
    started_at = dag_run.get("start_date") or dag_run.get("startDate")
    finished_at = dag_run.get("end_date") or dag_run.get("endDate")
    return {
        **base,
        "metric_id": f"{base['dag_run_id']}__pipeline",
        "metric_type": "pipeline",
        "task_id": None,
        "query_name": None,
        "query_id": None,
        "status": dag_run.get("state"),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": seconds_between(started_at, finished_at),
        "records_read": None,
        "records_written": None,
        "input_bytes": None,
        "output_bytes": None,
        "error_class": None,
        "error_message": None,
        "artifact_path": str(artifact_path),
    }


def query_metric(
    base: dict[str, Any],
    query: Query,
    query_repetition: int,
    result: dict[str, Any],
    started_at: str,
    finished_at: str,
    artifact_path: Path,
) -> dict[str, Any]:
    error = result.get("error") or {}
    return {
        **base,
        "metric_id": (
            f"{base['benchmark_run_id']}__{base['dataset']}_{base['year']}_"
            f"{base['month']:02d}__query__{query.name}__qr{query_repetition:02d}"
        ),
        "metric_type": "trino_query",
        "task_id": None,
        "query_name": query.name,
        "query_id": result.get("query_id"),
        "status": result.get("state"),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": result.get("duration_seconds"),
        "records_read": result.get("processed_rows"),
        "records_written": None,
        "result_rows": result.get("row_count"),
        "input_bytes": result.get("processed_bytes"),
        "output_bytes": None,
        "error_class": error.get("errorName") or error.get("errorType"),
        "error_message": error.get("message"),
        "artifact_path": str(artifact_path),
    }


def iceberg_partition_metric(
    base: dict[str, Any], table_name: str, result: dict[str, Any], artifact_path: Path
) -> dict[str, Any]:
    rows = result.get("row_dicts", [])
    row = rows[0] if rows else {}
    return {
        **base,
        "metric_id": (
            f"{base['benchmark_run_id']}__{base['dataset']}_{base['year']}_"
            f"{base['month']:02d}__iceberg__{table_name}__r{base['repetition']:02d}"
        ),
        "metric_type": "iceberg_partition",
        "task_id": None,
        "query_name": None,
        "query_id": result.get("query_id"),
        "status": result.get("state"),
        "table_name": table_name,
        "records_read": row.get("record_count"),
        "file_count": row.get("file_count"),
        "data_size_bytes": row.get("total_size"),
        "artifact_path": str(artifact_path),
    }


def iceberg_partition_queries(
    profile: dict[str, Any], partition: Partition
) -> list[dict[str, str]]:
    catalog = str(profile["data_catalog"])
    dataset_literal = sql_literal(partition.dataset)
    return [
        {
            "table_name": f"{catalog}.silver.{partition.dataset}_trips",
            "sql": (
                "SELECT sum(record_count) AS record_count, "
                "sum(file_count) AS file_count, "
                "sum(total_size) AS total_size "
                f'FROM "{catalog}".silver."{partition.dataset}_trips$partitions" '
                f"WHERE partition.year = {partition.year} "
                f"AND partition.month = {partition.month}"
            ),
        },
        {
            "table_name": f"{catalog}.gold.trip_revenue_monthly",
            "sql": (
                "SELECT sum(record_count) AS record_count, "
                "sum(file_count) AS file_count, "
                "sum(total_size) AS total_size "
                f'FROM "{catalog}".gold."trip_revenue_monthly$partitions" '
                f"WHERE partition.dataset = {dataset_literal} "
                f"AND partition.year = {partition.year} "
                f"AND partition.month = {partition.month}"
            ),
        },
    ]


def validate_repeated_write_consistency(metrics: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for metric in metrics:
        if metric.get("metric_type") != "iceberg_partition":
            continue
        key = (
            metric.get("table_name"),
            metric.get("dataset"),
            metric.get("year"),
            metric.get("month"),
        )
        grouped.setdefault(key, []).append(metric)

    for key, rows in grouped.items():
        if len(rows) < 2:
            continue
        reference = rows[0]
        for row in rows[1:]:
            for column in ("records_read", "file_count"):
                if row.get(column) != reference.get(column):
                    raise ValueError(
                        "Repeated write consistency failed for "
                        f"{key}: {column} changed from "
                        f"{reference.get(column)} to {row.get(column)}"
                    )


def environment_snapshot(profile: dict[str, Any]) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "host": {"node": platform.node(), "platform": platform.platform()},
        "profile": profile,
        "containers": {},
    }
    for key, container in profile.get("runtime", {}).items():
        if not key.endswith("_container"):
            continue
        try:
            result = subprocess.run(
                ["docker", "inspect", str(container)],
                check=True,
                capture_output=True,
                text=True,
            )
            inspected = json.loads(result.stdout)[0]
            snapshot["containers"][key] = {
                "name": container,
                "image": inspected.get("Config", {}).get("Image"),
                "image_id": inspected.get("Image"),
                "host_config": inspected.get("HostConfig"),
            }
        except (OSError, subprocess.CalledProcessError, ValueError) as error:
            snapshot["containers"][key] = {"name": container, "error": str(error)}

    runtime = profile.get("runtime", {})
    config_commands = {
        "spark": [
            "docker",
            "exec",
            str(runtime.get("spark_master_container", "spark-master")),
            "sh",
            "-lc",
            "/opt/spark/bin/spark-submit --version 2>&1; sed -n '1,240p' /opt/spark/conf/spark-defaults.conf",
        ],
        "trino": [
            "docker",
            "exec",
            str(runtime.get("trino_container", "trino-coordinator")),
            "sh",
            "-lc",
            "trino --version; sed -n '1,240p' /etc/trino/config.properties; sed -n '1,240p' /etc/trino/jvm.config",
        ],
    }
    snapshot["runtime_configuration"] = {}
    for name, command in config_commands.items():
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            snapshot["runtime_configuration"][name] = result.stdout
        except (OSError, subprocess.CalledProcessError) as error:
            snapshot["runtime_configuration"][name] = {"error": str(error)}
    return snapshot


def base_metric(
    benchmark_run_id: str,
    profile: dict[str, Any],
    workload: Workload,
    partition: Partition,
    repetition: int,
    dag_id: str,
    dag_run_id: str,
    git_sha: str,
    config_hash: str,
) -> dict[str, Any]:
    return {
        "benchmark_run_id": benchmark_run_id,
        "architecture": profile["architecture"],
        "environment": profile["environment"],
        "workload_name": workload.name,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "dataset": partition.dataset,
        "year": partition.year,
        "month": partition.month,
        "repetition": repetition,
        "git_sha": git_sha,
        "config_hash": config_hash,
        "processed_at": utc_now_iso(),
        "cache_state": profile.get("cache_state", "uncontrolled"),
    }


def query_base_metric(
    benchmark_run_id: str,
    profile: dict[str, Any],
    workload: Workload,
    partition: Partition,
    query_repetition: int,
    dag_id: str,
    git_sha: str,
    config_hash: str,
) -> dict[str, Any]:
    return {
        "benchmark_run_id": benchmark_run_id,
        "architecture": profile["architecture"],
        "environment": profile["environment"],
        "workload_name": workload.name,
        "dag_id": dag_id,
        "dag_run_id": None,
        "dataset": partition.dataset,
        "year": partition.year,
        "month": partition.month,
        "repetition": query_repetition,
        "git_sha": git_sha,
        "config_hash": config_hash,
        "processed_at": utc_now_iso(),
        "cache_state": profile.get("cache_state", "uncontrolled"),
    }


def rendered_runs(
    benchmark_run_id: str, workload: Workload, profile: dict[str, Any]
) -> list[dict[str, Any]]:
    input_base = str(profile["spark"]["input_base"])
    catalog = str(profile["data_catalog"])
    runs = []
    for partition in workload.partitions:
        for repetition in range(1, workload.pipeline_repetitions + 1):
            dag_run_id = render_dag_run_id(benchmark_run_id, partition, repetition)
            runs.append(
                {
                    "dag_run_id": dag_run_id,
                    "conf": {
                        "benchmark_run_id": benchmark_run_id,
                        "dataset": partition.dataset,
                        "year": partition.year,
                        "month": partition.month,
                        "repetition": repetition,
                        "input_base": input_base,
                        "catalog": catalog,
                        "application_name_stage": f"{dag_run_id}__stage",
                        "application_name_quality": f"{dag_run_id}__quality",
                        "application_name_gold": f"{dag_run_id}__gold",
                    },
                    "partition": partition,
                    "repetition": repetition,
                }
            )
    return runs


def dry_run_payload(
    benchmark_run_id: str,
    workload: Workload,
    profile_path: Path,
    profile: dict[str, Any],
    queries: list[Query],
    git_sha: str,
    config_hash: str,
) -> dict[str, Any]:
    rendered_queries = []
    for partition in workload.partitions:
        for query in queries:
            sql = render_query_sql(
                query, partition, str(profile["data_catalog"]), benchmark_run_id
            )
            for query_repetition in range(1, workload.query_repetitions + 1):
                rendered_queries.append(
                    {
                        "partition": partition.__dict__,
                        "query_name": query.name,
                        "query_repetition": query_repetition,
                        "sql": sql,
                    }
                )

    return {
        "dry_run": True,
        "benchmark_run_id": benchmark_run_id,
        "workload_path": str(workload.path),
        "profile_path": str(profile_path),
        "query_paths": [str(query.path) for query in queries],
        "git_sha": git_sha,
        "config_hash": config_hash,
        "dag_id": profile["airflow"]["dag_id"],
        "dag_runs": [
            {
                "dag_run_id": run["dag_run_id"],
                "conf": run["conf"],
            }
            for run in rendered_runs(benchmark_run_id, workload, profile)
        ],
        "queries": rendered_queries,
    }


def insert_metrics_from_artifact(
    artifact_path: Path, profile: dict[str, Any], trino: TrinoClient
) -> dict[str, Any]:
    payload = read_json(artifact_path)
    metrics = payload.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        raise ValueError(f"Artifact has no metrics to insert: {artifact_path}")

    metric_ids = [str(metric["metric_id"]) for metric in metrics]
    if len(metric_ids) != len(set(metric_ids)):
        raise ValueError("Artifact metric_id values are not unique")

    benchmark_run_id = str(payload["benchmark_run_id"])
    metrics_table = str(profile["metrics_table"])
    require_trino_success(
        trino.execute(
            f"delete from {metrics_table} "
            f"where benchmark_run_id = {sql_literal(benchmark_run_id)}"
        ),
        f"delete metrics for {benchmark_run_id}",
    )
    insert_result = require_trino_success(
        trino.execute(metric_insert_sql(metrics_table, metrics)),
        f"insert metrics for {benchmark_run_id}",
    )
    payload["metrics_insert"] = insert_result
    write_json(artifact_path, payload)
    return payload


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    workload = load_workload(args.workload)
    profile = load_toml(args.profile)
    cache_state = str(profile.get("cache_state", "uncontrolled"))
    if cache_state not in {"uncontrolled", "warm", "cold"}:
        raise ValueError(f"Unsupported cache_state: {cache_state}")
    if cache_state != "uncontrolled":
        raise ValueError(
            "cache_state values 'warm' and 'cold' require an automated "
            "warm-up/restart protocol; use cache_state='uncontrolled' for now"
        )
    queries = load_queries(args.queries_dir)
    git_sha = git_short_sha()
    config_hash = canonical_config_hash(workload, profile, queries)
    benchmark_run_id = args.benchmark_run_id or render_benchmark_run_id(
        profile, workload, git_sha
    )
    artifact_dir = args.artifact_root / benchmark_run_id
    artifact_path = artifact_dir / "benchmark_run.json"
    payload: dict[str, Any]

    if args.insert_metrics_from_artifact:
        trino = TrinoClient(profile["trino"])
        return insert_metrics_from_artifact(
            args.insert_metrics_from_artifact, profile, trino
        )

    if args.dry_run:
        payload = dry_run_payload(
            benchmark_run_id,
            workload,
            args.profile,
            profile,
            queries,
            git_sha,
            config_hash,
        )
        write_json(artifact_path, payload)
        return payload

    airflow = AirflowClient(profile["airflow"])
    trino = TrinoClient(profile["trino"])
    history = SparkHistoryClient(str(profile["history_server_base_url"]))
    metrics: list[dict[str, Any]] = []
    dag_results = []
    query_results = []
    poll_interval = int(profile["airflow"].get("poll_interval_seconds", 10))
    poll_timeout = int(profile["airflow"].get("poll_timeout_seconds", 7200))
    dag_id = str(profile["airflow"]["dag_id"])

    def current_payload() -> dict[str, Any]:
        return {
            "dry_run": False,
            "benchmark_run_id": benchmark_run_id,
            "workload_path": str(workload.path),
            "profile_path": str(args.profile),
            "query_paths": [str(query.path) for query in queries],
            "git_sha": git_sha,
            "config_hash": config_hash,
            "dag_results": dag_results,
            "query_results": query_results,
            "metrics": metrics,
        }

    try:
        for run in rendered_runs(benchmark_run_id, workload, profile):
            partition = run["partition"]
            repetition = int(run["repetition"])
            dag_run_id = str(run["dag_run_id"])
            base = base_metric(
                benchmark_run_id,
                profile,
                workload,
                partition,
                repetition,
                dag_id,
                dag_run_id,
                git_sha,
                config_hash,
            )

            trigger_response = airflow.trigger_dag_run(dag_run_id, run["conf"])
            dag_run = airflow.wait_for_dag_run(dag_run_id, poll_interval, poll_timeout)
            task_instances = airflow.list_task_instances(dag_run_id)
            task_logs = {}
            for task in task_instances:
                task_id = str(task.get("task_id") or task.get("taskId"))
                try_number = int(task.get("try_number") or task.get("tryNumber") or 1)
                task_logs[task_id] = airflow.fetch_task_log(
                    dag_run_id, task_id, try_number
                )

            dag_results.append(
                {
                    "dag_run_id": dag_run_id,
                    "conf": run["conf"],
                    "trigger_response": trigger_response,
                    "dag_run": dag_run,
                    "task_instances": task_instances,
                    "task_logs": task_logs,
                }
            )
            metrics.append(pipeline_metric(base, dag_run, artifact_path))
            if str(dag_run.get("state", "")).lower() != "success":
                raise RuntimeError(f"Benchmark DAG run failed: {dag_run_id}")
            for task in task_instances:
                metric = task_metric(base, task, artifact_path)
                task_id = str(task.get("task_id") or task.get("taskId"))
                app_key = {
                    "stage_trips": "application_name_stage",
                    "check_silver_quality": "application_name_quality",
                    "build_gold_revenue": "application_name_gold",
                }.get(task_id)
                if app_key:
                    app = history.find_completed_application(
                        str(run["conf"][app_key]),
                        metric["started_at"],
                        metric["finished_at"],
                    )
                    spark_metrics = history.application_artifacts(str(app["id"]))
                    metric.update(
                        {
                            key: value
                            for key, value in spark_metrics.items()
                            if key not in {"stage_attempts", "environment"}
                        }
                    )
                    metric["spark_history"] = spark_metrics
                metrics.append(metric)

            for layout_query in iceberg_partition_queries(profile, partition):
                layout_result = require_trino_success(
                    trino.execute(layout_query["sql"]),
                    f"iceberg layout {layout_query['table_name']} {dag_run_id}",
                )
                metrics.append(
                    iceberg_partition_metric(
                        base,
                        layout_query["table_name"],
                        layout_result,
                        artifact_path,
                    )
                )

        validate_repeated_write_consistency(metrics)

        for partition in workload.partitions:
            for query in queries:
                sql = render_query_sql(
                    query, partition, str(profile["data_catalog"]), benchmark_run_id
                )
                for query_repetition in range(1, workload.query_repetitions + 1):
                    query_base = query_base_metric(
                        benchmark_run_id,
                        profile,
                        workload,
                        partition,
                        query_repetition,
                        dag_id,
                        git_sha,
                        config_hash,
                    )
                    query_started_at = utc_now_iso()
                    result = trino.execute(sql)
                    query_finished_at = utc_now_iso()
                    query_results.append(
                        {
                            "dag_run_id": None,
                            "partition": partition.__dict__,
                            "query_name": query.name,
                            "query_repetition": query_repetition,
                            "sql": sql,
                            "result": result,
                        }
                    )
                    require_trino_success(
                        result, f"benchmark query {query.name} r{query_repetition:02d}"
                    )
                    metrics.append(
                        query_metric(
                            query_base,
                            query,
                            query_repetition,
                            result,
                            query_started_at,
                            query_finished_at,
                            artifact_path,
                        )
                    )

        write_json(
            artifact_dir / "environment_snapshot.json", environment_snapshot(profile)
        )

        metric_ids = [str(metric["metric_id"]) for metric in metrics]
        if len(metric_ids) != len(set(metric_ids)):
            raise ValueError("Generated metric_id values are not unique")

        payload = current_payload()
        write_json(artifact_path, payload)

        if metrics and not args.skip_metrics_insert:
            metrics_table = str(profile["metrics_table"])
            require_trino_success(
                trino.execute(
                    f"delete from {metrics_table} "
                    f"where benchmark_run_id = {sql_literal(benchmark_run_id)}"
                ),
                f"delete metrics for {benchmark_run_id}",
            )
            insert_result = require_trino_success(
                trino.execute(metric_insert_sql(metrics_table, metrics)),
                f"insert metrics for {benchmark_run_id}",
            )
            payload["metrics_insert"] = insert_result
            write_json(artifact_path, payload)

    except Exception as error:
        payload = current_payload()
        payload["error"] = {"type": type(error).__name__, "message": str(error)}
        write_json(artifact_path, payload)
        raise

    return payload

def main() -> int:
    try:
        payload = run_benchmark(parse_args())
    except Exception as error:
        print(f"benchmark failed: {error}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "benchmark_run_id": payload["benchmark_run_id"],
                "dry_run": payload["dry_run"],
                "config_hash": payload["config_hash"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
