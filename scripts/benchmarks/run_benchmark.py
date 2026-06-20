import argparse
import hashlib
import json
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
from clients.trino_client import TrinoClient  # noqa: E402
from clients.utils import utc_now_iso, utc_timestamp  # noqa: E402

METRICS_TABLE = "lakehouse.benchmark.run_metrics"


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


def render_query_sql(query: Query, partition: Partition) -> str:
    return query.template.format(
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


def metric_insert_sql(metrics: list[dict[str, Any]]) -> str:
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
        "input_bytes",
        "output_bytes",
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
        f"insert into {METRICS_TABLE} ("
        + ", ".join(columns)
        + ") values\n"
        + ",\n".join(rows)
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
        "records_written": result.get("row_count"),
        "input_bytes": result.get("processed_bytes"),
        "output_bytes": None,
        "error_class": error.get("errorName") or error.get("errorType"),
        "error_message": error.get("message"),
        "artifact_path": str(artifact_path),
    }


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
    }


def rendered_runs(
    benchmark_run_id: str, workload: Workload, profile: dict[str, Any]
) -> list[dict[str, Any]]:
    input_base = str(profile["spark"]["input_base"])
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
            sql = render_query_sql(query, partition)
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


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    workload = load_workload(args.workload)
    profile = load_toml(args.profile)
    queries = load_queries(args.queries_dir)
    git_sha = git_short_sha()
    config_hash = canonical_config_hash(workload, profile, queries)
    benchmark_run_id = render_benchmark_run_id(profile, workload, git_sha)
    artifact_dir = args.artifact_root / benchmark_run_id
    artifact_path = artifact_dir / "benchmark_run.json"
    payload: dict[str, Any]

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
    metrics: list[dict[str, Any]] = []
    dag_results = []
    query_results = []
    poll_interval = int(profile["airflow"].get("poll_interval_seconds", 10))
    poll_timeout = int(profile["airflow"].get("poll_timeout_seconds", 7200))
    dag_id = str(profile["airflow"]["dag_id"])

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
            task_logs[task_id] = airflow.fetch_task_log(dag_run_id, task_id, try_number)

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
        metrics.extend(
            task_metric(base, task, artifact_path) for task in task_instances
        )

    for partition in workload.partitions:
        for query in queries:
            sql = render_query_sql(query, partition)
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

    payload = {
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
    write_json(artifact_path, payload)

    if metrics and not args.skip_metrics_insert:
        insert_result = trino.execute(metric_insert_sql(metrics))
        payload["metrics_insert"] = insert_result
        write_json(artifact_path, payload)

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
