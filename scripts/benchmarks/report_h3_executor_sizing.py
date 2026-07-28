import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from clients.trino_client import TrinoClient  # noqa: E402
from scripts.benchmarks.run_h3_executor_sizing import (  # noqa: E402
    config_hash,
    git_sha,
    load,
    schedule,
)


def spark_properties(metric: dict[str, Any]) -> dict[str, str]:
    environment = (metric.get("spark_history") or {}).get("environment") or {}
    return {
        str(key): str(value) for key, value in environment.get("sparkProperties", [])
    }


def query_metrics(profile: dict[str, Any], comparison_id: str) -> list[dict[str, Any]]:
    result = TrinoClient(profile["trino"]).execute(
        f"select * from {profile['metrics_table']} "
        f"where comparison_id = '{comparison_id.replace(chr(39), chr(39) * 2)}'"
    )
    if result.get("state") != "FINISHED" or result.get("error"):
        raise RuntimeError(f"H3 metric query failed: {result}")
    return list(result.get("row_dicts", []))


def median(values: list[float]) -> float:
    return float(statistics.median(values))


def validate(
    spec_path: Path,
    spec: dict[str, Any],
    state: dict[str, Any],
    artifact_dir: Path,
) -> tuple[list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    expected_pairs = schedule(spec)
    if state.get("status") != "complete":
        errors.append(f"State is not complete: {state.get('status')}")
    if state.get("git_commit_sha") != git_sha():
        errors.append("Current commit differs from frozen H3 execution commit")
    if state.get("comparison_config_hash") != config_hash(spec_path, spec):
        errors.append("H3 comparison specification hash differs")
    if len(state.get("attempts", [])) != len(expected_pairs):
        errors.append("H3 pair count differs from the frozen schedule")

    records: list[dict[str, Any]] = []
    expected_members = {
        (pair["pair_id"], member["profile_name"]): (pair, member)
        for pair in expected_pairs
        for member in pair["members"]
    }
    observed = {}
    for attempt in state.get("attempts", []):
        if attempt.get("status") != "complete":
            errors.append(f"Incomplete pair: {attempt.get('pair_id')}")
        for member in attempt.get("members", []):
            key = (attempt.get("pair_id"), member.get("profile_name"))
            observed[key] = member
            expected = expected_members.get(key)
            if expected is None:
                errors.append(f"Unexpected member: {key}")
                continue
            pair, expected_member = expected
            for field in (
                "profile",
                "cores_max",
                "executor_cores",
                "sequence_position",
            ):
                if member.get(field) != expected_member.get(field):
                    errors.append(f"{key}: {field} differs from schedule")
            if member.get("status") != "complete":
                errors.append(f"Incomplete member: {key}")
            run_id = str(member["benchmark_run_id"])
            path = artifact_dir / "benchmarks" / run_id / "benchmark_run.json"
            if not path.exists():
                errors.append(f"Missing benchmark artifact: {path}")
                continue
            payload = json.loads(path.read_text())
            dag_results = payload.get("dag_results", [])
            metrics = payload.get("metrics", [])
            if payload.get("git_sha") != state.get("git_commit_sha"):
                errors.append(f"{run_id}: Git identity mismatch")
            if len(dag_results) != 1:
                errors.append(f"{run_id}: expected one DAG result")
            elif (
                str(dag_results[0].get("dag_run", {}).get("state", "")).lower()
                != "success"
            ):
                errors.append(f"{run_id}: DAG did not succeed")
            task_metrics = [
                metric
                for metric in metrics
                if metric.get("metric_type") == "airflow_task"
            ]
            if len(task_metrics) != 3:
                errors.append(f"{run_id}: expected three Spark task metrics")
            for metric in task_metrics:
                props = spark_properties(metric)
                if props.get("spark.cores.max") != str(member["cores_max"]):
                    errors.append(f"{run_id}: spark.cores.max was not applied")
                if props.get("spark.executor.cores") != str(member["executor_cores"]):
                    errors.append(f"{run_id}: spark.executor.cores was not applied")
            pipeline = [
                metric for metric in metrics if metric.get("metric_type") == "pipeline"
            ]
            if len(pipeline) != 1 or pipeline[0].get("status") != "success":
                errors.append(f"{run_id}: invalid pipeline metric")
                continue
            records.append(
                {
                    "pair_id": pair["pair_id"],
                    "trial": pair["trial"],
                    "workload_name": pair["workload_name"],
                    "profile_name": member["profile_name"],
                    "cores_max": member["cores_max"],
                    "executor_cores": member["executor_cores"],
                    "duration_seconds": float(pipeline[0]["duration_seconds"]),
                    "tasks": {
                        str(metric["task_id"]): float(metric["duration_seconds"])
                        for metric in task_metrics
                    },
                }
            )
    missing = sorted(set(expected_members) - set(observed))
    if missing:
        errors.append(f"Missing scheduled members: {missing}")

    profile = load(Path(spec["profiles"][0]["path"]))
    database_metrics = query_metrics(profile, str(state["comparison_id"]))
    expected_metric_ids = set()
    for attempt in state.get("attempts", []):
        for member in attempt.get("members", []):
            path = (
                artifact_dir
                / "benchmarks"
                / str(member["benchmark_run_id"])
                / "benchmark_run.json"
            )
            if path.exists():
                expected_metric_ids.update(
                    str(metric["metric_id"])
                    for metric in json.loads(path.read_text()).get("metrics", [])
                )
    observed_metric_ids = {str(metric["metric_id"]) for metric in database_metrics}
    if observed_metric_ids != expected_metric_ids:
        errors.append(
            "Database metric IDs do not exactly match the benchmark artifacts"
        )
    return records, errors


def render(
    state: dict[str, Any],
    records: list[dict[str, Any]],
    errors: list[str],
    accepted_on: str | None,
) -> str:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[(record["workload_name"], record["profile_name"])].append(record)
    lines = [
        "# H3 Spark Executor-Sizing Report",
        "",
        f"- Comparison ID: `{state['comparison_id']}`",
        f"- Commit SHA: `{state['git_commit_sha']}`",
        f"- Status: **{'INVALID' if errors else ('ACCEPTED ' + accepted_on if accepted_on else 'READY FOR MANUAL ACCEPTANCE')}**",
        "",
        "## Pipeline Runtime",
        "",
        "| Workload | Profile | Cores | n | Runs (s) | Median (s) |",
        "| --- | --- | ---: | ---: | --- | ---: |",
    ]
    medians: dict[tuple[str, str], float] = {}
    for key, values in sorted(grouped.items()):
        workload, profile = key
        durations = [item["duration_seconds"] for item in values]
        medians[key] = median(durations)
        lines.append(
            f"| {workload} | {profile} | {values[0]['cores_max']} | "
            f"{len(values)} | {', '.join(f'{value:.3f}' for value in durations)} | "
            f"{medians[key]:.3f} |"
        )
    lines.extend(["", "## Diminishing Returns", ""])
    for workload in sorted({key[0] for key in medians}):
        small = medians.get((workload, "small"))
        medium = medians.get((workload, "medium"))
        default = medians.get((workload, "default"))
        if None not in (small, medium, default):
            small_gain = (small - medium) / small * 100
            default_gain = (medium - default) / medium * 100
            lines.append(
                f"- {workload}: 4→8 cores changed median runtime by "
                f"{small_gain:.2f}%; 8→12 cores changed it by {default_gain:.2f}%."
            )
    lines.extend(
        [
            "",
            "## Interpretation Limits",
            "",
            "- This is a three-repetition experiment on two declared monthly partitions.",
            "- It isolates application core allocation on the existing 12-core cluster; it does not vary worker hardware, executor memory, network capacity, or S3 configuration.",
            "- Runtime and Spark History metrics support a local diminishing-return claim only; they do not identify network or object storage as the causal bottleneck.",
            "",
            "## Acceptance Gate",
            "",
        ]
    )
    if errors:
        lines.append("- Validation failed; this report is not eligible for acceptance.")
        lines.extend(f"- {error}" for error in errors)
    elif accepted_on:
        lines.append(f"- The user explicitly accepted this report on {accepted_on}.")
        lines.append("- H3 is canonical and Phase 4 experimental work is complete.")
    else:
        lines.append(
            "- All automated identity, completeness, Spark-configuration, artifact, database-metric, and success gates passed."
        )
        lines.append("- Explicit user acceptance is still required before Phase 5.")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and report H3 results.")
    parser.add_argument("comparison_id")
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("benchmarks/comparisons/phase4_h3_executor_sizing.toml"),
    )
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("benchmarks/artifacts/comparisons")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/research_results/phase4_h3_executor_sizing_report.md"),
    )
    parser.add_argument("--accepted-on")
    args = parser.parse_args()
    artifact_dir = args.artifact_root / args.comparison_id
    state = json.loads((artifact_dir / "comparison_run.json").read_text())
    spec = load(args.comparison)
    records, errors = validate(args.comparison, spec, state, artifact_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(state, records, errors, args.accepted_on))
    report_dir = artifact_dir / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "h3_executor_sizing_statistics.json").write_text(
        json.dumps({"records": records, "validation_errors": errors}, indent=2)
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
