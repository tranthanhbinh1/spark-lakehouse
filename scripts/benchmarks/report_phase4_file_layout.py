import argparse
import csv
import hashlib
import json
import math
import random
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from clients.trino_client import TrinoClient  # noqa: E402
from scripts.benchmarks.run_phase4_comparison import (  # noqa: E402
    comparison_hash,
    has_snapshot_error,
    schedule,
    validate_preflight,
)

MEASURES = {
    "duration_seconds": "latency (s)",
    "planning_time_ms": "planning time (ms)",
    "physical_input_bytes": "physical input (bytes)",
}
RECORDED_PROTOCOLS = {"warm_recorded", "service_cold_recorded"}
EXPECTED_TRIALS = 3
EXPECTED_PROTOCOL_EXECUTIONS = {
    "warmup": 1,
    "warm_recorded": 5,
    "service_cold_recorded": 3,
}
BOOTSTRAP_SEED = 20260727
BOOTSTRAP_RESAMPLES = 10_000


def load_toml(path: Path) -> dict[str, Any]:
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


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def percentile(values: list[float], probability: float) -> float:
    if not values:
        raise ValueError("Cannot calculate a percentile of an empty sample")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def descriptive(values: list[float]) -> dict[str, float | int] | None:
    if not values:
        return None
    return {
        "n": len(values),
        "median": statistics.median(values),
        "q1": percentile(values, 0.25),
        "q3": percentile(values, 0.75),
        "iqr": percentile(values, 0.75) - percentile(values, 0.25),
        "p95": percentile(values, 0.95),
    }


def bootstrap_median(
    values: list[float],
    seed_offset: int,
) -> dict[str, float | int] | None:
    if not values:
        return None
    rng = random.Random(BOOTSTRAP_SEED + seed_offset)
    estimates = [
        statistics.median(rng.choices(values, k=len(values)))
        for _ in range(BOOTSTRAP_RESAMPLES)
    ]
    return {
        "n": len(values),
        "median": statistics.median(values),
        "ci95_low": percentile(estimates, 0.025),
        "ci95_high": percentile(estimates, 0.975),
        "seed": BOOTSTRAP_SEED + seed_offset,
        "resamples": BOOTSTRAP_RESAMPLES,
    }


def resolve_artifact_path(path_value: str) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else ROOT / path


def validate_spec(spec: dict[str, Any]) -> list[str]:
    errors = []
    if int(spec.get("trial_repetitions", -1)) != EXPECTED_TRIALS:
        errors.append(
            f"Expected {EXPECTED_TRIALS} trials, got "
            f"{spec.get('trial_repetitions')}"
        )
    observed_protocols = {
        "warmup": int(spec.get("warmup_executions", -1)),
        "warm_recorded": int(spec.get("recorded_executions", -1)),
        "service_cold_recorded": int(spec.get("cold_executions", -1)),
    }
    if observed_protocols != EXPECTED_PROTOCOL_EXECUTIONS:
        errors.append(
            "Protocol execution counts changed: "
            f"expected={EXPECTED_PROTOCOL_EXECUTIONS} observed={observed_protocols}"
        )
    expected_cells = {
        ("onprem", "fragmented"),
        ("onprem", "compact"),
        ("hybrid_aws", "fragmented"),
        ("hybrid_aws", "compact"),
    }
    observed_cells = {
        (str(cell.get("architecture")), str(cell.get("layout")))
        for cell in spec.get("cells", [])
    }
    if observed_cells != expected_cells or len(spec.get("cells", [])) != 4:
        errors.append(
            f"Expected the declared 2x2 cells, observed={sorted(observed_cells)}"
        )
    return errors


def validate_state(
    spec: dict[str, Any],
    state: dict[str, Any],
    artifact_dir: Path,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    errors = validate_spec(spec)
    expected_pairs = schedule(spec)
    attempts = list(state.get("attempts", []))
    if state.get("status") != "complete":
        errors.append(f"Comparison status is not complete: {state.get('status')}")
    if state.get("git_commit_sha") != git_commit_sha():
        errors.append("Comparison commit does not match current HEAD")
    if state.get("comparison_config_hash") != comparison_hash(
        Path(str(state["comparison_path"])), spec
    ):
        errors.append("Comparison configuration hash does not match current inputs")
    if len(expected_pairs) != 486:
        errors.append(f"Expected 486 scheduled pairs, generated {len(expected_pairs)}")
    if len(attempts) != len(expected_pairs):
        errors.append(
            f"Expected {len(expected_pairs)} attempts, observed {len(attempts)}"
        )

    members: dict[str, dict[str, Any]] = {}
    positions: list[int] = []
    for index, (expected, attempt) in enumerate(
        zip(expected_pairs, attempts, strict=False),
        start=1,
    ):
        for field in ("pair_id", "trial", "protocol", "execution", "target"):
            if attempt.get(field) != expected.get(field):
                errors.append(
                    f"Attempt {index} field {field} differs from frozen schedule"
                )
        if attempt.get("status") != "complete":
            errors.append(
                f"Attempt {attempt.get('pair_id', index)} is "
                f"{attempt.get('status')}"
            )
        observed_members = list(attempt.get("members", []))
        expected_members = list(expected["members"])
        if len(observed_members) != len(expected_members):
            errors.append(
                f"{expected['pair_id']}: expected {len(expected_members)} members, "
                f"observed {len(observed_members)}"
            )
        for expected_member, member in zip(
            expected_members, observed_members, strict=False
        ):
            for field in ("architecture", "layout", "sequence_position"):
                if member.get(field) != expected_member.get(field):
                    errors.append(
                        f"{expected['pair_id']}: member field {field} differs "
                        "from frozen schedule"
                    )
            run_id = str(member.get("benchmark_run_id", ""))
            if not run_id:
                errors.append(f"{expected['pair_id']}: missing benchmark run ID")
                continue
            if run_id in members:
                errors.append(f"Duplicate benchmark run ID: {run_id}")
            if member.get("status") != "complete":
                errors.append(f"Incomplete member: {run_id}")
            position = int(member.get("sequence_position", -1))
            positions.append(position)
            resource = member.get("resource_samples", {})
            if resource.get("errors"):
                errors.append(f"Docker sampling errors: {run_id}")
            members[run_id] = {
                **member,
                "pair_id": expected["pair_id"],
                "trial": expected["trial"],
                "protocol": expected["protocol"],
                "execution": expected["execution"],
                "target": expected["target"],
            }

    expected_positions = list(range(1, len(expected_pairs) * 4 + 1))
    if sorted(positions) != expected_positions:
        errors.append(
            "Sequence positions are not the unique contiguous range "
            f"1..{len(expected_positions)}"
        )
    if len(members) != 1_944:
        errors.append(f"Expected 1,944 unique members, observed {len(members)}")
    if artifact_dir.name != state.get("comparison_id"):
        errors.append("Artifact directory does not match comparison ID")
    return members, errors


def validate_preflight_evidence(
    state: dict[str, Any],
    spec: dict[str, Any],
    comparison_path: Path,
) -> tuple[dict[str, Any] | None, list[str]]:
    errors = []
    raw_path = state.get("preflight_artifact")
    if not raw_path:
        return None, ["Comparison state has no preflight artifact"]
    path = resolve_artifact_path(str(raw_path))
    if not path.exists():
        return None, [f"Missing preflight artifact: {path}"]
    if sha256(path) != state.get("preflight_artifact_sha256"):
        errors.append("Preflight artifact hash differs from comparison state")
    try:
        payload = validate_preflight(
            path,
            comparison_path,
            git_commit_sha(),
            spec,
        )
    except (KeyError, TypeError, ValueError) as error:
        return None, [f"Preflight identity validation failed: {error}"]
    if payload.get("preflight_id") != state.get("preflight_id"):
        errors.append("Preflight ID differs from comparison state")

    records = list(payload.get("preflight", {}).get("records", []))
    if payload.get("preflight", {}).get("expected_cell_count") != 32:
        errors.append("Preflight did not declare 32 expected cells")
    if len(records) != 32:
        errors.append(f"Expected 32 preflight records, observed {len(records)}")
    expected_fragmented = int(spec["fragmented_file_count"])
    for record in records:
        layout = str(record.get("layout"))
        expected_files = expected_fragmented if layout == "fragmented" else 1
        files = record.get("files", {})
        if int(files.get("file_count") or 0) != expected_files:
            errors.append(
                f"Preflight file-count mismatch: {record.get('cell')} "
                f"{record.get('dataset')} {record.get('year')}-"
                f"{int(record.get('month', 0)):02d}"
            )
        if int(files.get("empty_file_count") or 0) != 0:
            errors.append(f"Preflight contains empty files: {record.get('cell')}")
    snapshots = payload.get("infrastructure_snapshots", {})
    if has_snapshot_error(snapshots):
        errors.append("Preflight infrastructure snapshot contains an error")
    return payload, errors


def fetch_metrics(comparison_id: str, profile: dict[str, Any]) -> list[dict[str, Any]]:
    table = str(profile["metrics_table"])
    result = TrinoClient(profile["trino"]).execute(
        f"select * from {table} where comparison_id = "
        f"{sql_literal(comparison_id)}"
    )
    if result.get("state") != "FINISHED" or result.get("error"):
        raise RuntimeError(f"Metric query failed: {result}")
    return list(result.get("row_dicts", []))


def comparable_rows(rows: list[list[Any]]) -> list[list[Any]]:
    return sorted(
        (list(row) for row in rows),
        key=lambda row: json.dumps(row, sort_keys=True, default=str),
    )


def values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-6)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            values_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    if isinstance(left, dict) and isinstance(right, dict):
        return set(left) == set(right) and all(
            values_equal(left[key], right[key]) for key in left
        )
    return left == right


def artifact_result(
    artifact_dir: Path,
    run_id: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    errors = []
    path = artifact_dir / "benchmarks" / run_id / "benchmark_run.json"
    if not path.exists():
        return None, [f"Missing benchmark artifact: {path}"]
    payload = json.loads(path.read_text())
    if payload.get("error"):
        errors.append(f"Benchmark artifact records an error: {run_id}")
    query_results = list(payload.get("query_results", []))
    if len(query_results) != 1:
        errors.append(
            f"Expected one query result in {run_id}, observed {len(query_results)}"
        )
        return payload, errors
    result = query_results[0].get("result", {})
    if result.get("state") != "FINISHED" or result.get("error"):
        errors.append(f"Artifact query did not finish successfully: {run_id}")
    result["rows"] = comparable_rows(result.get("rows", []))
    return payload, errors


def validate_metrics(
    metrics: list[dict[str, Any]],
    members: dict[str, dict[str, Any]],
    artifact_dir: Path,
) -> list[str]:
    errors = []
    recorded = {
        run_id: member
        for run_id, member in members.items()
        if member["protocol"] in RECORDED_PROTOCOLS
    }
    if len(recorded) != 1_728:
        errors.append(f"Expected 1,728 recorded members, observed {len(recorded)}")
    if len(metrics) != len(recorded):
        errors.append(
            f"Expected {len(recorded)} database metrics, observed {len(metrics)}"
        )

    by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
    metric_ids = []
    query_ids = []
    for metric in metrics:
        by_run[str(metric.get("benchmark_run_id"))].append(metric)
        metric_ids.append(str(metric.get("metric_id")))
        if metric.get("query_id"):
            query_ids.append(str(metric["query_id"]))
    if len(metric_ids) != len(set(metric_ids)):
        errors.append("Metric IDs are not unique")
    if len(query_ids) != len(set(query_ids)):
        errors.append("Trino query IDs are not unique")

    results_by_pair: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for run_id, member in members.items():
        run_metrics = by_run.get(run_id, [])
        if member["protocol"] == "warmup":
            if run_metrics:
                errors.append(f"Warm-up unexpectedly has database metrics: {run_id}")
            continue
        if len(run_metrics) != 1:
            errors.append(
                f"Expected one database metric for {run_id}, observed "
                f"{len(run_metrics)}"
            )
            continue
        metric = run_metrics[0]
        target = member["target"]
        expected_trial_id = (
            f"{member['pair_id']}__{member['architecture']}__{member['layout']}"
        )
        expected = {
            "comparison_id": artifact_dir.name,
            "trial_id": expected_trial_id,
            "sequence_position": member["sequence_position"],
            "measurement_protocol": member["protocol"],
            "retry_count": 0,
            "metric_type": "trino_query",
            "architecture": member["architecture"],
            "file_layout": member["layout"],
            "query_name": target["query_name"],
            "dataset": target["dataset"],
            "year": target["year"],
            "month": target["month"],
            "status": "FINISHED",
            "git_sha": git_commit_sha(),
        }
        for field, expected_value in expected.items():
            if metric.get(field) != expected_value:
                errors.append(
                    f"{run_id}: metric {field} expected={expected_value!r} "
                    f"observed={metric.get(field)!r}"
                )
        if not metric.get("query_id"):
            errors.append(f"{run_id}: missing Trino query ID")
        duration = metric.get("duration_seconds")
        if duration is None or float(duration) <= 0:
            errors.append(f"{run_id}: invalid query duration {duration}")
        artifact, artifact_errors = artifact_result(artifact_dir, run_id)
        errors.extend(artifact_errors)
        if artifact is None:
            continue
        artifact_metric_ids = {
            str(item.get("metric_id")) for item in artifact.get("metrics", [])
        }
        if artifact_metric_ids != {str(metric.get("metric_id"))}:
            errors.append(f"Artifact/database metric mismatch: {run_id}")
        query_result = artifact["query_results"][0]["result"]
        results_by_pair[member["pair_id"]].append((run_id, query_result))

    for run_id, member in members.items():
        if member["protocol"] != "warmup":
            continue
        artifact, artifact_errors = artifact_result(artifact_dir, run_id)
        errors.extend(artifact_errors)
        if artifact is not None and artifact.get("query_results"):
            query_result = artifact["query_results"][0]["result"]
            results_by_pair[member["pair_id"]].append((run_id, query_result))

    unexpected = sorted(set(by_run) - set(recorded))
    if unexpected:
        errors.append(f"Database contains unexpected benchmark runs: {unexpected}")
    for pair_id, results in results_by_pair.items():
        if len(results) != 4:
            errors.append(
                f"{pair_id}: expected four comparable results, observed {len(results)}"
            )
            continue
        reference_id, reference = results[0]
        for run_id, result in results[1:]:
            comparable_reference = {
                "columns": reference.get("columns"),
                "rows": reference.get("rows"),
                "row_count": reference.get("row_count"),
            }
            comparable_result = {
                "columns": result.get("columns"),
                "rows": result.get("rows"),
                "row_count": result.get("row_count"),
            }
            if not values_equal(comparable_reference, comparable_result):
                errors.append(
                    f"Query result mismatch in {pair_id}: "
                    f"{reference_id} vs {run_id}"
                )
    return errors


def percentage_delta(reference: float, treatment: float) -> float | None:
    if reference == 0:
        return None
    return ((treatment - reference) / reference) * 100


def summarize_effects(
    metrics: list[dict[str, Any]],
    members: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    metric_by_run = {
        str(metric["benchmark_run_id"]): metric
        for metric in metrics
        if str(metric.get("benchmark_run_id")) in members
    }
    grouped: dict[
        tuple[str, str, str],
        list[tuple[str, dict[tuple[str, str], dict[str, Any]]]],
    ] = defaultdict(list)
    by_pair: dict[str, dict[tuple[str, str], dict[str, Any]]] = defaultdict(dict)
    pair_members: dict[str, dict[str, Any]] = {}
    for run_id, member in members.items():
        if run_id not in metric_by_run:
            continue
        pair_members[member["pair_id"]] = member
        by_pair[member["pair_id"]][
            (member["architecture"], member["layout"])
        ] = metric_by_run[run_id]
    for pair_id, cells in by_pair.items():
        member = pair_members[pair_id]
        key = (
            str(member["protocol"]),
            str(member["target"]["query_name"]),
            str(member["target"]["scope"]),
        )
        grouped[key].append((pair_id, cells))

    summaries: dict[str, Any] = {}
    seed_offset = 0
    for (protocol, query_name, scope), samples in sorted(grouped.items()):
        for measure, label in MEASURES.items():
            effects: dict[str, list[float]] = defaultdict(list)
            cell_values: dict[str, list[float]] = defaultdict(list)
            missing = 0
            for _pair_id, cells in samples:
                values = {
                    cell: metric.get(measure) for cell, metric in cells.items()
                }
                if len(values) != 4 or any(value is None for value in values.values()):
                    missing += 1
                    continue
                numeric = {cell: float(value) for cell, value in values.items()}
                onprem_fragmented = numeric[("onprem", "fragmented")]
                onprem_compact = numeric[("onprem", "compact")]
                hybrid_fragmented = numeric[("hybrid_aws", "fragmented")]
                hybrid_compact = numeric[("hybrid_aws", "compact")]
                for (architecture, layout), value in numeric.items():
                    cell_values[f"{architecture}:{layout}"].append(value)
                onprem_penalty = percentage_delta(
                    onprem_compact, onprem_fragmented
                )
                hybrid_penalty = percentage_delta(
                    hybrid_compact, hybrid_fragmented
                )
                hybrid_fragmented_penalty = percentage_delta(
                    onprem_fragmented, hybrid_fragmented
                )
                hybrid_compact_penalty = percentage_delta(
                    onprem_compact, hybrid_compact
                )
                candidates = {
                    "onprem_fragmentation_penalty_percent": onprem_penalty,
                    "hybrid_fragmentation_penalty_percent": hybrid_penalty,
                    "hybrid_penalty_fragmented_percent": (
                        hybrid_fragmented_penalty
                    ),
                    "hybrid_penalty_compact_percent": hybrid_compact_penalty,
                }
                for name, value in candidates.items():
                    if value is not None:
                        effects[name].append(value)
                if onprem_penalty is not None and hybrid_penalty is not None:
                    effects["fragmentation_interaction_percentage_points"].append(
                        hybrid_penalty - onprem_penalty
                    )

            effect_summary = {}
            for name, values in sorted(effects.items()):
                seed_offset += 1
                effect_summary[name] = bootstrap_median(values, seed_offset)
            key = f"{protocol}:{query_name}:{measure}"
            summaries[key] = {
                "protocol": protocol,
                "query_name": query_name,
                "scope": scope,
                "measure": measure,
                "measure_label": label,
                "complete_pairs": len(samples) - missing,
                "missing_pairs": missing,
                "cells": {
                    name: descriptive(values)
                    for name, values in sorted(cell_values.items())
                },
                "effects": effect_summary,
            }
    return summaries


def preparation_summary(preflight: dict[str, Any] | None) -> dict[str, Any]:
    values: dict[str, list[float]] = defaultdict(list)
    if preflight is None:
        return {"jobs": 0, "by_cell": {}, "claim": "unavailable"}
    for job in preflight.get("preparation_jobs", []):
        result = job.get("result") or {}
        elapsed = result.get("elapsed_seconds")
        if elapsed is not None:
            values[str(job.get("cell"))].append(float(elapsed))
    return {
        "jobs": sum(len(items) for items in values.values()),
        "by_cell": {
            cell: descriptive(items) for cell, items in sorted(values.items())
        },
        "claim": "descriptive_only" if values else "unavailable",
    }


def write_csv(path: Path, metrics: list[dict[str, Any]]) -> None:
    columns = sorted({key for metric in metrics for key in metric})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(metrics)


def effect_text(effect: dict[str, Any] | None, suffix: str = "%") -> str:
    if effect is None:
        return "unavailable"
    return (
        f"{effect['median']:.2f}{suffix} "
        f"[{effect['ci95_low']:.2f}, {effect['ci95_high']:.2f}]"
    )


def report_markdown(
    state: dict[str, Any],
    summaries: dict[str, Any],
    preparation: dict[str, Any],
    errors: list[str],
    accepted_on: str | None,
) -> str:
    if errors:
        status = "ACCEPTANCE BLOCKED"
    elif accepted_on:
        status = f"ACCEPTED {accepted_on}"
    else:
        status = "READY FOR MANUAL ACCEPTANCE"
    lines = [
        "# Phase 4 File-Layout Experiment Report",
        "",
        f"- Comparison ID: `{state['comparison_id']}`",
        f"- Commit SHA: `{state['git_commit_sha']}`",
        f"- Comparison hash: `{state['comparison_config_hash']}`",
        f"- Preflight ID: `{state['preflight_id']}`",
        f"- Status: **{status}**",
        "",
        "## Read-Side Effects",
        "",
        "Positive fragmentation penalties mean fragmented files were slower or "
        "more expensive than compact files. Positive hybrid penalties mean hybrid "
        "was slower or more expensive than on-premises. The interaction is the "
        "hybrid fragmentation penalty minus the on-premises fragmentation penalty.",
        "",
        "| Protocol/query/measure | Complete pairs | On-prem fragmentation | "
        "Hybrid fragmentation | Interaction (pp) | Hybrid penalty fragmented | "
        "Hybrid penalty compact |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key, summary in sorted(summaries.items()):
        effects = summary["effects"]
        lines.append(
            f"| {key} | {summary['complete_pairs']} | "
            f"{effect_text(effects.get('onprem_fragmentation_penalty_percent'))} | "
            f"{effect_text(effects.get('hybrid_fragmentation_penalty_percent'))} | "
            f"{effect_text(effects.get('fragmentation_interaction_percentage_points'), ' pp')} | "
            f"{effect_text(effects.get('hybrid_penalty_fragmented_percent'))} | "
            f"{effect_text(effects.get('hybrid_penalty_compact_percent'))} |"
        )
    lines.extend(
        [
            "",
            "Each bracket is a deterministic 95% bootstrap interval for the median "
            f"using {BOOTSTRAP_RESAMPLES:,} resamples. Warm-recorded and "
            "service-cold results are kept separate. No p-value or universal "
            "performance claim is made.",
            "",
            "## Write-Side And Request Evidence",
            "",
        ]
    )
    if preparation["claim"] == "unavailable":
        lines.append(
            "- The frozen preflight was validation-only and contains no timed "
            "preparation jobs. Compaction runtime, write resource use, and write "
            "cost are unavailable for the official comparison."
        )
    else:
        lines.append(
            f"- Timed preparation jobs available: {preparation['jobs']}. These are "
            "reported descriptively and are not mixed with read-side effects."
        )
    lines.extend(
        [
            "- The comparison runner did not create isolated S3 request-metric "
            "windows. No causal request-count or request-cost claim is permitted.",
            "- Physical input bytes are Trino query-engine evidence, not an S3 API "
            "request count.",
            "",
            "## Permitted Interpretation",
            "",
            "- A positive hybrid fragmentation effect may support compaction as a "
            "mitigation for deliberately induced fragmentation in this workload.",
            "- A similar fragmentation effect on both architectures supports only "
            "a general file-layout benefit.",
            "- These results cannot show that small files caused the accepted Phase "
            "3 penalty or that compaction improved the single-file Phase 3 baseline.",
            "",
            "## Acceptance Gate",
            "",
        ]
    )
    if errors:
        lines.extend(f"- BLOCKED: {error}" for error in errors)
    elif accepted_on:
        lines.extend(
            [
                "- All automated completeness, identity, success, and correctness "
                "gates passed.",
                f"- The user explicitly accepted this comparison on {accepted_on}.",
                "- The file-layout result is canonical. The H3 executor-sizing "
                "experiment may begin.",
            ]
        )
    else:
        lines.extend(
            [
                "- All automated completeness, identity, success, and correctness "
                "gates passed.",
                "- Explicit user acceptance is still required. Do not treat this "
                "result as canonical and do not begin executor sizing.",
            ]
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate and report the Phase 4 file-layout comparison."
    )
    parser.add_argument("comparison_id")
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("benchmarks/comparisons/phase4_file_layout.toml"),
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=Path("benchmarks/artifacts/comparisons"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/research_results/phase4_file_layout_report.md"),
    )
    parser.add_argument(
        "--accepted-on",
        help="Record an already-made manual acceptance date in YYYY-MM-DD form.",
    )
    args = parser.parse_args()

    comparison_path = args.comparison.resolve()
    spec = load_toml(comparison_path)
    artifact_dir = args.artifact_root / args.comparison_id
    state_path = artifact_dir / "comparison_run.json"
    state = json.loads(state_path.read_text())
    members, errors = validate_state(spec, state, artifact_dir)
    preflight, preflight_errors = validate_preflight_evidence(
        state,
        spec,
        comparison_path,
    )
    errors.extend(preflight_errors)

    profile = load_toml(ROOT / str(spec["cells"][0]["profile"]))
    metrics = fetch_metrics(args.comparison_id, profile)
    errors.extend(validate_metrics(metrics, members, artifact_dir))
    summaries = summarize_effects(metrics, members)
    preparation = preparation_summary(preflight)

    report_dir = artifact_dir / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "phase4_file_layout_metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True, default=str)
    )
    write_csv(report_dir / "phase4_file_layout_metrics.csv", metrics)
    (report_dir / "phase4_file_layout_statistics.json").write_text(
        json.dumps(
            {
                "effects": summaries,
                "preparation": preparation,
                "validation_errors": errors,
            },
            indent=2,
            sort_keys=True,
        )
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        report_markdown(
            state,
            summaries,
            preparation,
            errors,
            accepted_on=args.accepted_on,
        )
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
