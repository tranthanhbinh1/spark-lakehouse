import argparse
import csv
import json
import math
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from clients.trino_client import TrinoClient  # noqa: E402
from scripts.benchmarks.run_phase3_comparison import schedule  # noqa: E402


def load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


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


def descriptive(values: list[float]) -> dict[str, float | int]:
    return {
        "n": len(values),
        "median": statistics.median(values),
        "q1": percentile(values, 0.25),
        "q3": percentile(values, 0.75),
        "iqr": percentile(values, 0.75) - percentile(values, 0.25),
        "p95": percentile(values, 0.95),
    }


def paired_bootstrap(
    onprem: list[float],
    hybrid: list[float],
    seed: int = 20260713,
    resamples: int = 10_000,
) -> dict[str, float | int]:
    if len(onprem) != len(hybrid) or not onprem:
        raise ValueError("Paired bootstrap requires equal non-empty samples")
    deltas = [
        ((hybrid_value - onprem_value) / onprem_value) * 100
        for onprem_value, hybrid_value in zip(onprem, hybrid, strict=True)
        if onprem_value != 0
    ]
    if len(deltas) != len(onprem):
        raise ValueError("Paired percentage delta is undefined for zero on-prem values")
    rng = random.Random(seed)
    estimates = [
        statistics.median(rng.choices(deltas, k=len(deltas))) for _ in range(resamples)
    ]
    return {
        "n": len(deltas),
        "median_paired_delta_percent": statistics.median(deltas),
        "ci95_low": percentile(estimates, 0.025),
        "ci95_high": percentile(estimates, 0.975),
        "seed": seed,
        "resamples": resamples,
    }


def accepted_members(
    state: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    members: dict[str, dict[str, Any]] = {}
    errors = []
    attempts_by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for attempt in state["attempts"]:
        attempts_by_pair[attempt["pair_id"]].append(attempt)
    for pair_id, attempts in attempts_by_pair.items():
        complete = [attempt for attempt in attempts if attempt["status"] == "complete"]
        if len(complete) != 1:
            errors.append(
                f"{pair_id}: expected one complete attempt, found {len(complete)}"
            )
            continue
        for member in complete[0]["members"]:
            members[member["benchmark_run_id"]] = {
                **member,
                "pair_id": pair_id,
                "protocol": complete[0]["protocol"],
            }
    return members, errors


def fetch_metrics(comparison_id: str, profile: dict[str, Any]) -> list[dict[str, Any]]:
    table = str(profile["metrics_table"])
    result = TrinoClient(profile["trino"]).execute(
        f"select * from {table} where comparison_id = {sql_literal(comparison_id)}"
    )
    if result.get("state") != "FINISHED" or result.get("error"):
        raise RuntimeError(f"Metric query failed: {result}")
    return list(result.get("row_dicts", []))


def metric_key(metric: dict[str, Any]) -> str | None:
    if metric["metric_type"] == "pipeline":
        return (
            f"pipeline:{metric.get('dataset')}:{metric.get('year')}:"
            f"{metric.get('month')}"
        )
    if metric["metric_type"] == "trino_query":
        return (
            f"query:{metric.get('query_name')}:{metric.get('dataset')}:"
            f"{metric.get('year')}:{metric.get('month')}"
        )
    return None


def summarize_pairs(
    metrics: list[dict[str, Any]],
    members: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    by_pair_key: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for metric in metrics:
        member = members.get(str(metric["benchmark_run_id"]))
        key = metric_key(metric)
        duration = metric.get("duration_seconds")
        if member is None or key is None or duration is None:
            continue
        scoped_key = f"{member['protocol']}:{key}"
        by_pair_key[(member["pair_id"], scoped_key)][member["architecture"]] = float(
            duration
        )

    samples: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: {"onprem": [], "hybrid_aws": []}
    )
    for (_pair_id, key), values in sorted(by_pair_key.items()):
        if set(values) == {"onprem", "hybrid_aws"}:
            samples[key]["onprem"].append(values["onprem"])
            samples[key]["hybrid_aws"].append(values["hybrid_aws"])

    summaries = {}
    for key, values in samples.items():
        summaries[key] = {
            "onprem": descriptive(values["onprem"]),
            "hybrid_aws": descriptive(values["hybrid_aws"]),
            "paired": paired_bootstrap(values["onprem"], values["hybrid_aws"]),
            "raw": values,
        }
    return summaries


def normalize_correctness(name: str, rows: list[list[Any]]) -> list[list[Any]]:
    normalized = []
    for row in rows:
        values = list(row)
        if name == "03_gold_revenue_check":
            for index in (3,):
                if values[index] is not None:
                    values[index] = round(float(values[index]), 2)
            for index in (4, 5):
                if values[index] is not None:
                    values[index] = round(float(values[index]), 6)
        normalized.append(values)
    return sorted(normalized, key=lambda item: json.dumps(item, default=str))


def verify_correctness(
    artifact_dir: Path,
    members: dict[str, dict[str, Any]],
) -> list[str]:
    errors = []
    correctness = [
        (run_id, member)
        for run_id, member in members.items()
        if member["protocol"] == "correctness_once"
    ]
    if len(correctness) != 2:
        return [f"Expected two correctness artifacts, found {len(correctness)}"]
    payloads = {}
    for run_id, member in correctness:
        path = artifact_dir / "benchmarks" / run_id / "benchmark_run.json"
        if not path.exists():
            errors.append(f"Missing correctness artifact: {path}")
            continue
        payloads[member["architecture"]] = json.loads(path.read_text())
    if len(payloads) != 2:
        return errors
    indexed: dict[str, dict[tuple[Any, ...], list[list[Any]]]] = {}
    for architecture, payload in payloads.items():
        rows = {}
        for result in payload.get("query_results", []):
            partition = result["partition"]
            key = (
                result["query_name"],
                partition["dataset"],
                partition["year"],
                partition["month"],
            )
            rows[key] = normalize_correctness(
                result["query_name"], result["result"].get("rows", [])
            )
        indexed[architecture] = rows
    keys = set(indexed["onprem"]) | set(indexed["hybrid_aws"])
    for key in sorted(keys):
        if indexed["onprem"].get(key) != indexed["hybrid_aws"].get(key):
            errors.append(f"Correctness mismatch: {key}")
    return errors


def completeness_errors(
    spec: dict[str, Any],
    state: dict[str, Any],
    members: dict[str, dict[str, Any]],
    metrics: list[dict[str, Any]],
    artifact_dir: Path,
) -> list[str]:
    errors = []
    expected_pairs = {pair["pair_id"] for pair in schedule(spec)}
    completed_pairs = {member["pair_id"] for member in members.values()}
    missing_pairs = sorted(expected_pairs - completed_pairs)
    if missing_pairs:
        errors.append(f"Missing complete pairs: {missing_pairs}")
    metric_ids = [str(metric["metric_id"]) for metric in metrics]
    if len(metric_ids) != len(set(metric_ids)):
        errors.append("Metric IDs are not unique")
    metrics_by_run: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for metric in metrics:
        metrics_by_run[str(metric["benchmark_run_id"])].append(metric)
    for run_id, member in members.items():
        path = artifact_dir / "benchmarks" / run_id / "benchmark_run.json"
        if not path.exists():
            errors.append(f"Missing artifact: {path}")
            continue
        payload = json.loads(path.read_text())
        if payload.get("error"):
            errors.append(f"Artifact records an error: {run_id}")
        if member["protocol"] == "warmup":
            continue
        run_metrics = metrics_by_run.get(run_id, [])
        if not run_metrics:
            errors.append(f"Missing database metrics: {run_id}")
            continue
        artifact_ids = {
            str(metric["metric_id"]) for metric in payload.get("metrics", [])
        }
        database_ids = {str(metric["metric_id"]) for metric in run_metrics}
        if artifact_ids != database_ids:
            errors.append(f"Artifact/database metric mismatch: {run_id}")
        for metric in run_metrics:
            if metric["metric_type"] == "trino_query" and not metric.get("query_id"):
                errors.append(f"Missing Trino query ID: {metric['metric_id']}")
            if (
                metric["metric_type"] == "trino_query"
                and str(metric.get("status", "")).upper() != "FINISHED"
            ):
                errors.append(f"Unsuccessful Trino metric: {metric['metric_id']}")
            if (
                metric["metric_type"] == "pipeline"
                and str(metric.get("status", "")).lower() != "success"
            ):
                errors.append(f"Unsuccessful pipeline metric: {metric['metric_id']}")
            if (
                metric["metric_type"] == "airflow_task"
                and metric.get("task_id")
                in {"stage_trips", "check_silver_quality", "build_gold_revenue"}
                and not metric.get("spark_application_id")
            ):
                errors.append(f"Missing Spark application ID: {metric['metric_id']}")
    layout_groups: dict[tuple[Any, ...], list[tuple[Any, Any]]] = defaultdict(list)
    for metric in metrics:
        member = members.get(str(metric["benchmark_run_id"]))
        if (
            member is None
            or member["protocol"] != "pipeline_paired"
            or metric["metric_type"] != "iceberg_partition"
        ):
            continue
        key = (
            member["architecture"],
            metric.get("table_name"),
            metric.get("dataset"),
            metric.get("year"),
            metric.get("month"),
        )
        layout_groups[key].append(
            (metric.get("records_read"), metric.get("file_count"))
        )
    for key, values in layout_groups.items():
        if len(set(values)) != 1:
            errors.append(f"Repeated-write layout changed: {key}: {values}")

    query_ids = [
        str(metric["query_id"])
        for metric in metrics
        if metric["metric_type"] == "trino_query" and metric.get("query_id")
    ]
    if len(query_ids) != len(set(query_ids)):
        errors.append("Trino query IDs are not unique")
    for attempt in state["attempts"]:
        if attempt["status"] != "complete":
            continue
        for member in attempt["members"]:
            resource = member.get("resource_samples", {})
            if resource.get("errors"):
                errors.append(f"Docker sampling errors: {member['benchmark_run_id']}")
            if not resource.get("samples"):
                errors.append(
                    f"Missing Docker resource samples: {member['benchmark_run_id']}"
                )
    evidence = state.get("evidence", {})
    if not evidence.get("request_metrics_disabled_at"):
        errors.append("Temporary S3 request metrics were not recorded as disabled")
    windows = evidence.get("windows", {})
    for block in ("pipeline", "warm_query", "cold_query"):
        request_metrics = windows.get(block, {}).get("request_metrics")
        if not request_metrics or not request_metrics.get("complete"):
            errors.append(f"Missing CloudWatch request datapoints: {block}")
    errors.extend(verify_correctness(artifact_dir, members))
    return errors


def write_csv(path: Path, metrics: list[dict[str, Any]]) -> None:
    columns = sorted({key for metric in metrics for key in metric})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(metrics)


def resilience_matrix() -> list[dict[str, str]]:
    return [
        {
            "dimension": "failure domain",
            "onprem": "single local site",
            "hybrid": "local compute plus regional S3 and Glue",
        },
        {
            "dimension": "redundancy",
            "onprem": "operator-managed MinIO deployment",
            "hybrid": "AWS-managed S3 and Glue service durability",
        },
        {
            "dimension": "recovery ownership",
            "onprem": "local operator",
            "hybrid": "shared responsibility",
        },
        {
            "dimension": "backup/versioning",
            "onprem": "local policy",
            "hybrid": "S3 versioning and lifecycle policy",
        },
        {
            "dimension": "monitoring",
            "onprem": "local container and service telemetry",
            "hybrid": "local telemetry plus CloudWatch",
        },
        {
            "dimension": "encryption",
            "onprem": "local configuration",
            "hybrid": "S3 and Glue configuration plus IAM",
        },
        {
            "dimension": "auditability",
            "onprem": "local logs",
            "hybrid": "local logs plus AWS API audit controls",
        },
        {
            "dimension": "network dependency",
            "onprem": "LAN",
            "hybrid": "internet or private AWS connectivity",
        },
        {
            "dimension": "service responsibility",
            "onprem": "operator owns all layers",
            "hybrid": "AWS owns storage/catalog service infrastructure",
        },
    ]


def aggregate_request_metrics(state: dict[str, Any]) -> dict[str, float]:
    totals: dict[str, float] = defaultdict(float)
    windows = state.get("evidence", {}).get("windows", {})
    for window in windows.values():
        for target in window.get("request_metrics", {}).get("targets", []):
            for name, datapoints in target.get("metrics", {}).items():
                totals[name] += sum(float(point.get("Sum", 0)) for point in datapoints)
    return dict(totals)


def pricing_rates(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    pricing = state.get("evidence", {}).get("static_snapshot", {}).get("pricing", {})
    entries = [
        *pricing.get("price_list", []),
        *pricing.get("data_transfer_price_list", []),
    ]
    rates: dict[str, dict[str, Any]] = {}
    for raw in entries:
        offer = json.loads(raw) if isinstance(raw, str) else raw
        product = offer.get("product", {})
        attributes = product.get("attributes", {})
        service_code = str(
            offer.get("serviceCode") or attributes.get("servicecode", "")
        )
        group = str(attributes.get("group", ""))
        group_description = str(attributes.get("groupDescription", ""))
        product_family = str(product.get("productFamily", ""))
        terms = offer.get("terms", {}).get("OnDemand", {})
        for term in terms.values():
            for dimension in term.get("priceDimensions", {}).values():
                description = str(dimension.get("description", "")).lower()
                unit = str(dimension.get("unit", ""))
                usd = dimension.get("pricePerUnit", {}).get("USD")
                if usd in {None, ""}:
                    continue
                candidate = {
                    "usd": float(usd),
                    "unit": unit,
                    "description": description,
                    "begin_range": str(dimension.get("beginRange", "")),
                    "end_range": str(dimension.get("endRange", "")),
                }
                if (
                    group == "S3-API-Tier2"
                    and group_description == "GET and all other requests"
                ):
                    rates["get_requests"] = candidate
                if (
                    group == "S3-API-Tier1"
                    and group_description == "PUT/COPY/POST or LIST requests"
                ):
                    rates["put_requests"] = candidate
                if (
                    service_code == "AWSDataTransfer"
                    and product_family == "Data Transfer"
                    and unit.lower() == "gb"
                    and "data transfer out" in description
                    and str(attributes.get("fromRegionCode", "")) == "us-east-1"
                    and str(attributes.get("transferType", "")) == "AWS Outbound"
                    and str(attributes.get("toLocation", "")) == "External"
                    and float(dimension.get("beginRange", -1)) == 0
                ):
                    rates["bytes_downloaded"] = candidate
    return rates


def aggregate_cost_estimate(state: dict[str, Any]) -> dict[str, Any]:
    usage = aggregate_request_metrics(state)
    rates = pricing_rates(state)
    components = {}
    mapping = {
        "GetRequests": "get_requests",
        "PutRequests": "put_requests",
        "BytesDownloaded": "bytes_downloaded",
    }
    missing = []
    total = 0.0
    for metric, rate_name in mapping.items():
        quantity = usage.get(metric, 0.0)
        rate = rates.get(rate_name)
        if rate is None:
            missing.append(rate_name)
            continue
        unit = str(rate["unit"]).lower()
        if metric == "BytesDownloaded":
            billable_quantity = quantity / 1024**3
        elif "1k" in unit or "1,000" in unit:
            billable_quantity = quantity / 1000
        else:
            billable_quantity = quantity
        cost = billable_quantity * float(rate["usd"])
        total += cost
        components[metric] = {
            "quantity": quantity,
            "rate": rate,
            "estimated_usd": cost,
        }
    return {
        "usage": usage,
        "rates": rates,
        "components": components,
        "estimated_usd": total,
        "missing_rates": missing,
        "scope": "Aggregate S3 request and transfer estimate across evidence blocks",
    }


def resource_summary(
    state: dict[str, Any],
    members: dict[str, dict[str, Any]],
    artifact_dir: Path,
) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "cpu_seconds": 0.0,
            "memory_gib_hours": 0.0,
            "configured_cpu_hours": 0.0,
            "configured_memory_gib_hours": 0.0,
        }
    )
    for attempt in state["attempts"]:
        if attempt["status"] != "complete":
            continue
        for member in attempt["members"]:
            run_id = member["benchmark_run_id"]
            if run_id not in members:
                continue
            architecture = member["architecture"]
            resource = member.get("resource_samples", {})
            interval = float(resource.get("interval_seconds", 5))
            for sample in resource.get("samples", []):
                summary[architecture]["cpu_seconds"] += (
                    float(sample["cpu_percent"]) / 100.0 * interval
                )
                summary[architecture]["memory_gib_hours"] += (
                    float(sample["memory_bytes"]) / 1024**3 * interval / 3600
                )
            started = member.get("started_at")
            finished = member.get("finished_at")
            if not started or not finished:
                continue
            from datetime import datetime

            duration_hours = (
                datetime.fromisoformat(finished) - datetime.fromisoformat(started)
            ).total_seconds() / 3600
            snapshot_path = (
                artifact_dir / "benchmarks" / run_id / "environment_snapshot.json"
            )
            if not snapshot_path.exists():
                continue
            snapshot = json.loads(snapshot_path.read_text())
            cpu_capacity = 0.0
            memory_capacity = 0.0
            for container in snapshot.get("containers", {}).values():
                host_config = container.get("host_config") or {}
                cpu_capacity += float(host_config.get("NanoCpus") or 0) / 1e9
                memory_capacity += float(host_config.get("Memory") or 0) / 1024**3
            summary[architecture]["configured_cpu_hours"] += (
                cpu_capacity * duration_hours
            )
            summary[architecture]["configured_memory_gib_hours"] += (
                memory_capacity * duration_hours
            )
    return dict(summary)


def report_markdown(
    comparison_id: str,
    state: dict[str, Any],
    summaries: dict[str, Any],
    resources: dict[str, dict[str, float]],
    cost: dict[str, Any],
    errors: list[str],
    accepted_on: str | None = None,
) -> str:
    if errors:
        status = "ACCEPTANCE BLOCKED"
    elif accepted_on:
        status = f"ACCEPTED {accepted_on}"
    else:
        status = "READY FOR MANUAL ACCEPTANCE"
    lines = [
        "# Phase 3 Baseline Tradeoff Report",
        "",
        f"- Comparison ID: `{comparison_id}`",
        f"- Commit SHA: `{state['git_commit_sha']}`",
        f"- Comparison hash: `{state['comparison_config_hash']}`",
        f"- Status: **{status}**",
        "",
        "## Performance Summary",
        "",
        "| Measurement | n | On-prem median/IQR/p95 (s) | Hybrid median/IQR/p95 (s) | Paired delta | 95% bootstrap interval |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for key, summary in sorted(summaries.items()):
        paired = summary["paired"]
        lines.append(
            f"| {key} | {paired['n']} | {summary['onprem']['median']:.6f} / "
            f"{summary['onprem']['iqr']:.6f} / {summary['onprem']['p95']:.6f} | "
            f"{summary['hybrid_aws']['median']:.6f} / "
            f"{summary['hybrid_aws']['iqr']:.6f} / "
            f"{summary['hybrid_aws']['p95']:.6f} | "
            f"{paired['median_paired_delta_percent']:.2f}% | "
            f"[{paired['ci95_low']:.2f}%, {paired['ci95_high']:.2f}%] |"
        )
    lines.extend(["", "## Resource Proxies", ""])
    lines.extend(
        [
            "| Architecture | CPU-seconds | Memory GiB-hours | Configured CPU-hours | Configured memory GiB-hours |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for architecture, values in sorted(resources.items()):
        lines.append(
            f"| {architecture} | {values['cpu_seconds']:.6f} | "
            f"{values['memory_gib_hours']:.6f} | "
            f"{values['configured_cpu_hours']:.6f} | "
            f"{values['configured_memory_gib_hours']:.6f} |"
        )
    lines.extend(
        [
            "",
            "## Aggregate AWS Cost Estimate",
            "",
            f"- Estimated marginal S3 cost: USD {cost['estimated_usd']:.8f}",
            f"- Scope: {cost['scope']}.",
            "- Cost Explorer reconciliation is intentionally deferred until daily service totals are available.",
            "",
            "Quartiles and p95 use linear interpolation. Paired intervals use 10,000 "
            "deterministic bootstrap resamples with seed 20260713. No significance "
            "or p-value claim is made.",
            "",
            "## Evidence Limitations",
            "",
            "CloudWatch storage metrics are daily bucket/storage-class snapshots, not "
            "per-prefix or per-query evidence. S3 cost evidence is aggregate and must "
            "be reconciled against later daily Cost Explorer service totals. Local "
            "CPU and memory samples are resource proxies and are not translated to USD.",
            "",
            "## Resilience Matrix",
            "",
            "| Dimension | On-prem | Hybrid |",
            "| --- | --- | --- |",
        ]
    )
    for row in resilience_matrix():
        lines.append(f"| {row['dimension']} | {row['onprem']} | {row['hybrid']} |")
    lines.extend(["", "## Acceptance Gate", ""])
    if errors:
        lines.extend(f"- BLOCKED: {error}" for error in errors)
    elif accepted_on:
        lines.extend(
            [
                "- All automated completeness and correctness gates passed.",
                f"- The user explicitly accepted this comparison on {accepted_on}.",
                "- This comparison is the canonical Phase 3 baseline. Phase 4 may "
                "proceed under the definitive plan; evidence cleanup remains a "
                "separate, explicit action.",
            ]
        )
    else:
        lines.append(
            "- All automated completeness and correctness gates passed. Manual user "
            "acceptance is still required before the canonical plan update or cleanup."
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate the Phase 3 baseline report."
    )
    parser.add_argument("comparison_id")
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("benchmarks/comparisons/phase3_baseline.toml"),
    )
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("benchmarks/artifacts/comparisons")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/research_results/phase3_baseline_tradeoff_report.md"),
    )
    parser.add_argument(
        "--accepted-on",
        help="Record an already-made manual acceptance date in YYYY-MM-DD form.",
    )
    args = parser.parse_args()

    spec = load_toml(args.comparison)
    artifact_dir = args.artifact_root / args.comparison_id
    state = json.loads((artifact_dir / "comparison_run.json").read_text())
    members, errors = accepted_members(state)
    profile = load_toml(Path(spec["architectures"][0]["profile"]))
    metrics = fetch_metrics(args.comparison_id, profile)
    accepted_metrics = [
        metric for metric in metrics if str(metric["benchmark_run_id"]) in members
    ]
    errors.extend(
        completeness_errors(spec, state, members, accepted_metrics, artifact_dir)
    )
    summaries = summarize_pairs(accepted_metrics, members)
    resources = resource_summary(state, members, artifact_dir)
    cost = aggregate_cost_estimate(state)
    if cost["missing_rates"]:
        errors.append(f"Missing pricing rates: {cost['missing_rates']}")

    raw_dir = artifact_dir / "report"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "phase3_metrics.json").write_text(
        json.dumps(accepted_metrics, indent=2, sort_keys=True, default=str)
    )
    write_csv(raw_dir / "phase3_metrics.csv", accepted_metrics)
    (raw_dir / "phase3_statistics.json").write_text(
        json.dumps(
            {"performance": summaries, "resources": resources, "cost": cost},
            indent=2,
            sort_keys=True,
        )
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        report_markdown(
            args.comparison_id,
            state,
            summaries,
            resources,
            cost,
            errors,
            accepted_on=args.accepted_on,
        )
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
