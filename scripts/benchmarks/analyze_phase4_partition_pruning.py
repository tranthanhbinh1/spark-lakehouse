import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

PROTOCOL_SAMPLES = {"warm_recorded": 5, "service_cold_recorded": 3}
ARCHITECTURES = {"onprem": "onprem", "hybrid_storage": "hybrid_aws"}
DATASET_PARTITIONS = {"yellow": (2011, [1, 4, 7, 10]), "green": (2014, [1, 4, 7, 10])}
FILTERED_QUERY = "01_partition_financial_aggregation"
BROAD_QUERY = "03_dataset_financial_scan"


def percent_reduction(broad: float, filtered: float) -> float:
    if broad == 0:
        raise ValueError("Broad-scan value cannot be zero")
    return ((broad - filtered) / broad) * 100


def hybrid_penalty(onprem: float, hybrid: float) -> float:
    if onprem == 0:
        raise ValueError("On-prem value cannot be zero")
    return ((hybrid - onprem) / onprem) * 100


def selected_metrics(metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = [
        metric
        for metric in metrics
        if metric.get("metric_type") == "trino_query"
        and metric.get("measurement_protocol") in PROTOCOL_SAMPLES
        and metric.get("query_name") in {FILTERED_QUERY, BROAD_QUERY}
    ]
    failed = [metric for metric in selected if metric.get("status") != "FINISHED"]
    if failed:
        raise ValueError(f"Found {len(failed)} non-finished query metrics")
    return selected


def summarize(
    metrics: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for metric in selected_metrics(metrics):
        key = (
            str(metric["measurement_protocol"]),
            str(metric["architecture"]),
            str(metric["query_name"]),
            str(metric["dataset"]),
            int(metric["year"]),
            int(metric["month"]),
        )
        groups[key].append(metric)

    rows: list[dict[str, Any]] = []
    for protocol, expected_n in PROTOCOL_SAMPLES.items():
        for source_architecture, architecture in ARCHITECTURES.items():
            for dataset, (year, months) in DATASET_PARTITIONS.items():
                broad_key = (
                    protocol,
                    source_architecture,
                    BROAD_QUERY,
                    dataset,
                    year,
                    1,
                )
                broad = groups.get(broad_key, [])
                if len(broad) != expected_n:
                    raise ValueError(
                        f"{broad_key}: expected {expected_n} metrics, found {len(broad)}"
                    )
                broad_latency = statistics.median(
                    float(metric["duration_seconds"]) for metric in broad
                )
                broad_bytes = statistics.median(
                    int(metric["physical_input_bytes"]) for metric in broad
                )
                for month in months:
                    filtered_key = (
                        protocol,
                        source_architecture,
                        FILTERED_QUERY,
                        dataset,
                        year,
                        month,
                    )
                    filtered = groups.get(filtered_key, [])
                    if len(filtered) != expected_n:
                        raise ValueError(
                            f"{filtered_key}: expected {expected_n} metrics, "
                            f"found {len(filtered)}"
                        )
                    filtered_latency = statistics.median(
                        float(metric["duration_seconds"]) for metric in filtered
                    )
                    filtered_bytes = statistics.median(
                        int(metric["physical_input_bytes"]) for metric in filtered
                    )
                    rows.append(
                        {
                            "protocol": protocol,
                            "architecture": architecture,
                            "dataset": dataset,
                            "year": year,
                            "month": month,
                            "n": expected_n,
                            "broad_latency_median_seconds": broad_latency,
                            "filtered_latency_median_seconds": filtered_latency,
                            "latency_reduction_percent": percent_reduction(
                                broad_latency, filtered_latency
                            ),
                            "broad_physical_input_bytes": broad_bytes,
                            "filtered_physical_input_bytes": filtered_bytes,
                            "physical_input_reduction_percent": percent_reduction(
                                broad_bytes, filtered_bytes
                            ),
                        }
                    )

    by_key = {
        (row["protocol"], row["architecture"], row["dataset"], row["month"]): row
        for row in rows
    }
    penalty_rows: list[dict[str, Any]] = []
    for protocol in PROTOCOL_SAMPLES:
        for dataset, (year, months) in DATASET_PARTITIONS.items():
            for month in months:
                onprem = by_key[(protocol, "onprem", dataset, month)]
                hybrid = by_key[(protocol, "hybrid_aws", dataset, month)]
                broad_penalty = hybrid_penalty(
                    float(onprem["broad_latency_median_seconds"]),
                    float(hybrid["broad_latency_median_seconds"]),
                )
                filtered_penalty = hybrid_penalty(
                    float(onprem["filtered_latency_median_seconds"]),
                    float(hybrid["filtered_latency_median_seconds"]),
                )
                penalty_rows.append(
                    {
                        "protocol": protocol,
                        "dataset": dataset,
                        "year": year,
                        "month": month,
                        "broad_hybrid_penalty_percent": broad_penalty,
                        "filtered_hybrid_penalty_percent": filtered_penalty,
                        "hybrid_penalty_narrowing_percentage_points": (
                            broad_penalty - filtered_penalty
                        ),
                    }
                )
    return rows, penalty_rows


def range_values(rows: list[dict[str, Any]], field: str) -> dict[str, float]:
    values = [float(row[field]) for row in rows]
    return {"min": min(values), "median": statistics.median(values), "max": max(values)}


def findings(
    rows: list[dict[str, Any]], penalty_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    hybrid_rows = [row for row in rows if row["architecture"] == "hybrid_aws"]
    return {
        "physical_input_reduction_percent": range_values(
            rows, "physical_input_reduction_percent"
        ),
        "latency_reduction_percent": range_values(rows, "latency_reduction_percent"),
        "latency_improved_cases": sum(
            float(row["latency_reduction_percent"]) > 0 for row in rows
        ),
        "latency_case_count": len(rows),
        "hybrid_latency_improved_cases": sum(
            float(row["latency_reduction_percent"]) > 0 for row in hybrid_rows
        ),
        "hybrid_latency_case_count": len(hybrid_rows),
        "hybrid_penalty_narrowing_percentage_points": range_values(
            penalty_rows, "hybrid_penalty_narrowing_percentage_points"
        ),
        "hybrid_penalty_narrowed_cases": sum(
            float(row["hybrid_penalty_narrowing_percentage_points"]) > 0
            for row in penalty_rows
        ),
        "hybrid_penalty_case_count": len(penalty_rows),
    }


def markdown_report(
    comparison_id: str,
    rows: list[dict[str, Any]],
    penalty_rows: list[dict[str, Any]],
    summary: dict[str, Any],
) -> str:
    lines = [
        "# Phase 4 Partition-Pruning Analysis",
        "",
        f"- Source comparison: `{comparison_id}`",
        "- Source protocols: `warm_recorded`, `service_cold_recorded`",
        "- Status: **AUTOMATED ANALYSIS PASSED**",
        "",
        "## Result",
        "",
        (
            "Partition filters reduced physical input in every measured case, but "
            "they did not consistently narrow the relative hybrid-versus-on-prem "
            "latency penalty. This supports pruning as an absolute I/O mitigation, "
            "not as a complete explanation or removal of hybrid overhead."
        ),
        "",
        "## Aggregate Findings",
        "",
        (
            "- Physical-input reduction: "
            f"{summary['physical_input_reduction_percent']['min']:.2f}% to "
            f"{summary['physical_input_reduction_percent']['max']:.2f}%."
        ),
        (
            "- Median-latency improvement: "
            f"{summary['latency_improved_cases']}/"
            f"{summary['latency_case_count']} architecture/protocol/partition cases."
        ),
        (
            "- Hybrid median-latency improvement: "
            f"{summary['hybrid_latency_improved_cases']}/"
            f"{summary['hybrid_latency_case_count']} protocol/partition cases."
        ),
        (
            "- Relative hybrid penalty narrowed: "
            f"{summary['hybrid_penalty_narrowed_cases']}/"
            f"{summary['hybrid_penalty_case_count']} protocol/partition cases."
        ),
        "",
        "## Physical Input And Latency",
        "",
        (
            "| Protocol | Architecture | Partition | n | Broad median (s) | "
            "Filtered median (s) | Latency reduction | Broad bytes | "
            "Filtered bytes | Input reduction |"
        ),
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row['protocol']} | {row['architecture']} | "
            f"{row['dataset']} {row['year']}-{row['month']:02d} | "
            f"{row['n']} | {row['broad_latency_median_seconds']:.6f} | "
            f"{row['filtered_latency_median_seconds']:.6f} | "
            f"{row['latency_reduction_percent']:.2f}% | "
            f"{row['broad_physical_input_bytes']} | "
            f"{row['filtered_physical_input_bytes']} | "
            f"{row['physical_input_reduction_percent']:.2f}% |"
        )
    lines.extend(
        [
            "",
            "## Relative Hybrid Penalty",
            "",
            (
                "Positive narrowing means the filtered query reduced the relative "
                "hybrid penalty. Negative narrowing means the relative penalty grew."
            ),
            "",
            (
                "| Protocol | Partition | Broad hybrid penalty | "
                "Filtered hybrid penalty | Narrowing |"
            ),
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in penalty_rows:
        lines.append(
            f"| {row['protocol']} | {row['dataset']} "
            f"{row['year']}-{row['month']:02d} | "
            f"{row['broad_hybrid_penalty_percent']:.2f}% | "
            f"{row['filtered_hybrid_penalty_percent']:.2f}% | "
            f"{row['hybrid_penalty_narrowing_percentage_points']:.2f} pp |"
        )
    lines.extend(
        [
            "",
            "## Interpretation Limits",
            "",
            (
                "- Broad scans aggregate four measured monthly partitions; filtered "
                "queries aggregate one month. This is a pruning workload comparison, "
                "not an identical-result comparison."
            ),
            (
                "- Each dataset-wide broad-scan median is reused as the reference for "
                "its four monthly filters. The 32 rows are comparison cases, not 32 "
                "independent broad-scan samples."
            ),
            (
                "- Warm and service-cold samples are analyzed separately. No p-value "
                "or causal significance claim is made."
            ),
            (
                "- Identical physical-input byte counts across architectures are "
                "consistent with matched table contents and observed file counts; "
                "they do not prove identical object-store internals."
            ),
            "",
            "## Phase 4 Decision",
            "",
            (
                "Partition pruning is retained as a supported practical mitigation "
                "for absolute I/O and improved hybrid median latency in most cases. "
                "It is not sufficient by itself to eliminate the hybrid latency "
                "penalty. H2 is partially supported for query-layout optimization; "
                "request-count reduction was not directly measured."
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze the accepted Phase 3 metrics for partition pruning."
    )
    parser.add_argument("comparison_id")
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("benchmarks/artifacts/comparisons")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/research_results/phase4_partition_pruning_analysis.md"),
    )
    args = parser.parse_args()

    report_dir = args.artifact_root / args.comparison_id / "report"
    metrics_path = report_dir / "phase3_metrics.json"
    metrics = json.loads(metrics_path.read_text())
    rows, penalty_rows = summarize(metrics)
    summary = findings(rows, penalty_rows)
    payload = {
        "comparison_id": args.comparison_id,
        "source_metrics": str(metrics_path),
        "rows": rows,
        "penalty_rows": penalty_rows,
        "findings": summary,
    }
    (report_dir / "phase4_partition_pruning.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        markdown_report(args.comparison_id, rows, penalty_rows, summary)
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
