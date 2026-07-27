import argparse
import hashlib
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

from scripts.benchmarks.phase3_evidence import (  # noqa: E402
    DockerSampler,
    runtime_containers,
)
from scripts.benchmarks.run_phase3_comparison import restart_trino  # noqa: E402


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


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def comparison_hash(path: Path, spec: dict[str, Any]) -> str:
    profiles = {
        str(cell["name"]): load(ROOT / str(cell["profile"])) for cell in spec["cells"]
    }
    query_files = {
        str(query["sql_file"]): (ROOT / str(query["sql_file"])).read_text()
        for query in spec["queries"]
    }
    payload = {
        "comparison_path": str(path),
        "comparison": spec,
        "workload": load(ROOT / str(spec["workload"])),
        "profiles": profiles,
        "queries": query_files,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def expanded_targets(spec: dict[str, Any]) -> list[dict[str, Any]]:
    workload = load(ROOT / str(spec["workload"]))
    partitions = list(workload["partitions"])
    targets: list[dict[str, Any]] = []
    for query in spec["queries"]:
        query_path = ROOT / str(query["sql_file"])
        query_name = query_path.stem
        scope = str(query["scope"])
        if scope == "partition":
            selected = partitions
        elif scope == "dataset":
            seen = set()
            selected = []
            for partition in partitions:
                dataset = str(partition["dataset"])
                if dataset in seen:
                    continue
                seen.add(dataset)
                selected.append(partition)
        else:
            raise ValueError(f"Unsupported query scope: {scope}")
        for partition in selected:
            targets.append(
                {
                    "name": (
                        f"{query['name']}_{partition['dataset']}_"
                        f"{partition['year']}_{int(partition['month']):02d}"
                    ),
                    "query_name": query_name,
                    "queries_dir": str(query_path.parent),
                    "dataset": str(partition["dataset"]),
                    "year": int(partition["year"]),
                    "month": int(partition["month"]),
                    "scope": scope,
                }
            )
    return targets


def ordered_cells(
    spec: dict[str, Any],
    trial: int,
    pair_index: int,
) -> list[dict[str, Any]]:
    cells = list(spec["cells"])
    architectures = []
    for cell in cells:
        architecture = str(cell["architecture"])
        if architecture not in architectures:
            architectures.append(architecture)
    if pair_index % 2:
        architectures.reverse()

    layout_order = ["fragmented", "compact"] if trial % 2 else ["compact", "fragmented"]
    ordered = []
    for architecture in architectures:
        by_layout = {
            str(cell["layout"]): cell
            for cell in cells
            if str(cell["architecture"]) == architecture
        }
        if set(by_layout) != {"fragmented", "compact"}:
            raise ValueError(f"Architecture {architecture} must have both file layouts")
        ordered.extend(dict(by_layout[layout]) for layout in layout_order)
    return ordered


def schedule(spec: dict[str, Any]) -> list[dict[str, Any]]:
    pairs = []
    pair_index = 0
    protocols = (
        ("warmup", int(spec["warmup_executions"]), True, False),
        ("warm_recorded", int(spec["recorded_executions"]), False, False),
        (
            "service_cold_recorded",
            int(spec["cold_executions"]),
            False,
            True,
        ),
    )
    targets = expanded_targets(spec)
    for trial in range(1, int(spec["trial_repetitions"]) + 1):
        for target in targets:
            for protocol, executions, skip_metrics, restart in protocols:
                for execution in range(1, executions + 1):
                    pair_index += 1
                    pairs.append(
                        {
                            "pair_id": (
                                f"trial-{trial:02d}__{target['name']}__"
                                f"{protocol}__e{execution:02d}"
                            ),
                            "trial": trial,
                            "protocol": protocol,
                            "execution": execution,
                            "target": target,
                            "skip_metrics": skip_metrics,
                            "restart_trino": restart,
                            "members": ordered_cells(spec, trial, pair_index),
                        }
                    )
    position = 0
    for pair in pairs:
        for member in pair["members"]:
            position += 1
            member["sequence_position"] = position
    return pairs


def benchmark_command(
    spec: dict[str, Any],
    pair: dict[str, Any],
    member: dict[str, Any],
    comparison_id: str,
    artifact_dir: Path,
) -> tuple[str, list[str]]:
    profile = ROOT / str(member["profile"])
    target = pair["target"]
    trial_id = f"{pair['pair_id']}__{member['architecture']}__{member['layout']}"
    benchmark_run_id = f"{comparison_id}__{trial_id}"
    command = [
        sys.executable,
        str(ROOT / "scripts/benchmarks/run_benchmark.py"),
        "--workload",
        str(ROOT / str(spec["workload"])),
        "--profile",
        str(profile),
        "--queries-dir",
        str(ROOT / target["queries_dir"]),
        "--query-name",
        str(target["query_name"]),
        "--query-dataset",
        str(target["dataset"]),
        "--query-year",
        str(target["year"]),
        "--query-month",
        str(target["month"]),
        "--artifact-root",
        str(artifact_dir / "benchmarks"),
        "--benchmark-run-id",
        benchmark_run_id,
        "--comparison-id",
        comparison_id,
        "--trial-id",
        trial_id,
        "--sequence-position",
        str(member["sequence_position"]),
        "--measurement-protocol",
        str(pair["protocol"]),
        "--skip-pipeline",
    ]
    if pair["skip_metrics"]:
        command.append("--skip-metrics-insert")
    return benchmark_run_id, command


def has_snapshot_error(value: Any) -> bool:
    if isinstance(value, dict):
        return "error" in value or any(
            has_snapshot_error(item) for item in value.values()
        )
    if isinstance(value, list):
        return any(has_snapshot_error(item) for item in value)
    return False


def validate_preflight(
    path: Path,
    comparison_path: Path,
    commit_sha: str,
    spec: dict[str, Any],
) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    if payload.get("preflight", {}).get("status") != "passed":
        raise ValueError(f"Preflight is not passed: {path}")
    if Path(str(payload.get("comparison_path"))).resolve() != comparison_path.resolve():
        raise ValueError("Preflight comparison path does not match")
    if payload.get("git_commit_sha") != commit_sha:
        raise ValueError(
            "Preflight commit does not match current commit; rerun preflight"
        )
    if payload.get("worktree_clean") is not True:
        raise ValueError(
            "Official Phase 4 runs require a preflight captured from a clean worktree"
        )
    current_profiles = {
        str(cell["name"]): load(ROOT / str(cell["profile"])) for cell in spec["cells"]
    }
    if payload.get("comparison_spec") != spec:
        raise ValueError("Preflight comparison specification changed")
    if payload.get("resolved_profiles") != current_profiles:
        raise ValueError("Preflight resolved profiles changed")
    snapshots = payload.get("infrastructure_snapshots")
    if not isinstance(snapshots, dict) or set(snapshots) != set(current_profiles):
        raise ValueError("Preflight infrastructure snapshots are incomplete")
    if has_snapshot_error(snapshots):
        raise ValueError("Preflight infrastructure snapshot contains errors")
    return payload


def write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the official Phase 4 paired file-layout comparison."
    )
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("benchmarks/comparisons/phase4_file_layout.toml"),
    )
    parser.add_argument("--comparison-id")
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=Path("benchmarks/artifacts/comparisons"),
    )
    parser.add_argument("--preflight-artifact", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not clean_worktree():
        raise RuntimeError("Official Phase 4 runs require a clean Git worktree")
    if not args.dry_run and args.preflight_artifact is None:
        raise ValueError("Official Phase 4 runs require --preflight-artifact")

    spec = load(args.comparison)
    commit_sha = git_commit_sha()
    config_hash = comparison_hash(args.comparison, spec)
    comparison_id = args.comparison_id or (
        "phase4_file_layout_"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + f"_{config_hash[:8]}"
    )
    artifact_dir = args.artifact_root / comparison_id
    state_path = artifact_dir / "comparison_run.json"
    if state_path.exists():
        raise FileExistsError(
            f"Phase 4 comparison IDs are single-use and cannot be resumed: {state_path}"
        )

    preflight = None
    if args.preflight_artifact is not None:
        preflight = validate_preflight(
            args.preflight_artifact,
            args.comparison,
            commit_sha,
            spec,
        )

    state: dict[str, Any] = {
        "comparison_id": comparison_id,
        "comparison_path": str(args.comparison),
        "comparison_config_hash": config_hash,
        "git_commit_sha": commit_sha,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "dry_run" if args.dry_run else "running",
        "preflight_artifact": (
            str(args.preflight_artifact)
            if args.preflight_artifact is not None
            else None
        ),
        "preflight_artifact_sha256": (
            sha256(args.preflight_artifact)
            if args.preflight_artifact is not None
            else None
        ),
        "preflight_id": preflight.get("preflight_id") if preflight else None,
        "attempts": [],
    }
    write_state(state_path, state)

    for pair in schedule(spec):
        attempt: dict[str, Any] = {
            "pair_id": pair["pair_id"],
            "trial": pair["trial"],
            "protocol": pair["protocol"],
            "execution": pair["execution"],
            "target": pair["target"],
            "status": "scheduled" if args.dry_run else "running",
            "members": [],
        }
        state["attempts"].append(attempt)
        write_state(state_path, state)
        try:
            for member in pair["members"]:
                benchmark_run_id, command = benchmark_command(
                    spec,
                    pair,
                    member,
                    comparison_id,
                    artifact_dir,
                )
                record: dict[str, Any] = {
                    "architecture": member["architecture"],
                    "layout": member["layout"],
                    "sequence_position": member["sequence_position"],
                    "benchmark_run_id": benchmark_run_id,
                    "command": command,
                    "status": "scheduled" if args.dry_run else "running",
                }
                attempt["members"].append(record)
                write_state(state_path, state)
                if args.dry_run:
                    continue

                profile = load(ROOT / str(member["profile"]))
                sampler = DockerSampler(runtime_containers(profile))
                sampler.start()
                record["started_at"] = datetime.now(timezone.utc).isoformat()
                try:
                    if pair["restart_trino"]:
                        restart_trino(profile)
                    subprocess.run(command, cwd=ROOT, check=True)
                    record["status"] = "complete"
                    record["finished_at"] = datetime.now(timezone.utc).isoformat()
                finally:
                    record["resource_samples"] = sampler.stop()
                write_state(state_path, state)
        except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
            attempt["status"] = "failed"
            attempt["error"] = {
                "type": type(error).__name__,
                "message": str(error),
                "returncode": getattr(error, "returncode", None),
            }
            state["status"] = "invalid"
            state["invalid_reason"] = (
                "A timed Phase 4 failure requires a fresh comparison ID; "
                "this ID cannot be resumed or repaired."
            )
            state["finished_at"] = datetime.now(timezone.utc).isoformat()
            write_state(state_path, state)
            raise
        attempt["status"] = "scheduled" if args.dry_run else "complete"
        write_state(state_path, state)

    state["status"] = "dry_run_complete" if args.dry_run else "complete"
    state["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_state(state_path, state)
    print(state_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
