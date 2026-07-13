import argparse
import atexit
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from clients.trino_client import TrinoClient  # noqa: E402
from scripts.benchmarks.phase3_evidence import (  # noqa: E402
    DockerSampler,
    collect_request_window,
    runtime_containers,
    set_request_metrics,
    static_snapshot,
)

ARCHITECTURE_COUNT = 2


def load(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def clean_worktree() -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain"], check=True, capture_output=True, text=True
    )
    return not result.stdout.strip()


def git_commit_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def comparison_hash(spec_path: Path, spec: dict[str, Any]) -> str:
    payload = {"spec": spec, "spec_path": str(spec_path)}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def query_step(target: dict[str, Any]) -> dict[str, Any]:
    step = {
        "queries": str(Path(target["sql_file"]).parent),
        "query_name": Path(target["sql_file"]).stem,
        "workload": target["workload"],
        "skip_pipeline": True,
        "query_dataset": target["dataset"],
    }
    if target.get("year") is not None:
        step["query_year"] = int(target["year"])
    if target.get("month") is not None:
        step["query_month"] = int(target["month"])
    return step


def schedule(spec: dict[str, Any]) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for repetition in range(1, int(spec["pipeline_pairs"]) + 1):
        pairs.append(
            {
                "pair_id": f"pipeline-{repetition:02d}",
                "protocol": "pipeline_paired",
                "skip_queries": True,
                "workload": spec["workload"],
            }
        )
    pairs.append(
        {
            "pair_id": "correctness-01",
            "protocol": "correctness_once",
            "queries": spec["correctness_queries"],
            "skip_pipeline": True,
            "workload": spec["workload"],
        }
    )
    cold_pairs: list[dict[str, Any]] = []
    for target in spec["query_targets"]:
        target_step = query_step(target)
        for execution in range(1, int(target.get("warmup_executions", 1)) + 1):
            pairs.append(
                {
                    "pair_id": f"warmup-{target['name']}-{execution:02d}",
                    "protocol": "warmup",
                    "skip_metrics_insert": True,
                    **target_step,
                }
            )
        for execution in range(1, int(target["recorded_executions"]) + 1):
            pairs.append(
                {
                    "pair_id": f"warm-{target['name']}-{execution:02d}",
                    "protocol": "warm_recorded",
                    **target_step,
                }
            )
        for execution in range(1, int(target.get("cold_executions", 0)) + 1):
            cold_pairs.append(
                {
                    "pair_id": f"cold-{target['name']}-{execution:02d}",
                    "protocol": "service_cold_recorded",
                    "restart_trino": True,
                    **target_step,
                }
            )
    pairs.extend(cold_pairs)

    architectures = spec["architectures"]
    if len(architectures) != ARCHITECTURE_COUNT:
        raise ValueError("Phase 3 requires exactly two architectures")
    position = 0
    for pair_index, pair in enumerate(pairs):
        order = architectures if pair_index % 2 == 0 else list(reversed(architectures))
        members = []
        for architecture in order:
            position += 1
            members.append(
                {
                    "architecture": architecture["name"],
                    "profile": architecture["profile"],
                    "sequence_position": position,
                }
            )
        pair["members"] = members
        pair["evidence_block"] = {
            "pipeline_paired": "pipeline",
            "warmup": "warm_query",
            "warm_recorded": "warm_query",
            "service_cold_recorded": "cold_query",
        }.get(str(pair["protocol"]))
    return pairs


def prior_attempts(state: dict[str, Any], pair_id: str) -> list[dict[str, Any]]:
    return [attempt for attempt in state["attempts"] if attempt["pair_id"] == pair_id]


def restart_trino(profile: dict[str, Any]) -> None:
    runtime = profile["runtime"]
    containers = [runtime["trino_container"], *runtime["trino_worker_containers"]]
    subprocess.run(["docker", "restart", *containers], check=True)
    client = TrinoClient(profile["trino"])
    deadline = time.monotonic() + int(
        runtime.get("trino_readiness_timeout_seconds", 180)
    )
    last_result: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        try:
            last_result = client.execute(
                "select count(*) as active_nodes from system.runtime.nodes "
                "where state = 'active'"
            )
            rows = last_result.get("rows", [])
            if (
                last_result.get("state") == "FINISHED"
                and not last_result.get("error")
                and rows
                and int(rows[0][0]) == len(containers)
            ):
                probe = client.execute("select 1")
                if probe.get("state") == "FINISHED" and not probe.get("error"):
                    return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(
        f"Trino cluster did not reach {len(containers)} active nodes: {last_result}"
    )


def benchmark_command(
    spec: dict[str, Any],
    pair: dict[str, Any],
    member: dict[str, Any],
    comparison_id: str,
    artifact_dir: Path,
    attempt_number: int,
    dry_run: bool,
) -> tuple[str, list[str]]:
    trial_id = f"{pair['pair_id']}__a{attempt_number:02d}__{member['architecture']}"
    benchmark_run_id = f"{comparison_id}__{trial_id}"
    command = [
        sys.executable,
        str(ROOT / "scripts/benchmarks/run_benchmark.py"),
        "--workload",
        str(pair.get("workload", spec["workload"])),
        "--profile",
        str(member["profile"]),
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
        "--retry-count",
        str(attempt_number - 1),
    ]
    if pair.get("queries"):
        command.extend(["--queries-dir", str(pair["queries"])])
    for key in ("query_name", "query_dataset", "query_year", "query_month"):
        if pair.get(key) is not None:
            command.extend(["--" + key.replace("_", "-"), str(pair[key])])
    for flag in ("skip_pipeline", "skip_queries", "skip_metrics_insert"):
        if pair.get(flag):
            command.append("--" + flag.replace("_", "-"))
    if dry_run:
        command.append("--dry-run")
    return benchmark_run_id, command


def write_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Phase 3 paired comparison.")
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("benchmarks/comparisons/phase3_baseline.toml"),
    )
    parser.add_argument("--comparison-id")
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("benchmarks/artifacts/comparisons")
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--skip-evidence", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not clean_worktree():
        raise RuntimeError("Official comparison runs require a clean Git worktree")

    spec = load(args.comparison)
    config_hash = comparison_hash(args.comparison, spec)
    comparison_id = args.comparison_id or (
        f"phase3_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_"
        f"{config_hash[:8]}"
    )
    artifact_dir = args.artifact_root / comparison_id
    state_path = artifact_dir / "comparison_run.json"
    state: dict[str, Any] = {
        "comparison_id": comparison_id,
        "comparison_config_hash": config_hash,
        "comparison_path": str(args.comparison),
        "git_commit_sha": git_commit_sha(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "attempts": [],
    }
    if args.resume:
        if not state_path.exists():
            raise FileNotFoundError(state_path)
        state = json.loads(state_path.read_text())
        if state["comparison_config_hash"] != config_hash:
            raise ValueError("Comparison specification changed; resume rejected")
        if state["git_commit_sha"] != git_commit_sha():
            raise ValueError("Commit changed; resume rejected")

    artifact_dir.mkdir(parents=True, exist_ok=True)
    evidence_profile = None
    if not args.dry_run and not args.skip_evidence:
        hybrid = next(
            architecture
            for architecture in spec["architectures"]
            if architecture["name"] == "hybrid_aws"
        )
        evidence_profile = load(Path(hybrid["profile"]))
        targets = set_request_metrics(comparison_id, evidence_profile, True)
        state["evidence"] = {
            "request_metric_targets": targets,
            "static_snapshot": static_snapshot(comparison_id, evidence_profile),
            "windows": {},
        }
        write_state(state_path, state)
        atexit.register(set_request_metrics, comparison_id, evidence_profile, False)

    for pair in schedule(spec):
        previous = prior_attempts(state, pair["pair_id"])
        if any(attempt["status"] == "complete" for attempt in previous):
            continue
        attempt_number = len(previous) + 1
        pair_started_at = datetime.now(timezone.utc)
        evidence_block = pair.get("evidence_block")
        if evidence_profile is not None and evidence_block:
            window = state["evidence"]["windows"].setdefault(
                evidence_block, {"start": pair_started_at.isoformat()}
            )
            window["end"] = pair_started_at.isoformat()
        attempt: dict[str, Any] = {
            "pair_id": pair["pair_id"],
            "protocol": pair["protocol"],
            "attempt_number": attempt_number,
            "retry_count": attempt_number - 1,
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
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
                    attempt_number,
                    args.dry_run,
                )
                record = {
                    **member,
                    "benchmark_run_id": benchmark_run_id,
                    "command": command,
                    "status": "running",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                }
                attempt["members"].append(record)
                write_state(state_path, state)
                member_profile = load(Path(member["profile"]))
                sampler = None
                if not args.dry_run:
                    sampler = DockerSampler(runtime_containers(member_profile))
                    sampler.start()
                try:
                    if pair.get("restart_trino") and not args.dry_run:
                        restart_trino(member_profile)
                    subprocess.run(command, cwd=ROOT, check=True)
                    record["status"] = "complete"
                    record["finished_at"] = datetime.now(timezone.utc).isoformat()
                finally:
                    if sampler is not None:
                        record["resource_samples"] = sampler.stop()
                write_state(state_path, state)
        except (OSError, subprocess.CalledProcessError, RuntimeError) as error:
            attempt["status"] = "failed"
            attempt["finished_at"] = datetime.now(timezone.utc).isoformat()
            attempt["error"] = {
                "type": type(error).__name__,
                "message": str(error),
                "returncode": getattr(error, "returncode", None),
            }
            write_state(state_path, state)
            raise
        pair_finished_at = datetime.now(timezone.utc)
        attempt["status"] = "complete"
        attempt["finished_at"] = pair_finished_at.isoformat()
        if evidence_profile is not None and evidence_block:
            state["evidence"]["windows"][evidence_block]["end"] = (
                pair_finished_at.isoformat()
            )
        write_state(state_path, state)

    if evidence_profile is not None:
        for window in state["evidence"]["windows"].values():
            window["request_metrics"] = collect_request_window(
                comparison_id,
                evidence_profile,
                datetime.fromisoformat(window["start"]),
                datetime.fromisoformat(window["end"]),
            )
            write_state(state_path, state)
        state["evidence"]["final_storage_snapshot"] = static_snapshot(
            comparison_id, evidence_profile
        )
        write_state(state_path, state)
        set_request_metrics(comparison_id, evidence_profile, False)
        atexit.unregister(set_request_metrics)
        state["evidence"]["request_metrics_disabled_at"] = datetime.now(
            timezone.utc
        ).isoformat()
        write_state(state_path, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
