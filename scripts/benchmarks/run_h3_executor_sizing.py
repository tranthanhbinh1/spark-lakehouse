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


def load(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def git_sha() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


def clean_worktree() -> bool:
    return not subprocess.run(
        ["git", "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def config_hash(path: Path, spec: dict[str, Any]) -> str:
    payload = {"path": str(path), "spec": spec}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def schedule(spec: dict[str, Any]) -> list[dict[str, Any]]:
    profiles = list(spec["profiles"])
    if [item["name"] for item in profiles] != ["small", "medium", "default"]:
        raise ValueError("H3 profiles must be ordered small, medium, default")
    expected = [(4, 4), (8, 4), (12, 4)]
    observed = [
        (int(item["cores_max"]), int(item["executor_cores"])) for item in profiles
    ]
    if observed != expected:
        raise ValueError(f"Unexpected H3 sizing profiles: {observed}")

    pairs = []
    position = 0
    orders = [
        profiles,
        profiles[1:] + profiles[:1],
        profiles[2:] + profiles[:2],
    ]
    for repetition in range(1, int(spec["repetitions"]) + 1):
        for workload_index, workload in enumerate(spec["workloads"]):
            order = orders[(repetition - 1 + workload_index) % len(orders)]
            members = []
            for profile in order:
                position += 1
                members.append(
                    {
                        "profile_name": profile["name"],
                        "profile": profile["path"],
                        "cores_max": int(profile["cores_max"]),
                        "executor_cores": int(profile["executor_cores"]),
                        "sequence_position": position,
                    }
                )
            pairs.append(
                {
                    "pair_id": (f"trial-{repetition:02d}__{workload['name']}"),
                    "trial": repetition,
                    "workload_name": workload["name"],
                    "workload": workload["path"],
                    "members": members,
                }
            )
    return pairs


def command(
    comparison_id: str,
    pair: dict[str, Any],
    member: dict[str, Any],
    artifact_dir: Path,
    dry_run: bool,
) -> tuple[str, list[str]]:
    trial_id = f"{pair['pair_id']}__{member['profile_name']}"
    run_id = f"{comparison_id}__{trial_id}"
    args = [
        sys.executable,
        str(ROOT / "scripts/benchmarks/run_benchmark.py"),
        "--workload",
        str(pair["workload"]),
        "--profile",
        str(member["profile"]),
        "--artifact-root",
        str(artifact_dir / "benchmarks"),
        "--benchmark-run-id",
        run_id,
        "--comparison-id",
        comparison_id,
        "--trial-id",
        trial_id,
        "--sequence-position",
        str(member["sequence_position"]),
        "--measurement-protocol",
        "h3_pipeline_paired",
        "--retry-count",
        "0",
        "--skip-queries",
    ]
    if dry_run:
        args.append("--dry-run")
    return run_id, args


def write(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the H3 Spark sizing comparison.")
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("benchmarks/comparisons/phase4_h3_executor_sizing.toml"),
    )
    parser.add_argument("--comparison-id")
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("benchmarks/artifacts/comparisons")
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not clean_worktree():
        raise RuntimeError("Official H3 runs require a clean Git worktree")
    spec = load(args.comparison)
    digest = config_hash(args.comparison, spec)
    comparison_id = args.comparison_id or (
        f"h3_executor_sizing_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        f"_{git_sha()[:7]}"
    )
    artifact_dir = args.artifact_root / comparison_id
    state_path = artifact_dir / "comparison_run.json"
    if state_path.exists():
        raise FileExistsError(
            f"H3 comparison IDs are single-use and cannot be resumed: {state_path}"
        )
    pairs = schedule(spec)
    state: dict[str, Any] = {
        "comparison_id": comparison_id,
        "comparison_path": str(args.comparison),
        "comparison_config_hash": digest,
        "git_commit_sha": git_sha(),
        "status": "running",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "expected_pairs": len(pairs),
        "expected_members": sum(len(pair["members"]) for pair in pairs),
        "attempts": [],
    }
    write(state_path, state)
    try:
        for pair in pairs:
            attempt = {
                "pair_id": pair["pair_id"],
                "trial": pair["trial"],
                "workload_name": pair["workload_name"],
                "workload": pair["workload"],
                "status": "running",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "members": [],
            }
            state["attempts"].append(attempt)
            write(state_path, state)
            for member in pair["members"]:
                run_id, args_command = command(
                    comparison_id, pair, member, artifact_dir, args.dry_run
                )
                record = {
                    **member,
                    "benchmark_run_id": run_id,
                    "command": args_command,
                    "status": "running",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                }
                attempt["members"].append(record)
                write(state_path, state)
                profile = load(Path(member["profile"]))
                sampler = None
                if not args.dry_run:
                    sampler = DockerSampler(runtime_containers(profile))
                    sampler.start()
                try:
                    subprocess.run(args_command, cwd=ROOT, check=True)
                    record["status"] = "complete"
                    record["finished_at"] = datetime.now(timezone.utc).isoformat()
                finally:
                    if sampler is not None:
                        record["resource_samples"] = sampler.stop()
                write(state_path, state)
            attempt["status"] = "complete"
            attempt["finished_at"] = datetime.now(timezone.utc).isoformat()
            write(state_path, state)
    except Exception as error:
        state["status"] = "invalid"
        state["invalid_reason"] = {
            "type": type(error).__name__,
            "message": str(error),
        }
        state["finished_at"] = datetime.now(timezone.utc).isoformat()
        write(state_path, state)
        raise
    state["status"] = "dry_run_complete" if args.dry_run else "complete"
    state["finished_at"] = datetime.now(timezone.utc).isoformat()
    write(state_path, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
