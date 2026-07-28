import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ARTIFACT_ROOT = ROOT / "benchmarks/artifacts/comparisons"

EXIT_COMPLETE = 0
EXIT_RUNNING = 1
EXIT_INVALID = 2
EXIT_UNKNOWN_OR_ORPHANED = 3


def load_state(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def newest_h3_state(artifact_root: Path) -> Path:
    candidates = list(artifact_root.glob("h3_executor_sizing_*/comparison_run.json"))
    if not candidates:
        raise FileNotFoundError(f"No H3 comparison state found under {artifact_root}")

    def created_at(path: Path) -> str:
        try:
            return str(load_state(path).get("created_at", ""))
        except (json.JSONDecodeError, OSError, TypeError):
            return ""

    return max(candidates, key=lambda path: (created_at(path), path.stat().st_mtime))


def process_matches(comparison_id: str) -> dict[str, list[int]]:
    matches = {
        "runner": [],
        "benchmark": [],
        "spark": [],
    }
    proc_root = Path("/proc")
    if not proc_root.exists():
        return matches

    for process_dir in proc_root.iterdir():
        if not process_dir.name.isdigit():
            continue
        pid = int(process_dir.name)
        if pid == os.getpid():
            continue
        try:
            arguments = [
                value.decode(errors="replace")
                for value in (process_dir / "cmdline").read_bytes().split(b"\0")
                if value
            ]
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if comparison_id not in arguments and not any(
            comparison_id in value for value in arguments
        ):
            continue

        if any("run_h3_executor_sizing.py" in value for value in arguments):
            matches["runner"].append(pid)
        if any("run_benchmark.py" in value for value in arguments):
            matches["benchmark"].append(pid)
        if any(
            marker in value
            for value in arguments
            for marker in ("org.apache.spark.deploy.SparkSubmit", "/opt/spark/jobs/")
        ):
            matches["spark"].append(pid)

    return {name: sorted(set(pids)) for name, pids in matches.items()}


def elapsed_seconds(created_at: Any, finished_at: Any) -> float | None:
    if not isinstance(created_at, str):
        return None
    try:
        created = datetime.fromisoformat(created_at)
        finished = (
            datetime.fromisoformat(finished_at)
            if isinstance(finished_at, str)
            else datetime.now(timezone.utc)
        )
    except ValueError:
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    if finished.tzinfo is None:
        finished = finished.replace(tzinfo=timezone.utc)
    return max((finished - created).total_seconds(), 0.0)


def status_payload(state_path: Path) -> tuple[dict[str, Any], int]:
    state = load_state(state_path)
    comparison_id = str(state["comparison_id"])
    attempts = list(state.get("attempts", []))
    members = [member for attempt in attempts for member in attempt.get("members", [])]
    expected_pairs = int(state["expected_pairs"])
    expected_members = int(state["expected_members"])
    completed_pairs = sum(attempt.get("status") == "complete" for attempt in attempts)
    failed_pairs = sum(attempt.get("status") == "failed" for attempt in attempts)
    completed_members = sum(member.get("status") == "complete" for member in members)
    running_members = sum(member.get("status") == "running" for member in members)
    failed_members = sum(member.get("status") == "failed" for member in members)
    pending_members = max(
        expected_members - completed_members - running_members - failed_members,
        0,
    )
    processes = process_matches(comparison_id)
    runner_active = bool(processes["runner"])
    status = str(state.get("status", "unknown"))

    if status in {"complete", "dry_run_complete"}:
        exit_code = EXIT_COMPLETE
        result = "complete"
    elif status in {"invalid", "failed"}:
        exit_code = EXIT_INVALID
        result = "invalid"
    elif status == "running" and runner_active:
        exit_code = EXIT_RUNNING
        result = "running"
    elif status == "running":
        exit_code = EXIT_UNKNOWN_OR_ORPHANED
        result = "orphaned"
    else:
        exit_code = EXIT_UNKNOWN_OR_ORPHANED
        result = "unknown"

    current_attempt = attempts[-1] if status == "running" and attempts else {}
    current_members = list(current_attempt.get("members", []))
    current_member = current_members[-1] if current_members else {}
    payload = {
        "comparison_id": comparison_id,
        "state_path": str(state_path),
        "state_status": status,
        "result": result,
        "runner_active": runner_active,
        "runner_pids": processes["runner"],
        "benchmark_pids": processes["benchmark"],
        "spark_pids": processes["spark"],
        "expected_pairs": expected_pairs,
        "scheduled_pairs": len(attempts),
        "completed_pairs": completed_pairs,
        "failed_pairs": failed_pairs,
        "expected_members": expected_members,
        "scheduled_members": len(members),
        "completed_members": completed_members,
        "running_members": running_members,
        "failed_members": failed_members,
        "pending_members": pending_members,
        "progress_percent": (
            completed_members / expected_members * 100 if expected_members else 0.0
        ),
        "current_pair": current_attempt.get("pair_id"),
        "current_benchmark_run_id": current_member.get("benchmark_run_id"),
        "current_profile": current_member.get("profile_name"),
        "current_workload": current_attempt.get("workload_name"),
        "created_at": state.get("created_at"),
        "finished_at": state.get("finished_at"),
        "elapsed_seconds": elapsed_seconds(
            state.get("created_at"),
            state.get("finished_at"),
        ),
        "invalid_reason": state.get("invalid_reason"),
        "exit_code": exit_code,
    }
    return payload, exit_code


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, remaining_seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"


def print_human(payload: dict[str, Any]) -> None:
    print(f"comparison: {payload['comparison_id']}")
    print(f"result: {payload['result']}")
    print(f"state: {payload['state_status']}")
    print(
        "runner: "
        + (
            f"active (PIDs: {', '.join(map(str, payload['runner_pids']))})"
            if payload["runner_active"]
            else "not found"
        )
    )
    print(
        f"progress: {payload['completed_members']}/"
        f"{payload['expected_members']} "
        f"({payload['progress_percent']:.2f}%); "
        f"{payload['running_members']} running; "
        f"{payload['pending_members']} pending; "
        f"{payload['failed_members']} failed"
    )
    print(
        f"pairs: {payload['completed_pairs']}/"
        f"{payload['expected_pairs']} complete; "
        f"{payload['failed_pairs']} failed"
    )
    print(f"current pair: {payload['current_pair'] or '-'}")
    print(f"current profile: {payload['current_profile'] or '-'}")
    print(f"current workload: {payload['current_workload'] or '-'}")
    print(f"current benchmark: {payload['current_benchmark_run_id'] or '-'}")
    print(f"elapsed: {format_duration(payload['elapsed_seconds'])}")
    print(
        "active child processes: "
        f"{len(payload['benchmark_pids'])} benchmark, "
        f"{len(payload['spark_pids'])} Spark"
    )
    if payload["invalid_reason"]:
        print(f"invalid reason: {payload['invalid_reason']}")
    print(f"state file: {payload['state_path']}")

    if payload["result"] == "orphaned":
        print(
            "WARNING: state says running but no H3 runner process exists. "
            "Do not resume this single-use comparison ID.",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check an H3 Spark executor-sizing comparison process and state.",
        epilog=(
            "Exit codes: 0=complete, 1=running, 2=invalid/failed, "
            "3=missing/unknown/orphaned."
        ),
    )
    parser.add_argument(
        "comparison_id",
        nargs="?",
        help="Comparison ID. Defaults to the newest H3 comparison state.",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=DEFAULT_ARTIFACT_ROOT,
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    try:
        state_path = (
            args.artifact_root / args.comparison_id / "comparison_run.json"
            if args.comparison_id
            else newest_h3_state(args.artifact_root)
        )
        if not state_path.exists():
            raise FileNotFoundError(f"Missing comparison state: {state_path}")
        payload, exit_code = status_payload(state_path)
    except (
        FileNotFoundError,
        json.JSONDecodeError,
        KeyError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        print(f"Could not read H3 comparison status: {error}", file=sys.stderr)
        return EXIT_UNKNOWN_OR_ORPHANED

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_human(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
