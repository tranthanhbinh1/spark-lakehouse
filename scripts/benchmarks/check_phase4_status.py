import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import tomllib

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COMPARISON_ID = "phase4_file_layout_20260727T102700Z_f62a1f5_official01"

EXIT_COMPLETE = 0
EXIT_RUNNING = 1
EXIT_INVALID = 2
EXIT_UNKNOWN_OR_ORPHANED = 3


def load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def expected_counts(
    comparison_path: Path,
) -> tuple[int, int]:
    spec = load_toml(comparison_path)
    workload = load_toml(ROOT / str(spec["workload"]))
    partitions = list(workload["partitions"])
    datasets = {str(partition["dataset"]) for partition in partitions}

    targets = 0
    for query in spec["queries"]:
        scope = str(query["scope"])
        if scope == "partition":
            targets += len(partitions)
        elif scope == "dataset":
            targets += len(datasets)
        else:
            raise ValueError(f"Unsupported query scope: {scope}")

    executions_per_target = (
        int(spec["warmup_executions"])
        + int(spec["recorded_executions"])
        + int(spec["cold_executions"])
    )
    expected_pairs = targets * int(spec["trial_repetitions"]) * executions_per_target
    expected_members = expected_pairs * len(spec["cells"])
    return expected_pairs, expected_members


def runner_pids(comparison_id: str) -> list[int]:
    matches = []
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
        if (
            any("run_phase4_comparison.py" in value for value in arguments)
            and comparison_id in arguments
        ):
            matches.append(pid)
    return sorted(matches)


def status_payload(
    state_path: Path,
    comparison_path: Path,
) -> tuple[dict[str, Any], int]:
    state = json.loads(state_path.read_text())
    comparison_id = str(state["comparison_id"])
    expected_pairs, expected_members = expected_counts(comparison_path)
    attempts = list(state.get("attempts", []))
    members = [member for attempt in attempts for member in attempt.get("members", [])]
    completed_pairs = sum(attempt.get("status") == "complete" for attempt in attempts)
    failed_pairs = sum(attempt.get("status") == "failed" for attempt in attempts)
    completed_members = sum(member.get("status") == "complete" for member in members)
    running_members = sum(member.get("status") == "running" for member in members)
    pids = runner_pids(comparison_id)
    status = str(state.get("status", "unknown"))

    if status == "complete":
        exit_code = EXIT_COMPLETE
        result = "complete"
    elif status in {"invalid", "failed"}:
        exit_code = EXIT_INVALID
        result = "invalid"
    elif status == "running" and pids:
        exit_code = EXIT_RUNNING
        result = "running"
    elif status == "running":
        exit_code = EXIT_UNKNOWN_OR_ORPHANED
        result = "orphaned"
    else:
        exit_code = EXIT_UNKNOWN_OR_ORPHANED
        result = "unknown"

    payload = {
        "comparison_id": comparison_id,
        "state_path": str(state_path),
        "state_status": status,
        "result": result,
        "runner_active": bool(pids),
        "runner_pids": pids,
        "expected_pairs": expected_pairs,
        "scheduled_pairs": len(attempts),
        "completed_pairs": completed_pairs,
        "failed_pairs": failed_pairs,
        "expected_members": expected_members,
        "scheduled_members": len(members),
        "completed_members": completed_members,
        "running_members": running_members,
        "progress_percent": (
            completed_members / expected_members * 100 if expected_members else 0.0
        ),
        "current_pair": attempts[-1].get("pair_id") if attempts else None,
        "created_at": state.get("created_at"),
        "finished_at": state.get("finished_at"),
        "invalid_reason": state.get("invalid_reason"),
        "exit_code": exit_code,
    }
    return payload, exit_code


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
        f"({payload['progress_percent']:.2f}%)"
    )
    print(
        f"pairs: {payload['completed_pairs']}/"
        f"{payload['expected_pairs']} complete; "
        f"{payload['failed_pairs']} failed"
    )
    print(f"current pair: {payload['current_pair'] or '-'}")
    if payload["invalid_reason"]:
        print(f"invalid reason: {payload['invalid_reason']}")
    print(f"state file: {payload['state_path']}")

    if payload["result"] == "orphaned":
        print(
            "WARNING: state says running but no runner process exists. "
            "Do not resume this single-use comparison ID.",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check a Phase 4 file-layout comparison process and state.",
        epilog=(
            "Exit codes: 0=complete, 1=running, 2=invalid/failed, "
            "3=missing/unknown/orphaned."
        ),
    )
    parser.add_argument(
        "comparison_id",
        nargs="?",
        default=DEFAULT_COMPARISON_ID,
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=ROOT / "benchmarks/artifacts/comparisons",
    )
    parser.add_argument(
        "--comparison",
        type=Path,
        default=ROOT / "benchmarks/comparisons/phase4_file_layout.toml",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    state_path = args.artifact_root / args.comparison_id / "comparison_run.json"
    if not state_path.exists():
        print(f"Missing comparison state: {state_path}", file=sys.stderr)
        return EXIT_UNKNOWN_OR_ORPHANED

    try:
        payload, exit_code = status_payload(state_path, args.comparison)
    except (json.JSONDecodeError, KeyError, OSError, TypeError, ValueError) as error:
        print(f"Could not read comparison status: {error}", file=sys.stderr)
        return EXIT_UNKNOWN_OR_ORPHANED

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_human(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
