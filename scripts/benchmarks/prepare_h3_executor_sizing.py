import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.benchmarks.prepare_phase3 import (  # noqa: E402
    client,
    render_ddl,
    spark_sql,
    verify_namespace_locations,
)
from scripts.benchmarks.run_h3_executor_sizing import (  # noqa: E402
    clean_worktree,
    git_sha,
    load,
)


def worker_capacity(profile: dict[str, Any]) -> dict[str, Any]:
    container = profile["runtime"]["spark_master_container"]
    result = subprocess.run(
        ["docker", "exec", container, "curl", "-fsS", "http://localhost:8080/json/"],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    workers = payload.get("workers", [])
    return {
        "alive_workers": int(payload.get("aliveworkers", 0)),
        "total_cores": int(payload.get("cores", 0)),
        "free_cores": int(payload.get("coresfree", 0)),
        "total_memory_mib": int(payload.get("memory", 0)),
        "active_applications": [
            {"id": app.get("id"), "name": app.get("name")}
            for app in payload.get("activeapps", [])
        ],
        "workers": [
            {
                "id": worker.get("id"),
                "state": worker.get("state"),
                "cores": int(worker.get("cores", 0)),
                "memory_mib": int(worker.get("memory", 0)),
            }
            for worker in workers
        ],
    }


def validate_profiles(spec: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    errors = []
    loaded = [(item, load(Path(item["path"]))) for item in spec["profiles"]]
    base = loaded[0][1]
    fixed = {
        key: base[key]
        for key in (
            "architecture",
            "data_catalog",
            "namespaces",
            "airflow",
            "trino",
            "object_store",
            "warehouse",
            "runtime",
        )
    }
    for declared, profile in loaded:
        for key, value in fixed.items():
            if profile[key] != value:
                errors.append(f"{declared['name']}: non-treatment field differs: {key}")
        if int(profile["spark"]["cores_max"]) != int(declared["cores_max"]):
            errors.append(f"{declared['name']}: cores_max differs")
        if int(profile["spark"]["executor_cores"]) != int(declared["executor_cores"]):
            errors.append(f"{declared['name']}: executor_cores differs")
    return base, errors


def validate_inputs(profile: dict[str, Any], spec: dict[str, Any]) -> list[dict[str, Any]]:
    store = profile["object_store"]
    s3 = client(profile)
    records = []
    for workload in spec["workloads"]:
        raw = load(Path(workload["path"]))
        partition = raw["partitions"][0]
        key = (
            f"{str(store['prefix']).strip('/')}/{partition['year']}/"
            f"{partition['dataset']}_tripdata_{partition['year']}-"
            f"{int(partition['month']):02d}.parquet"
        )
        head = s3.head_object(Bucket=store["bucket"], Key=key)
        records.append(
            {
                "workload": workload["name"],
                "uri": f"s3://{store['bucket']}/{key}",
                "size_bytes": int(head["ContentLength"]),
                "etag": str(head.get("ETag", "")).strip('"') or None,
                "version_id": head.get("VersionId"),
            }
        )
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare and validate isolated H3 tables.")
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("benchmarks/comparisons/phase4_h3_executor_sizing.toml"),
    )
    parser.add_argument("--preflight-id", required=True)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--artifact-root", type=Path, default=Path("benchmarks/artifacts/h3_preflight")
    )
    args = parser.parse_args()
    spec = load(args.comparison)
    profile, errors = validate_profiles(spec)
    if not args.validate_only:
        for statement in render_ddl(profile):
            spark_sql(profile, statement)
    namespace_evidence = verify_namespace_locations(profile)
    capacity = worker_capacity(profile)
    if (
        capacity["alive_workers"] != 3
        or capacity["total_cores"] != 12
        or capacity["free_cores"] != 12
        or capacity["active_applications"]
    ):
        errors.append(f"Expected an idle three-worker 12-core cluster, observed {capacity}")
    inputs = validate_inputs(profile, spec)
    if any(item["size_bytes"] <= 0 for item in inputs):
        errors.append("An H3 input object is empty")
    payload = {
        "preflight_id": args.preflight_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git_commit_sha": git_sha(),
        "worktree_clean": clean_worktree(),
        "validate_only": args.validate_only,
        "comparison": str(args.comparison),
        "profiles": spec["profiles"],
        "namespaces": namespace_evidence,
        "inputs": inputs,
        "capacity": capacity,
        "errors": errors,
        "status": "failed" if errors else "passed",
    }
    path = args.artifact_root / args.preflight_id / "preflight.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(path)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
