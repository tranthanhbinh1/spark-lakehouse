import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts.benchmarks.phase3_evidence import (
    collect_request_window,
    parse_size,
)
from scripts.benchmarks.prepare_phase3 import (
    cleanup_from_manifest,
    remote_record,
    spark_sql,
)
from scripts.benchmarks.report_phase3 import (
    aggregate_cost_estimate,
    paired_bootstrap,
    percentile,
)
from scripts.benchmarks.run_benchmark import (
    Partition,
    Workload,
    insert_metrics_from_artifact,
    selected_partitions,
)
from scripts.benchmarks.run_phase3_comparison import (
    benchmark_command,
    prior_attempts,
    schedule,
)


class Body:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.position = 0
        self.closed = False

    def read(self, size: int) -> bytes:
        block = self.content[self.position : self.position + size]
        self.position += len(block)
        return block

    def close(self) -> None:
        self.closed = True


class FakeS3:
    def __init__(self, remote: bytes) -> None:
        self.remote = remote
        self.body = Body(remote)

    def get_object(self, **_: object) -> dict[str, object]:
        return {"Body": self.body}

    def head_object(self, **_: object) -> dict[str, object]:
        return {
            "ContentLength": len(self.remote),
            "ETag": '"etag"',
            "Metadata": {},
        }


class FakeCloudWatch:
    def get_metric_statistics(self, **_: object) -> dict[str, object]:
        return {"Datapoints": []}


class FakeSession:
    def client(self, _: str) -> FakeCloudWatch:
        return FakeCloudWatch()


class FakeTrino:
    def __init__(self) -> None:
        self.sql = []

    def execute(self, sql: str) -> dict[str, object]:
        self.sql.append(sql)
        return {"state": "FINISHED", "error": None}


class Phase3Test(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = {
            "workload": "workload.toml",
            "pipeline_pairs": 1,
            "correctness_queries": "correctness",
            "architectures": [
                {"name": "onprem", "profile": "onprem.toml"},
                {"name": "hybrid_aws", "profile": "hybrid.toml"},
            ],
            "query_targets": [
                {
                    "name": "scan_yellow",
                    "sql_file": "queries/scan.sql",
                    "workload": "workload.toml",
                    "dataset": "yellow",
                    "year": 2011,
                    "month": 1,
                    "warmup_executions": 1,
                    "recorded_executions": 2,
                    "cold_executions": 1,
                }
            ],
        }

    def test_schedule_alternates_every_pair(self) -> None:
        pairs = schedule(self.spec)
        self.assertEqual(
            [pair["members"][0]["architecture"] for pair in pairs],
            ["onprem", "hybrid_aws", "onprem", "hybrid_aws", "onprem", "hybrid_aws"],
        )
        self.assertEqual(sum(pair["protocol"] == "warm_recorded" for pair in pairs), 2)
        self.assertEqual(
            sum(pair["protocol"] == "service_cold_recorded" for pair in pairs), 1
        )

    def test_retry_attempt_reruns_both_members_with_new_ids(self) -> None:
        pair = schedule(self.spec)[0]
        state = {
            "attempts": [
                {
                    "pair_id": pair["pair_id"],
                    "status": "failed",
                    "members": [{"status": "complete"}, {"status": "failed"}],
                }
            ]
        }
        self.assertEqual(len(prior_attempts(state, pair["pair_id"])), 1)
        commands = [
            benchmark_command(
                self.spec,
                pair,
                member,
                "comparison",
                Path("artifacts"),
                2,
                True,
            )
            for member in pair["members"]
        ]
        self.assertTrue(all("__a02__" in run_id for run_id, _ in commands))
        self.assertTrue(all("--retry-count" in command for _, command in commands))

    def test_dataset_target_is_not_repeated_per_month(self) -> None:
        workload = Workload(
            Path("workload.toml"),
            "workload",
            1,
            1,
            [
                Partition("yellow", 2011, 1),
                Partition("yellow", 2011, 4),
                Partition("green", 2014, 1),
            ],
        )
        selected = selected_partitions(workload, "yellow", 2011, 1)
        self.assertEqual(selected, [Partition("yellow", 2011, 1)])

    def test_remote_checksum_mismatch_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            remote_record(FakeS3(b"remote"), "bucket", "key", "bad", 6)

    @patch(
        "scripts.benchmarks.prepare_phase3.aws_environment",
        return_value={
            "AWS_ACCESS_KEY_ID": "access-value",
            "AWS_SECRET_ACCESS_KEY": "secret-value",
        },
    )
    @patch("scripts.benchmarks.prepare_phase3.subprocess.run")
    def test_spark_sql_forwards_aws_credentials_without_values_in_command(
        self,
        run: MagicMock,
        _: MagicMock,
    ) -> None:
        profile = {"runtime": {"spark_master_container": "spark-master"}}
        spark_sql(profile, "select 1")
        command = run.call_args.args[0]
        environment = run.call_args.kwargs["env"]
        self.assertIn("AWS_ACCESS_KEY_ID", command)
        self.assertIn("AWS_SECRET_ACCESS_KEY", command)
        self.assertNotIn("access-value", command)
        self.assertNotIn("secret-value", command)
        self.assertEqual(environment["AWS_ACCESS_KEY_ID"], "access-value")
        self.assertEqual(environment["AWS_SECRET_ACCESS_KEY"], "secret-value")

    def test_cleanup_requires_matching_acceptance_marker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            marker = root / "accepted"
            manifest.write_text(
                json.dumps(
                    {
                        "comparison_id": "expected",
                        "architectures": [],
                        "objects": [],
                    }
                )
            )
            marker.write_text("different")
            with self.assertRaisesRegex(ValueError, "does not match"):
                cleanup_from_manifest(manifest, marker, True)

    def test_missing_cloudwatch_datapoints_remain_incomplete(self) -> None:
        profile = {
            "object_store": {
                "bucket": "raw",
                "prefix": "phase3/raw",
                "profile": "profile",
            },
            "warehouse": {"bucket": "warehouse", "prefix": "warehouse/phase3"},
        }
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        with patch(
            "scripts.benchmarks.phase3_evidence.aws_session",
            return_value=FakeSession(),
        ):
            result = collect_request_window(
                "comparison",
                profile,
                now,
                now,
                poll_attempts=1,
                poll_interval_seconds=0,
            )
        self.assertFalse(result["complete"])

    def test_statistics_are_deterministic(self) -> None:
        self.assertEqual(percentile([1.0, 2.0, 3.0, 4.0], 0.25), 1.75)
        first = paired_bootstrap([1.0, 2.0, 3.0], [2.0, 3.0, 4.0], resamples=100)
        second = paired_bootstrap([1.0, 2.0, 3.0], [2.0, 3.0, 4.0], resamples=100)
        self.assertEqual(first, second)

    def test_metric_reload_deletes_before_each_insert(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "benchmark_run.json"
            artifact.write_text(
                json.dumps(
                    {
                        "benchmark_run_id": "run",
                        "metrics": [{"metric_id": "metric", "metric_type": "pipeline"}],
                    }
                )
            )
            trino = FakeTrino()
            profile = {"metrics_table": "lakehouse.benchmark.run_metrics"}
            insert_metrics_from_artifact(artifact, profile, trino)
            insert_metrics_from_artifact(artifact, profile, trino)
            self.assertEqual(len(trino.sql), 4)
            self.assertTrue(trino.sql[0].startswith("delete from"))
            self.assertTrue(trino.sql[1].startswith("insert into"))
            self.assertTrue(trino.sql[2].startswith("delete from"))
            self.assertTrue(trino.sql[3].startswith("insert into"))

    def test_aggregate_cost_uses_pricing_units(self) -> None:
        def offer(group: str, unit: str, price: str) -> str:
            return json.dumps(
                {
                    "product": {
                        "productFamily": "API Request",
                        "attributes": {"group": group},
                    },
                    "terms": {
                        "OnDemand": {
                            "term": {
                                "priceDimensions": {
                                    "dimension": {
                                        "description": group,
                                        "unit": unit,
                                        "pricePerUnit": {"USD": price},
                                    }
                                }
                            }
                        }
                    },
                }
            )

        state = {
            "evidence": {
                "static_snapshot": {
                    "pricing": {
                        "price_list": [
                            offer("S3-API-Tier1", "Requests", "0.000005"),
                            offer("S3-API-Tier2", "Requests", "0.0000004"),
                        ]
                    }
                },
                "windows": {
                    "warm_query": {
                        "request_metrics": {
                            "targets": [
                                {
                                    "metrics": {
                                        "PutRequests": [{"Sum": 2}],
                                        "GetRequests": [{"Sum": 10}],
                                    }
                                }
                            ]
                        }
                    }
                },
            }
        }
        result = aggregate_cost_estimate(state)
        self.assertAlmostEqual(result["estimated_usd"], 0.000014)

    def test_docker_size_parser(self) -> None:
        self.assertEqual(parse_size("1.5GiB"), 1.5 * 1024**3)
        self.assertEqual(parse_size("512 MiB"), 512 * 1024**2)


if __name__ == "__main__":
    unittest.main()
