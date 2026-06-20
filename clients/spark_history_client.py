from datetime import UTC, datetime
from typing import Any

import requests


class SparkHistoryClient:
    def __init__(self, base_url: str, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/") + "/api/v1"
        self.timeout = timeout
        self.session = requests.Session()

    def _get(self, path: str, **params: Any) -> Any:
        response = self.session.get(
            f"{self.base_url}/{path.lstrip('/')}", params=params, timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def find_completed_application(
        self, name: str, started_at: str | None, finished_at: str | None
    ) -> dict[str, Any]:
        applications = self._get("applications", status="completed")
        candidates = [app for app in applications if app.get("name") == name]
        if not candidates:
            raise LookupError(f"No completed Spark application named {name!r}")
        start = self._parse_time(started_at)
        end = self._parse_time(finished_at)
        for app in reversed(candidates):
            attempts = app.get("attempts") or []
            attempt = attempts[-1] if attempts else {}
            app_start = self._parse_time(attempt.get("startTime"))
            app_end = self._parse_time(attempt.get("endTime"))
            if (start is None or app_end is None or app_end >= start) and (
                end is None or app_start is None or app_start <= end
            ):
                return app
        raise LookupError(f"Spark application {name!r} did not match the task window")

    def application_artifacts(self, application_id: str) -> dict[str, Any]:
        stages = self._get(f"applications/{application_id}/stages")
        completed = [stage for stage in stages if stage.get("status") == "COMPLETE"]
        counters = {
            "records_read": "inputRecords",
            "records_written": "outputRecords",
            "input_bytes": "inputBytes",
            "output_bytes": "outputBytes",
            "executor_run_time_ms": "executorRunTime",
            "memory_bytes_spilled": "memoryBytesSpilled",
            "disk_bytes_spilled": "diskBytesSpilled",
            "shuffle_read_bytes": "shuffleReadBytes",
            "shuffle_read_records": "shuffleReadRecords",
            "shuffle_write_bytes": "shuffleWriteBytes",
            "shuffle_write_records": "shuffleWriteRecords",
        }
        totals = {
            target: sum(int(stage.get(source) or 0) for stage in completed)
            for target, source in counters.items()
        }
        return {
            "spark_application_id": application_id,
            "stage_attempts": completed,
            "environment": self._get(f"applications/{application_id}/environment"),
            **totals,
        }

    @staticmethod
    def _parse_time(value: Any) -> datetime | None:
        if not value:
            return None
        text = str(value).replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            try:
                return datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%S.%fGMT").replace(
                    tzinfo=UTC
                )
            except ValueError:
                return None
