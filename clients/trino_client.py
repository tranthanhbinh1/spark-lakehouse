import time
from typing import Any

import requests


class TrinoClient:
    def __init__(self, config: dict[str, Any]) -> None:
        self.base_url = str(config["base_url"]).rstrip("/")
        self.timeout = int(config.get("request_timeout_seconds", 300))
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Trino-User": str(config.get("user", "benchmark")),
                "X-Trino-Catalog": str(config.get("catalog", "lakehouse")),
                "X-Trino-Schema": str(config.get("schema", "benchmark")),
                "X-Trino-Source": str(
                    config.get("source", "lakehouse-benchmark-runner")
                ),
            }
        )

    def execute(self, sql: str) -> dict[str, Any]:
        started_at = time.monotonic()
        response = self.session.post(
            f"{self.base_url}/v1/statement",
            data=sql.encode(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        pages = [payload]
        rows = list(payload.get("data", []))
        while payload.get("nextUri"):
            response = self.session.get(payload["nextUri"], timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()
            pages.append(payload)
            rows.extend(payload.get("data", []))

        final = pages[-1]
        stats = final.get("stats", {})
        error = final.get("error")
        return {
            "query_id": final.get("id") or pages[0].get("id"),
            "state": stats.get("state", "FAILED" if error else "FINISHED"),
            "duration_seconds": time.monotonic() - started_at,
            "rows": rows,
            "row_count": len(rows),
            "processed_rows": stats.get("processedRows"),
            "processed_bytes": stats.get("processedBytes"),
            "elapsed_time": stats.get("elapsedTime"),
            "error": error,
            "pages": pages,
        }
