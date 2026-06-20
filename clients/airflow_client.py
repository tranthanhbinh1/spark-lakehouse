import os
import time
from typing import Any
from urllib.parse import quote

import requests

from .utils import utc_now_iso

TERMINAL_DAG_STATES = {"success", "failed"}


class AirflowClient:
    def __init__(self, config: dict[str, Any]) -> None:
        self.base_url = str(config["base_url"]).rstrip("/")
        self.dag_id = str(config["dag_id"])
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self._configure_auth(config)

    def _configure_auth(self, config: dict[str, Any]) -> None:
        token_env = config.get("bearer_token_env")
        if token_env and os.getenv(str(token_env)):
            self.session.headers.update(
                {"Authorization": f"Bearer {os.environ[str(token_env)]}"}
            )
            return

        if config.get("auth") != "token":
            return

        username = os.getenv(str(config.get("username_env", "")))
        password = os.getenv(str(config.get("password_env", "")))
        if not username or not password:
            return

        response = self.session.post(
            f"{self.base_url}/auth/token",
            json={"username": username, "password": password},
            timeout=30,
        )
        response.raise_for_status()
        token = response.json()["access_token"]
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def trigger_dag_run(self, dag_run_id: str, conf: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/api/v2/dags/{quote(self.dag_id)}/dagRuns"
        payload = {
            "dag_run_id": dag_run_id,
            "logical_date": utc_now_iso(),
            "conf": conf,
        }
        response = self.session.post(url, json=payload, timeout=30)
        if response.status_code == 400:
            payload = {
                "run_id": dag_run_id,
                "logical_date": utc_now_iso(),
                "conf": conf,
            }
            response = self.session.post(url, json=payload, timeout=30)
        if response.status_code >= 400:
            raise RuntimeError(
                "Airflow DAG-run creation failed: "
                f"{response.status_code} {response.text}"
            )
        return response.json()

    def get_dag_run(self, dag_run_id: str) -> dict[str, Any]:
        url = (
            f"{self.base_url}/api/v2/dags/{quote(self.dag_id)}/dagRuns/"
            f"{quote(dag_run_id, safe='')}"
        )
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()

    def wait_for_dag_run(
        self, dag_run_id: str, poll_interval: int, poll_timeout: int
    ) -> dict[str, Any]:
        deadline = time.monotonic() + poll_timeout
        while time.monotonic() < deadline:
            dag_run = self.get_dag_run(dag_run_id)
            state = str(dag_run.get("state", "")).lower()
            if state in TERMINAL_DAG_STATES:
                return dag_run
            time.sleep(poll_interval)
        raise TimeoutError(f"Timed out waiting for DAG run: {dag_run_id}")

    def list_task_instances(self, dag_run_id: str) -> list[dict[str, Any]]:
        url = (
            f"{self.base_url}/api/v2/dags/{quote(self.dag_id)}/dagRuns/"
            f"{quote(dag_run_id, safe='')}/taskInstances"
        )
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()
        return list(payload.get("task_instances", payload.get("taskInstances", [])))

    def fetch_task_log(
        self, dag_run_id: str, task_id: str, try_number: int
    ) -> dict[str, Any]:
        v2_url = (
            f"{self.base_url}/api/v2/dags/{quote(self.dag_id)}/dagRuns/"
            f"{quote(dag_run_id, safe='')}/taskInstances/{quote(task_id)}/logs/"
            f"{try_number}?full_content=true"
        )
        response = self.session.get(v2_url, timeout=30)
        if response.status_code == 404:
            v1_url = (
                f"{self.base_url}/api/v1/dags/{quote(self.dag_id)}/dagRuns/"
                f"{quote(dag_run_id, safe='')}/taskInstances/{quote(task_id)}/logs/"
                f"{try_number}?full_content=true"
            )
            response = self.session.get(v1_url, timeout=30)
        if response.status_code == 404:
            return {"available": False, "status_code": 404}
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            payload = response.json()
            content = str(payload.get("content", ""))
        else:
            payload = {"content": response.text}
            content = response.text
        return {
            "available": True,
            "status_code": response.status_code,
            "content_length": len(content),
            "content_tail": content[-4000:],
            "payload": payload if len(content) <= 4000 else None,
        }
