"""Bounded authenticated proof-only REST access; never prints request credentials."""

import json
import time
from typing import cast

import httpx
from deploy import LOCAL


class ProofAPI:
    def __init__(self) -> None:
        self.client = httpx.Client(base_url="http://127.0.0.1:18080", timeout=20)
        response = self.client.post(
            "/auth/token",
            json={
                "username": "operator",
                "password": (LOCAL / "secrets/operator_password").read_text().strip(),
            },
        )
        response.raise_for_status()
        self.client.headers["Authorization"] = f"Bearer {response.json()['access_token']}"

    def request(
        self, method: str, path: str, data: dict[str, object] | None = None
    ) -> dict[str, object]:
        response = self.client.request(method, "/api/v2" + path, json=data)
        if response.is_error:
            raise RuntimeError(f"Proof REST {method} {path}: {response.status_code}")
        value: object = json.loads(response.content)
        if not isinstance(value, dict):
            raise ValueError("REST response must be an object")
        return cast(dict[str, object], value)

    def tasks(self, dag: str, run: str) -> list[dict[str, object]]:
        return cast(
            list[dict[str, object]],
            self.request("GET", f"/dags/{dag}/dagRuns/{run}/taskInstances")["task_instances"],
        )

    def wait(self, dag: str, run: str, timeout: int = 420) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.request("GET", f"/dags/{dag}/dagRuns/{run}")["state"]
            if state in ("success", "failed"):
                return str(state)
            time.sleep(1)
        raise TimeoutError("Proof DAG deadline exceeded")
