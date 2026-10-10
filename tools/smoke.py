"""Live Phase 1 acceptance; no mocks, secrets or target repository access."""

import json
import subprocess
import time
from datetime import UTC, datetime

import httpx
from deploy import LOCAL, ROOT, compose


def check(response: httpx.Response, expected: int, label: str) -> None:
    if response.status_code != expected:
        raise RuntimeError(f"{label}: expected {expected}, got {response.status_code}")
    print(f"{label}: HTTP {expected}")


def main() -> None:
    policy = json.loads((ROOT / "infra/airflow/policy.json").read_text())
    payload = {
        "project": policy["project"],
        "workflow": policy["workflow"],
        "task": policy["task"],
        "base_ref": policy["approved_sha"],
        "model_settings": {"model": policy["model"]},
    }
    headers = {"Authorization": f"Bearer {(LOCAL / 'secrets/gateway_token').read_text().strip()}"}
    with httpx.Client(base_url="http://127.0.0.1:18081", timeout=20) as gateway:
        check(gateway.post("/platform-runs", json=payload), 401, "Unauthenticated trigger")
        check(
            gateway.post(
                "/platform-runs", json=payload, headers={"Authorization": "Bearer invalid"}
            ),
            401,
            "Incorrect credential",
        )
        check(gateway.post("/platform-runs", content="{", headers=headers), 422, "Malformed JSON")
        check(
            gateway.post("/platform-runs", json={**payload, "base_ref": "main"}, headers=headers),
            422,
            "Mutable base ref",
        )
        check(
            gateway.post(
                "/platform-runs", json={**payload, "command": "untrusted"}, headers=headers
            ),
            422,
            "Unknown field",
        )
        check(
            gateway.post(
                "/platform-runs",
                json={**payload, "model_settings": {"model": policy["model"], "prompt": "bad"}},
                headers=headers,
            ),
            422,
            "Nested unknown field",
        )
        for field, value in (
            ("project", "unapproved"),
            ("workflow", "unapproved"),
            ("task", "unapproved"),
            ("base_ref", "0" * 40),
            ("model_settings", {"model": "unapproved/model"}),
        ):
            check(
                gateway.post("/platform-runs", json={**payload, field: value}, headers=headers),
                403,
                f"Unapproved {field}",
            )
        response = gateway.post("/platform-runs", json=payload, headers=headers)
        check(response, 201, "Approved trigger")
        run = response.json()
        identifier = run["dag_run_id"]
        check(gateway.get(f"/platform-runs/{identifier}"), 401, "Unauthenticated status")
        check(
            gateway.get(f"/platform-runs/p1_{'0' * 32}", headers=headers), 404, "Missing run status"
        )
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            response = gateway.get(f"/platform-runs/{identifier}", headers=headers)
            response.raise_for_status()
            run = response.json()
            if run["state"] == "failed":
                raise RuntimeError(f"DAG failed: {identifier}")
            if run["state"] == "success":
                break
            time.sleep(3)
        else:
            raise TimeoutError(f"DAG did not succeed within 180 seconds: {identifier}")
    with httpx.Client(base_url="http://127.0.0.1:18080", timeout=20) as airflow:
        check(airflow.get("/api/v2/dags"), 401, "Unauthenticated Airflow REST")
        login = airflow.post(
            "/auth/token",
            json={
                "username": "gateway",
                "password": (LOCAL / "secrets/airflow_password").read_text(),
            },
        )
        login.raise_for_status()
        authorization = {"Authorization": f"Bearer {login.json()['access_token']}"}
        check(
            airflow.post(
                "/api/v2/pools",
                json={"name": "unapproved", "slots": 1, "description": "unapproved"},
                headers=authorization,
            ),
            403,
            "Gateway account cannot administer pools",
        )
        path = f"/api/v2/dags/{policy['dag_id']}/dagRuns/{identifier}/taskInstances/smoke/logs/1"
        logs = airflow.get(path, headers={**authorization, "Accept": "application/json"})
        logs.raise_for_status()
        if "PHASE1_SMOKE_OK" not in logs.text:
            raise RuntimeError("Task log lacks smoke success evidence")
    for worker in ("worker-1", "worker-2"):
        result = subprocess.check_output(
            compose(
                "exec",
                "-T",
                worker,
                "bash",
                "-c",
                "node --version && opencode --version && git --version && shellcheck --version "
                "&& test ! -w /mirrors/example-service.git",
            ),
            text=True,
            timeout=60,
        )
        print(f"{worker} tools/read-only mirror:\n{result.strip()}")
    health = httpx.get("http://127.0.0.1:19000/minio/health/ready", timeout=10)
    check(health, 200, "MinIO ready")
    evidence = {
        "verified_at": datetime.now(UTC).isoformat(),
        "dag_id": run["dag_id"],
        "dag_run_id": identifier,
        "state": run["state"],
        "approved_sha": policy["approved_sha"],
        "log_marker": "PHASE1_SMOKE_OK",
        "compose": (LOCAL / "compose.env").read_text().strip(),
    }
    (LOCAL / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    (LOCAL / "smoke-log.json").write_text(logs.text)
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
