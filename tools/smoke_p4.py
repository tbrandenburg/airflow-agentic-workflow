"""Real three-role P4 acceptance and a fixed failing validation branch, retaining evidence."""

import asyncio
import json
import time
from uuid import uuid4

import httpx
from airflow_core.artifacts import ArtifactStore, canonical, run_key
from airflow_core.roles import RESPONSE
from deploy import LOCAL
from run_api.airflow import Airflow
from smoke_p3 import read_report

TASK = 'Change service.py health() return value from "ok" to "healthy". No other changes.'


def payload() -> dict[str, object]:
    return {
        "project": "example-service",
        "workflow": "code-review",
        "task": TASK,
        "base_ref": "64b998b135f36de249d9708a2307d423e935c2e8",
        "model_settings": {"model": "github-copilot/gpt-6.1-sol"},
    }


async def fixture(store: ArtifactStore) -> dict[str, object]:
    airflow = Airflow(
        "http://127.0.0.1:18080",
        "operator",
        (LOCAL / "secrets/operator_password").read_text().strip(),
    )
    dag, identifier = "code_review_validation_fail", "p4fixture_" + uuid4().hex
    store.put(
        f"{run_key(dag, identifier)}/run-input.json", canonical(payload()), "application/json"
    )
    await airflow.request(
        "POST",
        f"/dags/{dag}/dagRuns",
        {
            "dag_run_id": identifier,
            "logical_date": None,
            "conf": payload(),
        },
    )
    deadline = time.monotonic() + 720
    while time.monotonic() < deadline:
        state = (await airflow.request("GET", f"/dags/{dag}/dagRuns/{identifier}")).state
        if state in ("failed", "success"):
            break
        await asyncio.sleep(3)
    else:
        raise TimeoutError(identifier)
    report = read_report(store, dag, identifier)
    assert state == "failed" and report.outcome == "validation_failed", report.model_dump()
    assert report.validation is not None and report.validation["gate_status"] == "failed"
    assert report.review is None
    states = {task.task_id: task.state for task in report.tasks}
    assert states["validate"] == "success" and states["review"] == "skipped"
    assert states["package"] == "success" and states["skip_review"] == "success"
    # Read actual terminal leaf states, not the report's pre-publication snapshot.
    async with httpx.AsyncClient(timeout=20) as client:
        login = await client.post(
            "http://127.0.0.1:18080/auth/token",
            json={"username": airflow.username, "password": airflow.password},
        )
        login.raise_for_status()
        headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
        response = await client.get(
            f"http://127.0.0.1:18080/api/v2/dags/{dag}/dagRuns/{identifier}/taskInstances",
            headers=headers,
        )
        response.raise_for_status()
        terminal = {task["task_id"]: task["state"] for task in response.json()["task_instances"]}
    assert terminal["publish_run"] == "success" and terminal["assert_outcome"] == "failed"
    evidence: dict[str, object] = {
        "dag_id": dag,
        "dag_run_id": identifier,
        "state": state,
        "outcome": report.outcome,
        "task_states": terminal,
        "publication": f"s3://artifacts/{run_key(dag, identifier)}/published.json",
    }
    print(json.dumps(evidence, indent=2))
    return evidence


def main() -> None:
    store = ArtifactStore.deployment("http://127.0.0.1:19000", LOCAL / "secrets")
    headers = {"Authorization": f"Bearer {(LOCAL / 'secrets/gateway_token').read_text().strip()}"}
    with httpx.Client(base_url="http://127.0.0.1:18081", headers=headers, timeout=20) as client:
        assert client.post("/runs", json={**payload(), "fixture": "success"}).status_code == 422
        assert (
            client.post(
                "/runs", json={**payload(), "workflow": "code_review_validation_fail"}
            ).status_code
            == 403
        )
        response = client.post("/runs", json=payload())
        response.raise_for_status()
        identifier = response.json()["dag_run_id"]
        print(f"Real P4 triggered {identifier}", flush=True)
        (LOCAL / "p4-active.json").write_text(json.dumps({"dag_run_id": identifier}))
        deadline = time.monotonic() + 720
        failures = 0
        while time.monotonic() < deadline:
            response = client.get(f"/runs/{identifier}")
            if response.status_code == 502 and failures < 3:
                failures += 1
                print(
                    f"Transient status 502 ({failures}/3); retaining run {identifier}", flush=True
                )
                time.sleep(3)
                continue
            response.raise_for_status()
            status = response.json()
            if status["state"] in ("success", "failed"):
                break
            time.sleep(3)
        else:
            raise TimeoutError(identifier)
        report = read_report(store, "code_review", identifier)
        evidence: dict[str, object] = {
            "verified_at": report.generated_at,
            "dag_id": "code_review",
            "dag_run_id": identifier,
            "state": status["state"],
            "outcome": report.outcome,
            "publication": f"s3://artifacts/{run_key('code_review', identifier)}/published.json",
            "reports": status["reports"],
            "patch": report.patch.model_dump() if report.patch else None,
        }
        (LOCAL / "p4-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(json.dumps(evidence, indent=2), flush=True)
        assert status["state"] == "success" and report.outcome == "passed", (
            report.errors,
            report.missing_evidence,
            [
                (task.task_id, task.state, [attempt.errors for attempt in task.attempts])
                for task in report.tasks
            ],
        )
        assert report.validation is not None and report.validation["gate_status"] == "passed"
        assert status["outcome"] == "passed" and status["artifacts"]
        response = client.get(f"/runs/{identifier}/artifacts")
        response.raise_for_status()
        assert response.json()["artifacts"] == status["artifacts"]
        for task in report.tasks:
            if task.task_id in ("plan", "implement", "review"):
                attempt = task.attempts[-1]
                RESPONSE.validate_json(store.get(attempt.outputs["response.json"]))
                metadata = json.loads(store.get(attempt.diagnostics["runtime.json"]))
                assert metadata["error_class"] is None
        assert report.review is not None
    evidence["validation_failure"] = asyncio.run(fixture(store))
    (LOCAL / "p4-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    main()
