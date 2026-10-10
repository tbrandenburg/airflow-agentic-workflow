"""Real gateway Planner plus deployment-controlled Airflow runtime fault acceptance."""

import asyncio
import json
import time
from uuid import uuid4

import httpx
from airflow_core.artifacts import ArtifactStore, canonical, run_key
from airflow_core.contracts import Publication, RunReport
from airflow_core.roles import RESPONSE
from deploy import LOCAL
from run_api.airflow import Airflow


async def fixtures(store: ArtifactStore) -> None:
    password = (LOCAL / "secrets/operator_password").read_text().strip()
    airflow = Airflow("http://127.0.0.1:18080", "operator", password)
    for mode in (
        "success",
        "transient",
        "permanent",
        "timeout",
        "malformed",
        "schema_invalid",
        "missing_patch",
        "oversized",
    ):
        dag = "agent_fixture_" + mode
        identifier = "p3fixture_" + uuid4().hex
        payload = request(dag)
        store.put(
            f"{run_key(dag, identifier)}/run-input.json", canonical(payload), "application/json"
        )
        await airflow.request(
            "POST",
            f"/dags/{dag}/dagRuns",
            {
                "dag_run_id": identifier,
                "logical_date": None,
                "conf": payload,
            },
        )
        deadline = time.monotonic() + 420
        while time.monotonic() < deadline:
            state = (await airflow.request("GET", f"/dags/{dag}/dagRuns/{identifier}")).state
            if state in ("success", "failed"):
                break
            await asyncio.sleep(3)
        else:
            raise TimeoutError(dag)
        report = read_report(store, dag, identifier)
        expected = "passed" if mode in ("success", "transient") else "execution_failed"
        assert report.outcome == expected, (
            dag,
            report.outcome,
            report.errors,
            report.missing_evidence,
        )
        for task in report.tasks:
            assert len(task.attempts) == (2 if mode == "transient" else 1)
            attempt = task.attempts[-1]
            assert {"events.ndjson", "runtime.json", "prompt.txt"} <= set(attempt.diagnostics)
            metadata = json.loads(store.get(attempt.diagnostics["runtime.json"]))
            expected_class = {
                "permanent": "permanent",
                "timeout": "timeout",
                "oversized": "oversized",
            }.get(mode, "invalid_output")
            assert metadata["error_class"] == (None if expected == "passed" else expected_class)
            if expected == "passed":
                RESPONSE.validate_json(store.get(attempt.outputs["response.json"]))
        print(
            f"Fixture {mode}: {state}/{report.outcome} "
            f"s3://artifacts/{run_key(dag, identifier)}/published.json"
        )


def request(workflow: str) -> dict[str, object]:
    return {
        "project": "example-service",
        "workflow": workflow,
        "task": "agent-smoke",
        "base_ref": "64b998b135f36de249d9708a2307d423e935c2e8",
        "model_settings": {"model": "github-copilot/gpt-6.1-sol"},
    }


def read_report(store: ArtifactStore, dag: str, identifier: str) -> RunReport:
    marker = Publication.model_validate_json(
        store.read(f"{run_key(dag, identifier)}/published.json")[1]
    )
    for ref in marker.reports.values():
        store.get(ref)
    return RunReport.model_validate_json(store.get(marker.reports["run-report.json"]))


def main() -> None:
    store = ArtifactStore.deployment("http://127.0.0.1:19000", LOCAL / "secrets")
    headers = {"Authorization": f"Bearer {(LOCAL / 'secrets/gateway_token').read_text().strip()}"}
    with httpx.Client(base_url="http://127.0.0.1:18081", timeout=20, headers=headers) as client:
        assert (
            client.post(
                "/agent-runs", json={**request("agent_smoke"), "fixture": "success"}
            ).status_code
            == 422
        )
        assert client.post("/agent-runs", json=request("agent_fixture_success")).status_code == 403
        response = client.post("/agent-runs", json=request("agent_smoke"))
        response.raise_for_status()
        identifier = response.json()["dag_run_id"]
        print(f"Real planner triggered {identifier}")
        deadline = time.monotonic() + 420
        while time.monotonic() < deadline:
            status = client.get(f"/agent-runs/{identifier}")
            status.raise_for_status()
            state = status.json()["state"]
            if state in ("success", "failed"):
                break
            time.sleep(3)
        else:
            raise TimeoutError(identifier)
    report = read_report(store, "agent_smoke", identifier)
    print(
        f"Real Planner: {state}/{report.outcome} "
        f"s3://artifacts/{run_key('agent_smoke', identifier)}/published.json"
    )
    for task in report.tasks:
        for attempt in task.attempts:
            print(
                json.dumps(
                    {
                        "errors": attempt.errors,
                        "diagnostics": {name: ref.uri for name, ref in attempt.diagnostics.items()},
                    }
                )
            )
    asyncio.run(fixtures(store))
    if state != "success" or report.outcome != "passed":
        raise RuntimeError("Real model Airflow acceptance failed; diagnostics published")
    RESPONSE.validate_json(store.get(report.tasks[0].attempts[-1].outputs["response.json"]))


if __name__ == "__main__":
    main()
