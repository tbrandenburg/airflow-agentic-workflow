"""Real API-triggered artifact-only DAG acceptance. Reads secrets without displaying them."""

import json
import subprocess
import time
from datetime import UTC, datetime

import httpx
from airflow_core.artifacts import ArtifactStore, run_key
from airflow_core.contracts import Publication, RunReport
from deploy import LOCAL, compose


def main() -> None:
    headers = {"Authorization": f"Bearer {(LOCAL / 'secrets/gateway_token').read_text().strip()}"}
    payload = {
        "project": "example-service",
        "workflow": "script-smoke",
        "task": "artifact-smoke",
        "base_ref": "64b998b135f36de249d9708a2307d423e935c2e8",
        "model_settings": {"model": "github-copilot/gpt-6.1-sol"},
    }
    with httpx.Client(base_url="http://127.0.0.1:18081", timeout=20, headers=headers) as client:
        rejected = client.post("/script-runs", json={**payload, "script": "untrusted"})
        assert rejected.status_code == 422
        response = client.post("/script-runs", json=payload)
        response.raise_for_status()
        identifier = response.json()["dag_run_id"]
        print(f"Triggered {identifier}")
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            status = client.get(f"/script-runs/{identifier}")
            status.raise_for_status()
            state = status.json()["state"]
            if state in ("success", "failed"):
                break
            time.sleep(3)
        else:
            raise TimeoutError(f"Run did not terminate: {identifier}")
    store = ArtifactStore.deployment("http://127.0.0.1:19000", LOCAL / "secrets")
    root = run_key("artifact_script_smoke", identifier)
    marker = Publication.model_validate_json(store.read(f"{root}/published.json")[1])
    for ref in marker.reports.values():
        store.get(ref)
    report = RunReport.model_validate_json(store.get(marker.reports["run-report.json"]))
    if state != "success" or report.outcome != "passed":
        raise RuntimeError(f"Terminal={state}, report={report.outcome}, errors={report.errors}")
    assert len(report.tasks) == 2
    assert report.tasks[0].attempts[0].workspace_path != report.tasks[1].attempts[0].workspace_path
    for task in report.tasks:
        assert task.state == "success" and task.completed is not None
        for ref in task.attempts[-1].outputs.values():
            store.get(ref)
        for worker in ("worker-1", "worker-2"):
            subprocess.run(
                compose("exec", "-T", worker, "test", "!", "-e", task.attempts[-1].workspace_path),
                check=True,
                timeout=20,
            )
    evidence = {
        "verified_at": datetime.now(UTC).isoformat(),
        "dag_run_id": identifier,
        "state": state,
        "outcome": report.outcome,
        "publication_uri": f"s3://artifacts/{root}/published.json",
        "reports": {name: ref.uri for name, ref in marker.reports.items()},
        "workers": [task.attempts[-1].worker for task in report.tasks],
    }
    (LOCAL / "p2-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
