"""P5 deployed routing, saturated pools, simultaneous runs, reuse and integrity proof."""

import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from airflow_core.artifacts import ArtifactStore, canonical, digest, run_key
from airflow_core.contracts import TaskResult
from deploy import LOCAL
from p5_airflow import ProofAPI
from p5_integrity import capacity, cleaned, inventory, worker
from p5_network import network
from smoke_p3 import read_report, request

DAG = "operational_proof"


def maximum(intervals: list[tuple[str, str]]) -> int:
    events = [(datetime.fromisoformat(start), 1) for start, _ in intervals]
    events += [(datetime.fromisoformat(end), -1) for _, end in intervals]
    current = peak = 0
    for _stamp, delta in sorted(events):
        current += delta
        peak = max(peak, current)
    return peak


def main() -> None:
    before = inventory()
    store = ArtifactStore.deployment("http://127.0.0.1:19000", LOCAL / "secrets")
    api = ProofAPI()
    configured = capacity()
    networking = network()
    for pool, slots in (("agent-model", 2), ("validation", 1)):
        assert api.request("GET", f"/pools/{pool}")["slots"] == slots
    runs = ["p5_" + uuid4().hex for _ in range(2)]
    for run in runs:
        payload = request(DAG)
        store.put(f"{run_key(DAG, run)}/run-input.json", canonical(payload), "application/json")
        api.request(
            "POST",
            f"/dags/{DAG}/dagRuns",
            {"dag_run_id": run, "logical_date": None, "conf": payload},
        )
    samples = []
    deadline = time.monotonic() + 420
    while time.monotonic() < deadline:
        states = [api.request("GET", f"/dags/{DAG}/dagRuns/{run}") for run in runs]
        tasks = [api.tasks(DAG, run) for run in runs]
        sample = {
            "runs": [state["state"] for state in states],
            "model": sum(
                task["state"] == "running" and str(task["task_id"]).startswith("model_")
                for group in tasks
                for task in group
            ),
            "validation": sum(
                task["state"] == "running" and str(task["task_id"]).startswith("validation_")
                for group in tasks
                for task in group
            ),
            "waiting_model": sum(
                task["state"] in (None, "scheduled", "queued")
                and str(task["task_id"]).startswith("model_")
                for group in tasks
                for task in group
            ),
        }
        assert isinstance(sample["model"], int) and isinstance(sample["validation"], int)
        assert sample["model"] <= 2 and sample["validation"] <= 1
        assert sum(state["state"] == "running" for state in states) <= 1
        samples.append(sample)
        if all(state["state"] in ("success", "failed") for state in states):
            assert all(state["state"] == "success" for state in states), states
            break
        time.sleep(1)
    else:
        raise TimeoutError("P5 saturation proof timed out")
    results: list[TaskResult] = []
    publications = []
    for run in runs:
        report = read_report(store, DAG, run)
        assert report.outcome == "passed"
        attempts = {task.task_id: task.attempts[-1] for task in report.tasks}
        terminal = {str(task["task_id"]): task for task in api.tasks(DAG, run)}
        assert terminal["model_0"]["queue"] == "worker-1"
        assert terminal["handoff"]["queue"] == "worker-2"
        for attempt in attempts.values():
            log = json.loads(store.get(attempt.diagnostics["task-log.json"]))
            assert log["cleanup_live_pids"] == [] and log["classification"] == "success"
            assert log["duration_seconds"] > 0 and log["input_uri"]
            if attempt.task_id.startswith("model_"):
                assert log["model"] == "github-copilot/gpt-6.1-sol"
                assert log["versions"]["node"] == "24.15.0"
                assert log["versions"]["opencode"] == "1.18.32"
        assert attempts["model_0"].worker != attempts["handoff"].worker
        assert attempts["model_0"].workspace_path != attempts["handoff"].workspace_path
        reconstructed = json.loads(store.get(attempts["handoff"].outputs["response.json"]))
        original = json.loads(store.get(attempts["model_0"].outputs["response.json"]))
        assert (
            reconstructed["base_sha"] == report.base_sha
            and reconstructed["inputs"]["response.json"] == original
        )
        assert reconstructed["source_sha256"] == digest(
            Path("projects/example-service/fixtures/service.py").read_bytes()
        )
        results.extend(attempts.values())
        publications.append(f"s3://artifacts/{run_key(DAG, run)}/published.json")
    model_peak = maximum(
        [
            (result.started_at, result.ended_at)
            for result in results
            if result.task_id.startswith("model_")
        ]
    )
    validation_peak = maximum(
        [
            (result.started_at, result.ended_at)
            for result in results
            if result.task_id.startswith("validation_")
        ]
    )
    assert model_peak == 2 and validation_peak == 1
    assert any(
        sample["model"] == 2
        and isinstance(sample["waiting_model"], int)
        and sample["waiting_model"] > 0
        for sample in samples
    )
    # Clear one completed logical task through the actual Airflow API.
    run = runs[0]
    prefix = f"{run_key(DAG, run)}/steps/model_0"
    marker = store.read(f"{prefix}/completed.json")
    original_keys = store.keys(prefix + "/attempts/")
    api.request(
        "POST",
        f"/dags/{DAG}/clearTaskInstances",
        {
            "dag_run_id": run,
            "task_ids": ["model_0"],
            "dry_run": False,
            "only_failed": False,
            "only_running": False,
            "reset_dag_runs": True,
        },
    )
    assert api.wait(DAG, run) == "success"
    assert store.read(f"{prefix}/completed.json") == marker
    assert store.keys(prefix + "/attempts/") == original_keys
    reruns = store.keys(prefix + "/reruns/")
    assert len(reruns) == 1
    reuse = json.loads(store.read(reruns[0])[1])
    assert reuse["model_invoked"] is False and reuse["completed"] == marker[0].model_dump(
        mode="json"
    )
    cleanup = cleaned()
    lifecycle = {
        service: worker(
            service,
            "import json; from airflow_core.operational_probe import lifecycle; "
            f"print(json.dumps(lifecycle('{run_key(DAG, run)}/proof/{service}')))",
        )
        for service in ("worker-1", "worker-2")
    }
    after = inventory()
    assert before == after
    network_ref = store.put(
        f"{run_key(DAG, run)}/proof/network.json",
        canonical({"workers": networking}),
        "application/json",
    )
    for run in runs:
        subprocess.run(
            [
                ".venv/bin/python",
                "tools/provider_auth.py",
                "verify",
                "--run-root",
                run_key(DAG, run),
            ],
            check=True,
            timeout=120,
        )
    evidence = {
        "verified_at": datetime.now().astimezone().isoformat(),
        "runs": runs,
        "publications": publications,
        "model_peak": model_peak,
        "validation_peak": validation_peak,
        "samples": samples,
        "reuse": reuse,
        "mirror_before": before,
        "mirror_after": after,
        "cleanup": cleanup,
        "lifecycle": lifecycle,
        "capacity": configured,
        "network": networking,
        "network_uri": network_ref.uri,
    }
    (LOCAL / "p5-evidence.json").write_bytes(canonical(evidence))
    print(
        json.dumps(
            {
                key: value
                for key, value in evidence.items()
                if key not in ("samples", "mirror_before", "mirror_after")
            },
            indent=2,
        )
    )
    api.client.close()


if __name__ == "__main__":
    main()
