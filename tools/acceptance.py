"""Run twelve real-stack P6 scenarios; fixed faults are never gateway-selectable."""

import json
import subprocess
from typing import cast
from uuid import uuid4

import httpx
from acceptance_checks import verify
from acceptance_criteria import checklist
from acceptance_evidence import publish_bundle, record, run_evidence, snapshot
from acceptance_implementation import collect
from airflow_core.artifacts import ArtifactStore, canonical, run_key
from deploy import LOCAL
from p5_airflow import ProofAPI
from smoke_p3 import read_report
from smoke_p3 import request as operational_request
from smoke_p4 import payload

SCENARIOS = (
    ("S01", "real", "code_review", "success", "passed"),
    ("S02", "validation_fail", "code_review_validation_fail", "failed", "validation_failed"),
    ("S03", "transient", "code_review_transient", "success", "passed"),
    ("S04", "malformed", "code_review_malformed", "failed", "execution_failed"),
    ("S05", "context_failure", "code_review_context_failure", "failed", "execution_failed"),
    ("S09", "timeout", "code_review_timeout", "failed", "execution_failed"),
    ("S10", "unsafe_patch", "code_review_unsafe_patch", "failed", "execution_failed"),
    ("S12", "package_failure", "code_review_package_failure", "failed", "execution_failed"),
)


def main() -> None:
    store = ArtifactStore.deployment("http://127.0.0.1:19000", LOCAL / "secrets")
    api = ProofAPI()
    records: list[dict[str, object]] = []
    headers = {"Authorization": f"Bearer {(LOCAL / 'secrets/gateway_token').read_text().strip()}"}
    initial = snapshot()
    with httpx.Client(base_url="http://127.0.0.1:18081", headers=headers, timeout=20) as gateway:
        implementation = collect(gateway, payload())
        for identifier, mode, dag, state, outcome in SCENARIOS:
            before = snapshot()
            request = payload()
            if mode == "real":
                assert (
                    gateway.post("/runs", json={**request, "fixture": "malformed"}).status_code
                    == 422
                )
                assert (
                    gateway.post(
                        "/runs", json={**request, "workflow": "code_review_malformed"}
                    ).status_code
                    == 403
                )
                response = gateway.post("/runs", json=request)
                response.raise_for_status()
                run = str(response.json()["dag_run_id"])
            else:
                run = "p6_" + uuid4().hex
                store.put(
                    f"{run_key(dag, run)}/run-input.json", canonical(request), "application/json"
                )
                api.request(
                    "POST",
                    f"/dags/{dag}/dagRuns",
                    {"dag_run_id": run, "logical_date": None, "conf": request},
                )
            (LOCAL / "p6-active.json").write_bytes(
                canonical({"scenario": identifier, "dag_id": dag, "dag_run_id": run})
            )
            print(f"{identifier} {mode}: triggered {dag}/{run}", flush=True)
            observed_state = api.wait(dag, run, timeout=720)
            observed = run_evidence(store, api, dag, run)
            report = read_report(store, dag, run)
            assert observed_state == state and report.outcome == outcome, (
                identifier,
                observed,
                report.errors,
            )
            states = {str(task["task_id"]): task["state"] for task in api.tasks(dag, run)}
            verify(store, report, states, mode)
            subprocess.run(
                [
                    ".venv/bin/python",
                    "tools/provider_auth.py",
                    "verify",
                    "--run-root",
                    run_key(dag, run),
                ],
                check=True,
                timeout=120,
            )
            if mode == "real":
                response = gateway.get(f"/runs/{run}/artifacts")
                response.raise_for_status()
                assert response.json()["outcome"] == "passed" and response.json()["artifacts"]
            records.append(
                record(
                    identifier,
                    mode,
                    {
                        "endpoint": "/runs" if mode == "real" else f"/api/v2/dags/{dag}/dagRuns",
                        "body": request
                        if mode == "real"
                        else {
                            "dag_run_id": run,
                            "logical_date": None,
                            "conf": request,
                        },
                    },
                    {"state": state, "outcome": outcome},
                    observed,
                    before,
                )
            )
            (LOCAL / "acceptance-progress.json").write_bytes(canonical({"scenarios": records}))
            print(f"{identifier}: PASS {observed_state}/{report.outcome}", flush=True)
    before = snapshot()
    subprocess.run([".venv/bin/python", "tools/smoke_p5.py"], check=True, timeout=720)
    proof = json.loads((LOCAL / "p5-evidence.json").read_bytes())
    runs = [run_evidence(store, api, "operational_proof", run) for run in proof["runs"]]
    shared: dict[str, object] = {"runs": runs, "proof": proof}
    for identifier, fixture, expected in (
        ("S06", "fixed-worker-handoff", {"workers": 2, "artifact_reconstruction": True}),
        (
            "S08",
            "saturated-fixed-agents",
            {"model_peak": 2, "validation_peak": 1, "parallelism": 4},
        ),
        ("S11", "airflow-clear-completed", {"model_invoked": False, "output_keys_unchanged": True}),
    ):
        records.append(
            record(
                identifier,
                fixture,
                {
                    "command": "make smoke-p5",
                    "workflow": "operational_proof",
                    "runs": proof["runs"],
                    "conf": operational_request("operational_proof"),
                    "trigger": {"logical_date": None},
                    "clear": {
                        "dag_run_id": proof["runs"][0],
                        "task_ids": ["model_0"],
                        "dry_run": False,
                        "only_failed": False,
                        "only_running": False,
                        "reset_dag_runs": True,
                    }
                    if identifier == "S11"
                    else None,
                },
                cast(dict[str, object], expected),
                shared,
                before,
            )
        )
    records.append(
        record(
            "S07",
            "independent-full-mirror-inventory",
            {"command": "make acceptance", "source": "synthetic service.py"},
            {"file_count": 21, "refs_and_head_unchanged": True},
            {"runs": runs, "checked_scenarios": [item["id"] for item in records]},
            initial,
        )
    )
    records.sort(key=lambda item: str(item["id"]))
    publish_bundle(records, checklist(records, implementation), implementation)
    api.client.close()


if __name__ == "__main__":
    main()
