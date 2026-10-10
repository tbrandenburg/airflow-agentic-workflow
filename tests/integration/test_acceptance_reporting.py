"""Failure-first reconciliation and partial CAS publication against real MinIO."""

from pathlib import Path
from uuid import uuid4

from airflow_core.artifacts import ArtifactStore, canonical, digest, run_key
from airflow_core.contracts import RunReport
from airflow_core.policy import Policy
from airflow_core.reporting import publish, reconcile


def test_first_step_failure_retains_request_without_attempt(store: ArtifactStore) -> None:
    policy = Policy.load(Path("projects/example-service/policy.py"), dag="code_review")
    run = "proof_" + uuid4().hex
    store.put(
        f"{run_key(policy.dag_id, run)}/run-input.json",
        canonical({"task": "persisted before any attempt"}),
        "application/json",
    )
    states: dict[str, str | None] = {task: "upstream_failed" for task in policy.tasks}
    states["prepare"] = "failed"
    report = reconcile(store, policy, policy.dag_id, run, states)
    assert report.outcome == "execution_failed"
    assert report.task_request == "persisted before any attempt" and report.failed_step == "prepare"
    assert report.patch is None and report.validation_artifact is None
    states["prepare"] = "success"
    states["implement"] = "failed"
    assert reconcile(store, policy, policy.dag_id, run, states).failed_step == "implement"
    marker = publish(store, report, digest(run.encode()))
    assert RunReport.model_validate_json(store.get(marker.reports["run-report.json"])) == report


def test_malformed_attempt_and_partial_publication_recover(store: ArtifactStore) -> None:
    policy = Policy.load(Path("projects/example-service/policy.py"), dag="code_review")
    run = "proof_" + uuid4().hex
    root = run_key(policy.dag_id, run)
    store.put(f"{root}/run-input.json", canonical({"task": "malformed proof"}), "application/json")
    store.put(f"{root}/steps/prepare/attempts/1/attempt-result.json", b"{", "application/json")
    states: dict[str, str | None] = {task: "upstream_failed" for task in policy.tasks}
    states["prepare"] = "failed"
    report = reconcile(store, policy, policy.dag_id, run, states)
    assert report.errors and report.outcome == "execution_failed"
    data = canonical(report)
    prefix = f"{root}/publications/{digest(data)}"
    partial = store.put(f"{prefix}/run-report.json", data, "application/json")
    assert store.optional(f"{root}/published.json") is None
    marker = publish(store, report, digest(run.encode()))
    assert marker.reports["run-report.json"] == partial
    assert len(marker.reports) == 3
    assert publish(store, report, digest(run.encode())) == marker
    for ref in marker.reports.values():
        store.get(ref)
