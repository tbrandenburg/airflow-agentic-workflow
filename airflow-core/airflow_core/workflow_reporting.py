"""Reconcile P4 trusted gate and branch evidence into the existing CAS report."""

from datetime import datetime
from typing import cast

from pydantic import JsonValue, ValidationError

from airflow_core.artifacts import ArtifactStore, ContractError, run_key
from airflow_core.contracts import RunReport, TaskEvidence
from airflow_core.roles import ReviewerResponse
from airflow_core.upstream import resolve
from airflow_core.validation import ValidationReport
from airflow_core.workflow_policy import WorkflowPolicy


def enrich(store: ArtifactStore, policy: WorkflowPolicy, report: RunReport) -> RunReport:
    dag, run = report.dag_id, report.dag_run_id
    errors = list(report.errors)
    validation: dict[str, JsonValue] | None = None
    review: dict[str, JsonValue] | None = None
    summary: dict[str, JsonValue] | None = None
    patch = None
    validation_artifact = None
    gate = None
    try:
        refs = resolve(store, dag, run, {"patch": ("implement", "patch.diff")}, {"patch"})
        patch = refs.get("patch")
        refs = resolve(store, dag, run, {"gate": ("validate", "validation.json")}, {"gate"})
        if "gate" in refs:
            gate = ValidationReport.model_validate_json(store.get(refs["gate"]))
            if patch is None or (gate.base_sha, gate.patch_sha256) != (
                policy.base_sha,
                patch.sha256,
            ):
                raise ContractError("Validation patch identity mismatch")
            validation = gate.model_dump(mode="json")
            validation_artifact = refs["gate"]
        refs = resolve(store, dag, run, {"review": ("review", "response.json")}, {"review"})
        if "review" in refs:
            review = ReviewerResponse.model_validate_json(store.get(refs["review"])).model_dump(
                mode="json"
            )
        refs = resolve(
            store, dag, run, {"summary": ("apply_patch", "patch-summary.json")}, {"summary"}
        )
        if "summary" in refs:
            import json

            summary = cast(dict[str, JsonValue], json.loads(store.get(refs["summary"])))
    except (ContractError, ValidationError, ValueError) as error:
        errors.append(f"Invalid workflow evidence: {type(error).__name__}")
    expected_skip = "skip_review" if gate and gate.gate_status == "passed" else "review"
    tasks = []
    for task in report.tasks:
        control = task.task_id in policy.controls
        skipped = task.task_id == expected_skip and task.state == "skipped" and gate is not None
        if control or skipped:
            task = task.model_copy(
                update={
                    "missing_evidence": [],
                    "skipped_reason": (
                        "validation_passed"
                        if expected_skip == "skip_review"
                        else "validation_failed"
                    )
                    if skipped
                    else task.skipped_reason,
                }
            )
        tasks.append(task)
    missing = [f"{task.task_id}: {item}" for task in tasks for item in task.missing_evidence]
    unsuccessful = [
        task
        for task in tasks
        if not (
            task.state == "success"
            or task.task_id == expected_skip
            and task.state == "skipped"
            and gate
        )
    ]
    if gate is None:
        missing.append("trustworthy validation gate")
    execution_failed = bool(
        errors or any(task.state in ("failed", "upstream_failed") for task in tasks)
    )
    outcome = (
        "execution_failed"
        if execution_failed
        else "cancelled"
        if any(attempt.outcome == "cancelled" for task in tasks for attempt in task.attempts[-1:])
        else "incomplete"
        if missing or unsuccessful
        else "validation_failed"
        if gate and gate.gate_status == "failed"
        else "passed"
    )
    attempts = [attempt for task in tasks for attempt in task.attempts]
    failed = [task for task in tasks if task.state == "failed"]
    started = min((attempt.started_at for attempt in attempts), default=report.generated_at)
    ended = max((attempt.ended_at for attempt in attempts), default=report.generated_at)
    request = store.optional(f"{run_key(dag, run)}/run-input.json")
    text = attempts[0].task_request if attempts else None
    if request is None:
        errors.append("Persisted run request missing")
        outcome = "execution_failed"
    if request is not None:
        import json

        try:
            stored: object = json.loads(request[1])
        except ValueError:
            stored = None
        if isinstance(stored, dict) and isinstance(stored.get("task"), str):
            text = stored["task"]
        if (
            not isinstance(stored, dict)
            or not isinstance(stored.get("task"), str)
            or any(attempt.task_request != text for attempt in attempts)
        ):
            errors.append("Persisted request differs from task evidence")
            outcome = "execution_failed"
    return RunReport.model_validate(
        {
            **report.model_dump(),
            "tasks": tasks,
            "missing_evidence": missing,
            "errors": errors,
            "outcome": outcome,
            "patch": patch,
            "validation": validation,
            "validation_artifact": validation_artifact,
            "review": review,
            "patch_summary": summary,
            "request_id": run,
            "task_request": text,
            "failed_step": failed[0].task_id
            if failed
            else unsuccessful[0].task_id
            if unsuccessful
            else "validate"
            if outcome == "validation_failed"
            else None,
            "dag_url": f"http://127.0.0.1:18080/dags/{dag}/runs/{run}",
            "started_at": started,
            "ended_at": ended,
            "duration_seconds": (
                datetime.fromisoformat(ended) - datetime.fromisoformat(started)
            ).total_seconds(),
        }
    )


def acceptable(task: TaskEvidence, report: RunReport) -> bool:
    if report.workflow != "code-review":
        return False
    if task.task_id in ("validation_branch", "skip_review") and task.state == "success":
        return not task.errors
    return task.task_id == "skip_review" and task.state == "skipped" and report.outcome == "passed"
