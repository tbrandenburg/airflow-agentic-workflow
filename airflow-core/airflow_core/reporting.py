"""Evidence reconciliation and transactional immutable report publication."""

from pydantic import ValidationError

from airflow_core import RUNTIME_VERSION
from airflow_core.artifacts import ArtifactStore, ContractError, canonical, digest, run_key
from airflow_core.completion import reuse
from airflow_core.contracts import (
    ArtifactRef,
    Completion,
    Outcome,
    Publication,
    RunReport,
    TaskEvidence,
    TaskResult,
)
from airflow_core.execution import now
from airflow_core.policy import Policy
from airflow_core.workflow_policy import WorkflowPolicy
from airflow_core.workflow_reporting import acceptable, enrich


def evidence(store: ArtifactStore, root: str, task: str, state: str | None) -> TaskEvidence:
    prefix = f"{root}/steps/{task}"
    attempts = []
    errors: list[str] = []
    missing: list[str] = []
    for key in store.keys(f"{prefix}/attempts/"):
        if key.endswith("/attempt-result.json"):
            try:
                attempt = TaskResult.model_validate_json(store.read(key)[1])
                if attempt.task_id != task or run_key(attempt.dag_id, attempt.dag_run_id) != root:
                    raise ContractError("Attempt identity mismatch")
                attempts.append(attempt)
            except (ContractError, ValidationError) as error:
                errors.append(f"Invalid attempt evidence: {type(error).__name__}")
    reference: ArtifactRef | None = None
    try:
        completed = store.optional(f"{prefix}/completed.json")
        if completed is not None:
            marker = Completion.model_validate_json(completed[1])
            result = reuse(store, prefix, marker.input_fingerprint)
            if (
                result is None
                or result.task_id != task
                or run_key(result.dag_id, result.dag_run_id) != root
            ):
                raise ContractError("Invalid completion identity")
            reference = completed[0]
    except (ContractError, ValidationError) as error:
        errors.append(f"Invalid completion evidence: {type(error).__name__}")
    if reference is None:
        missing.append("verified completion")
    if not attempts:
        missing.append("attempt diagnostics")
    return TaskEvidence(
        task_id=task,
        state=state,
        attempts=sorted(attempts, key=lambda item: item.attempt),
        completed=reference,
        missing_evidence=missing,
        skipped_reason=state if state in ("skipped", "upstream_failed", "removed") else None,
        errors=errors,
    )


def reconcile(
    store: ArtifactStore, policy: Policy, dag: str, run: str, states: dict[str, str | None]
) -> RunReport:
    root = run_key(dag, run)
    tasks = [evidence(store, root, task, states.get(task)) for task in policy.tasks]
    missing = [f"{task.task_id}: {item}" for task in tasks for item in task.missing_evidence]
    errors = [f"{task.task_id}: {item}" for task in tasks for item in task.errors]
    outcomes = [attempt.outcome for task in tasks for attempt in task.attempts[-1:]]
    outcome: Outcome = (
        "execution_failed"
        if errors or "validation_failed" in outcomes
        else "cancelled"
        if "cancelled" in outcomes
        else "execution_failed"
        if "execution_failed" in outcomes or "failed" in states.values()
        else "incomplete"
        if missing or any(task.state != "success" for task in tasks)
        else "passed"
    )
    index: dict[str, ArtifactRef] = {}
    for key in store.keys(f"{root}/steps/"):
        try:
            index[key] = store.read(key)[0]
        except (ContractError, ValidationError) as error:
            errors.append(f"Invalid artifact {key}: {type(error).__name__}")
    if errors:
        outcome = "execution_failed"
    report = RunReport(
        dag_id=dag,
        dag_run_id=run,
        project=policy.project,
        workflow=policy.workflow,
        base_sha=policy.base_sha,
        policy_version=policy.version,
        runtime_version=RUNTIME_VERSION,
        generated_at=now(),
        outcome=outcome,
        airflow_terminal_state=None,
        tasks=tasks,
        missing_evidence=missing,
        errors=errors,
        artifact_index=index,
        patch=None,
        validation=None,
        review=None,
    )
    return enrich(store, policy, report) if isinstance(policy, WorkflowPolicy) else report


def load_publication(store: ArtifactStore, root: str, identity: str) -> Publication | None:
    existing = store.optional(f"{root}/published.json")
    if existing is None:
        return None
    marker = Publication.model_validate_json(existing[1])
    if marker.input_fingerprint != identity or set(marker.reports) != {
        "run-report.json",
        "run-report.md",
        "artifact-index.json",
    }:
        raise ContractError("Publication fingerprint or report set mismatch")
    for ref in marker.reports.values():
        store.get(ref)
    return marker


def publish(store: ArtifactStore, report: RunReport, identity: str) -> Publication:
    root = run_key(report.dag_id, report.dag_run_id)
    previous = load_publication(store, root, identity)
    if previous is not None:
        return previous
    data = canonical(report)
    # Content-addressed bundles prevent partial competing publications mixing files.
    prefix = f"{root}/publications/{digest(data)}"
    markdown = f"# Run {report.dag_run_id}\n\nOutcome: **{report.outcome}**\n\n"
    markdown += "\n".join(
        f"- {task.task_id}: {task.state}; attempts={len(task.attempts)}; "
        f"missing={', '.join(task.missing_evidence) or 'none'}"
        for task in report.tasks
    )
    if report.workflow == "code-review":
        markdown += (
            f"\n\nRequest: {report.task_request}\n\nBase: `{report.base_sha}`\n"
            f"\nPolicy/runtime: {report.policy_version}/{report.runtime_version}\n"
            f"\nFailed step: {report.failed_step or 'none'}\n"
            f"\nPatch: {report.patch.uri if report.patch else 'unavailable'}\n"
            f"\nDAG: {report.dag_url}\n\nDuration: {report.duration_seconds} seconds\n"
            f"\nValidation: {report.validation}\n\nReview: {report.review}\n"
            f"\nErrors: {report.errors}\n"
        )
    index = canonical(
        {key: ref.model_dump(mode="json") for key, ref in report.artifact_index.items()}
    )
    refs = {
        "run-report.json": store.put(f"{prefix}/run-report.json", data, "application/json"),
        "run-report.md": store.put(f"{prefix}/run-report.md", markdown.encode(), "text/markdown"),
        "artifact-index.json": store.put(
            f"{prefix}/artifact-index.json", index, "application/json"
        ),
    }
    marker = Publication(input_fingerprint=identity, reports=refs)
    try:
        store.put(f"{root}/published.json", canonical(marker), "application/json")
    except ContractError:
        winner = load_publication(store, root, identity)
        if winner is None:
            raise
        return winner
    return marker


def assert_outcome(store: ArtifactStore, dag: str, run: str, identity: str) -> RunReport:
    marker = load_publication(store, run_key(dag, run), identity)
    if marker is None:
        raise ContractError("Run publication is missing")
    report = RunReport.model_validate_json(store.get(marker.reports["run-report.json"]))
    if (report.dag_id, report.dag_run_id) != (dag, run) or report.outcome != "passed":
        raise ContractError(f"Run outcome is {report.outcome}")
    if report.missing_evidence or report.errors:
        raise ContractError("Passed report contains missing or invalid evidence")
    if not report.tasks or any(
        not acceptable(task, report)
        and (
            task.state != "success"
            or task.completed is None
            or task.missing_evidence
            or task.errors
        )
        for task in report.tasks
    ):
        raise ContractError("Passed report contains unsuccessful task evidence")
    for task in report.tasks:
        if acceptable(task, report):
            continue
        assert task.completed is not None
        completion = Completion.model_validate_json(store.get(task.completed))
        result = reuse(
            store, f"{run_key(dag, run)}/steps/{task.task_id}", completion.input_fingerprint
        )
        if result is None:
            raise ContractError("Report completion missing")
    if report.workflow == "code-review":
        from airflow_core.validation import ValidationReport

        gate = ValidationReport.model_validate(report.validation)
        if (
            report.patch is None
            or gate.gate_status != "passed"
            or gate.patch_sha256 != report.patch.sha256
        ):
            raise ContractError("Final validation gate rejected")
    return report
