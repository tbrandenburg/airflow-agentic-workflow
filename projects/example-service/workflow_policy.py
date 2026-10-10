"""Fixed P4 executable, artifact and role mappings."""

from pathlib import Path

from airflow_core.agent_policy import AgentRegistration
from airflow_core.agent_process import Role
from airflow_core.contracts import ExpectedOutput
from airflow_core.workflow_policy import WorkflowPolicy


def output(path: str) -> ExpectedOutput:
    return ExpectedOutput(
        path=path,
        content_type="text/plain" if path.endswith(".diff") else "application/json",
        max_bytes=524288,
    )


def register(dag: str, failing: bool = False, fault: str | None = None) -> WorkflowPolicy:
    roles: dict[str, Role] = {"plan": "planner", "implement": "coder", "review": "reviewer"}
    ids = {
        "prepare": "workflow_prepare",
        "collect_context": "collect_context",
        "apply_patch": "apply_patch",
        "validate": "validate_fail" if failing else "validate",
        "package": "package",
    }
    if fault in ("context_failure", "package_failure"):
        ids["collect_context" if fault == "context_failure" else "package"] = "acceptance_failure"
    scripts = {task: Path(f"/deployment/scripts/{name}.sh") for task, name in ids.items()}
    scripts.update({task: Path(f"/deployment/prompts/{role}.txt") for task, role in roles.items()})
    outputs = {
        "prepare": [output("metadata.json")],
        "plan": [output("response.json")],
        "collect_context": [output("context.json")],
        "implement": [output("response.json"), output("patch.diff")],
        "apply_patch": [output("patch-summary.json")],
        "validate": [output("validation.json")],
        "review": [output("response.json")],
        "package": [output("package.json")],
    }
    inputs = {
        "prepare": [],
        "plan": ["metadata.json"],
        "collect_context": ["plan.json"],
        "implement": ["plan.json", "context.json"],
        "apply_patch": ["patch.diff", "context.json"],
        "validate": ["patch.diff", "patch-summary.json"],
        "review": ["patch.diff", "context.json", "validation.json"],
        "package": ["patch.diff", "patch-summary.json", "validation.json", "review.json"],
    }
    return WorkflowPolicy(
        version="4.0.0",
        project="example-service",
        workflow="code-review",
        dag_id=dag,
        base_sha="64b998b135f36de249d9708a2307d423e935c2e8",
        mirror=Path("/mirrors/example-service.git"),
        sources=["service.py"],
        scripts=scripts,
        outputs=outputs,
        timeout=3 if fault == "timeout" else 120,
        registry_digest="0" * 64,
        inputs=inputs,
        agents={
            task: AgentRegistration(
                role,
                role,
                {"malformed": "malformed", "unsafe_patch": "unsafe-patch"}.get(fault, fault)
                if task == "implement"
                and fault in ("malformed", "unsafe_patch", "transient", "timeout")
                else "success"
                if failing or fault
                else None,
            )
            for task, role in roles.items()
        },
        script_ids=ids,
    )


WORKFLOWS = {
    "code_review": register("code_review"),
    "code_review_validation_fail": register("code_review_validation_fail", True),
}
for fault in (
    "transient",
    "malformed",
    "context_failure",
    "timeout",
    "unsafe_patch",
    "package_failure",
):
    name = "code_review_" + fault
    WORKFLOWS[name] = register(name, fault=fault)
