"""Deployment-owned P4 scripts; source is data except bounded validation commands."""

import json
import sys
from pathlib import Path

from airflow_core.artifacts import ContractError, canonical, digest
from airflow_core.contracts import AgentOutput, InputManifest, OutputFile
from airflow_core.patches import apply_patch, approved_path
from airflow_core.roles import PlannerResponse, ReviewerResponse
from airflow_core.validation import ValidationReport
from airflow_core.workspace import read_file
from checks import validate

ROOT = Path.cwd()
SOURCES = ["service.py"]


def read(name: str) -> bytes:
    return read_file(ROOT, f"inputs/{name}", 524288)


def run(action: str, manifest: InputManifest) -> dict[str, dict[str, object]]:
    source = ROOT / "source"
    if action == "workflow_prepare":
        return {
            "metadata.json": {
                "base_sha": manifest.base_sha,
                "policy_version": manifest.policy_version,
                "task_request": manifest.task_request,
                "inventory": [
                    {
                        "path": path,
                        "size": len(read_file(source, path)),
                        "sha256": digest(read_file(source, path)),
                    }
                    for path in SOURCES
                ],
                "validation": "formatter, lint, health test expecting healthy, secret scan",
            }
        }
    if action == "collect_context":
        plan = PlannerResponse.model_validate_json(read("plan.json"))
        if not plan.suggested_paths or len(plan.suggested_paths) > 10:
            raise ContractError("Planner context path count rejected")
        paths = [approved_path(path, SOURCES) for path in plan.suggested_paths]
        if len(set(paths)) != len(paths):
            raise ContractError("Duplicate context paths")
        files = {path: read_file(source, path, 65536).decode() for path in paths}
        if sum(len(value.encode()) for value in files.values()) > 262144:
            raise ContractError("Context size exceeded")
        return {
            "context.json": {
                "base_sha": manifest.base_sha,
                "approved_paths": paths,
                "files": files,
                "plan": plan.model_dump(mode="json"),
            }
        }
    if action == "apply_patch":
        context = json.loads(read("context.json"))
        if context["base_sha"] != manifest.base_sha:
            raise ContractError("Context base mismatch")
        patch = read("patch.diff")
        paths = apply_patch(source, patch, context["approved_paths"])
        return {
            "patch-summary.json": {
                "base_sha": manifest.base_sha,
                "patch_sha256": digest(patch),
                "files": paths,
                "size": len(patch),
                "added_lines": sum(
                    line.startswith(b"+") and not line.startswith(b"+++")
                    for line in patch.splitlines()
                ),
                "deleted_lines": sum(
                    line.startswith(b"-") and not line.startswith(b"---")
                    for line in patch.splitlines()
                ),
            }
        }
    if action in ("validate", "validate_fail"):
        patch = read("patch.diff")
        summary = json.loads(read("patch-summary.json"))
        if summary["base_sha"] != manifest.base_sha or summary["patch_sha256"] != digest(patch):
            raise ContractError("Applied patch identity mismatch")
        apply_patch(source, patch, SOURCES)
        report = validate(source, patch, manifest.base_sha, action == "validate_fail")
        return {"validation.json": report.model_dump(mode="json")}
    if action == "package":
        patch = read("patch.diff")
        summary = json.loads(read("patch-summary.json"))
        gate = ValidationReport.model_validate_json(read("validation.json"))
        if (gate.base_sha, gate.patch_sha256) != (manifest.base_sha, digest(patch)):
            raise ContractError("Package gate identity mismatch")
        apply_patch(source, patch, SOURCES)
        review = (
            ReviewerResponse.model_validate_json(read("review.json")).model_dump(mode="json")
            if "review.json" in manifest.input_artifacts
            else None
        )
        if (gate.gate_status == "passed") != (review is not None):
            raise ContractError("Review presence differs from validation branch")
        return {
            "package.json": {
                "base_sha": manifest.base_sha,
                "patch_summary": summary,
                "validation": gate.model_dump(mode="json"),
                "review": review,
                "skipped_reason": "validation_failed" if review is None else None,
                "artifacts": {
                    name: ref.model_dump(mode="json")
                    for name, ref in manifest.input_artifacts.items()
                },
            }
        }
    raise ContractError("Unknown registered script")


def main() -> None:
    action = sys.argv[1]
    if sys.argv[2] != "--input-manifest" or sys.argv[4] != "--output-manifest":
        raise ContractError("Script invocation rejected")
    manifest = InputManifest.model_validate_json(Path(sys.argv[3]).read_bytes())
    results = run(action, manifest)
    files = []
    for expected in manifest.expected_outputs:
        data = canonical(results[expected.path])
        (ROOT / expected.path).write_bytes(data)
        files.append(OutputFile(**expected.model_dump(), size=len(data), sha256=digest(data)))
    output = AgentOutput(
        dag_id=manifest.dag_id,
        dag_run_id=manifest.dag_run_id,
        task_id=manifest.task_id,
        attempt=manifest.attempt,
        base_sha=manifest.base_sha,
        outputs=files,
        summary=f"{action} completed",
    )
    Path(sys.argv[5]).write_bytes(canonical(output))
    print(f"P4_{action.upper()}_OK")


if __name__ == "__main__":
    main()
