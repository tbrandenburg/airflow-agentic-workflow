from pathlib import Path

import pytest
from airflow_core.artifacts import ContractError, canonical, digest
from airflow_core.contracts import AgentOutput, ExpectedOutput, InputManifest, OutputFile
from airflow_core.execution import validate_outputs
from pydantic import ValidationError


def manifest(root: Path) -> InputManifest:
    return InputManifest(
        dag_id="dag",
        dag_run_id="run",
        task_id="task",
        attempt=1,
        project="project",
        workflow="workflow",
        base_sha="a" * 40,
        task_request="request",
        policy_version="2",
        runtime_version="2",
        input_artifacts={},
        expected_outputs=[
            ExpectedOutput(path="data.json", content_type="application/json", max_bytes=10)
        ],
        workspace_path=str(root),
        timeout_seconds=5,
    )


@pytest.mark.parametrize(
    "change", ["identity", "size", "checksum", "type", "path", "missing", "extra"]
)
def test_output_contract_rejection(tmp_path: Path, change: str) -> None:
    (tmp_path / "data.json").write_bytes(b"{}")
    output = AgentOutput(
        dag_id="dag",
        dag_run_id="run",
        task_id="task",
        attempt=1,
        base_sha="a" * 40,
        outputs=[
            OutputFile(
                path="data.json",
                content_type="application/json",
                max_bytes=10,
                size=2,
                sha256=digest(b"{}"),
            )
        ],
        summary="output",
    )
    data = output.model_dump()
    updates: dict[str, object] = {
        "size": 3,
        "checksum": "0" * 64,
        "type": "text/plain",
        "path": "../escape",
    }
    if change == "identity":
        data["dag_run_id"] = "other"
    elif change == "missing":
        (tmp_path / "data.json").unlink()
    elif change == "extra":
        data["command"] = "bad"
    else:
        field = {"checksum": "sha256", "type": "content_type"}.get(change, change)
        output = output.model_copy(
            update={"outputs": [output.outputs[0].model_copy(update={field: updates[change]})]}
        )
        data = output.model_dump()
    with pytest.raises((ContractError, ValidationError)):
        validate_outputs(manifest(tmp_path), tmp_path, canonical(data))
