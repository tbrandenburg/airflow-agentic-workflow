"""Slow validation pool probe and artifact-only cross-worker reconstruction."""

import json
import sys
import time
from pathlib import Path

from airflow_core.artifacts import canonical, digest
from airflow_core.contracts import AgentOutput, InputManifest, OutputFile


def main() -> None:
    if sys.argv[1] != "--input-manifest" or sys.argv[3] != "--output-manifest":
        raise ValueError("Fixed invocation required")
    manifest = InputManifest.model_validate_json(Path(sys.argv[2]).read_bytes())
    source = Path("source/service.py").read_bytes()
    inputs = {
        name: json.loads(Path(f"inputs/{name}").read_bytes()) for name in manifest.input_artifacts
    }
    if manifest.task_id.startswith("validation_"):
        time.sleep(12)
    data = canonical(
        {"base_sha": manifest.base_sha, "source_sha256": digest(source), "inputs": inputs}
    )
    Path("response.json").write_bytes(data)
    output = AgentOutput(
        dag_id=manifest.dag_id,
        dag_run_id=manifest.dag_run_id,
        task_id=manifest.task_id,
        attempt=manifest.attempt,
        base_sha=manifest.base_sha,
        outputs=[
            OutputFile(
                **manifest.expected_outputs[0].model_dump(), size=len(data), sha256=digest(data)
            )
        ],
        summary="Operational probe completed",
    )
    Path(sys.argv[4]).write_bytes(canonical(output))


if __name__ == "__main__":
    main()
