"""Generate or verify the checked-in versioned JSON schema contracts."""

import argparse
import json
from pathlib import Path

from airflow_core.contracts import AgentOutput, InputManifest, RunReport, TaskResult
from airflow_core.roles import RESPONSE
from airflow_core.validation import ValidationReport
from pydantic import TypeAdapter


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    write = parser.parse_args().write
    root = Path("packages/agent-contracts/schemas/v1")
    for name, model in (
        ("input-manifest", InputManifest),
        ("task-result", TaskResult),
        ("agent-output", AgentOutput),
        ("run-report", RunReport),
        ("role-response", RESPONSE),
        ("validation-report", ValidationReport),
    ):
        schema = (
            model.json_schema() if isinstance(model, TypeAdapter) else model.model_json_schema()
        )
        schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        schema["$id"] = f"urn:local-airflow-poc:{name}:1.0"
        content = json.dumps(schema, indent=2, sort_keys=True) + "\n"
        path = root / f"{name}.schema.json"
        if write:
            root.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        elif path.read_text() != content:
            raise RuntimeError(f"Schema drift: {path}; run make schemas")
        print(f"Verified {name} schema 1.0")


if __name__ == "__main__":
    main()
