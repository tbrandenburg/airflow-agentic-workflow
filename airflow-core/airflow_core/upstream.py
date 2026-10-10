"""Resolve declared upstream outputs through verified immutable completions."""

from airflow_core.artifacts import ArtifactStore, ContractError, run_key
from airflow_core.completion import reuse
from airflow_core.contracts import ArtifactRef, Completion


def resolve(
    store: ArtifactStore,
    dag: str,
    run: str,
    refs: dict[str, tuple[str, str]],
    optional: set[str] | None = None,
) -> dict[str, ArtifactRef]:
    inputs: dict[str, ArtifactRef] = {}
    for name, (task, output) in refs.items():
        prefix = f"{run_key(dag, run)}/steps/{task}"
        marker = store.optional(f"{prefix}/completed.json")
        if marker is None:
            if optional and name in optional:
                continue
            raise ContractError("Upstream completion missing")
        completion = Completion.model_validate_json(marker[1])
        result = reuse(store, prefix, completion.input_fingerprint)
        if result is None or output not in result.outputs:
            raise ContractError("Upstream output missing")
        if (result.dag_id, result.dag_run_id, result.task_id) != (dag, run, task):
            raise ContractError("Upstream identity mismatch")
        inputs[name] = result.outputs[output]
    return inputs
