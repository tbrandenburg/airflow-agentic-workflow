"""Completion markers are committed only after independent verification of their evidence."""

from airflow_core.artifacts import ArtifactStore, ContractError, canonical, digest
from airflow_core.contracts import AgentOutput, ArtifactRef, Completion, InputManifest, TaskResult


def verify_result(store: ArtifactStore, result: TaskResult, fingerprint: str) -> None:
    if result.input_fingerprint != fingerprint or result.outcome != "passed":
        raise ContractError("Completion fingerprint or outcome mismatch")
    if not result.outputs or result.output_manifest is None:
        raise ContractError("Completion lacks verified output evidence")
    manifest = InputManifest.model_validate(
        {name: getattr(result, name) for name in InputManifest.model_fields}
    )
    if digest(canonical(manifest)) != result.input_digest:
        raise ContractError("Completion input digest mismatch")
    output_bytes = store.get(result.output_manifest)
    if digest(output_bytes) != result.output_digest:
        raise ContractError("Completion output digest mismatch")
    output = AgentOutput.model_validate_json(output_bytes)
    for field in ("dag_id", "dag_run_id", "task_id", "attempt", "base_sha"):
        if getattr(output, field) != getattr(result, field):
            raise ContractError("Completion output identity mismatch")
    if len(output.outputs) != len(result.outputs):
        raise ContractError("Completion output set mismatch")
    expected = {item.path: item for item in result.expected_outputs}
    if set(expected) != set(result.outputs):
        raise ContractError("Completion differs from expected output set")
    for item in output.outputs:
        ref = result.outputs.get(item.path)
        registered = expected[item.path]
        if item.content_type != registered.content_type or item.size > registered.max_bytes:
            raise ContractError("Completion expected output bounds mismatch")
        if ref is None or (ref.sha256, ref.size, ref.content_type) != (
            item.sha256,
            item.size,
            item.content_type,
        ):
            raise ContractError("Completion output reference mismatch")
    if output.input_digest != result.input_digest or output.output_digest != digest(
        canonical({name: ref.sha256 for name, ref in result.outputs.items()})
    ):
        raise ContractError("Completion enriched output digests mismatch")
    for ref in [*result.outputs.values(), *result.diagnostics.values(), result.output_manifest]:
        store.get(ref)


def reuse(store: ArtifactStore, prefix: str, fingerprint: str) -> TaskResult | None:
    existing = store.optional(f"{prefix}/completed.json")
    if existing is None:
        return None
    marker = Completion.model_validate_json(existing[1])
    if marker.input_fingerprint != fingerprint:
        raise ContractError("Completed task input fingerprint mismatch")
    result = TaskResult.model_validate_json(store.get(marker.result))
    verify_result(store, result, fingerprint)
    return result


def complete(
    store: ArtifactStore, prefix: str, fingerprint: str, result_ref: ArtifactRef
) -> TaskResult:
    result = TaskResult.model_validate_json(store.get(result_ref))
    verify_result(store, result, fingerprint)
    marker = Completion(input_fingerprint=fingerprint, result=result_ref)
    try:
        store.put(f"{prefix}/completed.json", canonical(marker), "application/json")
    except ContractError:
        # Another valid attempt may have won the CAS. Its verified result is authoritative.
        winner = reuse(store, prefix, fingerprint)
        if winner is None:
            raise
        return winner
    return result
