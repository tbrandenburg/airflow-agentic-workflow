"""Restricted run publication lookup; no caller-controlled bucket keys."""

import json

from airflow_core.artifacts import ArtifactStore, ContractError, run_key
from airflow_core.contracts import Publication, RunReport
from airflow_core.reporting import load_publication


def lookup(run: str) -> dict[str, object]:
    store = ArtifactStore.deployment()
    root = run_key("code_review", run)
    stored = store.optional(f"{root}/published.json")
    if stored is None:
        return {"reports": None}
    publication = Publication.model_validate_json(stored[1])
    marker = load_publication(store, root, publication.input_fingerprint)
    if marker is None:
        raise ContractError("Publication vanished")
    report = RunReport.model_validate_json(store.get(marker.reports["run-report.json"]))
    if (report.dag_id, report.dag_run_id) != ("code_review", run):
        raise ContractError("Report identity mismatch")
    artifacts: object = json.loads(store.get(marker.reports["artifact-index.json"]))
    expected = {name: ref.model_dump(mode="json") for name, ref in report.artifact_index.items()}
    if artifacts != expected:
        raise ContractError("Published artifact index differs from report")
    return {
        "reports": marker.model_dump(mode="json")["reports"],
        "report": report.model_dump(mode="json"),
        "outcome": report.outcome,
        "artifacts": expected,
    }
