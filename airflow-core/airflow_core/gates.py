"""Branch only on a verified completed validation artifact."""

from airflow.sdk import get_current_context
from airflow.sdk.exceptions import AirflowFailException
from pydantic import ValidationError

from airflow_core.artifacts import ArtifactStore, ContractError
from airflow_core.policy import Policy
from airflow_core.upstream import resolve
from airflow_core.validation import ValidationReport


def validation_branch() -> str:
    ti = get_current_context()["ti"]
    store = ArtifactStore.deployment()
    try:
        refs = resolve(store, ti.dag_id, ti.run_id, {"gate": ("validate", "validation.json")})
        report = ValidationReport.model_validate_json(store.get(refs["gate"]))
        patch = resolve(store, ti.dag_id, ti.run_id, {"patch": ("implement", "patch.diff")})
        if (report.base_sha, report.patch_sha256) != (
            Policy.load(dag=ti.dag_id).base_sha,
            patch["patch"].sha256,
        ):
            raise ContractError("Gate patch identity mismatch")
        return "review" if report.gate_status == "passed" else "skip_review"
    except (ContractError, ValidationError) as error:
        raise AirflowFailException("No trustworthy validation gate") from error
