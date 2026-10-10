from pathlib import Path

import pytest
from pydantic import ValidationError
from run_api.models import Policy, RunRequest

POLICY = Policy.load(str(Path("infra/airflow/policy.json")))


def payload() -> dict[str, object]:
    return {
        "project": POLICY.project,
        "workflow": POLICY.workflow,
        "task": POLICY.task,
        "base_ref": POLICY.approved_sha,
        "model_settings": {"model": POLICY.model},
    }


def test_approved_request() -> None:
    assert POLICY.permits(RunRequest.model_validate(payload()))


@pytest.mark.parametrize("field", ["project", "workflow", "task", "base_ref", "model_settings"])
def test_policy_rejects_overrides(field: str) -> None:
    request = payload()
    request[field] = (
        {"model": "unapproved/model"}
        if field == "model_settings"
        else "0" * 40
        if field == "base_ref"
        else "unapproved"
    )
    assert not POLICY.permits(RunRequest.model_validate(request))


@pytest.mark.parametrize("field", ["command", "prompt", "script", "policy", "dag_id"])
def test_extra_fields_forbidden(field: str) -> None:
    with pytest.raises(ValidationError):
        RunRequest.model_validate({**payload(), field: "untrusted"})


def test_nested_extra_forbidden() -> None:
    request = {**payload(), "model_settings": {"model": POLICY.model, "command": "untrusted"}}
    with pytest.raises(ValidationError):
        RunRequest.model_validate(request)


@pytest.mark.parametrize("value", ["main", "../main", "a" * 39, 123, None])
def test_base_ref_requires_full_sha(value: object) -> None:
    with pytest.raises(ValidationError):
        RunRequest.model_validate({**payload(), "base_ref": value})
