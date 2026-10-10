from pathlib import Path

import pytest
from airflow_core.agent_policy import AgentPolicy
from airflow_core.artifacts import ContractError
from airflow_core.policy import Policy
from airflow_core.roles import RESPONSE
from pydantic import ValidationError


def test_registered_agent_policy_rejects_caller_fault_and_model() -> None:
    policy = Policy.load(Path("projects/example-service/policy.py"), dag="agent_smoke")
    assert isinstance(policy, AgentPolicy)
    assert policy.tasks == ["planner"]
    request: dict[str, object] = {
        "project": policy.project,
        "workflow": policy.workflow,
        "task": "agent-smoke",
        "base_ref": policy.base_sha,
        "model_settings": {"model": "github-copilot/gpt-6.1-sol"},
    }
    for extra in (
        {"fixture": "success"},
        {"model_settings": {"model": "unapproved"}},
        {"workflow": "agent_fixture_success"},
        {"prompt_id": "../host"},
    ):
        with pytest.raises(ContractError, match="not registered"):
            policy.approve({**request, **extra}, policy.dag_id, "planner")
    with pytest.raises(ContractError, match="not registered"):
        policy.approve(request, policy.dag_id, "unregistered")


def test_role_schema_rejects_missing_patch_and_unknown_fields() -> None:
    for data in (
        b'{"role":"coder","summary":"No patch"}',
        b'{"role":"planner","plan":"p","suggested_paths":[],"assumptions":[],"validation_focus":[],"extra":1}',
        b'{"role":"reviewer","findings":[],"recommendations":[],"verdict":"maybe"}',
    ):
        with pytest.raises(ValidationError):
            RESPONSE.validate_json(data)
