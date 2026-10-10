"""Deployment-owned script/prompt/source registry. Caller input cannot select executable paths."""

import importlib.util
from pathlib import Path

from airflow_core.agent_policy import AgentPolicy, AgentRegistration
from airflow_core.agent_process import Role
from airflow_core.contracts import ExpectedOutput

POLICY_VERSION = "2.0.0"
PROJECT = "example-service"
WORKFLOW = "script-smoke"
DAG_ID = "artifact_script_smoke"
BASE_SHA = "64b998b135f36de249d9708a2307d423e935c2e8"
MIRROR = Path("/mirrors/example-service.git")
SOURCE_PATHS = ["service.py"]
PROMPTS: dict[str, Path] = {}
SCRIPTS = {
    "prepare": Path("/deployment/scripts/prepare.sh"),
    "inventory": Path("/deployment/scripts/inventory.sh"),
}
OUTPUTS = {
    "prepare": [
        ExpectedOutput(path="source.tar", content_type="application/x-tar", max_bytes=1048576)
    ],
    "inventory": [
        ExpectedOutput(path="inventory.json", content_type="application/json", max_bytes=65536)
    ],
}
TIMEOUT = 30
INPUTS = {"prepare": [], "inventory": ["source.tar"]}


def agent_policy(dag: str, fixture: str | None) -> AgentPolicy:
    roles: dict[str, Role] = {"planner": "planner"}
    if fixture == "success":
        roles.update({"coder": "coder", "reviewer": "reviewer"})
    if fixture == "missing-patch":
        roles = {"coder": "coder"}
    outputs = {
        task: [
            ExpectedOutput(path="response.json", content_type="application/json", max_bytes=524288)
        ]
        + (
            [ExpectedOutput(path="patch.diff", content_type="text/plain", max_bytes=262144)]
            if role == "coder"
            else []
        )
        for task, role in roles.items()
    }
    return AgentPolicy(
        version="3.1.0",
        project=PROJECT,
        workflow=dag,
        dag_id=dag,
        base_sha=BASE_SHA,
        mirror=MIRROR,
        sources=SOURCE_PATHS,
        scripts={task: Path(f"/deployment/prompts/{role}.txt") for task, role in roles.items()},
        outputs=outputs,
        timeout=3 if fixture == "timeout" else 120,
        registry_digest="0" * 64,
        inputs={task: [] for task in roles},
        agents={task: AgentRegistration(role, role, fixture) for task, role in roles.items()},
    )


AGENT_POLICIES = {"agent_smoke": agent_policy("agent_smoke", None)}
for mode in (
    "success",
    "transient",
    "permanent",
    "timeout",
    "malformed",
    "schema-invalid",
    "missing-patch",
    "oversized",
):
    name = "agent_fixture_" + mode.replace("-", "_")
    AGENT_POLICIES[name] = agent_policy(name, mode)

# Loaded by file location: target source is never imported as deployment code.
spec = importlib.util.spec_from_file_location(
    "workflow_registry", Path(__file__).with_name("workflow_policy.py")
)
if spec is None or spec.loader is None:
    raise RuntimeError("Workflow registry unavailable")
registry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(registry)
AGENT_POLICIES.update(registry.WORKFLOWS)

spec = importlib.util.spec_from_file_location(
    "operational_registry", Path(__file__).with_name("operational_policy.py")
)
if spec is None or spec.loader is None:
    raise RuntimeError("Operational registry unavailable")
operational = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operational)
AGENT_POLICIES["operational_proof"] = operational.register(
    agent_policy("operational_proof", "slow")
)
