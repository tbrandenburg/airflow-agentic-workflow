"""Fixed P5 fixtures: callers cannot select executables, modes or delays."""

from dataclasses import dataclass
from pathlib import Path

from airflow_core.agent_policy import AgentPolicy, AgentRegistration
from airflow_core.contracts import ExpectedOutput


@dataclass(frozen=True)
class OperationalPolicy(AgentPolicy):
    @property
    def independent_base(self) -> bool:
        return True


def register(base: AgentPolicy) -> AgentPolicy:
    models = [f"model_{index}" for index in range(4)]
    scripts = ["handoff", "validation_0", "validation_1"]
    tasks = [*models, *scripts]
    return OperationalPolicy(
        version="5.0.0",
        project=base.project,
        base_sha=base.base_sha,
        mirror=base.mirror,
        timeout=120,
        registry_digest=base.registry_digest,
        dag_id="operational_proof",
        workflow="operational_proof",
        sources=["service.py"],
        scripts={
            **{task: Path("/deployment/prompts/planner.txt") for task in models},
            **{task: Path("/deployment/scripts/operational.sh") for task in scripts},
        },
        outputs={
            task: [
                ExpectedOutput(
                    path="response.json", content_type="application/json", max_bytes=524288
                )
            ]
            for task in tasks
        },
        inputs={task: ["response.json"] if task == "handoff" else [] for task in tasks},
        agents={task: AgentRegistration("planner", "planner", "slow") for task in models},
    )
