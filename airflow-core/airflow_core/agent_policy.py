"""Deployment-controlled agent registrations. Faults never come from API input."""

from dataclasses import dataclass
from pathlib import Path

from airflow_core.agent_process import MODEL, Role
from airflow_core.artifacts import ContractError
from airflow_core.policy import Policy


@dataclass(frozen=True)
class AgentRegistration:
    prompt_id: str
    role: Role
    fixture: str | None = None


@dataclass(frozen=True)
class AgentPolicy(Policy):
    agents: dict[str, AgentRegistration]

    def approve(self, request: dict[str, object], dag_id: str, task_id: str) -> Path:
        if (
            request
            != {
                "project": self.project,
                "workflow": self.workflow,
                "task": "agent-smoke",
                "base_ref": self.base_sha,
                "model_settings": {"model": MODEL},
            }
            or dag_id != self.dag_id
            or task_id not in self.scripts
        ):
            raise ContractError("Agent request not registered")
        prompt = self.scripts[task_id]
        if prompt.is_symlink() or not prompt.is_file():
            raise ContractError("Registered prompt missing")
        return prompt
