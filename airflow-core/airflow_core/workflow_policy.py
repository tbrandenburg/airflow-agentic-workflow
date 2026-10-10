"""P4 deployment registry: independent base exports and explicit optional branch inputs."""

from dataclasses import dataclass
from pathlib import Path

from airflow_core.agent_policy import AgentPolicy
from airflow_core.agent_process import MODEL
from airflow_core.artifacts import ContractError


@dataclass(frozen=True)
class WorkflowPolicy(AgentPolicy):
    script_ids: dict[str, str]
    controls: tuple[str, ...] = ("validation_branch", "skip_review")

    @property
    def tasks(self) -> list[str]:
        return [*self.scripts, *self.controls]

    @property
    def independent_base(self) -> bool:
        return True

    def optional_inputs(self, task: str) -> set[str]:
        return {"review.json"} if task == "package" else set()

    def script_id(self, task: str) -> str:
        return self.script_ids[task]

    def approve(self, request: dict[str, object], dag_id: str, task_id: str) -> Path:
        task = request.get("task")
        if (
            request.get("project") != self.project
            or request.get("workflow") != self.workflow
            or not isinstance(task, str)
            or not 1 <= len(task.strip()) <= 500
            or "\x00" in task
            or request.get("base_ref") != self.base_sha
            or request.get("model_settings") != {"model": MODEL}
            or set(request) != {"project", "workflow", "task", "base_ref", "model_settings"}
            or dag_id != self.dag_id
            or task_id not in self.scripts
        ):
            raise ContractError("Workflow request not approved")
        script = self.scripts[task_id]
        if script.is_symlink() or not script.is_file():
            raise ContractError("Registered workflow executable missing")
        return script
