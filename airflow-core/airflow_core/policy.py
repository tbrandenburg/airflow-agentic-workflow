"""Load the fixed deployment registry without importing target source code."""

import importlib.util
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import cast

from airflow_core.artifacts import ContractError
from airflow_core.contracts import ExpectedOutput
from airflow_core.registry import registry_digest


@dataclass(frozen=True)
class Policy:
    version: str
    project: str
    workflow: str
    dag_id: str
    base_sha: str
    mirror: Path
    sources: list[str]
    scripts: dict[str, Path]
    outputs: dict[str, list[ExpectedOutput]]
    timeout: int
    registry_digest: str
    inputs: dict[str, list[str]]

    @property
    def tasks(self) -> list[str]:
        return list(self.scripts)

    @property
    def independent_base(self) -> bool:
        return False

    def optional_inputs(self, task: str) -> set[str]:
        return set()

    def script_id(self, task: str) -> str:
        return task

    @classmethod
    def load(
        cls, path: Path = Path("/deployment/project_policy.py"), dag: str | None = None
    ) -> "Policy":
        spec = importlib.util.spec_from_file_location("deployment_policy", path)
        if spec is None or spec.loader is None:
            raise ContractError("Deployment policy unavailable")
        module: ModuleType = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if dag is not None and dag != module.DAG_ID:
            registered = cast(dict[str, Policy], module.AGENT_POLICIES)
            if dag not in registered:
                raise ContractError("Workflow not registered")
            return replace(registered[dag], registry_digest=registry_digest(path))
        return cls(
            version=cast(str, module.POLICY_VERSION),
            project=cast(str, module.PROJECT),
            workflow=cast(str, module.WORKFLOW),
            dag_id=cast(str, module.DAG_ID),
            base_sha=cast(str, module.BASE_SHA),
            mirror=cast(Path, module.MIRROR),
            sources=cast(list[str], module.SOURCE_PATHS),
            scripts=cast(dict[str, Path], module.SCRIPTS),
            outputs=cast(dict[str, list[ExpectedOutput]], module.OUTPUTS),
            timeout=cast(int, module.TIMEOUT),
            registry_digest=registry_digest(path),
            inputs=cast(dict[str, list[str]], module.INPUTS),
        )

    def approve(self, request: dict[str, object], dag_id: str, task_id: str) -> Path:
        if (
            request.get("project") != self.project
            or request.get("workflow") != self.workflow
            or request.get("task") != "artifact-smoke"
            or request.get("base_ref") != self.base_sha
            or dag_id != self.dag_id
            or task_id not in self.scripts
            or set(request) != {"project", "workflow", "task", "base_ref", "model_settings"}
            or request.get("model_settings") != {"model": "github-copilot/gpt-6.1-sol"}
        ):
            raise ContractError("Run or script not approved by fixed deployment policy")
        script = self.scripts[task_id]
        if script.is_symlink() or not script.is_file():
            raise ContractError("Registered deployment script missing or symlinked")
        return script
