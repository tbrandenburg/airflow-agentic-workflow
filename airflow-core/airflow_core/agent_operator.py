"""Generic AgentRunOperator with explicit storage-only upstream references."""

from typing import Unpack

from airflow.sdk import BaseOperator, Context
from airflow.sdk.exceptions import AirflowFailException
from pydantic import ValidationError

from airflow_core.agent_policy import AgentPolicy
from airflow_core.agent_process import MODEL, AgentProcess
from airflow_core.artifacts import ArtifactStore, ContractError, run_key
from airflow_core.execution import execute_script
from airflow_core.operators import OperatorOptions, request
from airflow_core.policy import Policy
from airflow_core.task_logging import preflight_failure
from airflow_core.upstream import resolve


class AgentRunOperator(BaseOperator):
    def __init__(
        self,
        *,
        task_id: str,
        project: str,
        prompt_id: str,
        model_id: str,
        upstream_refs: dict[str, tuple[str, str]],
        **kwargs: Unpack[OperatorOptions],
    ) -> None:
        super().__init__(task_id=task_id, **kwargs)
        self.project, self.prompt_id, self.model_id = project, prompt_id, model_id
        self.upstream_refs = upstream_refs
        self.process: AgentProcess | None = None

    def execute(self, context: Context) -> dict[str, object]:
        ti = context["ti"]
        store = ArtifactStore.deployment()
        try:
            policy = Policy.load(dag=ti.dag_id)
            if not isinstance(policy, AgentPolicy):
                raise ContractError("Agent policy required")
            registration = policy.agents.get(self.task_id)
            if (
                registration is None
                or self.project != policy.project
                or self.prompt_id != registration.prompt_id
                or self.model_id != MODEL
                or set(self.upstream_refs) != set(policy.inputs[self.task_id])
                or not {task for task, _ in self.upstream_refs.values()} <= self.upstream_task_ids
            ):
                raise ContractError("Agent operator configuration rejected")
            inputs = resolve(store, ti.dag_id, ti.run_id, self.upstream_refs)
            self.process = AgentProcess(registration.role, self.model_id, registration.fixture)
            result = execute_script(
                store,
                policy,
                request(context),
                ti.dag_id,
                ti.run_id,
                self.task_id,
                ti.try_number,
                inputs,
                self.process,
            )
            return {
                "completed_uri": f"s3://{store.bucket}/{run_key(ti.dag_id, ti.run_id)}"
                f"/steps/{self.task_id}/completed.json",
                "outcome": result.outcome,
                "attempt": result.attempt,
                "input_fingerprint": result.input_fingerprint,
            }
        except (ContractError, ValidationError) as error:
            preflight_failure(store, ti.dag_id, ti.run_id, self.task_id, ti.try_number, error)
            raise AirflowFailException("Agent policy or runtime response rejected") from error

    def on_kill(self) -> None:
        if self.process is not None:
            self.process.cancel()
