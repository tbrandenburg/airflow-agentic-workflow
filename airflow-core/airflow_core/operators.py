"""Airflow 3 adapters. Only compact references cross XCom; subprocesses receive no secrets."""

import json
from datetime import timedelta
from typing import TypedDict, Unpack, cast

from airflow.sdk import DAG, BaseOperator, Context
from airflow.sdk.exceptions import AirflowFailException
from pydantic import ValidationError

from airflow_core.artifacts import ArtifactStore, ContractError, canonical, digest, run_key
from airflow_core.completion import reuse
from airflow_core.contracts import Completion
from airflow_core.execution import execute_script
from airflow_core.policy import Policy
from airflow_core.process import ScriptProcess
from airflow_core.reporting import assert_outcome, publish, reconcile
from airflow_core.task_logging import preflight_failure
from airflow_core.upstream import resolve


def request(context: Context) -> dict[str, object]:
    ti = context["ti"]
    stored = ArtifactStore.deployment().read(f"{run_key(ti.dag_id, ti.run_id)}/run-input.json")[1]
    data: object = json.loads(stored)
    if not isinstance(data, dict) or data != context["dag_run"].conf:
        raise ContractError("Persisted run input differs from trigger metadata")
    return cast(dict[str, object], data)


def publication_identity(policy: Policy, dag: str, run: str) -> str:
    return digest(canonical({"dag": dag, "run": run, "policy": policy.registry_digest}))


class OperatorOptions(TypedDict, total=False):
    dag: DAG
    default_args: dict[str, object]
    retries: int
    retry_delay: timedelta
    execution_timeout: timedelta
    pool: str
    queue: str
    retry_exponential_backoff: bool
    max_retry_delay: timedelta
    trigger_rule: str


class ProjectScriptOperator(BaseOperator):
    def __init__(
        self,
        *,
        task_id: str,
        source_task: str | None = None,
        project: str | None = None,
        script_id: str | None = None,
        upstream_refs: dict[str, tuple[str, str]] | None = None,
        **kwargs: Unpack[OperatorOptions],
    ) -> None:
        super().__init__(task_id=task_id, **kwargs)
        self.source_task = source_task
        self.project, self.script_id = project, script_id
        self.upstream_refs = upstream_refs
        self.process: ScriptProcess | None = None

    def execute(self, context: Context) -> dict[str, object]:
        store = ArtifactStore.deployment()
        ti = context["ti"]
        dag, run = ti.dag_id, ti.run_id
        try:
            policy = Policy.load(dag=ti.dag_id)
            inputs = {}
            if self.upstream_refs is not None:
                if (
                    self.project != policy.project
                    or self.script_id != policy.script_id(self.task_id)
                    or set(self.upstream_refs) != set(policy.inputs[self.task_id])
                    or not {task for task, _ in self.upstream_refs.values()}
                    <= self.upstream_task_ids
                ):
                    raise ContractError("Script operator configuration rejected")
                inputs = resolve(
                    store, dag, run, self.upstream_refs, policy.optional_inputs(self.task_id)
                )
            if self.source_task is not None:
                prefix = f"{run_key(dag, run)}/steps/{self.source_task}"
                marker = store.optional(f"{prefix}/completed.json")
                if marker is None:
                    raise ContractError("Upstream completion missing")
                completion = Completion.model_validate_json(marker[1])
                upstream = reuse(store, prefix, completion.input_fingerprint)
                if upstream is None:
                    raise ContractError("Upstream result missing")
                inputs = upstream.outputs
            self.process = ScriptProcess()
            result = execute_script(
                store,
                policy,
                request(context),
                dag,
                run,
                self.task_id,
                ti.try_number,
                inputs,
                self.process,
            )
            return {
                "completed_uri": (
                    f"s3://{store.bucket}/{run_key(dag, run)}/steps/{self.task_id}/completed.json"
                ),
                "input_fingerprint": result.input_fingerprint,
                "outcome": result.outcome,
                "attempt": result.attempt,
            }
        except (ContractError, ValidationError) as error:
            preflight_failure(store, dag, run, self.task_id, ti.try_number, error)
            raise AirflowFailException("Script policy or artifact contract rejected") from error

    def on_kill(self) -> None:
        if self.process is not None:
            self.process.cancel()


class PublishRunOperator(BaseOperator):
    def __init__(self, *, task_id: str, **kwargs: Unpack[OperatorOptions]) -> None:
        kwargs["trigger_rule"] = "all_done"
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: Context) -> dict[str, object]:
        store = ArtifactStore.deployment()
        ti = context["ti"]
        policy = Policy.load(dag=ti.dag_id)
        try:
            if set(self.upstream_task_ids) != set(policy.tasks):
                raise ContractError("Publisher must explicitly depend on every registered task")
            raw: object = ti.get_task_states(
                dag_id=ti.dag_id, run_ids=[ti.run_id], task_ids=policy.tasks
            )
            if not isinstance(raw, dict) or not isinstance(raw.get(ti.run_id, {}), dict):
                raise ContractError("Invalid task API state response")
            states = cast(dict[str, str | None], raw.get(ti.run_id, {}))
            report = reconcile(store, policy, ti.dag_id, ti.run_id, states)
            identity = publication_identity(policy, ti.dag_id, ti.run_id)
            marker = publish(store, report, identity)
            return {"outcome": report.outcome, "reports": marker.model_dump(mode="json")["reports"]}
        except (ContractError, ValidationError) as error:
            raise AirflowFailException("Run report contract rejected") from error


class AssertOutcomeOperator(BaseOperator):
    def __init__(self, *, task_id: str, **kwargs: Unpack[OperatorOptions]) -> None:
        kwargs["trigger_rule"] = "all_done"
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: Context) -> dict[str, str]:
        store = ArtifactStore.deployment()
        ti = context["ti"]
        policy = Policy.load(dag=ti.dag_id)
        try:
            report = assert_outcome(
                store, ti.dag_id, ti.run_id, publication_identity(policy, ti.dag_id, ti.run_id)
            )
            return {"outcome": report.outcome}
        except (ContractError, ValidationError) as error:
            raise AirflowFailException("Run report missing, invalid or unsuccessful") from error
