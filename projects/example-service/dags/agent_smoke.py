"""P3 only: real Planner and deployment-registered fault fixtures."""

from datetime import timedelta

from airflow.sdk import DAG
from airflow_core.agent_operator import AgentRunOperator
from airflow_core.agent_process import MODEL
from airflow_core.operations import active_runs, defaults
from airflow_core.operators import AssertOutcomeOperator, PublishRunOperator

for workflow in (
    "agent_smoke",
    "agent_fixture_success",
    "agent_fixture_transient",
    "agent_fixture_permanent",
    "agent_fixture_timeout",
    "agent_fixture_malformed",
    "agent_fixture_schema_invalid",
    "agent_fixture_missing_patch",
    "agent_fixture_oversized",
):
    with DAG(
        dag_id=workflow,
        schedule=None,
        catchup=False,
        is_paused_upon_creation=False,
        max_active_runs=active_runs(),
        max_active_tasks=4,
        dagrun_timeout=timedelta(minutes=7),
        default_args=defaults(),
        tags=["phase-3"],
    ) as dag:
        roles = (
            ["planner", "coder", "reviewer"]
            if workflow == "agent_fixture_success"
            else ["coder" if workflow == "agent_fixture_missing_patch" else "planner"]
        )
        agents = [
            AgentRunOperator(
                task_id=role,
                project="example-service",
                prompt_id=role,
                model_id=MODEL,
                upstream_refs={},
                pool="agent-model",
            )
            for role in roles
        ]
        publisher = PublishRunOperator(task_id="publish")
        assertion = AssertOutcomeOperator(task_id="assert_outcome")
        agents >> publisher >> assertion
        globals()[workflow] = dag
