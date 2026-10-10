"""P5 fixed proof with runnable demand above both pool capacities."""

from datetime import timedelta

from airflow.sdk import DAG
from airflow_core.agent_operator import AgentRunOperator
from airflow_core.agent_process import MODEL
from airflow_core.operations import active_runs, defaults
from airflow_core.operators import AssertOutcomeOperator, ProjectScriptOperator, PublishRunOperator

with DAG(
    dag_id="operational_proof",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=active_runs(),
    max_active_tasks=4,
    dagrun_timeout=timedelta(minutes=7),
    default_args=defaults(),
    tags=["phase-5", "deployment-fixture"],
) as operational_proof:
    models = [
        AgentRunOperator(
            task_id=f"model_{index}",
            project="example-service",
            prompt_id="planner",
            model_id=MODEL,
            upstream_refs={},
            pool="agent-model",
            queue="worker-1" if index % 2 == 0 else "worker-2",
        )
        for index in range(4)
    ]
    handoff = ProjectScriptOperator(
        task_id="handoff",
        source_task="model_0",
        queue="worker-2",
    )
    models[0] >> handoff
    validations = [
        ProjectScriptOperator(task_id=f"validation_{index}", pool="validation")
        for index in range(2)
    ]
    publisher = PublishRunOperator(task_id="publish")
    [*models, handoff, *validations] >> publisher
    publisher >> AssertOutcomeOperator(task_id="assert_outcome", retries=0)
