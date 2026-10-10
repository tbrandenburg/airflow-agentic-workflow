"""Phase 2: independent worker workspaces linked exclusively through verified artifacts."""

from datetime import timedelta

from airflow.sdk import DAG
from airflow_core.operators import AssertOutcomeOperator, ProjectScriptOperator, PublishRunOperator

with DAG(
    dag_id="artifact_script_smoke",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=4),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
        "execution_timeout": timedelta(seconds=90),
        "pool": "platform",
    },
    tags=["phase-2", "example-service"],
) as dag:
    prepare = ProjectScriptOperator(task_id="prepare")
    inventory = ProjectScriptOperator(task_id="inventory", source_task="prepare")
    publisher = PublishRunOperator(task_id="publish")
    assertion = AssertOutcomeOperator(task_id="assert_outcome")
    prepare >> inventory
    [prepare, inventory] >> publisher >> assertion
