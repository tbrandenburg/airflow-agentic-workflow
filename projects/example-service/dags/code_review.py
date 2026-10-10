"""P4 full workflow and one fixed deployment-owned failing validation DAG."""

from datetime import timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import DAG
from airflow_core.agent_operator import AgentRunOperator
from airflow_core.agent_process import MODEL
from airflow_core.gates import validation_branch
from airflow_core.operations import active_runs, defaults
from airflow_core.operators import AssertOutcomeOperator, ProjectScriptOperator, PublishRunOperator

for name in (
    "code_review",
    "code_review_validation_fail",
    "code_review_transient",
    "code_review_malformed",
    "code_review_context_failure",
    "code_review_timeout",
    "code_review_unsafe_patch",
    "code_review_package_failure",
):
    with DAG(
        dag_id=name,
        schedule=None,
        catchup=False,
        is_paused_upon_creation=False,
        max_active_runs=active_runs(),
        max_active_tasks=4,
        dagrun_timeout=timedelta(minutes=12),
        default_args=defaults(),
        tags=["phase-4"],
    ) as dag:
        prepare = ProjectScriptOperator(
            task_id="prepare",
            project="example-service",
            script_id="workflow_prepare",
            upstream_refs={},
            queue="worker-1",
        )
        plan = AgentRunOperator(
            task_id="plan",
            project="example-service",
            prompt_id="planner",
            model_id=MODEL,
            upstream_refs={"metadata.json": ("prepare", "metadata.json")},
            pool="agent-model",
            queue="worker-2",
        )
        collect = ProjectScriptOperator(
            task_id="collect_context",
            project="example-service",
            script_id="acceptance_failure"
            if name == "code_review_context_failure"
            else "collect_context",
            upstream_refs={"plan.json": ("plan", "response.json")},
        )
        implement = AgentRunOperator(
            task_id="implement",
            project="example-service",
            prompt_id="coder",
            model_id=MODEL,
            upstream_refs={
                "plan.json": ("plan", "response.json"),
                "context.json": ("collect_context", "context.json"),
            },
            pool="agent-model",
        )
        apply = ProjectScriptOperator(
            task_id="apply_patch",
            project="example-service",
            script_id="apply_patch",
            upstream_refs={
                "patch.diff": ("implement", "patch.diff"),
                "context.json": ("collect_context", "context.json"),
            },
        )
        validate = ProjectScriptOperator(
            task_id="validate",
            project="example-service",
            script_id="validate_fail" if name == "code_review_validation_fail" else "validate",
            upstream_refs={
                "patch.diff": ("implement", "patch.diff"),
                "patch-summary.json": ("apply_patch", "patch-summary.json"),
            },
            pool="validation",
        )
        branch = BranchPythonOperator(
            task_id="validation_branch", python_callable=validation_branch
        )
        review = AgentRunOperator(
            task_id="review",
            project="example-service",
            prompt_id="reviewer",
            model_id=MODEL,
            upstream_refs={
                "patch.diff": ("implement", "patch.diff"),
                "context.json": ("collect_context", "context.json"),
                "validation.json": ("validate", "validation.json"),
            },
            pool="agent-model",
        )
        skip = EmptyOperator(task_id="skip_review")
        package = ProjectScriptOperator(
            task_id="package",
            project="example-service",
            script_id="acceptance_failure" if name == "code_review_package_failure" else "package",
            upstream_refs={
                "patch.diff": ("implement", "patch.diff"),
                "patch-summary.json": ("apply_patch", "patch-summary.json"),
                "validation.json": ("validate", "validation.json"),
                "review.json": ("review", "response.json"),
            },
            trigger_rule="none_failed_min_one_success",
        )
        prepare >> plan >> collect >> implement >> apply >> validate >> branch
        plan >> implement
        collect >> apply
        implement >> validate
        branch >> [review, skip] >> package
        [implement, collect, validate] >> review
        [implement, apply, validate] >> package
        publisher = PublishRunOperator(task_id="publish_run")
        [
            prepare,
            plan,
            collect,
            implement,
            apply,
            validate,
            branch,
            review,
            skip,
            package,
        ] >> publisher
        publisher >> AssertOutcomeOperator(task_id="assert_outcome", retries=0)
        globals()[name] = dag
