"""Phase 1 proves Celery execution against an immutable synthetic mirror."""

import json
import logging
import subprocess
from datetime import timedelta
from pathlib import Path

from airflow.sdk import DAG, task

with DAG(
    dag_id="platform_smoke",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=3),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
        "execution_timeout": timedelta(seconds=60),
        "pool": "platform",
    },
    tags=["phase-1", "example-service"],
) as dag:

    @task
    def smoke() -> str:
        policy = json.loads(Path("/deployment/policy.json").read_text())
        result = subprocess.run(
            ["git", "--git-dir", policy["mirror"], "rev-parse", "refs/heads/main"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
        if result != policy["approved_sha"]:
            raise RuntimeError("Synthetic source SHA mismatch")
        logging.info("PHASE1_SMOKE_OK approved_sha=%s", result)
        return result

    smoke()
