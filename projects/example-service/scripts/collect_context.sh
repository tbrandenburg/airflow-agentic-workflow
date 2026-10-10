#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH=/deployment/airflow-core:/home/airflow/.local/lib/python3.12/site-packages
exec python /deployment/scripts/workflow.py collect_context "$@"
