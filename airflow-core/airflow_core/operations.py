"""Deployment-owned operational bounds, shared by DAGs and bootstrap."""

import os
from datetime import timedelta


def bounded(name: str, default: int, maximum: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


def active_runs() -> int:
    return bounded("POC_MAX_ACTIVE_RUNS", 1, 2)


def defaults() -> dict[str, object]:
    return {
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(seconds=180),
        "pool": "platform",
    }
