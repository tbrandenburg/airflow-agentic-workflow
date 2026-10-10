"""Bounded operational evidence; user text and credential values never enter argv logs."""

import logging
import socket
from datetime import datetime
from pathlib import Path

from airflow_core import RUNTIME_VERSION
from airflow_core.artifacts import ArtifactStore, canonical
from airflow_core.contracts import ArtifactRef, TaskResult
from airflow_core.process import ScriptProcess


def event(
    store: ArtifactStore,
    prefix: str,
    result: TaskResult,
    process: ScriptProcess,
) -> ArtifactRef:
    classification = (
        "cancelled"
        if process.cancelled
        else "timeout"
        if process.result is not None and process.result.timed_out
        else "success"
        if result.outcome == "passed"
        else "execution_failed"
    )
    runtime = result.diagnostics.get("runtime.json")
    metadata: dict[str, object] = {}
    if runtime is not None:
        import json

        try:
            value = json.loads(store.get(runtime))
            if isinstance(value, dict):
                metadata = value
                classification = str(metadata.get("error_class") or classification)
        except ValueError:
            classification = "invalid_metadata"
    data = {
        "dag": result.dag_id,
        "run": result.dag_run_id,
        "task": result.task_id,
        "attempt": result.attempt,
        "worker": result.worker,
        "input_uris": {name: ref.uri for name, ref in result.input_artifacts.items()},
        "output_uris": {name: ref.uri for name, ref in result.outputs.items()},
        "input_uri": f"s3://{store.bucket}/{prefix}/input.json",
        "started_at": result.started_at,
        "ended_at": result.ended_at,
        "duration_seconds": (
            datetime.fromisoformat(result.ended_at) - datetime.fromisoformat(result.started_at)
        ).total_seconds(),
        "exit_code": result.exit_code,
        "classification": classification,
        "model": metadata.get("model"),
        "versions": {
            "runtime": RUNTIME_VERSION,
            "node": metadata.get("node_version"),
            "opencode": metadata.get("version"),
        },
        "argv": [Path(arg).name if "/" in arg else arg for arg in process.argv],
        "cleanup_live_pids": process.tree.live(),
    }
    logging.getLogger(__name__).info("TASK_OPERATION %s", canonical(data).decode())
    return store.put(f"{prefix}/task-log.json", canonical(data), "application/json")


def reused(store: ArtifactStore, prefix: str, attempt: int, result: TaskResult) -> None:
    marker = store.read(f"{prefix}/completed.json")[0]
    data = {
        "task": result.task_id,
        "attempt": attempt,
        "original_attempt": result.attempt,
        "worker": socket.gethostname(),
        "completed": marker.model_dump(mode="json"),
        "classification": "reused",
        "model_invoked": False,
    }
    store.put(f"{prefix}/reruns/{attempt}.json", canonical(data), "application/json")
    logging.getLogger(__name__).info("TASK_OPERATION %s", canonical(data).decode())


def preflight_failure(
    store: ArtifactStore, dag: str, run: str, task: str, attempt: int, error: Exception
) -> None:
    from airflow_core.artifacts import run_key

    data = {
        "dag": dag,
        "run": run,
        "task": task,
        "attempt": attempt,
        "worker": socket.gethostname(),
        "classification": "preflight_failed",
        "exception": type(error).__name__,
        "model_invoked": False,
    }
    try:
        store.put(
            f"{run_key(dag, run)}/steps/{task}/preflight/{attempt}.json",
            canonical(data),
            "application/json",
        )
    except Exception as diagnostic:
        logging.getLogger(__name__).error(
            "Preflight evidence unavailable: %s", type(diagnostic).__name__
        )
