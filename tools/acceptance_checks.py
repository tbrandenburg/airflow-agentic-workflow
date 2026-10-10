"""Scenario-specific checks over actual task state and checksum-verified artifacts."""

import json
from datetime import datetime

from airflow_core.artifacts import ArtifactStore, run_key
from airflow_core.contracts import RunReport
from airflow_core.roles import RESPONSE
from p5_integrity import worker


def verify(store: ArtifactStore, report: RunReport, states: dict[str, object], mode: str) -> None:
    tasks = {task.task_id: task for task in report.tasks}
    assert report.task_request and report.request_id == report.dag_run_id
    assert states["publish_run"] == "success"
    assert states["assert_outcome"] == ("success" if mode in ("real", "transient") else "failed")
    if mode in ("real", "transient", "validation_fail", "package_failure"):
        assert report.patch is not None and report.validation_artifact is not None
        assert json.loads(store.get(report.validation_artifact)) == report.validation
        assert report.patch_summary is not None
    if mode == "real":
        assert report.outcome == "passed" and report.review is not None
        assert report.validation is not None and report.validation["gate_status"] == "passed"
        for name in ("plan", "implement", "review"):
            attempt = tasks[name].attempts[-1]
            RESPONSE.validate_json(store.get(attempt.outputs["response.json"]))
            metadata = json.loads(store.get(attempt.diagnostics["runtime.json"]))
            assert metadata["error_class"] is None and metadata["version"] == "1.18.32"
            assert metadata["model"] == "github-copilot/gpt-6.1-sol"
        patch = store.get(report.patch).decode() if report.patch else ""
        assert '-    return "ok"' in patch and '+    return "healthy"' in patch
    if mode == "validation_fail":
        assert report.outcome == "validation_failed" and report.review is None
        assert states["validate"] == "success" and states["review"] == "skipped"
        assert states["package"] == "success"
        assert report.validation is not None and report.validation["gate_status"] == "failed"
        assert tasks["validate"].attempts[-1].exit_code == 0
    if mode == "transient":
        attempts = tasks["implement"].attempts
        assert len(attempts) == 2 and [item.outcome for item in attempts] == [
            "execution_failed",
            "passed",
        ]
        assert report.outcome == "passed"
        root = run_key(report.dag_id, report.dag_run_id)
        assert len(store.keys(f"{root}/steps/implement/attempts/2/outputs/")) == 2
        assert store.optional(f"{root}/steps/implement/attempts/1/output.json") is None
    if mode in ("malformed", "timeout"):
        attempts = tasks["implement"].attempts
        assert len(attempts) == 1 and states["implement"] == "failed"
        assert states["apply_patch"] == "upstream_failed"
        metadata = json.loads(store.get(attempts[0].diagnostics["runtime.json"]))
        assert metadata["error_class"] == ("timeout" if mode == "timeout" else "invalid_output")
        assert report.failed_step == "implement" and report.outcome == "execution_failed"
        if mode == "timeout":
            duration = (
                datetime.fromisoformat(attempts[0].ended_at)
                - datetime.fromisoformat(attempts[0].started_at)
            ).total_seconds()
            assert duration < 45
            stderr = store.get(attempts[0].diagnostics["opencode-stderr.log"]).decode()
            pid = int(stderr.split("descendant=")[1].split()[0])
            probe = worker(
                "worker-1"
                if attempts[0].worker
                == worker(
                    "worker-1",
                    "import json,socket; print(json.dumps({'hostname':socket.gethostname()}))",
                )["hostname"]
                else "worker-2",
                f"import json; from pathlib import Path; p=Path('/proc/{pid}/stat'); "
                "print(json.dumps({'stopped':not p.exists() or "
                "p.read_text().split(') ')[1].startswith('Z')}))",
            )
            assert probe["stopped"] is True
    if mode in ("context_failure", "package_failure", "unsafe_patch"):
        name = {
            "context_failure": "collect_context",
            "package_failure": "package",
            "unsafe_patch": "apply_patch",
        }[mode]
        assert states[name] == "failed" and report.failed_step == name
        assert report.outcome == "execution_failed"
        for attempt in tasks[name].attempts:
            stderr = store.get(attempt.diagnostics["stderr"]).decode()
            assert (
                "P6_FIXED_BASH_FAILURE exit=42" in stderr
                if mode != "unsafe_patch"
                else "ContractError" in stderr
            )
        if mode != "unsafe_patch":
            assert len(tasks[name].attempts) == 2
            assert tasks[name].attempts[-1].exit_code == 42
        if mode == "context_failure":
            assert states["implement"] == "upstream_failed" and report.patch is None
        if mode == "unsafe_patch":
            assert states["validate"] == "upstream_failed" and report.patch is not None
            assert ".git/config" in store.get(report.patch).decode()
