"""Real subprocess/completion/MinIO gate reconciliation without model calls."""

from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from airflow_core.artifacts import ArtifactStore, ContractError, canonical, digest, run_key
from airflow_core.contracts import ExpectedOutput
from airflow_core.execution import execute_script
from airflow_core.policy import Policy
from airflow_core.process import ScriptProcess
from airflow_core.reporting import assert_outcome, publish, reconcile
from airflow_core.workflow_policy import WorkflowPolicy

PATCH = b"diff --git a/service.py b/service.py\n"


def executable(path: Path, contents: dict[str, bytes]) -> Path:
    code = (
        "import hashlib,json,sys\nfrom pathlib import Path\n"
        "manifest=json.loads(Path(sys.argv[1]).read_bytes())\n"
        f"contents={contents!r}\n"
        "files=[]\n"
        "for item in manifest['expected_outputs']:\n"
        " data=contents[item['path']]\n"
        " Path(item['path']).write_bytes(data)\n"
        " files.append({**item,'size':len(data),'sha256':hashlib.sha256(data).hexdigest()})\n"
        "output={name:manifest[name] for name in "
        "('dag_id','dag_run_id','task_id','attempt','base_sha')}\n"
        "output.update(schema_version='1.0',outputs=files,summary='Gate fixture')\n"
        "Path(sys.argv[2]).write_text(json.dumps(output))\n"
    )
    python = path.with_suffix(".py")
    python.write_text(code)
    path.write_text(f'exec {Path(".venv/bin/python").resolve()} {python} "$2" "$4"\n')
    return path


@pytest.mark.parametrize("failed", [False, True])
def test_real_completed_gate_report_and_authoritative_assertion(
    store: ArtifactStore,
    tmp_path: Path,
    failed: bool,
) -> None:
    loaded = Policy.load(Path("projects/example-service/policy.py"), dag="code_review")
    assert isinstance(loaded, WorkflowPolicy)
    gate: dict[str, object] = {
        "base_sha": loaded.base_sha,
        "patch_sha256": digest(PATCH),
        "gate_status": "failed" if failed else "passed",
        "checks": [
            {
                "name": name,
                "argv": ["deployment-fixed"],
                "exit_code": int(failed and name == "tests"),
                "stdout": "captured",
                "stderr": "",
                "duration_seconds": 0.01,
            }
            for name in ("formatter", "lint", "tests", "secret_scan")
        ],
    }
    policy = replace(
        loaded,
        controls=(),
        scripts={
            "implement": executable(tmp_path / "implement.sh", {"patch.diff": PATCH}),
            "validate": executable(tmp_path / "validate.sh", {"validation.json": canonical(gate)}),
        },
        outputs={
            "implement": [
                ExpectedOutput(path="patch.diff", content_type="text/plain", max_bytes=1000)
            ],
            "validate": [
                ExpectedOutput(
                    path="validation.json", content_type="application/json", max_bytes=10000
                )
            ],
        },
        inputs={"implement": ["seed"], "validate": ["patch.diff"]},
    )

    # These proof scripts do not consume source; avoid a host mirror dependency.
    class ProofPolicy(WorkflowPolicy):
        @property
        def independent_base(self) -> bool:
            return False

    policy = ProofPolicy(**policy.__dict__)
    run = "proof_" + uuid4().hex
    request: dict[str, object] = {
        "project": policy.project,
        "workflow": policy.workflow,
        "task": "fixed gate proof",
        "base_ref": policy.base_sha,
        "model_settings": {"model": "github-copilot/gpt-6.1-sol"},
    }
    store.put(
        f"{run_key(policy.dag_id, run)}/run-input.json", canonical(request), "application/json"
    )
    seed = store.put(f"proof/{run}/seed", b"seed", "text/plain")
    coder = execute_script(
        store, policy, request, policy.dag_id, run, "implement", 1, {"seed": seed}, ScriptProcess()
    )
    result = execute_script(
        store, policy, request, policy.dag_id, run, "validate", 1, coder.outputs, ScriptProcess()
    )
    assert result.outcome == "passed" and result.exit_code == 0
    report = reconcile(
        store, policy, policy.dag_id, run, {"implement": "success", "validate": "success"}
    )
    assert report.outcome == ("validation_failed" if failed else "passed")
    identity = digest(run.encode())
    publish(store, report, identity)
    if failed:
        with pytest.raises(ContractError, match="validation_failed"):
            assert_outcome(store, policy.dag_id, run, identity)
        return
    assert assert_outcome(store, policy.dag_id, run, identity).validation is not None
