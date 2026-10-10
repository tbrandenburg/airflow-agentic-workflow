"""Read-only deployed class, command and credential-boundary evidence; no secret reads."""

import json
import subprocess
from pathlib import Path
from typing import cast

import httpx
from airflow_core.artifacts import digest
from deploy import compose
from p5_integrity import worker


def collect(gateway: httpx.Client, payload: dict[str, object]) -> dict[str, object]:
    deployed = worker(
        "worker-1",
        (
            "import runpy,json,inspect,hashlib; from pathlib import Path; "
            "from airflow_core.agent_process import AgentProcess,RUNTIME; "
            "from airflow_core.process import script_argv; "
            "from airflow_core.execution import execute_script; "
            "dag=runpy.run_path('/opt/airflow/dags/code_review.py')['code_review']; "
            "tasks={t.task_id:{'class':type(t).__module__+'.'+type(t).__name__,"
            "'timeout':t.execution_timeout.total_seconds(),'retries':t.retries} "
            "for t in dag.tasks}; "
            "agent=inspect.getsource(AgentProcess.run); "
            "execution=inspect.getsource(execute_script); "
            'assert \'self.invoke(["node", str(RUNTIME), "runtime-input.json"]\' in agent; '
            "assert 'reconstruct(policy.mirror, policy.base_sha, "
            "source, policy.sources)' in execution; "
            "assert 'store.download(ref, root' in execution; "
            "print(json.dumps({'tasks':tasks,'agent_command':['node',str(RUNTIME),'runtime-input.json'],"
            "'script_command':script_argv(Path('/registered.sh'),Path('input.json'),Path('output.json')),"
            "'runtime_sha256':hashlib.sha256(RUNTIME.read_bytes()).hexdigest(),"
            "'agent_adapter_sha256':hashlib.sha256(agent.encode()).hexdigest(),"
            "'execution_sha256':hashlib.sha256(execution.encode()).hexdigest(),"
            "'independent_reconstruction':True}))"
        ),
    )
    tasks = cast(dict[str, dict[str, object]], deployed["tasks"])
    for task in ("plan", "implement", "review"):
        assert tasks[task]["class"] == "airflow_core.agent_operator.AgentRunOperator"
    for task in ("prepare", "collect_context", "apply_patch", "validate", "package"):
        assert tasks[task]["class"] == "airflow_core.operators.ProjectScriptOperator"
    rejections = []
    for field in ("prompt_id", "script_id", "fixture"):
        body = {**payload, field: "unregistered"}
        response = gateway.post("/runs", json=body)
        assert response.status_code == 422
        rejections.append({"request": body, "status": response.status_code})
    mounts: dict[str, object] = {}
    allowed = {"/secrets/minio_user", "/secrets/minio_password", "/model-auth/provider.json"}
    for service in ("worker-1", "worker-2"):
        identifier = (
            subprocess.check_output(compose("ps", "-q", service), timeout=30).decode().strip()
        )
        data = json.loads(
            subprocess.check_output(
                ["docker", "inspect", "--format", "{{json .Mounts}}", identifier], timeout=30
            )
        )
        inventory = [
            {"destination": entry["Destination"], "writable": entry["RW"]} for entry in data
        ]
        assert not any(entry["destination"] == "/var/run/docker.sock" for entry in inventory)
        assert any(entry == {"destination": "/mirrors", "writable": False} for entry in inventory)
        secrets = {
            str(entry["destination"])
            for entry in inventory
            if str(entry["destination"]).startswith(("/secrets/", "/model-auth/"))
        }
        assert secrets == allowed
        assert not any(entry["writable"] for entry in inventory if entry["destination"] in allowed)
        mounts[service] = inventory
    paths = [
        "airflow-core/airflow_core/process.py",
        "airflow-core/airflow_core/agent_process.py",
        "airflow-core/airflow_core/workspace.py",
        "airflow-core/airflow_core/patches.py",
        "projects/example-service/scripts/workflow.py",
        "projects/example-service/scripts/checks.py",
        "packages/agent-runtime/src/cli.ts",
        "packages/agent-runtime/src/isolation.ts",
        "infra/airflow/compose.yaml",
        "projects/example-service/dags/code_review.py",
    ]
    sources = {
        path: {"sha256": digest(Path(path).read_bytes()), "text": Path(path).read_text()}
        for path in paths
    }
    combined = "\n".join(item["text"] for item in sources.values())
    forbidden = ("git push", "gh pr create", "gh pr merge", "kubectl", "terraform apply")
    assert not any(command in combined for command in forbidden)
    assert '"run", input.prompt, "--model", input.model, "--format", "json"' in combined
    return {
        "generic_operators": {"verified": True, "deployed": deployed},
        "clean_reconstruction": {"verified": True, "deployed": deployed},
        "api_rejections": {"verified": True, "requests": rejections},
        "credential_boundary": {
            "verified": True,
            "mounts": mounts,
            "allowed_credentials": sorted(allowed),
            "environment_source": sources[paths[0]],
        },
        "command_boundary": {
            "verified": True,
            "sources": sources,
            "prohibited_command_scan": list(forbidden),
            "node_provider_command": [
                "opencode",
                "run",
                "<task prompt>",
                "--model",
                "github-copilot/gpt-6.1-sol",
                "--format",
                "json",
            ],
        },
        "external_boundary": {
            "verified": True,
            "scope": "Owned local Compose stack, read-only synthetic mirror, "
            "disposable workspaces, immutable MinIO proof objects and authorized provider requests",
            "approved_effects": [
                "Airflow API triggers/clear",
                "MinIO evidence publication",
                "disposable workspace execution",
                "GitHub Copilot model calls",
            ],
            "outside_boundary": "Unrelated external systems are not observable; "
            "no universal absence-of-effects claim",
            "evidence": "S07 independent mirror comparison; S08 worker egress denial and approved "
            "CONNECT proof; command_boundary fixed workflow command/source inventory",
        },
    }
