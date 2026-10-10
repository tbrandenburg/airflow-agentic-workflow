import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from airflow_core.artifacts import ArtifactStore, ContractError, canonical, digest, run_key
from airflow_core.completion import complete, reuse
from airflow_core.contracts import AgentOutput, Completion, InputManifest, RunReport, TaskResult
from airflow_core.execution import ExecutionError, execute_script
from airflow_core.policy import Policy
from airflow_core.process import ScriptProcess
from airflow_core.reporting import assert_outcome, publish, reconcile


@pytest.fixture
def policy(tmp_path: Path) -> Policy:
    source = tmp_path / "source"
    source.mkdir()
    subprocess.run(
        ["git", "init", "--bare", str(tmp_path / "mirror.git")],
        check=True,
        capture_output=True,
        timeout=10,
    )
    # Build the exact approved fixture objects without changing any mirror refs.
    mirror = tmp_path / "mirror.git"
    content = Path("projects/example-service/fixtures/service.py").read_bytes()
    blob = (
        subprocess.check_output(
            ["git", "--git-dir", str(mirror), "hash-object", "-w", "--stdin"],
            input=content,
            timeout=10,
        )
        .decode()
        .strip()
    )
    tree = (
        subprocess.check_output(
            ["git", "--git-dir", str(mirror), "mktree"],
            input=f"100644 blob {blob}\tservice.py\n".encode(),
            timeout=10,
        )
        .decode()
        .strip()
    )
    commit = (
        f"tree {tree}\nauthor Synthetic Fixture <fixture@example.invalid> 0 +0000\n"
        "committer Synthetic Fixture <fixture@example.invalid> 0 +0000\n\n"
        "Phase 1 synthetic fixture\n"
    ).encode()
    sha = (
        subprocess.check_output(
            ["git", "--git-dir", str(mirror), "hash-object", "-t", "commit", "-w", "--stdin"],
            input=commit,
            timeout=10,
        )
        .decode()
        .strip()
    )
    loaded = Policy.load(Path("projects/example-service/policy.py"))
    assert sha == loaded.base_sha
    return replace(
        loaded,
        mirror=mirror,
        scripts={
            name: Path(f"projects/example-service/scripts/{name}.sh").resolve()
            for name in loaded.scripts
        },
    )


def request(policy: Policy) -> dict[str, object]:
    return {
        "project": policy.project,
        "workflow": policy.workflow,
        "task": "artifact-smoke",
        "base_ref": policy.base_sha,
        "model_settings": {"model": "github-copilot/gpt-6.1-sol"},
    }


def test_completion_reuse_no_reexecution_mismatch_and_publication(
    store: ArtifactStore,
    policy: Policy,
) -> None:
    run = f"proof_{uuid4().hex}"
    first = execute_script(
        store, policy, request(policy), policy.dag_id, run, "prepare", 1, {}, ScriptProcess()
    )
    assert not Path(first.workspace_path).exists()
    second = execute_script(
        store, policy, request(policy), policy.dag_id, run, "prepare", 2, {}, ScriptProcess()
    )
    assert first == second and second.attempt == 1
    assert not store.keys(f"{run_key(policy.dag_id, run)}/steps/prepare/attempts/2/")
    import json

    rerun = json.loads(store.read(f"{run_key(policy.dag_id, run)}/steps/prepare/reruns/2.json")[1])
    assert rerun["original_attempt"] == 1 and rerun["model_invoked"] is False
    log = json.loads(store.get(first.diagnostics["task-log.json"]))
    assert log["classification"] == "success" and log["cleanup_live_pids"] == []
    assert log["duration_seconds"] > 0 and log["output_uris"]["source.tar"]
    with pytest.raises(ContractError, match="fingerprint"):
        execute_script(
            store,
            replace(policy, registry_digest="0" * 64),
            request(policy),
            policy.dag_id,
            run,
            "prepare",
            2,
            {},
            ScriptProcess(),
        )
    inventory = execute_script(
        store,
        policy,
        request(policy),
        policy.dag_id,
        run,
        "inventory",
        1,
        first.outputs,
        ScriptProcess(),
    )
    assert first.workspace_path != inventory.workspace_path
    report = reconcile(
        store, policy, policy.dag_id, run, {"prepare": "success", "inventory": "success"}
    )
    assert report.outcome == "passed"
    identity = digest(b"publication")
    marker = publish(store, report, identity)
    assert (
        publish(store, report.model_copy(update={"generated_at": "different"}), identity) == marker
    )
    assert assert_outcome(store, policy.dag_id, run, identity).outcome == "passed"
    with pytest.raises(ContractError):
        assert_outcome(store, policy.dag_id, "missing", identity)


def test_completion_marker_race(store: ArtifactStore, policy: Policy) -> None:
    run = f"proof_{uuid4().hex}"
    result = execute_script(
        store, policy, request(policy), policy.dag_id, run, "prepare", 1, {}, ScriptProcess()
    )
    root = f"{run_key(policy.dag_id, run)}/steps/prepare"
    existing = Completion.model_validate_json(store.read(f"{root}/completed.json")[1])
    prefix = f"proof/{uuid4().hex}/steps/prepare"
    assert result.output_manifest is not None
    manifest = InputManifest.model_validate(
        {name: getattr(result, name) for name in InputManifest.model_fields}
    ).model_copy(update={"attempt": 2})
    output = AgentOutput.model_validate_json(store.get(result.output_manifest)).model_copy(
        update={"attempt": 2, "input_digest": digest(canonical(manifest))}
    )
    output_bytes = canonical(output)
    other = result.model_copy(
        update={
            "attempt": 2,
            "input_digest": digest(canonical(manifest)),
            "output_digest": digest(output_bytes),
            "output_manifest": store.put(
                f"{prefix}/attempts/2/output.json", output_bytes, "application/json"
            ),
        }
    )
    other_ref = store.put(
        f"{prefix}/attempts/2/attempt-result.json", canonical(other), "application/json"
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(complete, store, prefix, result.input_fingerprint, ref)
            for ref in (existing.result, other_ref)
        ]
        winners = [future.result(timeout=20) for future in futures]
    assert winners[0] == winners[1]
    assert reuse(store, prefix, result.input_fingerprint) == winners[0]
    with pytest.raises(ContractError):
        reuse(store, prefix, "0" * 64)


@pytest.mark.parametrize(
    "script, timeout", [("printf failure >&2; exit 9", 5), ("printf waiting; sleep 5", 1)]
)
def test_failed_attempt_diagnostics(
    store: ArtifactStore,
    policy: Policy,
    tmp_path: Path,
    script: str,
    timeout: int,
) -> None:
    path = tmp_path / "failure.sh"
    path.write_text(script + "\n")
    failing = replace(policy, scripts={**policy.scripts, "prepare": path}, timeout=timeout)
    run = f"proof_{uuid4().hex}"
    with pytest.raises(ExecutionError):
        execute_script(
            store, failing, request(policy), policy.dag_id, run, "prepare", 1, {}, ScriptProcess()
        )
    prefix = f"{run_key(policy.dag_id, run)}/steps/prepare"
    evidence = TaskResult.model_validate_json(
        store.read(f"{prefix}/attempts/1/attempt-result.json")[1]
    )
    assert evidence.outcome == "execution_failed" and evidence.errors
    assert any(store.get(ref) for ref in evidence.diagnostics.values())
    assert store.optional(f"{prefix}/completed.json") is None
    report = reconcile(
        store, policy, policy.dag_id, run, {"prepare": "failed", "inventory": "upstream_failed"}
    )
    assert report.outcome == "execution_failed"
    assert report.tasks[1].skipped_reason == "upstream_failed"
    marker = publish(store, report, digest(b"failure"))
    assert len(marker.reports) == 3
    with pytest.raises(ContractError):
        assert_outcome(store, policy.dag_id, run, digest(b"failure"))


def test_schema_failure_diagnostic_and_cancellation(
    store: ArtifactStore,
    policy: Policy,
    tmp_path: Path,
) -> None:
    import threading

    script = tmp_path / "invalid.sh"
    script.write_text('printf \'{"unexpected":"field"}\' > "$4"\n')
    invalid = replace(policy, scripts={**policy.scripts, "prepare": script})
    run = f"proof_{uuid4().hex}"
    with pytest.raises(ValueError):
        execute_script(
            store, invalid, request(policy), policy.dag_id, run, "prepare", 1, {}, ScriptProcess()
        )
    report = reconcile(
        store, invalid, policy.dag_id, run, {"prepare": "failed", "inventory": "skipped"}
    )
    assert report.outcome == "execution_failed"
    attempt = report.tasks[0].attempts[0]
    assert attempt.outcome == "execution_failed" and attempt.exit_code == 0
    assert attempt.errors == ["ValidationError"]
    assert report.validation is None
    marker = publish(store, report, digest(run.encode()))
    persisted = RunReport.model_validate_json(store.get(marker.reports["run-report.json"]))
    assert persisted.outcome == "execution_failed"
    script.write_text("printf started; sleep 10\n")
    process = ScriptProcess()
    timer = threading.Timer(0.5, process.cancel)
    timer.start()
    run = f"proof_{uuid4().hex}"
    try:
        with pytest.raises(ExecutionError):
            execute_script(
                store, invalid, request(policy), policy.dag_id, run, "prepare", 1, {}, process
            )
    finally:
        timer.join(timeout=2)
    report = reconcile(
        store, invalid, policy.dag_id, run, {"prepare": "failed", "inventory": "upstream_failed"}
    )
    assert report.outcome == "cancelled"


def test_publication_race_cannot_mix_bundles(store: ArtifactStore, policy: Policy) -> None:
    run = f"proof_{uuid4().hex}"
    report = reconcile(store, policy, policy.dag_id, run, {"prepare": None, "inventory": "skipped"})
    assert report.outcome == "incomplete"
    identity = digest(run.encode())
    other = report.model_copy(update={"generated_at": "different contender"})
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(publish, store, contender, identity) for contender in (report, other)
        ]
        results = [future.result(timeout=20) for future in futures]
    assert results[0] == results[1]
    prefixes = {ref.uri.rsplit("/", 1)[0] for ref in results[0].reports.values()}
    assert len(prefixes) == 1
    with pytest.raises(ContractError):
        assert_outcome(store, policy.dag_id, run, identity)


@pytest.mark.parametrize("corruption", ["schema", "integrity", "type"])
def test_corrupt_completion_still_publishes_execution_failure(
    store: ArtifactStore,
    policy: Policy,
    corruption: str,
) -> None:
    run = f"proof_{uuid4().hex}"
    key = f"{run_key(policy.dag_id, run)}/steps/prepare/completed.json"
    if corruption == "schema":
        store.put(key, b'{"unexpected":"field"}', "application/json")
    else:
        data = b"{}"
        store.client.put_object(
            Bucket=store.bucket,
            Key=key,
            Body=data,
            ContentType="text/html" if corruption == "type" else "application/json",
            Metadata={"sha256": "0" * 64 if corruption == "integrity" else digest(data)},
            IfNoneMatch="*",
        )
    report = reconcile(
        store, policy, policy.dag_id, run, {"prepare": "success", "inventory": "skipped"}
    )
    task = report.tasks[0]
    assert report.outcome == "execution_failed" and report.validation is None
    assert task.completed is None and "verified completion" in task.missing_evidence
    assert any("Invalid completion evidence" in error for error in task.errors)
    assert report.errors and report.missing_evidence
    identity = digest(run.encode())
    marker = publish(store, report, identity)
    persisted = RunReport.model_validate_json(store.get(marker.reports["run-report.json"]))
    assert persisted.outcome == "execution_failed"
    with pytest.raises(ContractError):
        assert_outcome(store, policy.dag_id, run, identity)


def test_completion_storage_outage_propagates(store: ArtifactStore) -> None:
    import socket

    import boto3
    from airflow_core.reporting import evidence
    from botocore.config import Config
    from botocore.exceptions import EndpointConnectionError

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        client = boto3.client(
            "s3",
            endpoint_url=f"http://127.0.0.1:{port}",
            aws_access_key_id="synthetic",
            aws_secret_access_key="synthetic",
            region_name="us-east-1",
            config=Config(connect_timeout=1, read_timeout=1, retries={"max_attempts": 0}),
        )
        unavailable = ArtifactStore(client)

        # An empty attempts prefix reaches the real unavailable completion-object read.
        class CompletionOutageStore(ArtifactStore):
            def keys(self, prefix: str) -> list[str]:
                return store.keys(prefix)

        with pytest.raises(EndpointConnectionError):
            evidence(CompletionOutageStore(unavailable.client), uuid4().hex, "prepare", "success")
