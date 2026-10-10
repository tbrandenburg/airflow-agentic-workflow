"""Script runtime: storage-only inputs, isolated workspaces and immutable attempt evidence."""

import socket
from datetime import UTC, datetime
from pathlib import Path

from airflow_core import RUNTIME_VERSION
from airflow_core.artifacts import ArtifactStore, ContractError, canonical, digest, run_key
from airflow_core.completion import complete, reuse
from airflow_core.contracts import AgentOutput, ArtifactRef, InputManifest, Outcome, TaskResult
from airflow_core.policy import Policy
from airflow_core.process import LogLimitError, ScriptProcess
from airflow_core.task_logging import event, reused
from airflow_core.workspace import export, read_file, reconstruct, workspace


class ExecutionError(RuntimeError):
    """A script failed after its diagnostics were persisted."""


def now() -> str:
    return datetime.now(UTC).isoformat()


def fingerprint(
    policy: Policy,
    request: dict[str, object],
    dag: str,
    run: str,
    task: str,
    inputs: dict[str, ArtifactRef],
    script: Path,
) -> str:
    return digest(
        canonical(
            {
                "request": request,
                "dag": dag,
                "run": run,
                "task": task,
                "inputs": {name: ref.model_dump(mode="json") for name, ref in inputs.items()},
                "policy": policy.registry_digest,
                "runtime": RUNTIME_VERSION,
                "script": digest(script.read_bytes()),
            }
        )
    )


def validate_outputs(manifest: InputManifest, root: Path, data: bytes) -> dict[str, bytes]:
    output = AgentOutput.model_validate_json(data)
    for field in ("dag_id", "dag_run_id", "task_id", "attempt", "base_sha"):
        if getattr(output, field) != getattr(manifest, field):
            raise ContractError("Output manifest identity mismatch")
    expected = {item.path: item for item in manifest.expected_outputs}
    if len(expected) != len(output.outputs) or set(expected) != {f.path for f in output.outputs}:
        raise ContractError("Output set differs from policy")
    files: dict[str, bytes] = {}
    for item in output.outputs:
        registered = expected[item.path]
        if item.content_type != registered.content_type or item.max_bytes != registered.max_bytes:
            raise ContractError("Output bounds or type differ from policy")
        content = read_file(root, item.path, registered.max_bytes)
        if len(content) != item.size or digest(content) != item.sha256:
            raise ContractError("Output file checksum or size mismatch")
        if item.content_type == "application/json":
            import json

            json.loads(content)
        files[item.path] = content
    return files


def execute_script(
    store: ArtifactStore,
    policy: Policy,
    request: dict[str, object],
    dag: str,
    run: str,
    task: str,
    attempt: int,
    inputs: dict[str, ArtifactRef],
    process: ScriptProcess,
) -> TaskResult:
    script = policy.approve(request, dag, task)
    identity = fingerprint(policy, request, dag, run, task, inputs, script)
    if process.identity() != "script":
        identity = digest(canonical({"execution": identity, "transport": process.identity()}))
    prefix = f"{run_key(dag, run)}/steps/{task}"
    previous = reuse(store, prefix, identity)
    if previous is not None:
        reused(store, prefix, attempt, previous)
        return previous
    attempt_prefix = f"{prefix}/attempts/{attempt}"
    started = now()
    with workspace() as root:
        manifest = InputManifest(
            dag_id=dag,
            dag_run_id=run,
            task_id=task,
            attempt=attempt,
            project=policy.project,
            workflow=policy.workflow,
            base_sha=policy.base_sha,
            task_request=str(request["task"]),
            policy_version=policy.version,
            runtime_version=RUNTIME_VERSION,
            input_artifacts=inputs,
            expected_outputs=policy.outputs[task],
            workspace_path=str(root),
            timeout_seconds=policy.timeout,
        )
        input_bytes = canonical(manifest)
        store.put(f"{attempt_prefix}/input.json", input_bytes, "application/json")
        (root / "input.json").write_bytes(input_bytes)
        code: int | None = None
        stdout = b""
        stderr = b""
        outputs: dict[str, ArtifactRef] = {}
        output_ref: ArtifactRef | None = None
        output_digest: str | None = None
        outcome: Outcome = "passed"
        errors: list[str] = []
        failure: BaseException | None = None
        try:
            source = root / "source"
            source.mkdir()
            required = set(policy.inputs[task]) - policy.optional_inputs(task)
            if not required <= set(inputs) <= set(policy.inputs[task]):
                raise ContractError("Input artifact set differs from registered policy")
            if policy.independent_base or not policy.inputs[task]:
                reconstruct(policy.mirror, policy.base_sha, source, policy.sources)
            for name, ref in inputs.items():
                store.download(ref, root, f"inputs/{name}")
                if ref.content_type == "application/x-tar" and not policy.independent_base:
                    export(store.get(ref), source)
            result = process.run(script, root, policy.timeout)
            code, stdout, stderr = result.exit_code, result.stdout, result.stderr
            if result.cancelled:
                outcome = "cancelled"
                raise ExecutionError("Script cancelled")
            if result.timed_out:
                raise ExecutionError("Script timeout")
            if code != 0:
                raise ExecutionError(f"Script exit code {code}")
            output_bytes = read_file(root, "output.json", 65536)
            files = validate_outputs(manifest, root, output_bytes)
            if policy.independent_base and task == "validate":
                from airflow_core.validation import ValidationReport

                gate = ValidationReport.model_validate_json(files["validation.json"])
                if (gate.base_sha, gate.patch_sha256) != (
                    policy.base_sha,
                    inputs["patch.diff"].sha256,
                ):
                    raise ContractError("Validation gate identity differs from verified inputs")
            output = AgentOutput.model_validate_json(output_bytes).model_copy(
                update={
                    "policy_version": policy.version,
                    "runtime_version": RUNTIME_VERSION,
                    "input_digest": digest(input_bytes),
                    "output_digest": digest(
                        canonical({name: digest(data) for name, data in files.items()})
                    ),
                    "started_at": started,
                    "ended_at": now(),
                    "worker": socket.gethostname(),
                }
            )
            output_bytes = canonical(output)
            for expected in manifest.expected_outputs:
                outputs[expected.path] = store.put(
                    f"{attempt_prefix}/outputs/{expected.path}",
                    files[expected.path],
                    expected.content_type,
                )
            output_ref = store.put(
                f"{attempt_prefix}/output.json", output_bytes, "application/json"
            )
            output_digest = digest(output_bytes)
        except BaseException as error:
            process.kill()
            if process.result is not None:
                code = process.result.exit_code
                stdout, stderr = process.result.stdout, process.result.stderr
            if isinstance(error, LogLimitError):
                stdout, stderr = error.stdout, error.stderr
            failure = error
            outcome = "cancelled" if process.cancelled else "execution_failed"
            errors = [
                str(error)
                if isinstance(error, (ContractError, ExecutionError))
                else type(error).__name__
            ]
        diagnostics = {
            "stdout": store.put(f"{attempt_prefix}/stdout.log", stdout, "text/plain"),
            "stderr": store.put(f"{attempt_prefix}/stderr.log", stderr, "text/plain"),
        }
        for name, data in process.diagnostics(root).items():
            diagnostics[name] = store.put(f"{attempt_prefix}/{name}", data, "text/plain")
        evidence = TaskResult(
            **manifest.model_dump(),
            outcome=outcome,
            input_fingerprint=identity,
            input_digest=digest(input_bytes),
            output_digest=output_digest,
            started_at=started,
            ended_at=now(),
            worker=socket.gethostname(),
            exit_code=code,
            errors=errors,
            outputs=outputs,
            diagnostics=diagnostics,
            output_manifest=output_ref,
        )
        operational = event(store, attempt_prefix, evidence, process)
        evidence = evidence.model_copy(
            update={"diagnostics": {**diagnostics, "task-log.json": operational}}
        )
        ref = store.put(
            f"{attempt_prefix}/attempt-result.json", canonical(evidence), "application/json"
        )
        if failure is not None:
            raise failure
        return complete(store, prefix, identity, ref)
