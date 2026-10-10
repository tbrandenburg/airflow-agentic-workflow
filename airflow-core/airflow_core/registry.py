"""Fingerprint executable dependencies without inspecting credential files."""

from pathlib import Path

from airflow_core.artifacts import canonical, digest


def registry_digest(policy: Path) -> str:
    deployment = policy.parent
    core = Path(__file__).parent
    runtime = Path("/deployment/agent-runtime")
    if not runtime.exists():
        runtime = core.parents[1] / "packages/agent-runtime"
    paths = {policy, deployment / "workflow_policy.py", deployment / "operational_policy.py"}
    for directory, patterns in (
        (deployment / "scripts", ("*.py", "*.sh")),
        (deployment / "prompts", ("*.txt",)),
        (core, ("*.py",)),
        (runtime / "dist", ("*.js", "*.json")),
        (runtime / "src", ("*.ts",)),
    ):
        for pattern in patterns:
            paths.update(directory.glob(pattern))
    paths.update(
        {runtime / "package.json", runtime / "package-lock.json", deployment / "policy.json"}
    )
    # Missing optional source files in the built image are explicit identity entries.
    identity: dict[str, object] = {
        str(path): digest(path.read_bytes()) if path.is_file() else None for path in sorted(paths)
    }
    return digest(canonical(identity))
