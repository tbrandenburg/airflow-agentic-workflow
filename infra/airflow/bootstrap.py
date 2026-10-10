"""Initialize only deployment-owned volumes; never the host source repository."""

import json
import os
import shutil
import subprocess
from pathlib import Path

from airflow_core.operations import bounded


def git(*arguments: str, cwd: Path) -> str:
    return subprocess.check_output(["git", *arguments], cwd=cwd, text=True, timeout=30).strip()


def main() -> None:
    shutil.copyfile("/secrets/airflow_passwords.json", "/auth/airflow_passwords.json")
    os.chown("/auth", 50000, 0)
    os.chown("/auth/airflow_passwords.json", 50000, 0)
    os.chmod("/auth/airflow_passwords.json", 0o600)
    policy = json.loads(Path("/deployment/policy.json").read_text())
    mirror = Path(policy["mirror"])
    if not mirror.exists():
        source = Path("/tmp/synthetic")
        source.mkdir(exist_ok=True)
        shutil.copyfile("/fixture/service.py", source / "service.py")
        git("init", "--initial-branch=main", cwd=source)
        git("add", "service.py", cwd=source)
        identity = "Synthetic Fixture"
        os.environ.update(
            GIT_AUTHOR_NAME=identity,
            GIT_COMMITTER_NAME=identity,
            GIT_AUTHOR_EMAIL="fixture@example.invalid",
            GIT_COMMITTER_EMAIL="fixture@example.invalid",
            GIT_AUTHOR_DATE="@0 +0000",
            GIT_COMMITTER_DATE="@0 +0000",
        )
        git("commit", "-m", "Phase 1 synthetic fixture", cwd=source)
        git("clone", "--mirror", str(source), str(mirror), cwd=source)
    actual = git("--git-dir", str(mirror), "rev-parse", "refs/heads/main", cwd=Path("/tmp"))
    if actual != policy["approved_sha"]:
        raise RuntimeError("Synthetic mirror does not match deployment-approved SHA")
    for directory in (Path("/mirrors"), Path("/opt/airflow/logs")):
        for path in [directory, *directory.rglob("*")]:
            os.chown(path, 50000, 0)
    os.chown("/data", 1000, 1000)
    subprocess.run(["/entrypoint", "airflow", "db", "migrate"], check=True, timeout=180)
    subprocess.run(
        [
            "/entrypoint",
            "airflow",
            "pools",
            "set",
            "platform",
            "2",
            "Phase 1 bounded platform tasks",
        ],
        check=True,
        timeout=30,
    )
    print(f"Initialized synthetic mirror approved_sha={actual}")
    for pool, slots in (
        ("agent-model", bounded("POC_AGENT_POOL_SLOTS", 2, 2)),
        ("validation", bounded("POC_VALIDATION_POOL_SLOTS", 1, 1)),
    ):
        subprocess.run(
            ["/entrypoint", "airflow", "pools", "set", pool, str(slots), "P5 bounded tasks"],
            check=True,
            timeout=30,
        )


if __name__ == "__main__":
    main()
