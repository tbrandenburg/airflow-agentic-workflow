"""Fixed validation commands; each check captures failure independently."""

import time
from pathlib import Path

from airflow_core.artifacts import ContractError, digest
from airflow_core.process import ScriptProcess
from airflow_core.validation import CheckReport, ValidationReport


def validate(source: Path, patch: bytes, sha: str, failing: bool) -> ValidationReport:
    test = (
        "/deployment/scripts/test_health_fail.py"
        if failing
        else "/deployment/scripts/test_health.py"
    )
    commands = {
        "formatter": ["python", "-m", "ruff", "format", "--check", "service.py"],
        "lint": ["python", "-m", "ruff", "check", "--isolated", "service.py"],
        "tests": ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider", test],
        "secret_scan": ["python", "/deployment/scripts/secret_scan.py", "service.py"],
    }
    reports = []
    environment = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(source.parent),
        "TMPDIR": str(source.parent),
        "LANG": "C.UTF-8",
        "PYTHONPATH": f"{source}:/home/airflow/.local/lib/python3.12/site-packages",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
    }
    for name, argv in commands.items():
        started = time.monotonic()
        result = ScriptProcess().invoke(argv, source, 20, environment=environment, limit=65536)
        if result.timed_out or result.cancelled:
            raise ContractError("Validation subprocess interrupted")
        reports.append(
            CheckReport.model_validate(
                {
                    "name": name,
                    "argv": argv,
                    "exit_code": result.exit_code,
                    "stdout": result.stdout.decode(errors="replace"),
                    "stderr": result.stderr.decode(errors="replace"),
                    "duration_seconds": time.monotonic() - started,
                }
            )
        )
    return ValidationReport(
        base_sha=sha,
        patch_sha256=digest(patch),
        checks=reports,
        gate_status="passed" if all(check.exit_code == 0 for check in reports) else "failed",
    )
