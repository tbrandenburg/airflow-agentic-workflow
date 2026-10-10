from pathlib import Path

import pytest
from airflow_core.artifacts import ContractError, digest
from airflow_core.patches import apply_patch, inspect_patch
from airflow_core.validation import ValidationReport

PATCH = (
    b"diff --git a/service.py b/service.py\n--- a/service.py\n+++ b/service.py\n"
    b'@@ -1,2 +1,2 @@\n def health() -> str:\n-    return "ok"\n+    return "healthy"\n'
)


def test_disposable_patch_applies_and_bad_context_is_atomic(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    path = source / "service.py"
    path.write_bytes(Path("projects/example-service/fixtures/service.py").read_bytes())
    assert apply_patch(source, PATCH, ["service.py"]) == ["service.py"]
    assert 'return "healthy"' in path.read_text()
    before = path.read_bytes()
    with pytest.raises(ContractError, match="apply cleanly"):
        apply_patch(source, PATCH, ["service.py"])
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "patch",
    [
        PATCH.replace(b"service.py", b"../service.py"),
        PATCH.replace(b"service.py", b"/tmp/service.py"),
        PATCH.replace(b"service.py", b".env"),
        PATCH.replace(b"service.py", b"secrets.py"),
        PATCH.replace(b"b/service.py\n---", b"b/other.py\n---"),
        PATCH.replace(b"--- a/service.py", b"new file mode 120000\n--- /dev/null"),
        PATCH.replace(b"--- a/service.py", b"old mode 100644\nnew mode 100755\n--- a/service.py"),
        PATCH.replace(b"--- a/service.py", b"GIT binary patch\n--- a/service.py"),
        PATCH.replace(b"a/service.py b/service.py", b'"a/service.py" "b/service.py"'),
        PATCH + PATCH,
        b"\x00" + PATCH,
        b"diff --git ",
        b"\xff" + PATCH,
    ],
)
def test_unsafe_patch_rejected(patch: bytes) -> None:
    with pytest.raises(ContractError):
        inspect_patch(patch, ["service.py"])


def test_symlink_patch_rejected(tmp_path: Path) -> None:
    original = tmp_path / "original"
    original.write_bytes(b"immutable")
    source = tmp_path / "source"
    source.mkdir()
    (source / "service.py").symlink_to(original)
    with pytest.raises(ContractError, match="Symlink"):
        apply_patch(source, PATCH, ["service.py"])
    assert original.read_bytes() == b"immutable"


def test_gate_status_cannot_hide_failed_check() -> None:
    data: dict[str, object] = {
        "base_sha": "0" * 40,
        "patch_sha256": digest(PATCH),
        "gate_status": "failed",
        "checks": [
            {
                "name": name,
                "argv": ["fixed"],
                "exit_code": int(name == "tests"),
                "stdout": "",
                "stderr": "",
                "duration_seconds": 0.0,
            }
            for name in ("formatter", "lint", "tests", "secret_scan")
        ],
    }
    assert ValidationReport.model_validate(data).gate_status == "failed"
    with pytest.raises(ValueError, match="differs"):
        ValidationReport.model_validate({**data, "gate_status": "passed"})
