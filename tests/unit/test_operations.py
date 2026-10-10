import sys
import threading
from pathlib import Path

import pytest
from airflow_core.operations import bounded
from airflow_core.process import LogLimitError, ScriptProcess
from airflow_core.registry import registry_digest


def test_invalid_operational_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    for value in ("0", "3", "garbage"):
        monkeypatch.setenv("POC_AGENT_POOL_SLOTS", value)
        with pytest.raises(ValueError):
            bounded("POC_AGENT_POOL_SLOTS", 2, 2)


def test_helper_and_runtime_dependencies_invalidate_registry(tmp_path: Path) -> None:
    policy = tmp_path / "project_policy.py"
    policy.write_text("fixed registry")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    helper = scripts / "checks.py"
    helper.write_text("first")
    before = registry_digest(policy)
    helper.write_text("second")
    assert before != registry_digest(policy)
    helper.unlink()
    assert before != registry_digest(policy)


def test_bounded_capture_and_file_limits(tmp_path: Path) -> None:
    process = ScriptProcess()
    with pytest.raises(LogLimitError):
        process.invoke(
            [sys.executable, "-c", "import os; os.write(1,b'x'*131072)"], tmp_path, 5, limit=1024
        )
    assert process.tree.live() == []
    result = ScriptProcess().invoke(
        [
            sys.executable,
            "-c",
            "import resource; print(resource.getrlimit(resource.RLIMIT_FSIZE)); "
            "print(resource.getrlimit(resource.RLIMIT_CPU))",
        ],
        tmp_path,
        5,
    )
    assert b"(8388608, 8388608)" in result.stdout and b"(120, 120)" in result.stdout


@pytest.mark.parametrize("cancel", [False, True])
def test_detached_descendant_cleanup(tmp_path: Path, cancel: bool) -> None:
    script = tmp_path / "tree.py"
    script.write_text(
        "import subprocess,time\np=subprocess.Popen(['sleep','30'],start_new_session=True)\n"
        "open('pid','w').write(str(p.pid))\ntime.sleep(30)\n"
    )
    process = ScriptProcess()
    timer = threading.Timer(1, process.cancel)
    if cancel:
        timer.start()
    try:
        result = process.invoke([sys.executable, str(script)], tmp_path, 2)
    finally:
        if cancel:
            timer.join(timeout=5)
    assert result.cancelled if cancel else result.timed_out
    assert process.tree.live() == []
    pid = int((tmp_path / "pid").read_text())
    path = Path(f"/proc/{pid}/stat")
    assert not path.exists() or path.read_text().split(") ")[1].startswith("Z")
