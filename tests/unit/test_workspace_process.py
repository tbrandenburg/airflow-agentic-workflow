import io
import os
import tarfile
import threading
import time
from pathlib import Path

import pytest
from airflow_core.artifacts import ContractError, run_key, safe_path
from airflow_core.process import ScriptProcess, script_argv
from airflow_core.workspace import export, read_file, workspace


@pytest.mark.parametrize("path", ["../x", "/x", "a/../b", "a//b", "./a", "a\\b", "a\nx"])
def test_unsafe_paths(path: str) -> None:
    with pytest.raises(ContractError):
        safe_path(path)


def test_run_keys_are_safe_and_unambiguous() -> None:
    assert len(run_key("a/b", "../run")) == 64
    assert run_key("a", "b:c") != run_key("a:b", "c")


def test_independent_workspaces_cleaned_on_failure() -> None:
    with workspace() as first, workspace() as second:
        assert first != second
        (first / "file").write_text("one")
        assert not (second / "file").exists()
    assert not first.exists() and not second.exists()


@pytest.mark.parametrize("kind", [tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE])
def test_export_rejects_links_and_special_files(tmp_path: Path, kind: bytes) -> None:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        member = tarfile.TarInfo("bad")
        member.type = kind
        member.linkname = "/outside"
        archive.addfile(member)
    with pytest.raises(ContractError):
        export(buffer.getvalue(), tmp_path)


def test_output_symlinks_hardlinks_and_bounds(tmp_path: Path) -> None:
    (tmp_path / "file").write_bytes(b"content")
    (tmp_path / "link").symlink_to(tmp_path / "file")
    with pytest.raises(ContractError):
        read_file(tmp_path, "link")
    with pytest.raises(ContractError):
        read_file(tmp_path, "file", 2)
    os.link(tmp_path / "file", tmp_path / "hard")
    with pytest.raises(ContractError):
        read_file(tmp_path, "hard")


def test_fixed_argv_clean_environment_and_nonzero(tmp_path: Path) -> None:
    script = tmp_path / "script.sh"
    script.write_text('test "$#" = 4; env; printf error >&2; exit 7\n')
    assert script_argv(script, tmp_path / "input.json", tmp_path / "output.json") == [
        "bash",
        str(script),
        "--input-manifest",
        str(tmp_path / "input.json"),
        "--output-manifest",
        str(tmp_path / "output.json"),
    ]
    os.environ["TEST_SECRET_CREDENTIAL"] = "must-not-leak"
    try:
        result = ScriptProcess().run(script, tmp_path, 5)
    finally:
        del os.environ["TEST_SECRET_CREDENTIAL"]
    assert result.exit_code == 7 and result.stderr == b"error"
    assert b"TEST_SECRET" not in result.stdout


def test_timeout_kills_descendant_group(tmp_path: Path) -> None:
    script = tmp_path / "script.sh"
    script.write_text("printf start; (sleep 3; touch escaped) & wait\n")
    result = ScriptProcess().run(script, tmp_path, 1)
    assert result.timed_out and result.exit_code != 0 and result.stdout == b"start"
    time.sleep(3)
    assert not (tmp_path / "escaped").exists()


def test_timeout_with_closed_capture_pipes(tmp_path: Path) -> None:
    script = tmp_path / "closed.sh"
    script.write_text("exec 1>&- 2>&-; sleep 10\n")
    started = time.monotonic()
    result = ScriptProcess().run(script, tmp_path, 1)
    assert result.timed_out and time.monotonic() - started < 3


def test_cancellation_kills_process(tmp_path: Path) -> None:
    script = tmp_path / "script.sh"
    script.write_text("sleep 10\n")
    process = ScriptProcess()
    timer = threading.Timer(0.5, process.cancel)
    timer.start()
    try:
        result = process.run(script, tmp_path, 5)
    finally:
        timer.join(timeout=2)
    assert result.cancelled and result.exit_code != 0
