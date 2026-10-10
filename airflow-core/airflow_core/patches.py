"""Conservative unified-diff admission before disposable git apply."""

import re
import subprocess
from pathlib import Path

from airflow_core.artifacts import ContractError, safe_path
from airflow_core.workspace import read_file


def approved_path(path: str, allowlist: list[str]) -> str:
    safe_path(path)
    if (
        path not in allowlist
        or not re.fullmatch(r"[A-Za-z0-9_./-]+", path)
        or any(part.startswith(".") for part in path.split("/"))
        or any(word in path.lower() for word in ("secret", "credential", "policy", "auth"))
    ):
        raise ContractError("Protected or unapproved source path")
    return path


def inspect_patch(data: bytes, allowlist: list[str], maximum: int = 10) -> list[str]:
    if not 1 <= len(data) <= 262144 or b"\x00" in data:
        raise ContractError("Invalid patch size or binary content")
    try:
        text = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ContractError("Patch is not UTF-8 text") from error
    sections = re.split(r"(?m)^diff --git ", text)
    if sections[0] or not 1 <= len(sections) - 1 <= maximum:
        raise ContractError("Patch must contain bounded git diff sections")
    paths = []
    for section in sections[1:]:
        lines = section.splitlines()
        if not lines:
            raise ContractError("Empty diff section")
        header = re.fullmatch(r"a/([A-Za-z0-9_./-]+) b/([A-Za-z0-9_./-]+)", lines[0])
        if header is None or header[1] != header[2]:
            raise ContractError("Quoted, renamed or unsafe diff path")
        path = approved_path(header[1], allowlist)
        if path in paths:
            raise ContractError("Duplicate patch file")
        paths.append(path)
        position = 1
        if position < len(lines) and lines[position].startswith("index "):
            if not re.fullmatch(r"index [0-9a-f]+\.\.[0-9a-f]+(?: 100644)?", lines[position]):
                raise ContractError("Unsafe patch index or mode")
            position += 1
        if lines[position : position + 2] != [f"--- a/{path}", f"+++ b/{path}"]:
            raise ContractError("Only regular existing-file modifications permitted")
        if not any(line.startswith("@@ ") for line in lines[position + 2 :]):
            raise ContractError("Patch has no unified hunk")
        for line in lines[position + 2 :]:
            if not line.startswith(("@@ ", " ", "+", "-", "\\ No newline at end of file")):
                raise ContractError("Unsupported patch directive")
    return paths


def apply_patch(source: Path, data: bytes, allowlist: list[str]) -> list[str]:
    paths = inspect_patch(data, allowlist)
    for path in paths:
        read_file(source, path, 65536)
    environment = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(source.parent),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CEILING_DIRECTORIES": str(source.parent),
    }
    for flags in (["--check"], []):
        result = subprocess.run(
            ["git", "apply", "--whitespace=error-all", *flags, "-"],
            input=data,
            cwd=source,
            env=environment,
            capture_output=True,
            timeout=15,
        )
        if result.returncode:
            raise ContractError("Patch does not apply cleanly to approved base")
    for path in paths:
        read_file(source, path, 65536)
    return paths
