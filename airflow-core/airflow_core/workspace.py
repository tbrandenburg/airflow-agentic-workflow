"""Disposable independent exports; never checkout or modify the bare source mirror."""

import io
import os
import stat
import subprocess
import tarfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow_core.artifacts import LIMIT, ContractError, safe_path


def file_path(root: Path, relative: str) -> Path:
    safe_path(relative)
    if root.is_symlink() or root.resolve() != root.absolute():
        raise ContractError("Workspace root is not canonical")
    path = root / relative
    for candidate in [path, *path.parents]:
        if candidate == root.parent:
            break
        if candidate.is_symlink():
            raise ContractError("Symlink path rejected")
    if not path.resolve().is_relative_to(root):
        raise ContractError("Path outside workspace")
    return path


def read_file(root: Path, relative: str, limit: int = LIMIT) -> bytes:
    path = file_path(root, relative)
    if not path.exists():
        raise ContractError("Required output file missing")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
            raise ContractError("Output must be a bounded independent regular file")
        data = stream.read(limit + 1)
        if len(data) > limit:
            raise ContractError("Output size limit exceeded")
        return data


def export(data: bytes, destination: Path) -> None:
    total = 0
    seen: set[str] = set()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        for member in archive:
            relative = member.name.rstrip("/")
            safe_path(relative)
            if relative in seen or len(seen) >= 1000:
                raise ContractError("Duplicate or excessive archive members")
            seen.add(relative)
            total += member.size
            if total > LIMIT or not (member.isfile() or member.isdir()):
                raise ContractError("Unsafe or oversized archive")
            archive.extract(member, destination, filter="data")


@contextmanager
def workspace() -> Iterator[Path]:
    with TemporaryDirectory(prefix="airflow-p2-") as directory:
        yield Path(directory).resolve()


def reconstruct(mirror: Path, sha: str, destination: Path, paths: list[str]) -> None:
    for path in paths:
        safe_path(path)
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "GIT_CONFIG_NOSYSTEM": "1", "HOME": "/tmp"}
    actual = subprocess.check_output(
        ["git", "--git-dir", str(mirror), "rev-parse", f"{sha}^{{commit}}"],
        env=env,
        timeout=15,
        text=True,
    ).strip()
    if actual != sha:
        raise ContractError("Approved commit mismatch")
    data = subprocess.check_output(
        ["git", "--git-dir", str(mirror), "archive", "--format=tar", sha, "--", *paths],
        env=env,
        timeout=15,
    )
    if len(data) > LIMIT:
        raise ContractError("Source archive too large")
    export(data, destination)
