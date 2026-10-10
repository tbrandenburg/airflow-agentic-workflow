"""Read-only mirror inventory for reproducible before/after operational proofs."""

import subprocess
from pathlib import Path

from airflow_core.artifacts import canonical, digest


def mirror_inventory(mirror: Path) -> dict[str, object]:
    files: dict[str, object] = {}
    directories = []
    for path in sorted(mirror.rglob("*")):
        name = str(path.relative_to(mirror))
        if path.is_symlink():
            raise ValueError("Mirror inventory rejects symlinks")
        if path.is_dir():
            directories.append(name)
            continue
        files[name] = {"sha256": digest(path.read_bytes()), "mode": path.stat().st_mode & 0o777}
    refs = subprocess.check_output(
        ["git", "--git-dir", str(mirror), "show-ref", "--head"], timeout=10
    ).decode()
    return {
        "files": files,
        "directories": directories,
        "refs": refs,
        "head": (mirror / "HEAD").read_text(),
        "digest": digest(canonical({"files": files, "directories": directories, "refs": refs})),
    }
