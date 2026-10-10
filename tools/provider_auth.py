"""Explicitly authorized one-provider staging; credential values never enter output."""

import argparse
import hashlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOST = Path.home() / ".local/share/opencode/auth.json"
DESTINATION = ROOT / "infra/airflow/.local/secrets/agent_provider.json"
PROVIDER = "github-copilot"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["metadata", "stage", "verify"])
    parser.add_argument("--run-root")
    arguments = parser.parse_args()
    action = arguments.action
    before = HOST.read_bytes()
    data: object = json.loads(before)
    if not isinstance(data, dict):
        raise ValueError("Invalid credential store")
    if action == "metadata":
        print(
            json.dumps(
                {
                    name: {"type": value.get("type")}
                    for name, value in data.items()
                    if isinstance(name, str) and isinstance(value, dict)
                }
            )
        )
        return
    if action == "verify":
        original = (DESTINATION.parent / "agent_host.sha256").read_text()
        unchanged = hashlib.sha256(before).hexdigest() == original
        print(json.dumps({"host_auth_unchanged": unchanged}))
        if not unchanged:
            raise RuntimeError("Original provider auth changed")
        if arguments.run_root:
            from airflow_core.artifacts import ArtifactStore

            store = ArtifactStore.deployment("http://127.0.0.1:19000", DESTINATION.parent)
            selected = json.loads(DESTINATION.read_bytes())[PROVIDER]
            secrets = [selected[name].encode() for name in ("access", "refresh")]
            keys = store.keys(arguments.run_root + "/")
            clean = bool(keys) and all(
                not any(secret in store.read(key)[1] for secret in secrets) for key in keys
            )
            print(
                json.dumps(
                    {"artifact_objects_checked": len(keys), "credential_values_absent": clean}
                )
            )
            if not clean:
                raise RuntimeError("Credential artifact check failed")
        return
    credential = data.get(PROVIDER)
    if not isinstance(credential, dict) or credential.get("type") != "oauth":
        raise ValueError("Approved OAuth provider unavailable")
    selected = {name: credential[name] for name in ("type", "refresh", "access", "expires")}
    if any(
        not isinstance(selected[name], str) or not selected[name] for name in ("refresh", "access")
    ):
        raise ValueError("Approved credential invalid")
    if credential.get("enterpriseUrl"):
        raise ValueError("Only approved public GitHub Copilot endpoint is supported")
    descriptor = os.open(DESTINATION, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump({PROVIDER: selected}, stream)
    # Parent stays owner-only; the read-only file bind is readable by worker UID 50000.
    DESTINATION.chmod(0o444)
    (DESTINATION.parent / "agent_host.sha256").write_text(hashlib.sha256(before).hexdigest())
    print(
        json.dumps(
            {
                "provider": PROVIDER,
                "type": "oauth",
                "staged": True,
                "host_auth_unchanged": HOST.read_bytes() == before,
            }
        )
    )


if __name__ == "__main__":
    main()
