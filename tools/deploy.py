"""Local deployment ownership, secret generation and port preflight."""

import argparse
import base64
import json
import os
import secrets
import shutil
import socket
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / "infra/airflow/.local"
PORTS = (18080, 18081, 19000, 19001)


def compose(*arguments: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        str(LOCAL / "compose.env"),
        "-f",
        str(ROOT / "infra/airflow/compose.yaml"),
        *arguments,
    ]


def write(path: Path, content: str, mode: int = 0o600) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(content)


def bootstrap() -> None:
    if (LOCAL / "compose.env").exists():
        print("Deployment identity retained")
        return
    LOCAL.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory = LOCAL / "secrets"
    directory.mkdir(exist_ok=True, mode=0o700)
    password = secrets.token_hex(24)
    gateway = os.environ.get("POC_GATEWAY_TOKEN", secrets.token_hex(32))
    airflow = os.environ.get("POC_AIRFLOW_PASSWORD", secrets.token_hex(32))
    operator = os.environ.get("POC_OPERATOR_PASSWORD", secrets.token_hex(32))
    if min(len(gateway), len(airflow), len(operator)) < 32:
        raise ValueError("Deployment credentials must contain at least 32 characters")
    for name, value in {
        "gateway_token": gateway,
        "airflow_password": airflow,
        "operator_password": operator,
        "airflow_passwords.json": json.dumps({"gateway": airflow, "operator": operator}),
        "minio_user": "local-poc",
        "minio_password": os.environ.get("POC_MINIO_PASSWORD", secrets.token_hex(32)),
    }.items():
        write(directory / name, value, 0o444)
    write(
        LOCAL / "postgres.env",
        f"POSTGRES_USER=airflow\nPOSTGRES_DB=airflow\nPOSTGRES_PASSWORD={password}\n",
    )
    database = f"postgresql+psycopg2://airflow:{password}@postgres/airflow"
    write(
        LOCAL / "airflow.env",
        f"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN={database}\n"
        f"AIRFLOW__CELERY__RESULT_BACKEND=db+{database}\n"
        f"AIRFLOW__API_AUTH__JWT_SECRET={secrets.token_hex(32)}\n"
        f"AIRFLOW__CORE__FERNET_KEY={base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()}\n",
    )
    project = f"airflow-p1-{secrets.token_hex(4)}"
    write(LOCAL / "compose.env", f"COMPOSE_PROJECT_NAME={project}\n")
    print(f"Initialized local deployment project={project}; credentials not printed")


def preflight() -> None:
    for port in PORTS:
        with socket.socket() as listener:
            try:
                listener.bind(("127.0.0.1", port))
            except OSError as error:
                raise RuntimeError(
                    f"Required port {port} is occupied; stop this stack first"
                ) from error
    print(f"Free loopback ports: {PORTS}")


def clean() -> None:
    running = subprocess.check_output(compose("ps", "-q"), text=True, timeout=30).strip()
    if running:
        raise RuntimeError("Stop the owned stack before cleaning")
    for name in (".pytest_cache", ".ruff_cache", ".mypy_cache"):
        if (ROOT / name).exists():
            shutil.rmtree(ROOT / name)
    print("Removed check caches; deployment volumes and credentials retained")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["bootstrap", "preflight", "clean"])
    action = parser.parse_args().action
    {"bootstrap": bootstrap, "preflight": preflight, "clean": clean}[action]()


if __name__ == "__main__":
    main()
