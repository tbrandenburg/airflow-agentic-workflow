"""Worker-local read-only inventory and cleanup probes for the owned Compose stack."""

import json
import subprocess
from typing import cast

from deploy import compose


def worker(service: str, code: str) -> dict[str, object]:
    output = subprocess.check_output(
        compose("exec", "-T", service, "python", "-c", code), timeout=30
    )
    return cast(dict[str, object], json.loads(output))


def inventory() -> dict[str, dict[str, object]]:
    code = (
        "import json; from pathlib import Path; "
        "from airflow_core.integrity import mirror_inventory; "
        "print(json.dumps(mirror_inventory(Path('/mirrors/example-service.git'))))"
    )
    results = {service: worker(service, code) for service in ("worker-1", "worker-2")}
    assert results["worker-1"] == results["worker-2"]
    return results


def cleaned() -> dict[str, dict[str, object]]:
    code = (
        "import json,os; from pathlib import Path; "
        "print(json.dumps({'workspaces': [str(p) "
        "for pattern in ('airflow-p2-*','agent-session-*') "
        "for p in Path('/tmp').glob(pattern)], 'uid':os.getuid(), "
        "'docker_socket':Path('/var/run/docker.sock').exists()}))"
    )
    result = {service: worker(service, code) for service in ("worker-1", "worker-2")}
    for entry in result.values():
        assert entry["workspaces"] == [] and entry["uid"] != 0 and not entry["docker_socket"]
    return result


def capacity() -> dict[str, dict[str, object]]:
    code = (
        "import json; from airflow.configuration import conf; "
        "print(json.dumps({'parallelism':conf.getint('core','parallelism'), "
        "'concurrency':conf.getint('celery','worker_concurrency')}))"
    )
    result = {service: worker(service, code) for service in ("worker-1", "worker-2")}
    for entry in result.values():
        assert entry["parallelism"] == 4 and entry["concurrency"] == 2
    return result
