import asyncio
import socket

import pytest
from run_api.airflow import Airflow, UpstreamError


def test_unavailable_airflow_propagates_explicit_error() -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        client = Airflow(f"http://127.0.0.1:{port}", "fixture", "fixture")
        with pytest.raises(UpstreamError) as failure:
            asyncio.run(client.request("POST", "/dags/platform_smoke/dagRuns", {}))
        assert failure.value.status == 502
        assert str(failure.value) == "Airflow request failed"
