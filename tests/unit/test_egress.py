import asyncio
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
from airflow_core.process_tree import ProcessTree


def proxy_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("egress_proxy", Path("infra/egress/proxy.py"))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_proxy_rejects_arbitrary_authorities_over_real_socket() -> None:
    async def verify() -> None:
        proxy = proxy_module().Proxy()
        server = await asyncio.start_server(proxy.handle, "127.0.0.1", 0)
        async with server:
            port = server.sockets[0].getsockname()[1]
            for target in (
                "example.com:443",
                "localhost:443",
                "127.0.0.1:443",
                "minio:9000",
                "github.com:80",
                "api.github.com.evil.invalid:443",
                "githubcopilot.com:443",
                "user@github.com:443",
                "api.github.com:443/path",
            ):
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.write(f"CONNECT {target} HTTP/1.1\r\n\r\n".encode())
                await writer.drain()
                assert (await asyncio.wait_for(reader.read(), 5)).startswith(b"HTTP/1.1 403")
                writer.close()
                await writer.wait_closed()
            assert proxy.denied == 9 and proxy.accepted == 0

    asyncio.run(verify())


def test_pid_reuse_cannot_admit_unowned_descendants(monkeypatch: pytest.MonkeyPatch) -> None:
    tree = ProcessTree()
    tree.owned = {100: "original", 101: "child-original"}
    snapshot = {
        100: (1, "replacement", "S"),
        101: (1, "child-replacement", "S"),
        102: (100, "new-child", "S"),
        103: (101, "new-descendant", "S"),
    }
    monkeypatch.setattr("airflow_core.process_tree.processes", lambda: snapshot)
    tree.track(100)
    assert tree.owned == {100: "original", 101: "child-original"}
    tree.track(200)
    assert tree.owned == {100: "original", 101: "child-original"}
