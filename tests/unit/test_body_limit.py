from collections.abc import Iterator

from fastapi import FastAPI, Request
from starlette.middleware.body_limit import RequestBodyLimitMiddleware
from starlette.testclient import TestClient


def test_raw_and_streamed_body_limits_before_parsing() -> None:
    app = FastAPI()
    app.add_middleware(RequestBodyLimitMiddleware, max_body_size=16384)

    @app.post("/")
    async def endpoint(request: Request) -> dict[str, int]:
        return {"size": len(await request.body())}

    def chunks() -> Iterator[bytes]:
        yield b"x" * 8192
        yield b"x" * 8193

    with TestClient(app) as client:
        assert client.post("/", content=b"x" * 16384).json() == {"size": 16384}
        assert client.post("/", content=b"x" * 16385).status_code == 413
        assert client.post("/", content=chunks()).status_code == 413
