from typing import Literal

import httpx
from pydantic import ValidationError

from run_api.models import AirflowRun, Token


class UpstreamError(Exception):
    def __init__(self, status: int = 502) -> None:
        self.status = status
        super().__init__("Airflow request failed")


class Airflow:
    def __init__(self, url: str, username: str, password: str) -> None:
        self.url = url
        self.username = username
        self.password = password

    async def request(
        self, method: Literal["GET", "POST"], path: str, body: dict[str, object] | None = None
    ) -> AirflowRun:
        try:
            async with httpx.AsyncClient(base_url=self.url, timeout=15.0) as client:
                login = await client.post(
                    "/auth/token", json={"username": self.username, "password": self.password}
                )
                login.raise_for_status()
                token = Token.model_validate_json(login.content).access_token
                response = await client.request(
                    method,
                    f"/api/v2{path}",
                    json=body,
                    headers={"Authorization": f"Bearer {token}"},
                )
                if response.status_code == 404:
                    raise UpstreamError(404)
                response.raise_for_status()
                return AirflowRun.model_validate_json(response.content)
        except (httpx.HTTPError, ValidationError) as error:
            raise UpstreamError() from error
