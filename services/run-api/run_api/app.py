import asyncio
import hmac
import os
from pathlib import Path
from typing import Annotated
from uuid import uuid4

from airflow_core.artifacts import ArtifactStore, ContractError, canonical, run_key
from airflow_core.policy import Policy as DeploymentPolicy
from fastapi import Depends, FastAPI, HTTPException
from fastapi import Path as ApiPath
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import ValidationError
from starlette.middleware.body_limit import RequestBodyLimitMiddleware

from run_api.airflow import Airflow, UpstreamError
from run_api.models import Policy, RunRequest, RunStatus
from run_api.results import lookup


def create_app() -> FastAPI:
    policy = Policy.load(os.environ.get("POLICY_PATH", "/deployment/policy.json"))
    secret = (
        Path(os.environ.get("GATEWAY_TOKEN_FILE", "/secrets/gateway_token")).read_text().strip()
    )
    if len(secret) < 32:
        raise RuntimeError("Gateway token must contain at least 32 characters")
    passwords = Path("/secrets/airflow_password").read_text().strip()
    airflow = Airflow(os.environ.get("AIRFLOW_URL", "http://api-server:8080"), "gateway", passwords)
    app = FastAPI(title="Local workflow Run API", docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(RequestBodyLimitMiddleware, max_body_size=16384)
    security = HTTPBearer(auto_error=False)

    def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
    ) -> None:
        if credentials is None or not hmac.compare_digest(
            credentials.credentials.encode(), secret.encode()
        ):
            raise HTTPException(
                401, "Authentication required", headers={"WWW-Authenticate": "Bearer"}
            )

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_request: object, _error: RequestValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": "Invalid run request"})

    @app.exception_handler(UpstreamError)
    async def upstream_error(_request: object, error: UpstreamError) -> JSONResponse:
        detail = "Run not found" if error.status == 404 else "Airflow unavailable"
        return JSONResponse(status_code=error.status, content={"detail": detail})

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/platform-runs", status_code=201, dependencies=[Depends(authenticate)])
    async def trigger(request: RunRequest) -> RunStatus:
        if not policy.permits(request):
            raise HTTPException(403, "Request is not approved by deployment policy")
        run = await airflow.request(
            "POST",
            f"/dags/{policy.dag_id}/dagRuns",
            {
                "dag_run_id": f"p1_{uuid4().hex}",
                "logical_date": None,
                "conf": request.model_dump(),
            },
        )
        return RunStatus(**run.model_dump(), approved_sha=policy.approved_sha)

    @app.get("/platform-runs/{run_id}", dependencies=[Depends(authenticate)])
    async def status(run_id: Annotated[str, ApiPath(pattern=r"^p1_[0-9a-f]{32}$")]) -> RunStatus:
        run = await airflow.request("GET", f"/dags/{policy.dag_id}/dagRuns/{run_id}")
        return RunStatus(**run.model_dump(), approved_sha=policy.approved_sha)

    @app.post("/runs", status_code=201, dependencies=[Depends(authenticate)])
    async def workflow_trigger(request: RunRequest) -> RunStatus:
        registry = DeploymentPolicy.load(dag="code_review")
        try:
            registry.approve(request.model_dump(), "code_review", "prepare")
        except ContractError as error:
            raise HTTPException(403, "Request not approved") from error
        identifier = f"p4_{uuid4().hex}"
        store = ArtifactStore.deployment()
        await asyncio.to_thread(store.bootstrap)
        await asyncio.to_thread(
            store.put,
            f"{run_key('code_review', identifier)}/run-input.json",
            canonical(request.model_dump()),
            "application/json",
        )
        run = await airflow.request(
            "POST",
            "/dags/code_review/dagRuns",
            {
                "dag_run_id": identifier,
                "logical_date": None,
                "conf": request.model_dump(),
            },
        )
        return RunStatus(
            **run.model_dump(),
            approved_sha=policy.approved_sha,
            status_url=f"/runs/{identifier}",
            report_url=f"/runs/{identifier}",
            artifacts_url=f"/runs/{identifier}/artifacts",
        )

    async def result(run_id: str) -> dict[str, object]:
        run = await airflow.request("GET", f"/dags/code_review/dagRuns/{run_id}")
        try:
            publication = await asyncio.to_thread(lookup, run_id)
        except (ContractError, ValidationError) as error:
            raise HTTPException(502, "Run publication invalid") from error
        return {**run.model_dump(), "approved_sha": policy.approved_sha, **publication}

    @app.get("/runs/{run_id}", dependencies=[Depends(authenticate)])
    async def workflow_status(
        run_id: Annotated[str, ApiPath(pattern=r"^p4_[0-9a-f]{32}$")],
    ) -> dict[str, object]:
        return await result(run_id)

    @app.get("/runs/{run_id}/artifacts", dependencies=[Depends(authenticate)])
    async def workflow_artifacts(
        run_id: Annotated[str, ApiPath(pattern=r"^p4_[0-9a-f]{32}$")],
    ) -> dict[str, object]:
        return await result(run_id)

    @app.post("/script-runs", status_code=201, dependencies=[Depends(authenticate)])
    async def script_trigger(request: RunRequest) -> RunStatus:
        if (
            (request.project, request.workflow, request.task)
            != (policy.project, "script-smoke", "artifact-smoke")
            or request.base_ref != policy.approved_sha
            or request.model_settings.model != policy.model
        ):
            raise HTTPException(403, "Request is not approved by deployment policy")
        identifier = f"p2_{uuid4().hex}"
        dag = "artifact_script_smoke"
        store = ArtifactStore.deployment()
        await asyncio.to_thread(store.bootstrap)
        await asyncio.to_thread(
            store.put,
            f"{run_key(dag, identifier)}/run-input.json",
            canonical(request.model_dump()),
            "application/json",
        )
        run = await airflow.request(
            "POST",
            f"/dags/{dag}/dagRuns",
            {"dag_run_id": identifier, "logical_date": None, "conf": request.model_dump()},
        )
        return RunStatus(**run.model_dump(), approved_sha=policy.approved_sha)

    @app.get("/script-runs/{run_id}", dependencies=[Depends(authenticate)])
    async def script_status(
        run_id: Annotated[str, ApiPath(pattern=r"^p2_[0-9a-f]{32}$")],
    ) -> RunStatus:
        run = await airflow.request("GET", f"/dags/artifact_script_smoke/dagRuns/{run_id}")
        return RunStatus(**run.model_dump(), approved_sha=policy.approved_sha)

    @app.post("/agent-runs", status_code=201, dependencies=[Depends(authenticate)])
    async def agent_trigger(request: RunRequest) -> RunStatus:
        if request.workflow != "agent_smoke":
            raise HTTPException(403, "Workflow not approved")
        registry = DeploymentPolicy.load(dag="agent_smoke")
        try:
            registry.approve(request.model_dump(), "agent_smoke", "planner")
        except ContractError as error:
            raise HTTPException(403, "Request not approved") from error
        identifier = f"p3_{uuid4().hex}"
        store = ArtifactStore.deployment()
        await asyncio.to_thread(store.bootstrap)
        await asyncio.to_thread(
            store.put,
            f"{run_key('agent_smoke', identifier)}/run-input.json",
            canonical(request.model_dump()),
            "application/json",
        )
        run = await airflow.request(
            "POST",
            "/dags/agent_smoke/dagRuns",
            {
                "dag_run_id": identifier,
                "logical_date": None,
                "conf": request.model_dump(),
            },
        )
        return RunStatus(**run.model_dump(), approved_sha=policy.approved_sha)

    @app.get("/agent-runs/{run_id}", dependencies=[Depends(authenticate)])
    async def agent_status(
        run_id: Annotated[str, ApiPath(pattern=r"^p3_[0-9a-f]{32}$")],
    ) -> RunStatus:
        run = await airflow.request("GET", f"/dags/agent_smoke/dagRuns/{run_id}")
        return RunStatus(**run.model_dump(), approved_sha=policy.approved_sha)

    return app
