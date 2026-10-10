from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class ModelSettings(StrictModel):
    model: str = Field(min_length=1, max_length=100)


class RunRequest(StrictModel):
    project: str = Field(min_length=1, max_length=64)
    workflow: str = Field(min_length=1, max_length=64)
    task: str = Field(min_length=1, max_length=500)
    base_ref: str = Field(pattern=r"^[0-9a-f]{40}$")
    model_settings: ModelSettings


class Policy(StrictModel):
    project: str
    workflow: str
    task: str
    dag_id: str
    approved_sha: str
    model: str
    mirror: str

    def permits(self, request: RunRequest) -> bool:
        return (
            (request.project, request.workflow, request.task)
            == (self.project, self.workflow, self.task)
            and request.base_ref == self.approved_sha
            and request.model_settings.model == self.model
        )

    @classmethod
    def load(cls, path: str) -> "Policy":
        return cls.model_validate_json(Path(path).read_text())


class Token(BaseModel):
    access_token: str


class AirflowRun(BaseModel):
    dag_run_id: str
    dag_id: str
    state: Literal["queued", "running", "success", "failed"]


class RunStatus(AirflowRun):
    approved_sha: str
    status_url: str | None = None
    report_url: str | None = None
    artifacts_url: str | None = None
