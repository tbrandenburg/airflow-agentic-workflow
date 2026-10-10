"""Versioned wire contracts; JSON schemas are generated from these strict models."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Identifier = Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]{1,80}$")]
ContentType = Literal["application/json", "text/plain", "text/markdown", "application/x-tar"]
Outcome = Literal["passed", "validation_failed", "execution_failed", "cancelled", "incomplete"]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    schema_version: Literal["1.0"] = "1.0"


class ArtifactRef(Contract):
    uri: str = Field(pattern=r"^s3://[a-z0-9-]+/[a-zA-Z0-9_./-]+$")
    content_type: ContentType
    size: int = Field(ge=0, le=8_388_608)
    sha256: Digest


class ExpectedOutput(Contract):
    path: str = Field(min_length=1, max_length=200)
    content_type: ContentType
    max_bytes: int = Field(ge=1, le=8_388_608)


class InputManifest(Contract):
    dag_id: Identifier
    dag_run_id: str = Field(min_length=1, max_length=250)
    task_id: Identifier
    attempt: int = Field(ge=1)
    project: Identifier
    workflow: Identifier
    base_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    task_request: str = Field(min_length=1, max_length=500)
    policy_version: str
    runtime_version: str
    input_artifacts: dict[str, ArtifactRef] = Field(max_length=20)
    expected_outputs: list[ExpectedOutput] = Field(max_length=20)
    workspace_path: str = Field(min_length=1)
    timeout_seconds: int = Field(ge=1, le=120)


class OutputFile(ExpectedOutput):
    size: int = Field(ge=0, le=8_388_608)
    sha256: Digest


class AgentOutput(Contract):
    """Also the script output contract; worker identity/digests are added by the runtime."""

    dag_id: Identifier
    dag_run_id: str
    task_id: Identifier
    attempt: int = Field(ge=1)
    base_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    outputs: list[OutputFile] = Field(max_length=20)
    summary: str = Field(max_length=2000)
    policy_version: str | None = None
    runtime_version: str | None = None
    input_digest: Digest | None = None
    output_digest: Digest | None = None
    started_at: str | None = None
    ended_at: str | None = None
    worker: str | None = None
    patch: ArtifactRef | None = None
    validation: dict[str, JsonValue] | None = None
    review: dict[str, JsonValue] | None = None


class TaskResult(InputManifest):
    outcome: Outcome
    input_fingerprint: Digest
    input_digest: Digest
    output_digest: Digest | None
    started_at: str
    ended_at: str
    worker: str
    exit_code: int | None
    errors: list[str]
    outputs: dict[str, ArtifactRef]
    diagnostics: dict[str, ArtifactRef]
    output_manifest: ArtifactRef | None


class Completion(Contract):
    input_fingerprint: Digest
    result: ArtifactRef


class TaskEvidence(Contract):
    task_id: Identifier
    state: str | None
    attempts: list[TaskResult]
    completed: ArtifactRef | None
    missing_evidence: list[str]
    skipped_reason: str | None
    errors: list[str]


class RunReport(Contract):
    dag_id: Identifier
    dag_run_id: str
    project: Identifier
    workflow: Identifier
    base_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    policy_version: str
    runtime_version: str
    generated_at: str
    outcome: Outcome
    airflow_terminal_state: str | None
    tasks: list[TaskEvidence]
    missing_evidence: list[str]
    errors: list[str]
    artifact_index: dict[str, ArtifactRef]
    patch: ArtifactRef | None
    validation: dict[str, JsonValue] | None
    review: dict[str, JsonValue] | None
    request_id: str | None = None
    task_request: str | None = None
    failed_step: str | None = None
    dag_url: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    duration_seconds: float | None = None
    patch_summary: dict[str, JsonValue] | None = None
    validation_artifact: ArtifactRef | None = None


class Publication(Contract):
    input_fingerprint: Digest
    reports: dict[str, ArtifactRef]
