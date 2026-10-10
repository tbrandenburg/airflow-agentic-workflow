"""Strict application responses, separate from OpenCode transport events."""

from typing import Annotated, Literal

from pydantic import Field, TypeAdapter

from airflow_core.contracts import Contract

Text = Annotated[str, Field(min_length=1, max_length=16000)]
Items = Annotated[list[Text], Field(max_length=100)]


class PlannerResponse(Contract):
    role: Literal["planner"]
    plan: Text
    suggested_paths: Items
    assumptions: Items
    validation_focus: Items


class CoderResponse(Contract):
    role: Literal["coder"]
    summary: Text
    unified_diff: str = Field(min_length=1, max_length=262144, pattern=r"^diff --git ")


class Finding(Contract):
    severity: Literal["info", "warning", "error", "critical"]
    description: Text
    recommendations: Items


class ReviewerResponse(Contract):
    role: Literal["reviewer"]
    findings: list[Finding] = Field(max_length=100)
    recommendations: Items
    verdict: Literal["approved", "changes_requested"]


RoleResponse = Annotated[
    PlannerResponse | CoderResponse | ReviewerResponse, Field(discriminator="role")
]
RESPONSE: TypeAdapter[RoleResponse] = TypeAdapter(RoleResponse)
