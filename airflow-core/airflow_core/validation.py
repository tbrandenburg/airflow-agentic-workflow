"""Trusted validation gate contract, independent of Airflow task execution success."""

from typing import Literal

from pydantic import Field, model_validator

from airflow_core.contracts import Contract, Digest


class CheckReport(Contract):
    name: Literal["formatter", "lint", "tests", "secret_scan"]
    argv: list[str] = Field(min_length=1, max_length=20)
    exit_code: int
    stdout: str = Field(max_length=65536)
    stderr: str = Field(max_length=65536)
    duration_seconds: float = Field(ge=0)


class ValidationReport(Contract):
    base_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    patch_sha256: Digest
    gate_status: Literal["passed", "failed"]
    checks: list[CheckReport] = Field(min_length=4, max_length=4)

    @model_validator(mode="after")
    def valid_gate(self) -> "ValidationReport":
        if {check.name for check in self.checks} != {"formatter", "lint", "tests", "secret_scan"}:
            raise ValueError("Required validation check set differs")
        expected = "passed" if all(check.exit_code == 0 for check in self.checks) else "failed"
        if self.gate_status != expected:
            raise ValueError("Gate status differs from check results")
        return self
