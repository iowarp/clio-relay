"""Wire shape of the remote FRP transport-probe cleanup payload.

Split out of ``transport_probe.py`` (iowarp/clio-relay#231): the bounded,
strict pydantic models the remote cleanup script (rendered inline inside
``transport_probe_remote_cleanup.py``'s ``_cleanup_remote_probe``) emits as
its single trailing JSON line, and that ``_cleanup_remote_probe`` validates
the parsed line against before trusting any resource outcome it reports.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_REMOTE_CLEANUP_OUTPUT_BYTES = 1024 * 1024
MAX_REMOTE_CLEANUP_RESOURCES = 128


class _RemoteCleanupResource(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: str = Field(min_length=1, max_length=128)
    pid: int = Field(gt=0)
    outcome: Literal["missing", "replaced", "refused", "stopped", "residual"]
    ownership_verified: bool


class _RemoteResidualProcess(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    pid: int = Field(gt=0)
    pgid: int = Field(gt=0)
    state: str = Field(min_length=1, max_length=32)


class _RemoteCleanupPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    outcome: Literal[
        "passed",
        "failed",
        "metadata_missing",
        "invalid_metadata",
        "ownership_refused",
    ]
    completed_at: str | None = Field(default=None, max_length=128)
    resources: list[_RemoteCleanupResource] = Field(
        default_factory=lambda: list[_RemoteCleanupResource](),
        max_length=MAX_REMOTE_CLEANUP_RESOURCES,
    )
    residual_processes: list[_RemoteResidualProcess] = Field(
        default_factory=lambda: list[_RemoteResidualProcess](),
        max_length=MAX_REMOTE_CLEANUP_RESOURCES,
    )
    errors: list[str] = Field(default_factory=list, max_length=MAX_REMOTE_CLEANUP_RESOURCES)
    error: str | None = Field(default=None, max_length=8192)

    @model_validator(mode="after")
    def validate_shape_for_outcome(self) -> _RemoteCleanupPayload:
        if len(self.resources) + len(self.residual_processes) > MAX_REMOTE_CLEANUP_RESOURCES:
            raise ValueError("remote cleanup contains too many aggregate resources")
        if self.outcome in {"passed", "failed"} and self.completed_at is None:
            raise ValueError("completed remote cleanup requires completed_at")
        if self.outcome == "passed" and (self.errors or self.residual_processes):
            raise ValueError("passed remote cleanup cannot contain errors or residual processes")
        if self.outcome in {"invalid_metadata", "ownership_refused"} and self.error is None:
            raise ValueError("refused remote cleanup requires an error detail")
        return self
