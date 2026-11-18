"""A neutral provenance block for what a job or service produced.

It replaces ``models_gateway``'s lifting of the CLIO-specific
``clio.provenance.v1`` envelope. Any client reads it from an artifact's
``metadata["provenance"]``; nothing in it names a particular agent.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.jobs.handles import OutputRef, utc_now
from clio_relay.jobs.records import OperationRecord

PROVENANCE_KEY = "provenance"
SCHEMA = "relay.provenance.v1"


class PackageProvenance(BaseModel):
    """The reviewed package that produced an output, when there was one."""

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str
    sha256: str


class Provenance(BaseModel):
    """Where one output came from."""

    model_config = ConfigDict(extra="forbid")

    schema_: str = Field(default=SCHEMA, alias="schema")
    operation_id: str
    site: str
    adapter: str
    native_id: str | None = None
    spec_digest: str | None = None
    input_digests: list[str] = Field(default_factory=list[str])
    package: PackageProvenance | None = None
    uri: str | None = None
    size_bytes: int | None = None
    sha256: str | None = None
    created_at: datetime = Field(default_factory=utc_now)

    def block(self) -> dict[str, Any]:
        """Return the block as it sits under ``metadata["provenance"]``."""
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)


def for_output(record: OperationRecord, output: OutputRef) -> Provenance:
    """Return the provenance of one output of an operation."""
    package: PackageProvenance | None = None
    raw_job: Any = record.spec.get("job", record.spec)
    job: dict[str, Any] = cast(dict[str, Any], raw_job) if isinstance(raw_job, dict) else {}
    raw_package: Any = job.get("package")
    if isinstance(raw_package, dict):
        fields = cast(dict[str, Any], raw_package)
        package = PackageProvenance.model_validate(
            {key: fields.get(key) for key in ("name", "version", "sha256")}
        )
    return Provenance(
        operation_id=record.operation_id,
        site=record.site,
        adapter=record.adapter,
        native_id=record.handle.native_id if record.handle else None,
        spec_digest=record.spec_digest,
        input_digests=list(record.input_digests),
        package=package,
        uri=output.path,
        size_bytes=output.size,
    )
