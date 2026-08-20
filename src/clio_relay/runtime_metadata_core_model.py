"""The normalized runtime metadata document every producer flow builds.

Extracted from ``runtime_metadata.py`` (clio-relay split/runtime-metadata-w2):
:class:`JarvisRuntimeMetadata` is the durable, source-tagged contract every
other ``runtime_metadata_*`` builder (native-document normalization, MCP
result decoding, sidecar decoding, merge) constructs and merges -- along with
its two component documents, :class:`PackageProvenance` and
:class:`TerminalRuntimeMetadata`, and :func:`_field_sources`, the helper that
stamps a fresh document's already-populated fields with one shared source of
provenance.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.models import utc_now
from clio_relay.runtime_metadata_types import RUNTIME_METADATA_SCHEMA, RuntimeMetadataSource


class PackageProvenance(BaseModel):
    """Package identity captured by the execution owner."""

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str | None = None
    package_type: str | None = None
    package_id: str | None = None
    source: str | None = None
    path: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TerminalRuntimeMetadata(BaseModel):
    """Execution state reported by the runtime owner."""

    model_config = ConfigDict(extra="forbid")

    state: str | None = None
    terminal: bool | None = None
    returncode: int | None = None
    reason: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


class JarvisRuntimeMetadata(BaseModel):
    """Normalized, durable runtime metadata for one JARVIS-owned execution."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = RUNTIME_METADATA_SCHEMA
    source: RuntimeMetadataSource
    observed_at: datetime = Field(default_factory=utc_now)
    execution_id: str | None = None
    pipeline_id: str | None = None
    scheduler_provider: str | None = None
    scheduler_type: str | None = None
    scheduler_job_id: str | None = None
    scheduler_phase: str | None = None
    script_path: str | None = None
    hostfile_path: str | None = None
    output_path: str | None = None
    error_path: str | None = None
    allocated_nodes: list[str] = Field(default_factory=list)
    packages: list[PackageProvenance] = Field(default_factory=lambda: list[PackageProvenance]())
    terminal: TerminalRuntimeMetadata = Field(default_factory=TerminalRuntimeMetadata)
    field_sources: dict[str, RuntimeMetadataSource] = Field(default_factory=dict)
    details: dict[str, Any] = Field(default_factory=dict)


def _field_sources(
    metadata: JarvisRuntimeMetadata,
    source: RuntimeMetadataSource,
) -> dict[str, RuntimeMetadataSource]:
    sources: dict[str, RuntimeMetadataSource] = {}
    for field_name in (
        "execution_id",
        "pipeline_id",
        "scheduler_provider",
        "scheduler_type",
        "scheduler_job_id",
        "scheduler_phase",
        "script_path",
        "hostfile_path",
        "output_path",
        "error_path",
    ):
        if getattr(metadata, field_name) is not None:
            sources[field_name] = source
    if metadata.allocated_nodes:
        sources["allocated_nodes"] = source
    if metadata.packages:
        sources["packages"] = source
    for field_name in (
        "state",
        "terminal",
        "returncode",
        "reason",
        "started_at",
        "finished_at",
    ):
        if getattr(metadata.terminal, field_name) is not None:
            sources[f"terminal.{field_name}"] = source
    return sources
