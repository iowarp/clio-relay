"""Registry-backed virtualization for remote MCP servers.

Remote discovery is deliberately separated from local MCP ``tools/list``.
Operators refresh schemas through durable relay jobs, and the local MCP server
only renders validated, fresh cache entries. This keeps agent discovery fast,
deterministic, and free of cluster-side execution side effects.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from filelock import FileLock
from jsonschema import (
    Draft3Validator,
    Draft4Validator,
    Draft6Validator,
    Draft7Validator,
    Draft201909Validator,
    Draft202012Validator,
)
from jsonschema.exceptions import SchemaError
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.cluster_config import (
    ClusterRegistry,
    RemoteMcpProfile,
    RemoteMcpServerConfig,
    default_registry_path,
    ensure_private_configuration_path,
    open_private_atomic_file,
    read_bounded_configuration_bytes,
)

if TYPE_CHECKING:
    from clio_relay.validation_report import LiveValidationReport, ValidationResource

JSON = dict[str, Any]
REMOTE_MCP_CACHE_ENV = "CLIO_RELAY_REMOTE_MCP_CACHE"
REMOTE_MCP_CACHE_VERSION = 1
REMOTE_MCP_CACHE_SOURCE = "durable_relay_mcp_tools_list"
MAX_REMOTE_MCP_CACHE_BYTES = 16 * 1024 * 1024
MAX_REMOTE_MCP_DISCOVERY_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_REMOTE_MCP_CACHE_ENTRIES = 1_024
MAX_REMOTE_MCP_TOOLS_PER_SERVER = 2_048
MAX_REMOTE_MCP_TOOL_SCHEMA_BYTES = 1024 * 1024
MAX_REMOTE_MCP_PROVENANCE_BYTES = 1024 * 1024
MAX_REMOTE_MCP_JSON_DEPTH = 64
MAX_REMOTE_MCP_JSON_NODES = 100_000
MAX_REMOTE_MCP_DIAGNOSTIC_CHARS = 4_096
MAX_VIRTUAL_REMOTE_MCP_CANDIDATES = 10_000
MAX_REMOTE_MCP_CATALOG_ISSUES = 10_000
REMOTE_MCP_REPLACE_ATTEMPTS = 25
REMOTE_MCP_REPLACE_RETRY_SECONDS = 0.02
CLIO_KIT_SPACK_USER_CONTRACT_VERSION = "3.0.0"
# Digest the MCP wire ``tools/list`` result. FastMCP's in-process FunctionTool
# schemas retain ``$defs`` that its protocol serializer dereferences, so their
# digest is intentionally not the relay contract.
CLIO_KIT_SPACK_USER_CONTRACT_SHA256 = (
    "d0709c552f6042ce4143d49706ddfbd8d05a80e4425412fbc3393d1eb00a216c"
)
_COMPOSED_SCHEMA_KEYS = {
    "$dynamicRef",
    "$recursiveRef",
    "$ref",
    "allOf",
    "anyOf",
    "else",
    "if",
    "oneOf",
    "not",
    "then",
}
_FLAT_SCHEMA_KEYS = {
    "$comment",
    "$defs",
    "$id",
    "$schema",
    "additionalProperties",
    "default",
    "definitions",
    "deprecated",
    "description",
    "examples",
    "properties",
    "readOnly",
    "required",
    "title",
    "type",
    "writeOnly",
}
_JSON_SCHEMA_VALIDATORS = {
    str(validator.META_SCHEMA.get("$id") or validator.META_SCHEMA.get("id")).rstrip("#"): validator
    for validator in (
        Draft3Validator,
        Draft4Validator,
        Draft6Validator,
        Draft7Validator,
        Draft201909Validator,
        Draft202012Validator,
    )
}
_JSON_SCHEMA_VALIDATORS.update(
    {
        dialect.replace("http://", "https://", 1): validator
        for dialect, validator in tuple(_JSON_SCHEMA_VALIDATORS.items())
        if dialect.startswith("http://")
    }
)


class _NonFiniteJsonError(ValueError):
    """Non-standard NaN or infinity token in a purported JSON artifact."""


_SAFE_NAME_PATTERN = re.compile(r"[^a-z0-9_]+")
VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA: JSON = {
    "type": "object",
    "properties": {
        "cluster": {"type": "string"},
        "job_id": {"type": "string"},
        "state": {
            "type": "string",
            "enum": ["queued", "leased", "running", "succeeded", "failed", "canceled"],
        },
        "kind": {"type": "string", "const": "mcp_call"},
        "terminal": {"type": "boolean"},
        "remote": {"type": "boolean"},
        "route_revision": {"type": "string"},
    },
    "required": ["cluster", "job_id", "state", "kind", "terminal", "route_revision"],
    "additionalProperties": False,
}


class RemoteMcpToolSchema(BaseModel):
    """Validated tool contract returned by a remote MCP ``tools/list`` call."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(max_length=512)
    title: str | None = Field(default=None, max_length=4_096)
    description: str | None = Field(default=None, max_length=65_536)
    input_schema: JSON
    output_schema: JSON | None = None
    annotations: JSON | None = None

    @field_validator("name")
    @classmethod
    def _name_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("remote MCP tool name must not be blank")
        return value

    @model_validator(mode="after")
    def _schema_must_be_bounded(self) -> RemoteMcpToolSchema:
        _require_bounded_json_structure(self.input_schema, label="inputSchema")
        _require_finite_json(self.input_schema, label="inputSchema")
        _validate_json_schema(self.input_schema, label="inputSchema")
        if self.output_schema is not None:
            _require_bounded_json_structure(self.output_schema, label="outputSchema")
            _require_finite_json(self.output_schema, label="outputSchema")
            _validate_json_schema(self.output_schema, label="outputSchema")
        if self.annotations is not None:
            _require_bounded_json_structure(self.annotations, label="annotations")
            _require_finite_json(self.annotations, label="annotations")
        payload = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(payload) > MAX_REMOTE_MCP_TOOL_SCHEMA_BYTES:
            raise ValueError(
                f"remote MCP tool schema exceeds {MAX_REMOTE_MCP_TOOL_SCHEMA_BYTES} bytes"
            )
        return self


class RemoteMcpDiscoveryProvenance(BaseModel):
    """Durable evidence associated with one cached remote discovery."""

    model_config = ConfigDict(extra="forbid")

    source: str = REMOTE_MCP_CACHE_SOURCE
    discovery_job_id: str
    artifact_id: str
    artifact_sha256: str
    protocol_version: str | None = None
    server_info: JSON = Field(default_factory=dict)
    server_artifact: JSON = Field(default_factory=dict)

    @field_validator("artifact_sha256")
    @classmethod
    def _artifact_digest_must_be_sha256(cls, value: str) -> str:
        normalized = value.strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError("remote MCP discovery artifact SHA-256 must be 64 hex characters")
        return normalized

    @model_validator(mode="after")
    def _provenance_must_be_bounded(self) -> RemoteMcpDiscoveryProvenance:
        _require_bounded_json_structure(self.server_info, label="server_info")
        _require_bounded_json_structure(self.server_artifact, label="server_artifact")
        payload = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(payload) > MAX_REMOTE_MCP_PROVENANCE_BYTES:
            raise ValueError(
                f"remote MCP provenance exceeds {MAX_REMOTE_MCP_PROVENANCE_BYTES} bytes"
            )
        return self


class RemoteMcpSchemaCacheEntry(BaseModel):
    """Cluster-scoped schema snapshot for one registered remote MCP server."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(max_length=256)
    server_name: str = Field(max_length=256)
    execution_fingerprint: str
    discovered_at: datetime
    expires_at: datetime
    schema_digest: str
    tools: list[RemoteMcpToolSchema] = Field(max_length=MAX_REMOTE_MCP_TOOLS_PER_SERVER)
    provenance: RemoteMcpDiscoveryProvenance

    @field_validator("cluster", "server_name")
    @classmethod
    def _identity_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("remote MCP cache identity fields must not be blank")
        return value

    @field_validator("discovered_at", "expires_at")
    @classmethod
    def _timestamps_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("remote MCP cache timestamps must be timezone-aware")
        return value

    @field_validator("tools")
    @classmethod
    def _tool_names_must_be_unique(
        cls, value: list[RemoteMcpToolSchema]
    ) -> list[RemoteMcpToolSchema]:
        names = [tool.name for tool in value]
        if len(names) != len(set(names)):
            raise ValueError("remote MCP discovery returned duplicate tool names")
        return value

    @model_validator(mode="after")
    def _schema_digest_must_match_tools(self) -> RemoteMcpSchemaCacheEntry:
        observed = remote_mcp_schema_digest(self.tools)
        if self.schema_digest != observed:
            raise ValueError("remote MCP cache schema digest does not match cached tools")
        return self

    def is_fresh(self, *, now: datetime | None = None) -> bool:
        """Return whether the schema snapshot has not reached its expiry time."""
        current = now or datetime.now(UTC)
        return current < self.expires_at


class RemoteMcpSchemaCache(BaseModel):
    """Versioned, atomically persisted remote MCP schema cache."""

    model_config = ConfigDict(extra="forbid")

    version: int = REMOTE_MCP_CACHE_VERSION
    entries: list[RemoteMcpSchemaCacheEntry] = Field(
        default_factory=lambda: list[RemoteMcpSchemaCacheEntry](),
        max_length=MAX_REMOTE_MCP_CACHE_ENTRIES,
    )

    @field_validator("version")
    @classmethod
    def _version_must_be_supported(cls, value: int) -> int:
        if value != REMOTE_MCP_CACHE_VERSION:
            raise ValueError(f"unsupported remote MCP cache version: {value}")
        return value

    @field_validator("entries")
    @classmethod
    def _entry_keys_must_be_unique(
        cls, value: list[RemoteMcpSchemaCacheEntry]
    ) -> list[RemoteMcpSchemaCacheEntry]:
        keys = [(entry.cluster, entry.server_name) for entry in value]
        if len(keys) != len(set(keys)):
            raise ValueError("remote MCP cache entries must be unique per cluster and server")
        return value

    @classmethod
    def load(cls, path: Path) -> RemoteMcpSchemaCache:
        """Load a cache without creating a file for read-only MCP operations."""
        if not path.exists():
            return cls()
        return cls.model_validate_json(
            read_bounded_configuration_bytes(path, max_bytes=MAX_REMOTE_MCP_CACHE_BYTES)
        )

    def entry_for(self, cluster: str, server_name: str) -> RemoteMcpSchemaCacheEntry | None:
        """Return one cluster/server cache entry when present."""
        return next(
            (
                entry
                for entry in self.entries
                if entry.cluster == cluster and entry.server_name == server_name
            ),
            None,
        )

    @classmethod
    def update_entry(
        cls,
        path: Path,
        entry: RemoteMcpSchemaCacheEntry,
    ) -> RemoteMcpSchemaCache:
        """Atomically replace one cache entry while serializing concurrent refreshes."""
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        ensure_private_configuration_path(path.parent, directory=True)
        with FileLock(f"{path}.lock"):
            cache = cls.load(path)
            entries = [
                current
                for current in cache.entries
                if (current.cluster, current.server_name) != (entry.cluster, entry.server_name)
            ]
            entries.append(entry)
            updated = cls(
                entries=sorted(entries, key=lambda item: (item.cluster, item.server_name))
            )
            updated._write_atomic(path)
            return updated

    @classmethod
    def remove_entry(cls, path: Path, cluster: str, server_name: str) -> RemoteMcpSchemaCache:
        """Atomically remove a cache entry after an operator unregisters a server."""
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        ensure_private_configuration_path(path.parent, directory=True)
        with FileLock(f"{path}.lock"):
            cache = cls.load(path)
            updated = cls(
                entries=[
                    entry
                    for entry in cache.entries
                    if (entry.cluster, entry.server_name) != (cluster, server_name)
                ]
            )
            updated._write_atomic(path)
            return updated

    def _write_atomic(self, path: Path) -> None:
        payload = (self.model_dump_json(indent=2) + "\n").encode("utf-8")
        if len(payload) > MAX_REMOTE_MCP_CACHE_BYTES:
            raise ValueError(f"remote MCP cache exceeds {MAX_REMOTE_MCP_CACHE_BYTES} bytes")
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
        try:
            with open_private_atomic_file(temporary) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            for attempt in range(REMOTE_MCP_REPLACE_ATTEMPTS):
                try:
                    os.replace(temporary, path)
                    break
                except PermissionError:
                    if attempt + 1 >= REMOTE_MCP_REPLACE_ATTEMPTS:
                        raise
                    time.sleep(REMOTE_MCP_REPLACE_RETRY_SECONDS)
            _fsync_cache_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)


class RemoteMcpCatalogIssue(BaseModel):
    """Reason a registered or discovered remote capability is not exposed."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    server_name: str
    reason: str
    tool_name: str | None = None


class RemoteMcpAcceptanceCheck(BaseModel):
    """One canonical remote MCP release-validation assertion."""

    model_config = ConfigDict(extra="forbid")

    name: str
    passed: bool
    message: str
    evidence: JSON = Field(default_factory=dict)


class RemoteMcpAcceptanceReport(BaseModel):
    """Machine-readable evidence for one virtual remote MCP acceptance call."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "1.0"
    report_type: str = "clio-relay.remote-mcp-acceptance"
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    cluster: str
    server_name: str
    remote_tool_name: str
    virtual_alias: str | None = None
    profile: str
    passed: bool
    checks: list[RemoteMcpAcceptanceCheck]
    discovery: JSON = Field(default_factory=dict)
    call_job: JSON = Field(default_factory=dict)
    artifacts: list[JSON] = Field(default_factory=lambda: list[JSON]())
    mcp_stdio: JSON = Field(default_factory=dict)

    def to_live_validation_report(
        self,
        *,
        launcher: str | None = None,
        install_source: str | None = None,
        artifact_sha256: str | None = None,
    ) -> LiveValidationReport:
        """Convert domain assertions into the canonical release evidence schema."""
        from clio_relay.validation_report import (
            EvidenceReference,
            ValidationCheck,
            ValidationResource,
            ValidationStatus,
            new_live_validation_report,
        )

        report = new_live_validation_report(
            scenario="remote-mcp",
            cluster=self.cluster,
            launcher=launcher,
            install_source=install_source,
            artifact_sha256=artifact_sha256,
        )
        report.started_at = self.generated_at
        report.completed_at = datetime.now(UTC)
        report.checks = [
            ValidationCheck(
                check_id=check.name,
                summary=check.message,
                status=(ValidationStatus.PASSED if check.passed else ValidationStatus.FAILED),
                started_at=self.generated_at,
                completed_at=report.completed_at,
                evidence=[
                    EvidenceReference(
                        kind="remote_mcp_acceptance",
                        excerpt=check.message,
                        metadata=check.evidence,
                    )
                ],
                error=None if check.passed else check.message,
            )
            for check in self.checks
        ]
        report.status = ValidationStatus.PASSED if self.passed else ValidationStatus.FAILED
        report.error = None if self.passed else "one or more remote MCP checks failed"
        call_job_id = self.call_job.get("job_id")
        if isinstance(call_job_id, str):
            call_metadata = {
                **self.call_job,
                "remote_mcp_server_name": self.server_name,
                "remote_mcp_tool_name": self.remote_tool_name,
                "virtual_alias": self.virtual_alias,
                "profile": self.profile,
            }
            report.resources.append(
                ValidationResource(
                    kind="relay_job",
                    resource_id=call_job_id,
                    role="virtual_remote_mcp_call",
                    cluster=self.cluster,
                    state=(
                        str(self.call_job["state"])
                        if self.call_job.get("state") is not None
                        else None
                    ),
                    metadata=call_metadata,
                )
            )
        raw_provenance = self.discovery.get("provenance")
        discovery_provenance = (
            cast(JSON, raw_provenance) if isinstance(raw_provenance, dict) else {}
        )
        discovery_job_id = discovery_provenance.get("discovery_job_id")
        if isinstance(discovery_job_id, str):
            report.resources.append(
                ValidationResource(
                    kind="relay_job",
                    resource_id=discovery_job_id,
                    role="remote_mcp_discovery",
                    cluster=self.cluster,
                    state="succeeded",
                    metadata=discovery_provenance,
                )
            )
        discovery_artifact_id = discovery_provenance.get("artifact_id")
        if isinstance(discovery_artifact_id, str):
            report.resources.append(
                ValidationResource(
                    kind="artifact",
                    resource_id=discovery_artifact_id,
                    role="remote_mcp_schema",
                    cluster=self.cluster,
                    metadata=discovery_provenance,
                )
            )
        for artifact in self.artifacts:
            resource = _acceptance_artifact_resource(self.cluster, artifact)
            if resource is None:
                continue
            report.resources.append(resource)
            report.artifacts.append(
                EvidenceReference(
                    kind=resource.role or "artifact",
                    reference=(
                        resource.references[0]
                        if resource.references
                        else f"relay-artifact://{self.cluster}/{resource.resource_id}"
                    ),
                    sha256=(
                        str(artifact["sha256"]) if isinstance(artifact.get("sha256"), str) else None
                    ),
                )
            )
        server_check = next(
            (check for check in self.checks if check.name == "remote-mcp.server-artifact"),
            None,
        )
        raw_server_artifact = (
            server_check.evidence.get("call_server_artifact") if server_check is not None else None
        )
        if isinstance(raw_server_artifact, dict):
            server_artifact = cast(JSON, raw_server_artifact)
            identity = (
                str(server_artifact.get("install_spec"))
                if server_artifact.get("install_spec") is not None
                else str(server_artifact.get("resolved_executable", self.server_name))
            )
            report.resources.append(
                ValidationResource(
                    kind="mcp_server",
                    resource_id=f"{self.server_name}:{identity}",
                    role="remote_mcp_server",
                    cluster=self.cluster,
                    state=(
                        "verified"
                        if server_check is not None and server_check.passed
                        else "unverified"
                    ),
                    metadata={
                        "server_name": self.server_name,
                        "server_info": discovery_provenance.get("server_info", {}),
                        "remote_tool_names": self.discovery.get("remote_tool_names", []),
                        "allowlisted_tool_names": self.discovery.get("allowlisted_tool_names", []),
                        **server_artifact,
                    },
                )
            )
        return report


def _acceptance_artifact_resource(
    cluster: str,
    artifact: JSON,
) -> ValidationResource | None:
    from clio_relay.validation_report import ValidationResource

    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str):
        return None
    uri = artifact.get("uri")
    return ValidationResource(
        kind="artifact",
        resource_id=artifact_id,
        role=str(artifact.get("kind", "artifact")),
        cluster=cluster,
        references=[str(uri)] if isinstance(uri, str) else [],
        metadata=artifact,
    )


@dataclass(frozen=True)
class RemoteMcpRoute:
    """Execution route selected by a virtual tool alias and cluster argument."""

    cluster: str
    server_name: str
    command: str
    args: tuple[str, ...]
    env_from: tuple[tuple[str, str], ...]
    expected_server_artifact_digest: str | None
    remote_tool_name: str
    timeout_seconds: int
    contract: str | None


@dataclass(frozen=True)
class VirtualRemoteMcpTool:
    """One agent-facing alias backed by equivalent remote schemas."""

    alias: str
    namespace: str
    remote_tool: RemoteMcpToolSchema
    routes: dict[str, RemoteMcpRoute]
    arguments_wrapped: bool

    def definition(self) -> JSON:
        """Render the asynchronous relay-submission contract for a remote tool."""
        clusters = sorted(self.routes)
        input_schema = inject_cluster_argument(self.remote_tool.input_schema, clusters=clusters)
        description = self.remote_tool.description or f"Call {self.remote_tool.name}."
        definition: JSON = {
            "name": self.alias,
            "description": (
                f"{description} Routed through registered remote MCP namespace "
                f"'{self.namespace}' on the selected cluster. The call is submitted "
                "as a durable relay job; use relay job tools to retrieve the remote "
                "tool result."
            ),
            "inputSchema": input_schema,
            "outputSchema": deepcopy(VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA),
        }
        if self.remote_tool.title is not None:
            definition["title"] = self.remote_tool.title
        if self.remote_tool.annotations is not None:
            definition["annotations"] = deepcopy(self.remote_tool.annotations)
        return definition

    def forwarded_arguments(self, arguments: JSON) -> JSON:
        """Remove local routing fields and return the exact remote arguments object."""
        if not self.arguments_wrapped:
            return {key: value for key, value in arguments.items() if key != "cluster"}
        unexpected = sorted(set(arguments) - {"cluster", "arguments"})
        if unexpected:
            raise ValueError(
                "wrapped virtual remote MCP arguments contain unexpected local fields: "
                + ", ".join(unexpected)
            )
        remote_arguments = arguments.get("arguments")
        if not isinstance(remote_arguments, dict):
            raise ValueError("wrapped virtual remote MCP call requires an arguments object")
        return deepcopy(cast(JSON, remote_arguments))


@dataclass(frozen=True)
class VirtualRemoteMcpCatalog:
    """Resolved virtual tool catalog for one local MCP profile."""

    revision: str
    tools: dict[str, VirtualRemoteMcpTool]
    issues: tuple[RemoteMcpCatalogIssue, ...]

    def tool_definitions(self) -> list[JSON]:
        """Return deterministic agent-facing tool definitions."""
        return [self.tools[name].definition() for name in sorted(self.tools)]

    def resolve(self, alias: str, cluster: str) -> RemoteMcpRoute:
        """Resolve an alias and cluster without forwarding the selector remotely."""
        try:
            tool = self.tools[alias]
        except KeyError as exc:
            raise ValueError(f"unknown or unavailable virtual remote MCP tool: {alias}") from exc
        try:
            return tool.routes[cluster]
        except KeyError as exc:
            available = ", ".join(sorted(tool.routes))
            raise ValueError(
                f"virtual remote MCP tool {alias} is not available on cluster {cluster}; "
                f"available clusters: {available}"
            ) from exc

    def forwarded_arguments(self, alias: str, arguments: JSON) -> JSON:
        """Return arguments for the remote tool without local routing structure."""
        try:
            tool = self.tools[alias]
        except KeyError as exc:
            raise ValueError(f"unknown or unavailable virtual remote MCP tool: {alias}") from exc
        return tool.forwarded_arguments(arguments)


@dataclass(frozen=True)
class _Candidate:
    cluster: str
    server_name: str
    namespace: str
    registration: RemoteMcpServerConfig
    tool: RemoteMcpToolSchema
    base_alias: str
    identity: str
    expected_server_artifact_digest: str | None


def unavailable_virtual_remote_mcp_catalog(reason: str) -> VirtualRemoteMcpCatalog:
    """Return a fail-closed catalog while preserving built-in MCP safety tools."""
    bounded_reason = reason[:4_096]
    return VirtualRemoteMcpCatalog(
        revision=_stable_digest({"unavailable": bounded_reason}),
        tools={},
        issues=(
            RemoteMcpCatalogIssue(
                cluster="*",
                server_name="*",
                reason=f"remote MCP catalog unavailable: {bounded_reason}",
            ),
        ),
    )


def default_remote_mcp_cache_path(*, registry_path: Path | None = None) -> Path:
    """Return the operator-local schema cache path."""
    configured = os.getenv(REMOTE_MCP_CACHE_ENV)
    if configured:
        return Path(configured).expanduser().resolve()
    resolved_registry = (registry_path or default_registry_path()).expanduser().resolve()
    return (resolved_registry.parent / "remote-mcp-cache.json").resolve()


def remote_mcp_execution_fingerprint(registration: RemoteMcpServerConfig) -> str:
    """Hash the command and environment references that produced a schema snapshot."""
    return _stable_digest(
        {
            "command": registration.command,
            "args": registration.args,
            "env_from": registration.env_from,
        }
    )


def remote_mcp_schema_digest(tools: list[RemoteMcpToolSchema]) -> str:
    """Return a stable digest for a discovered tool collection."""
    return _stable_digest(
        {
            "tools": [
                tool.model_dump(mode="json") for tool in sorted(tools, key=lambda item: item.name)
            ]
        }
    )


def remote_mcp_server_artifact_digest(server_artifact: JSON) -> str:
    """Return the canonical digest used to bind discovery to later execution."""
    return _stable_digest({"server_artifact": server_artifact})


def cache_entry_from_discovery_artifact(
    *,
    cluster: str,
    server_name: str,
    registration: RemoteMcpServerConfig,
    discovery_job_id: str,
    artifact_id: str,
    artifact_sha256: str | None,
    artifact_payload: bytes,
    discovered_at: datetime | None = None,
) -> RemoteMcpSchemaCacheEntry:
    """Validate a durable MCP result artifact and convert it to a cache entry."""
    if len(artifact_payload) > MAX_REMOTE_MCP_DISCOVERY_ARTIFACT_BYTES:
        raise ValueError(
            f"remote MCP discovery artifact exceeds {MAX_REMOTE_MCP_DISCOVERY_ARTIFACT_BYTES} bytes"
        )
    observed_artifact_sha256 = hashlib.sha256(artifact_payload).hexdigest()
    if artifact_sha256 is None:
        raise ValueError("remote MCP discovery requires a durable artifact SHA-256")
    if not hmac.compare_digest(artifact_sha256.strip().lower(), observed_artifact_sha256):
        raise ValueError("remote MCP discovery artifact SHA-256 does not match its payload")
    try:
        decoded = json.loads(
            artifact_payload.decode("utf-8-sig"),
            parse_constant=_reject_nonfinite_json_constant,
        )
    except _NonFiniteJsonError as exc:
        raise ValueError(str(exc)) from exc
    except RecursionError as exc:
        raise ValueError(
            f"remote MCP discovery artifact exceeds {MAX_REMOTE_MCP_JSON_DEPTH} nesting levels"
        ) from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("remote MCP discovery artifact must be valid UTF-8 JSON") from exc
    if not isinstance(decoded, dict):
        raise ValueError("remote MCP discovery artifact must be a JSON object")
    artifact = cast(JSON, decoded)
    _require_bounded_json_structure(artifact, label="discovery artifact")
    _require_finite_json(artifact, label="discovery artifact")
    if artifact.get("operation") != "tools/list":
        raise ValueError("remote MCP discovery artifact operation must be tools/list")
    if artifact.get("server") != registration.command:
        raise ValueError("remote MCP discovery artifact server does not match registration")
    if artifact.get("server_args") != registration.args:
        raise ValueError("remote MCP discovery artifact server_args do not match registration")
    if artifact.get("env_from", {}) != registration.env_from:
        raise ValueError("remote MCP discovery artifact env_from does not match registration")
    if artifact.get("returncode") != 0:
        raise ValueError("remote MCP discovery job did not exit successfully")
    if artifact.get("timed_out") is True:
        raise ValueError("remote MCP discovery job timed out")
    if artifact.get("protocol_error") is not None:
        raise ValueError(
            "remote MCP discovery protocol error: "
            + _bounded_diagnostic(artifact["protocol_error"])
        )
    protocol_result = artifact.get("protocol_result")
    if not isinstance(protocol_result, dict):
        raise ValueError("remote MCP discovery artifact is missing protocol_result")
    raw_tools = cast(JSON, protocol_result).get("tools")
    if not isinstance(raw_tools, list):
        raise ValueError("remote MCP tools/list result must contain a tools array")
    typed_raw_tools = cast(list[object], raw_tools)
    if len(typed_raw_tools) > MAX_REMOTE_MCP_TOOLS_PER_SERVER:
        raise ValueError(f"remote MCP tools/list exceeds {MAX_REMOTE_MCP_TOOLS_PER_SERVER} tools")
    tools = [_parse_remote_tool(item) for item in typed_raw_tools]
    names = [tool.name for tool in tools]
    if len(names) != len(set(names)):
        raise ValueError("remote MCP tools/list result contains duplicate tool names")
    initialized_at = discovered_at or datetime.now(UTC)
    protocol_version = artifact.get("protocol_version")
    server_info = artifact.get("server_info", {})
    server_artifact = artifact.get("server_artifact", {})
    if protocol_version is not None and not isinstance(protocol_version, str):
        raise ValueError("remote MCP protocol_version must be a string")
    if not isinstance(server_info, dict):
        raise ValueError("remote MCP server_info must be an object")
    if not isinstance(server_artifact, dict):
        raise ValueError("remote MCP server_artifact must be an object")
    return RemoteMcpSchemaCacheEntry(
        cluster=cluster,
        server_name=server_name,
        execution_fingerprint=remote_mcp_execution_fingerprint(registration),
        discovered_at=initialized_at,
        expires_at=initialized_at + timedelta(seconds=registration.schema_cache_ttl_seconds),
        schema_digest=remote_mcp_schema_digest(tools),
        tools=tools,
        provenance=RemoteMcpDiscoveryProvenance(
            discovery_job_id=discovery_job_id,
            artifact_id=artifact_id,
            artifact_sha256=observed_artifact_sha256,
            protocol_version=protocol_version,
            server_info=cast(JSON, server_info),
            server_artifact=cast(JSON, server_artifact),
        ),
    )


def build_virtual_remote_mcp_catalog(
    registry: ClusterRegistry,
    cache: RemoteMcpSchemaCache,
    *,
    profile: str,
    reserved_names: set[str] | None = None,
    now: datetime | None = None,
) -> VirtualRemoteMcpCatalog:
    """Build deterministic aliases from fresh, allowlisted remote schemas."""
    current = now or datetime.now(UTC)
    candidates: list[_Candidate] = []
    issues: list[RemoteMcpCatalogIssue] = []
    issues_capped = False

    def record_issue(issue: RemoteMcpCatalogIssue) -> None:
        nonlocal issues_capped
        if issues_capped:
            return
        if len(issues) < MAX_REMOTE_MCP_CATALOG_ISSUES - 1:
            issues.append(issue)
            return
        issues.append(
            RemoteMcpCatalogIssue(
                cluster="*",
                server_name="*",
                reason=(
                    "remote MCP catalog diagnostics reached the "
                    f"{MAX_REMOTE_MCP_CATALOG_ISSUES} issue limit"
                ),
            )
        )
        issues_capped = True

    candidate_limit_reached = False
    for cluster_name, cluster in sorted(registry.clusters.items()):
        if candidate_limit_reached:
            break
        for server_name, registration in sorted(cluster.remote_mcp_servers.items()):
            if candidate_limit_reached:
                break
            if not registration.enabled:
                continue
            if not _profile_allows(registration.profiles, profile):
                continue
            entry = cache.entry_for(cluster_name, server_name)
            if entry is None:
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=cluster_name,
                        server_name=server_name,
                        reason="schema cache is missing; run remote-mcp refresh",
                    )
                )
                continue
            if entry.execution_fingerprint != remote_mcp_execution_fingerprint(registration):
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=cluster_name,
                        server_name=server_name,
                        reason="registered command changed; run remote-mcp refresh",
                    )
                )
                continue
            effective_expires_at = entry.discovered_at + timedelta(
                seconds=registration.schema_cache_ttl_seconds
            )
            if current >= effective_expires_at:
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=cluster_name,
                        server_name=server_name,
                        reason=(
                            "schema cache expired at "
                            f"{effective_expires_at.astimezone(UTC).isoformat()}"
                        ),
                    )
                )
                continue
            server_artifact_verified = _server_artifact_verified(entry.provenance.server_artifact)
            if not server_artifact_verified and not registration.allow_mutable_artifact:
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=cluster_name,
                        server_name=server_name,
                        reason=(
                            "discovery server artifact identity is unverified; refresh from an "
                            "immutable executable or exact artifact"
                        ),
                    )
                )
                continue
            if registration.contract is not None:
                contract_check = _declared_contract_check(entry, registration)
                if not contract_check.passed:
                    record_issue(
                        RemoteMcpCatalogIssue(
                            cluster=cluster_name,
                            server_name=server_name,
                            reason=(
                                f"declared contract {registration.contract!r} failed: "
                                f"{contract_check.message}"
                            ),
                        )
                    )
                    continue
            discovered_names = {tool.name for tool in entry.tools}
            for allowed_tool in registration.allow_tools:
                if allowed_tool != "*" and allowed_tool not in discovered_names:
                    record_issue(
                        RemoteMcpCatalogIssue(
                            cluster=cluster_name,
                            server_name=server_name,
                            tool_name=allowed_tool,
                            reason="allowlisted tool was not returned by remote tools/list",
                        )
                    )
            for tool in entry.tools:
                if not registration.allows_tool(tool.name):
                    continue
                schema_error = virtual_schema_error(tool.input_schema)
                if schema_error is not None:
                    record_issue(
                        RemoteMcpCatalogIssue(
                            cluster=cluster_name,
                            server_name=server_name,
                            tool_name=tool.name,
                            reason=schema_error,
                        )
                    )
                    continue
                namespace = (registration.namespace or server_name).casefold()
                base_alias = f"remote_{_safe_name(namespace)}_{_safe_name(tool.name)}"
                if len(candidates) >= MAX_VIRTUAL_REMOTE_MCP_CANDIDATES:
                    record_issue(
                        RemoteMcpCatalogIssue(
                            cluster=cluster_name,
                            server_name=server_name,
                            tool_name=tool.name,
                            reason=(
                                "virtual remote MCP catalog exceeds the "
                                f"{MAX_VIRTUAL_REMOTE_MCP_CANDIDATES} candidate limit"
                            ),
                        )
                    )
                    candidate_limit_reached = True
                    break
                identity = _stable_digest(
                    {
                        "namespace": namespace,
                        "remote_tool": tool.name,
                        "schema": tool.model_dump(mode="json"),
                        "contract": registration.contract,
                    }
                )
                candidates.append(
                    _Candidate(
                        cluster=cluster_name,
                        server_name=server_name,
                        namespace=namespace,
                        registration=registration,
                        tool=tool,
                        base_alias=base_alias,
                        identity=identity,
                        expected_server_artifact_digest=(
                            remote_mcp_server_artifact_digest(entry.provenance.server_artifact)
                            if not registration.allow_mutable_artifact
                            else None
                        ),
                    )
                )

    grouped: dict[str, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.identity, []).append(candidate)
    unambiguous_groups: dict[str, list[_Candidate]] = {}
    for identity, group in sorted(grouped.items()):
        cluster_counts: dict[str, int] = {}
        for candidate in group:
            cluster_counts[candidate.cluster] = cluster_counts.get(candidate.cluster, 0) + 1
        ambiguous_clusters = {cluster for cluster, count in cluster_counts.items() if count > 1}
        for candidate in group:
            if candidate.cluster in ambiguous_clusters:
                record_issue(
                    RemoteMcpCatalogIssue(
                        cluster=candidate.cluster,
                        server_name=candidate.server_name,
                        tool_name=candidate.tool.name,
                        reason=(
                            "multiple registrations provide the same namespace, tool, schema, "
                            "and contract on this cluster; the route is ambiguous"
                        ),
                    )
                )
        remaining = [
            candidate for candidate in group if candidate.cluster not in ambiguous_clusters
        ]
        if remaining:
            unambiguous_groups[identity] = remaining
    grouped = unambiguous_groups
    aliases = _assign_aliases(grouped, reserved_names=reserved_names or set())
    virtual_tools: dict[str, VirtualRemoteMcpTool] = {}
    for identity, group in sorted(grouped.items()):
        alias = aliases[identity]
        first = group[0]
        routes = {
            candidate.cluster: RemoteMcpRoute(
                cluster=candidate.cluster,
                server_name=candidate.server_name,
                command=candidate.registration.command,
                args=tuple(candidate.registration.args),
                env_from=tuple(sorted(candidate.registration.env_from.items())),
                expected_server_artifact_digest=candidate.expected_server_artifact_digest,
                remote_tool_name=candidate.tool.name,
                timeout_seconds=candidate.registration.call_timeout_seconds,
                contract=candidate.registration.contract,
            )
            for candidate in group
        }
        virtual_tools[alias] = VirtualRemoteMcpTool(
            alias=alias,
            namespace=first.namespace,
            remote_tool=first.tool,
            routes=routes,
            arguments_wrapped=remote_input_schema_requires_wrapper(first.tool.input_schema),
        )
    revision = _stable_digest(
        {
            "profile": profile,
            "tools": {
                alias: {
                    "namespace": tool.namespace,
                    "remote_tool": tool.remote_tool.model_dump(mode="json"),
                    "arguments_wrapped": tool.arguments_wrapped,
                    "routes": {
                        cluster: {
                            "server_name": route.server_name,
                            "execution_fingerprint": remote_mcp_execution_fingerprint(
                                registry.clusters[cluster].remote_mcp_servers[route.server_name]
                            ),
                            "expected_server_artifact_digest": (
                                route.expected_server_artifact_digest
                            ),
                        }
                        for cluster, route in sorted(tool.routes.items())
                    },
                }
                for alias, tool in sorted(virtual_tools.items())
            },
            "issues": [issue.model_dump(mode="json") for issue in issues],
        }
    )
    return VirtualRemoteMcpCatalog(
        revision=revision,
        tools=virtual_tools,
        issues=tuple(issues),
    )


def load_virtual_remote_mcp_catalog(
    *,
    profile: str,
    reserved_names: set[str] | None = None,
    registry_path: Path | None = None,
    cache_path: Path | None = None,
    now: datetime | None = None,
) -> VirtualRemoteMcpCatalog:
    """Load current config and cache on every call for explicit reload semantics."""
    resolved_registry_path = registry_path or default_registry_path()
    if not resolved_registry_path.exists():
        registry = ClusterRegistry.default()
    else:
        registry = ClusterRegistry.load(resolved_registry_path)
    resolved_cache_path = cache_path or default_remote_mcp_cache_path(
        registry_path=resolved_registry_path
    )
    cache = RemoteMcpSchemaCache.load(resolved_cache_path)
    return build_virtual_remote_mcp_catalog(
        registry,
        cache,
        profile=profile,
        reserved_names=reserved_names,
        now=now,
    )


def build_remote_mcp_acceptance_report(
    *,
    registry: ClusterRegistry,
    cache: RemoteMcpSchemaCache,
    cluster: str,
    server_name: str,
    remote_tool_name: str,
    profile: str,
    call_job_id: str,
    call_status: JSON,
    artifacts: list[JSON],
    mcp_result: JSON | None,
    provenance: JSON | None,
    mcp_stdio_evidence: JSON | None = None,
    now: datetime | None = None,
    reserved_names: set[str] | None = None,
) -> RemoteMcpAcceptanceReport:
    """Build canonical release checks from live durable job evidence."""
    current = now or datetime.now(UTC)
    definition = registry.clusters.get(cluster)
    registration = (
        definition.remote_mcp_servers.get(server_name) if definition is not None else None
    )
    registration_passed = (
        registration is not None
        and registration.enabled
        and registration.allows_tool(remote_tool_name)
        and _profile_allows(registration.profiles, profile)
    )
    registration_evidence: JSON = {
        "cluster_configured": definition is not None,
        "server_registered": registration is not None,
        "enabled": registration.enabled if registration is not None else False,
        "tool_allowlisted": (
            registration.allows_tool(remote_tool_name) if registration is not None else False
        ),
        "profile_allowed": (
            _profile_allows(registration.profiles, profile) if registration is not None else False
        ),
        "declared_contract": registration.contract if registration is not None else None,
    }
    checks = [
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.register",
            passed=registration_passed,
            message=(
                "registered server, allowlist, and profile are active"
                if registration_passed
                else "registered server, allowlist, or profile gate is not active"
            ),
            evidence=registration_evidence,
        )
    ]

    entry = cache.entry_for(cluster, server_name)
    effective_expires_at = (
        entry.discovered_at + timedelta(seconds=registration.schema_cache_ttl_seconds)
        if entry is not None and registration is not None
        else None
    )
    discovery_passed = (
        entry is not None
        and registration is not None
        and entry.execution_fingerprint == remote_mcp_execution_fingerprint(registration)
        and effective_expires_at is not None
        and current < effective_expires_at
        and entry.provenance.source == REMOTE_MCP_CACHE_SOURCE
        and bool(entry.provenance.discovery_job_id)
        and bool(entry.provenance.artifact_id)
    )
    discovery_evidence: JSON = (
        {
            "schema_digest": entry.schema_digest,
            "discovered_at": entry.discovered_at.isoformat(),
            "effective_expires_at": (
                effective_expires_at.isoformat() if effective_expires_at is not None else None
            ),
            "execution_fingerprint": entry.execution_fingerprint,
            "provenance": entry.provenance.model_dump(mode="json"),
            "remote_tool_names": sorted(tool.name for tool in entry.tools),
            "allowlisted_tool_names": (
                sorted(registration.allow_tools) if registration is not None else []
            ),
        }
        if entry is not None
        else {}
    )
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.discover",
            passed=discovery_passed,
            message=(
                "fresh schema is backed by a durable discovery job and artifact"
                if discovery_passed
                else "fresh durable discovery evidence is missing or invalid"
            ),
            evidence=discovery_evidence,
        )
    )
    if registration is not None and registration.contract is not None:
        checks.append(_declared_contract_check(entry, registration))

    catalog = build_virtual_remote_mcp_catalog(
        registry,
        cache,
        profile=profile,
        reserved_names=reserved_names,
        now=current,
    )
    matching_aliases = [
        alias
        for alias, virtual in catalog.tools.items()
        if virtual.remote_tool.name == remote_tool_name
        and cluster in virtual.routes
        and virtual.routes[cluster].server_name == server_name
    ]
    virtual_alias = matching_aliases[0] if len(matching_aliases) == 1 else None
    stdio_initialize_passed = _stdio_initialize_passed(mcp_stdio_evidence)
    stdio_listed_tools = _stdio_listed_tool_names(mcp_stdio_evidence)
    stdio_tools_list_passed = mcp_stdio_evidence is None or (
        stdio_initialize_passed
        and virtual_alias is not None
        and virtual_alias in stdio_listed_tools
    )
    tools_list_passed = virtual_alias is not None and stdio_tools_list_passed
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.tools-list",
            passed=tools_list_passed,
            message=(
                "one deterministic virtual alias exposes the selected cluster"
                if tools_list_passed
                else "the refreshed schema did not produce exactly one eligible virtual alias"
            ),
            evidence={
                "catalog_revision": catalog.revision,
                "matching_aliases": sorted(matching_aliases),
                "catalog_issues": [issue.model_dump(mode="json") for issue in catalog.issues],
                "packaged_stdio": mcp_stdio_evidence or {},
            },
        )
    )

    raw_job = call_status.get("job")
    job = cast(JSON, raw_job) if isinstance(raw_job, dict) else {}
    raw_spec = job.get("spec")
    spec = cast(JSON, raw_spec) if isinstance(raw_spec, dict) else {}
    stdio_call_job_id = _stdio_call_job_id(mcp_stdio_evidence)
    stdio_call_passed = mcp_stdio_evidence is None or (
        stdio_initialize_passed and stdio_call_job_id == call_job_id
    )
    call_passed = (
        job.get("job_id") == call_job_id
        and job.get("cluster") == cluster
        and job.get("kind") == "mcp_call"
        and registration is not None
        and spec.get("server") == registration.command
        and spec.get("server_args") == registration.args
        and spec.get("env_from", {}) == registration.env_from
        and spec.get("operation") == "tools/call"
        and spec.get("tool") == remote_tool_name
        and stdio_call_passed
    )
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.call",
            passed=call_passed,
            message=(
                "virtual alias created the expected durable MCP call job"
                if call_passed
                else "durable call job does not match the selected virtual route"
            ),
            evidence={
                "job_id": job.get("job_id"),
                "cluster": job.get("cluster"),
                "kind": job.get("kind"),
                "spec": spec,
                "stdio_call_job_id": stdio_call_job_id,
                "packaged_stdio": mcp_stdio_evidence or {},
            },
        )
    )

    call_server_artifact = (
        cast(JSON, mcp_result.get("server_artifact"))
        if mcp_result is not None and isinstance(mcp_result.get("server_artifact"), dict)
        else None
    )
    discovery_server_artifact = entry.provenance.server_artifact if entry is not None else None
    server_artifact_passed = (
        call_server_artifact is not None
        and call_server_artifact.get("verified") is True
        and call_server_artifact.get("server_process_artifact_verified") is True
        and bool(call_server_artifact.get("executable"))
        and call_server_artifact.get("install_source") == "wheel"
        and _is_sha256(call_server_artifact.get("install_artifact_sha256"))
        and call_server_artifact == discovery_server_artifact
    )
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.server-artifact",
            passed=server_artifact_passed,
            message=(
                "discovery and call used the same verified MCP server artifact"
                if server_artifact_passed
                else "MCP server artifact identity is missing, mutable, or changed after discovery"
            ),
            evidence={
                "discovery_server_artifact": discovery_server_artifact or {},
                "call_server_artifact": call_server_artifact or {},
            },
        )
    )

    artifacts_by_kind = {
        str(artifact.get("kind")): artifact
        for artifact in artifacts
        if isinstance(artifact.get("kind"), str)
    }
    required_artifact_kinds = {"stdout", "stderr", "mcp_result", "provenance"}
    protocol_result = (
        cast(JSON, mcp_result.get("protocol_result"))
        if mcp_result is not None and isinstance(mcp_result.get("protocol_result"), dict)
        else None
    )
    mcp_result_matches = (
        mcp_result is not None
        and registration is not None
        and mcp_result.get("returncode") == 0
        and mcp_result.get("operation") == "tools/call"
        and mcp_result.get("server") == registration.command
        and mcp_result.get("server_args") == registration.args
        and mcp_result.get("env_from", {}) == registration.env_from
        and mcp_result.get("tool") == remote_tool_name
        and mcp_result.get("arguments", {}) == spec.get("arguments", {})
        and mcp_result.get("protocol_error") is None
        and protocol_result is not None
        and protocol_result.get("isError") is not True
    )
    provenance_job = provenance.get("job") if provenance is not None else None
    provenance_matches = (
        isinstance(provenance_job, dict) and cast(JSON, provenance_job).get("job_id") == call_job_id
    )
    durable_result_passed = (
        job.get("state") == "succeeded"
        and call_status.get("terminal") is True
        and required_artifact_kinds.issubset(artifacts_by_kind)
        and mcp_result_matches
        and provenance_matches
    )
    checks.append(
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.durable-result",
            passed=durable_result_passed,
            message=(
                "terminal call has logs, MCP result, and matching provenance artifacts"
                if durable_result_passed
                else "terminal state or required durable result provenance is incomplete"
            ),
            evidence={
                "state": job.get("state"),
                "terminal": call_status.get("terminal"),
                "artifact_kinds": sorted(artifacts_by_kind),
                "required_artifact_kinds": sorted(required_artifact_kinds),
                "mcp_result_matches": mcp_result_matches,
                "provenance_matches": provenance_matches,
            },
        )
    )
    passed = all(check.passed for check in checks)
    return RemoteMcpAcceptanceReport(
        cluster=cluster,
        server_name=server_name,
        remote_tool_name=remote_tool_name,
        virtual_alias=virtual_alias,
        profile=profile,
        passed=passed,
        checks=checks,
        discovery=discovery_evidence,
        call_job=job,
        artifacts=artifacts,
        mcp_stdio=mcp_stdio_evidence or {},
    )


def _spack_user_contract_check(
    entry: RemoteMcpSchemaCacheEntry | None,
    registration: RemoteMcpServerConfig | None,
) -> RemoteMcpAcceptanceCheck:
    """Require the exact stateless Spack surface approved for desktop agents."""
    expected_names = {"spack_find", "spack_locate", "spack_install"}
    tools = {tool.name: tool for tool in entry.tools} if entry is not None else {}
    actual_names = set(tools)
    allowlisted_names: set[str] = (
        set(registration.allow_tools) if registration is not None else set()
    )
    observed_contract_digest = remote_mcp_schema_digest(list(tools.values()))

    annotation_expectations: dict[str, dict[str, bool]] = {
        "spack_find": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "openWorldHint": False,
        },
        "spack_locate": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "openWorldHint": False,
        },
        "spack_install": {
            "readOnlyHint": False,
            "destructiveHint": False,
            "openWorldHint": True,
        },
    }
    annotation_matches: dict[str, bool] = {}
    schema_matches: dict[str, bool] = {}
    for name, expected_annotations in annotation_expectations.items():
        tool = tools.get(name)
        annotations = tool.annotations if tool is not None else None
        annotation_matches[name] = annotations is not None and all(
            annotations.get(key) is value for key, value in expected_annotations.items()
        )
        schema = tool.input_schema if tool is not None else {}
        raw_required = schema.get("required", [])
        required = cast(list[object], raw_required) if isinstance(raw_required, list) else []
        raw_properties = schema.get("properties", {})
        properties = cast(JSON, raw_properties) if isinstance(raw_properties, dict) else {}
        schema_matches[name] = (
            schema.get("type") == "object"
            and schema.get("additionalProperties") is False
            and (
                name == "spack_find"
                or ("spec" in required and isinstance(properties.get("spec"), dict))
            )
        )

    locate_output = tools.get("spack_locate")
    output_schema = locate_output.output_schema if locate_output is not None else None
    output_properties = (
        cast(JSON, output_schema.get("properties"))
        if output_schema is not None and isinstance(output_schema.get("properties"), dict)
        else {}
    )
    output_required_value: object = (
        output_schema.get("required", []) if output_schema is not None else []
    )
    output_required = (
        cast(list[object], output_required_value) if isinstance(output_required_value, list) else []
    )
    locate_load_spec_matches = (
        output_schema is not None
        and isinstance(output_properties.get("load_spec"), dict)
        and cast(JSON, output_properties["load_spec"]).get("type") == "string"
        and "load_spec" in output_required
    )

    passed = (
        actual_names == expected_names
        and allowlisted_names == expected_names
        and registration is not None
        and registration.contract == "clio-kit-spack-user-v3"
        and "user" in registration.profiles
        and all(annotation_matches.values())
        and all(schema_matches.values())
        and locate_load_spec_matches
        and observed_contract_digest == CLIO_KIT_SPACK_USER_CONTRACT_SHA256
    )
    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.spack-user-contract",
        passed=passed,
        message=(
            "Spack exposes only find, locate, and install with the audited user schemas"
            if passed
            else "Spack user tools, allowlist, schemas, or safety annotations drifted"
        ),
        evidence={
            "expected_tool_names": sorted(expected_names),
            "remote_tool_names": sorted(actual_names),
            "allowlisted_tool_names": sorted(allowlisted_names),
            "profiles": registration.profiles if registration is not None else [],
            "declared_contract": registration.contract if registration is not None else None,
            "annotations_match": annotation_matches,
            "schemas_match": schema_matches,
            "locate_load_spec_matches": locate_load_spec_matches,
            "stateful_load_exposed": "spack_load" in actual_names,
            "expected_contract_sha256": CLIO_KIT_SPACK_USER_CONTRACT_SHA256,
            "expected_clio_kit_version": CLIO_KIT_SPACK_USER_CONTRACT_VERSION,
            "observed_contract_sha256": observed_contract_digest,
        },
    )


def _declared_contract_check(
    entry: RemoteMcpSchemaCacheEntry | None,
    registration: RemoteMcpServerConfig,
) -> RemoteMcpAcceptanceCheck:
    """Evaluate the semantic contract explicitly declared by an operator."""
    if registration.contract == "clio-kit-spack-user-v3":
        return _spack_user_contract_check(entry, registration)
    raise ValueError(f"unsupported remote MCP semantic contract: {registration.contract}")


def _stdio_initialize_passed(evidence: JSON | None) -> bool:
    if evidence is None:
        return True
    response = _as_json(evidence.get("initialize_response"))
    if response is None or response.get("error") is not None:
        return False
    result = _as_json(response.get("result"))
    if result is None:
        return False
    server_info = _as_json(result.get("serverInfo"))
    return (
        evidence.get("boundary") == "packaged_clio_relay_mcp_server_stdio"
        and evidence.get("returncode") == 0
        and isinstance(result.get("protocolVersion"), str)
        and server_info is not None
        and server_info.get("name") == "clio-relay"
    )


def _stdio_listed_tool_names(evidence: JSON | None) -> set[str]:
    if evidence is None:
        return set()
    response = _as_json(evidence.get("tools_list_response"))
    result = _as_json(response.get("result")) if response is not None else None
    tools = result.get("tools") if result is not None else None
    if not isinstance(tools, list):
        return set()
    names: set[str] = set()
    for value in cast(list[object], tools):
        tool = _as_json(value)
        if tool is not None and isinstance(tool.get("name"), str):
            names.add(cast(str, tool["name"]))
    return names


def _stdio_call_job_id(evidence: JSON | None) -> str | None:
    if evidence is None:
        return None
    response = _as_json(evidence.get("tools_call_response"))
    result = _as_json(response.get("result")) if response is not None else None
    if result is None:
        return None
    structured = _as_json(result.get("structuredContent"))
    if structured is not None and isinstance(structured.get("job_id"), str):
        return cast(str, structured["job_id"])
    content = result.get("content")
    if not isinstance(content, list):
        return None
    for value in cast(list[object], content):
        item = _as_json(value)
        if item is None or item.get("type") != "text":
            continue
        text = item.get("text")
        if not isinstance(text, str):
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        typed_payload = _as_json(payload)
        if typed_payload is not None and isinstance(typed_payload.get("job_id"), str):
            return cast(str, typed_payload["job_id"])
    return None


def _as_json(value: object) -> JSON | None:
    return cast(JSON, value) if isinstance(value, dict) else None


def _fsync_cache_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def inject_cluster_argument(input_schema: JSON, *, clusters: list[str]) -> JSON:
    """Copy a remote input schema and add a local-only cluster selector.

    Plain object contracts remain flat for agent ergonomics. Contracts whose
    root composition or own ``cluster`` field makes flat augmentation unsafe
    are preserved under an ``arguments`` object instead.
    """
    _require_bounded_json_structure(input_schema, label="inputSchema")
    error = virtual_schema_error(input_schema)
    if error is not None:
        raise ValueError(error)
    cluster_schema: JSON = {
        "type": "string",
        "enum": sorted(clusters),
        "description": "Configured clio-relay cluster target.",
    }
    if remote_input_schema_requires_wrapper(input_schema):
        nested_schema = deepcopy(input_schema)
        identifier_keyword = _schema_identifier_keyword(nested_schema)
        if identifier_keyword == "id":
            _relocate_legacy_local_references(
                nested_schema,
                pointer_prefix="/properties/arguments",
            )
        elif not _schema_establishes_embedded_resource(
            nested_schema,
            identifier_keyword=identifier_keyword,
        ):
            nested_schema[identifier_keyword] = (
                "urn:clio-relay:remote-mcp-schema:" + _stable_digest({"input_schema": input_schema})
            )
        wrapper: JSON = {
            "type": "object",
            "properties": {
                "cluster": cluster_schema,
                "arguments": nested_schema,
            },
            "required": ["cluster", "arguments"],
            "additionalProperties": False,
        }
        dialect = input_schema.get("$schema")
        if isinstance(dialect, str):
            wrapper["$schema"] = dialect
        return wrapper
    rendered = deepcopy(input_schema)
    properties = cast(JSON, rendered.setdefault("properties", {}))
    properties["cluster"] = cluster_schema
    required = cast(list[str], rendered.setdefault("required", []))
    rendered["required"] = ["cluster", *required]
    rendered["type"] = "object"
    return rendered


def virtual_schema_error(input_schema: JSON) -> str | None:
    """Return why a remote input contract cannot be safely virtualized."""
    schema_type = input_schema.get("type", "object")
    if schema_type != "object":
        return "remote inputSchema must have type object"
    properties = input_schema.get("properties", {})
    if not isinstance(properties, dict):
        return "remote inputSchema properties must be an object"
    required = input_schema.get("required", [])
    if not isinstance(required, list) or not all(
        isinstance(item, str) for item in cast(list[object], required)
    ):
        return "remote inputSchema required must be a string array"
    typed_required = cast(list[str], required)
    if len(typed_required) != len(set(typed_required)):
        return "remote inputSchema required entries must be unique"
    return None


def remote_input_schema_requires_wrapper(input_schema: JSON) -> bool:
    """Return whether a remote schema must be nested below local routing fields."""
    _require_bounded_json_structure(input_schema, label="inputSchema")
    properties = input_schema.get("properties", {})
    required = input_schema.get("required", [])
    return (
        any(key in input_schema for key in _COMPOSED_SCHEMA_KEYS)
        or bool(set(input_schema) - _FLAT_SCHEMA_KEYS)
        or _contains_document_root_reference(input_schema)
        or (isinstance(properties, dict) and "cluster" in properties)
        or (isinstance(required, list) and "cluster" in required)
    )


def _contains_document_root_reference(value: object) -> bool:
    """Return whether a nested schema reference depends on the document root."""
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            mapping = cast(dict[object, object], current)
            for key, item in mapping.items():
                if (
                    key in {"$ref", "$dynamicRef", "$recursiveRef"}
                    and isinstance(item, str)
                    and (item == "#" or item.startswith("#/"))
                ):
                    return True
                stack.append(item)
        elif isinstance(current, list):
            stack.extend(cast(list[object], current))
    return False


def _schema_identifier_keyword(schema: JSON) -> str:
    """Return the resource identifier keyword for a declared JSON Schema dialect."""
    dialect = schema.get("$schema")
    if isinstance(dialect, str) and ("draft-03" in dialect or "draft-04" in dialect):
        return "id"
    return "$id"


def _schema_establishes_embedded_resource(
    schema: JSON,
    *,
    identifier_keyword: str,
) -> bool:
    """Return whether an identifier gives an embedded schema its own resource base."""
    schema_id = schema.get(identifier_keyword)
    if not isinstance(schema_id, str):
        return False
    return bool(schema_id.partition("#")[0])


def _relocate_legacy_local_references(
    value: object,
    *,
    pointer_prefix: str,
    nested_resource: bool = False,
    root: bool = True,
) -> None:
    """Retarget Draft 3/4 document-root references after schema embedding."""
    if isinstance(value, dict):
        mapping = cast(JSON, value)
        establishes_nested_resource = not root and (
            isinstance(mapping.get("id"), str) and bool(cast(str, mapping["id"]).partition("#")[0])
        )
        rewrite_here = not nested_resource and not establishes_nested_resource
        child_nested_resource = nested_resource or establishes_nested_resource
        for key, item in list(mapping.items()):
            if key == "$ref" and isinstance(item, str) and rewrite_here:
                if item == "#":
                    mapping[key] = f"#{pointer_prefix}"
                elif item.startswith("#/"):
                    mapping[key] = f"#{pointer_prefix}{item[1:]}"
                continue
            _relocate_legacy_local_references(
                item,
                pointer_prefix=pointer_prefix,
                nested_resource=child_nested_resource,
                root=False,
            )
    elif isinstance(value, list):
        for item in cast(list[object], value):
            _relocate_legacy_local_references(
                item,
                pointer_prefix=pointer_prefix,
                nested_resource=nested_resource,
                root=False,
            )


def _validate_json_schema(schema: JSON, *, label: str) -> None:
    """Reject malformed or unsupported JSON Schema contracts at ingestion."""
    _require_bounded_json_structure(schema, label=label)
    declared_dialect = schema.get("$schema")
    if isinstance(declared_dialect, str):
        normalized_dialect = declared_dialect.rstrip("#")
        validator = _JSON_SCHEMA_VALIDATORS.get(normalized_dialect)
        if validator is None:
            raise ValueError(f"remote MCP {label} declares an unsupported JSON Schema dialect")
    else:
        validator = Draft202012Validator
    try:
        validator.check_schema(schema)
    except RecursionError as exc:
        raise ValueError(
            f"remote MCP {label} exceeds {MAX_REMOTE_MCP_JSON_DEPTH} nesting levels"
        ) from exc
    except SchemaError as exc:
        raise ValueError(
            f"remote MCP {label} is not valid JSON Schema: " + _bounded_diagnostic(exc.message)
        ) from exc


def _require_bounded_json_structure(value: object, *, label: str) -> None:
    """Bound untrusted JSON before recursive validators or transformations run."""
    stack: list[tuple[object, int]] = [(value, 0)]
    node_count = 0
    while stack:
        current, depth = stack.pop()
        node_count += 1
        if node_count > MAX_REMOTE_MCP_JSON_NODES:
            raise ValueError(f"remote MCP {label} exceeds {MAX_REMOTE_MCP_JSON_NODES} JSON nodes")
        if depth > MAX_REMOTE_MCP_JSON_DEPTH:
            raise ValueError(
                f"remote MCP {label} exceeds {MAX_REMOTE_MCP_JSON_DEPTH} nesting levels"
            )
        if isinstance(current, dict):
            stack.extend((item, depth + 1) for item in cast(dict[object, object], current).values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in cast(list[object], current))


def _require_finite_json(value: object, *, label: str) -> None:
    """Reject non-finite numbers that cannot round-trip through strict JSON."""
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, float) and not math.isfinite(current):
            raise ValueError(f"remote MCP {label} contains a non-finite JSON number")
        if isinstance(current, dict):
            stack.extend(cast(dict[object, object], current).values())
        elif isinstance(current, list):
            stack.extend(cast(list[object], current))


def _bounded_diagnostic(value: object) -> str:
    """Render an untrusted diagnostic without allowing unbounded error output."""
    rendered = value if isinstance(value, str) else repr(value)
    if len(rendered) <= MAX_REMOTE_MCP_DIAGNOSTIC_CHARS:
        return rendered
    return rendered[:MAX_REMOTE_MCP_DIAGNOSTIC_CHARS] + "... [truncated]"


def _reject_nonfinite_json_constant(value: str) -> None:
    """Reject NaN and infinity tokens accepted by Python's permissive decoder."""
    raise _NonFiniteJsonError(
        f"remote MCP discovery artifact contains non-finite JSON token: {value}"
    )


def _parse_remote_tool(value: object) -> RemoteMcpToolSchema:
    if not isinstance(value, dict):
        raise ValueError("remote MCP tools/list entries must be objects")
    tool = cast(JSON, value)
    name = tool.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("remote MCP tool name must be a non-empty string")
    input_schema = tool.get("inputSchema")
    if not isinstance(input_schema, dict):
        raise ValueError(f"remote MCP tool {name} inputSchema must be an object")
    title = tool.get("title")
    description = tool.get("description")
    output_schema = tool.get("outputSchema")
    annotations = tool.get("annotations")
    if title is not None and not isinstance(title, str):
        raise ValueError(f"remote MCP tool {name} title must be a string")
    if description is not None and not isinstance(description, str):
        raise ValueError(f"remote MCP tool {name} description must be a string")
    if output_schema is not None and not isinstance(output_schema, dict):
        raise ValueError(f"remote MCP tool {name} outputSchema must be an object")
    if annotations is not None and not isinstance(annotations, dict):
        raise ValueError(f"remote MCP tool {name} annotations must be an object")
    return RemoteMcpToolSchema(
        name=name,
        title=title,
        description=description,
        input_schema=cast(JSON, input_schema),
        output_schema=cast(JSON | None, output_schema),
        annotations=cast(JSON | None, annotations),
    )


def _assign_aliases(
    grouped: dict[str, list[_Candidate]],
    *,
    reserved_names: set[str],
) -> dict[str, str]:
    bases: dict[str, list[str]] = {}
    for identity, candidates in grouped.items():
        bases.setdefault(candidates[0].base_alias, []).append(identity)
    all_bases = set(bases)
    assigned: dict[str, str] = {}
    used = set(reserved_names)
    for base, identities in sorted(bases.items()):
        sorted_identities = sorted(identities)
        if len(sorted_identities) == 1 and base not in used:
            identity = sorted_identities[0]
            assigned[identity] = base
            used.add(base)
            continue
        for identity in sorted_identities:
            alias = _collision_alias(
                base,
                identity,
                blocked=used | all_bases,
            )
            assigned[identity] = alias
            used.add(alias)
    return assigned


def _collision_alias(base: str, identity: str, *, blocked: set[str]) -> str:
    for length in range(10, len(identity) + 1):
        candidate = f"{base}_{identity[:length]}"
        if candidate not in blocked:
            return candidate
    suffix = 2
    while f"{base}_{identity}_{suffix}" in blocked:
        suffix += 1
    return f"{base}_{identity}_{suffix}"


def _profile_allows(profiles: list[RemoteMcpProfile], profile: str) -> bool:
    if profile == "all":
        return True
    normalized = "user" if profile in {"", "agent", "user"} else profile
    return normalized in profiles


def _safe_name(value: str) -> str:
    normalized = _SAFE_NAME_PATTERN.sub("_", value.strip().lower()).strip("_")
    if normalized:
        return normalized
    return f"unnamed_{hashlib.sha256(value.encode('utf-8')).hexdigest()[:10]}"


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value.lower())
    )


def _server_artifact_verified(server_artifact: JSON) -> bool:
    return (
        server_artifact.get("verified") is True
        and server_artifact.get("server_process_artifact_verified") is True
        and isinstance(server_artifact.get("executable"), dict)
    )


def _stable_digest(value: JSON) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
