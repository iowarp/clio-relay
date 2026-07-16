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
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast
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
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.cluster_config import (
    ClusterRegistry,
    RemoteMcpProfile,
    RemoteMcpServerConfig,
    cluster_route_revision,
    default_registry_path,
    ensure_private_configuration_directory,
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
MAX_REMOTE_MCP_RESULT_SCHEMA_ERRORS = 8
MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL = 64
MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS = 64
MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES = 16 * 1024 * 1024
MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES = 64 * 1024
MAX_VIRTUAL_REMOTE_MCP_CANDIDATES = 10_000
MAX_REMOTE_MCP_CATALOG_ISSUES = 10_000
MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH = 64
REMOTE_MCP_REPLACE_ATTEMPTS = 25
REMOTE_MCP_REPLACE_RETRY_SECONDS = 0.02
CLIO_KIT_SPACK_USER_WHEEL_VERSION = "2.4.8"
CLIO_KIT_SPACK_USER_CONTRACT_ID = "clio-kit-spack-user-v2"
# Digest the MCP wire ``tools/list`` result. FastMCP's in-process FunctionTool
# schemas retain ``$defs`` that its protocol serializer dereferences, so their
# digest is intentionally not the relay contract.
CLIO_KIT_SPACK_USER_CONTRACT_SHA256 = (
    "3c5412148c770f4844e98eb893c4db0d0afdbf13afe967df67bd5f7d25e1f7db"
)
CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID = "clio-kit-scientific-catalog-user-v1"
CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256 = (
    "a53006f24f4698f659f0a7c8bf61fc7bd7ad23274b06d2eed2ccfca68b9ecb0a"
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


class _JsonSchemaInstanceValidator(Protocol):
    """Typed subset of a jsonschema validator used for instance checks."""

    def iter_errors(self, instance: object) -> Iterable[JsonSchemaValidationError]:
        """Yield every schema violation observed in one JSON-compatible instance."""
        ...


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
        "catalog_revision": {"type": "string"},
    },
    "required": [
        "cluster",
        "job_id",
        "state",
        "kind",
        "terminal",
        "route_revision",
        "catalog_revision",
    ],
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
        ensure_private_configuration_directory(path.parent)
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
        ensure_private_configuration_directory(path.parent)
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


class RemoteMcpStructuredResultExpectation(BaseModel):
    """Operator-supplied semantic expectations for one structured MCP result."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.remote-mcp-result-expectation.v1"] = (
        "clio-relay.remote-mcp-result-expectation.v1"
    )
    contract: Literal["clio-kit-spack-user-v2"]
    tool: Literal["spack_find", "spack_locate", "spack_install"]
    package_name: str = Field(min_length=1, max_length=255, pattern=r"^[A-Za-z0-9_.+-]+$")
    dag_hash: str = Field(pattern=r"^[a-z0-9]{32}$")
    requested_spec: str | None = Field(default=None, min_length=1, max_length=4_096)
    prefix: str | None = Field(default=None, min_length=2, max_length=4_096)
    reuse: bool | None = None
    fresh_install_store_root: str | None = Field(default=None, min_length=2, max_length=4_096)
    fresh_install_configuration_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    fresh_install_configuration_manifest_path: str | None = Field(
        default=None,
        min_length=2,
        max_length=4_096,
    )

    @model_validator(mode="after")
    def validate_operation_fields(self) -> RemoteMcpStructuredResultExpectation:
        """Require only the operation-specific expectations used by the contract."""
        if self.tool == "spack_find":
            if (
                self.requested_spec is not None
                or self.prefix is not None
                or self.reuse is not None
                or self.fresh_install_store_root is not None
                or self.fresh_install_configuration_sha256 is not None
                or self.fresh_install_configuration_manifest_path is not None
            ):
                raise ValueError(
                    "spack_find must not declare requested_spec, prefix, reuse, "
                    "or fresh-install configuration expectations"
                )
            return self
        if self.requested_spec is None:
            raise ValueError(f"{self.tool} requires requested_spec")
        if self.tool == "spack_locate":
            if (
                self.reuse is not None
                or self.fresh_install_store_root is not None
                or self.fresh_install_configuration_sha256 is not None
                or self.fresh_install_configuration_manifest_path is not None
            ):
                raise ValueError("spack_locate must not declare reuse or fresh_install_store_root")
            if not _is_canonical_absolute_posix_path(self.prefix):
                raise ValueError("spack_locate requires a canonical absolute POSIX prefix")
        if self.tool == "spack_install":
            if self.prefix is not None:
                raise ValueError("spack_install must not declare prefix")
            if self.reuse is None:
                raise ValueError("spack_install requires reuse")
            configuration_fields = (
                self.fresh_install_store_root,
                self.fresh_install_configuration_sha256,
                self.fresh_install_configuration_manifest_path,
            )
            if any(value is not None for value in configuration_fields):
                if not all(value is not None for value in configuration_fields):
                    raise ValueError(
                        "fresh install requires store root, configuration SHA-256, and "
                        "configuration manifest path together"
                    )
                if self.reuse is not False:
                    raise ValueError("fresh_install_store_root requires spack_install reuse=false")
                if not _is_canonical_absolute_posix_path(self.fresh_install_store_root):
                    raise ValueError(
                        "fresh_install_store_root must be a canonical absolute POSIX path"
                    )
                if not _is_canonical_absolute_posix_path(
                    self.fresh_install_configuration_manifest_path
                ):
                    raise ValueError(
                        "fresh_install_configuration_manifest_path must be a canonical "
                        "absolute POSIX path"
                    )
        return self


class RemoteMcpSpackTransitionArtifactEvidence(BaseModel):
    """Bounded identity for one durable artifact in a Spack transition call."""

    model_config = ConfigDict(extra="forbid")

    artifact_id: str | None = Field(default=None, max_length=1_024)
    job_id: str | None = Field(default=None, max_length=1_024)
    kind: str | None = Field(default=None, max_length=128)
    sha256: str | None = Field(default=None, max_length=64)
    uri: str | None = Field(default=None, max_length=4_096)


class RemoteMcpSpackTransitionStdioEvidence(BaseModel):
    """Bounded packaged-stdio proof associated with one transition call."""

    model_config = ConfigDict(extra="forbid")

    boundary: str | None = Field(default=None, max_length=128)
    returncode: int | None = None
    initialize_passed: bool
    tools_list_passed: bool
    call_job_id: str | None = Field(default=None, max_length=1_024)


class RemoteMcpSpackConfigurationComponentObservation(BaseModel):
    """One regular file bound into an observed fresh-install configuration."""

    model_config = ConfigDict(extra="forbid")

    relative_path: str = Field(min_length=1, max_length=1_024)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(
        ge=0,
        le=MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
    )
    regular_file: Literal[True] = True

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        """Reject absolute, traversing, or non-canonical component paths."""
        if not _is_canonical_relative_posix_path(value):
            raise ValueError("configuration component path must be canonical and relative")
        return value


class RemoteMcpSpackConfigurationObservation(BaseModel):
    """Independent digest observation of one bounded configuration manifest."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.spack-configuration-observation.v1"] = (
        "clio-relay.spack-configuration-observation.v1"
    )
    phase: Literal["preinstall", "postinstall"]
    manifest_path: str = Field(min_length=2, max_length=4_096)
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_size_bytes: int = Field(
        ge=1,
        le=MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES,
    )
    manifest_regular_file: Literal[True] = True
    components: list[RemoteMcpSpackConfigurationComponentObservation] = Field(
        min_length=1,
        max_length=MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS,
    )

    @model_validator(mode="after")
    def validate_manifest(self) -> RemoteMcpSpackConfigurationObservation:
        """Require an absolute manifest and one canonical, sorted component set."""
        if not _is_canonical_absolute_posix_path(self.manifest_path):
            raise ValueError("configuration manifest path must be canonical and absolute")
        paths = [component.relative_path for component in self.components]
        if paths != sorted(paths) or len(paths) != len(set(paths)):
            raise ValueError("configuration component paths must be unique and sorted")
        return self


class RemoteMcpSpackTransitionCallEvidence(BaseModel):
    """Bounded durable call evidence for one phase of a fresh Spack install."""

    model_config = ConfigDict(extra="forbid")

    phase: Literal["preinstall", "install", "postinstall"]
    report_passed: bool
    cluster: str = Field(min_length=1, max_length=255)
    server_name: str = Field(min_length=1, max_length=255)
    profile: str = Field(min_length=1, max_length=64)
    remote_tool_name: str = Field(min_length=1, max_length=64)
    virtual_alias: str | None = Field(default=None, max_length=64)
    job_id: str | None = Field(default=None, max_length=1_024)
    state: str | None = Field(default=None, max_length=128)
    arguments: JSON = Field(default_factory=dict)
    artifacts: list[RemoteMcpSpackTransitionArtifactEvidence] = Field(
        default_factory=lambda: list[RemoteMcpSpackTransitionArtifactEvidence](),
        max_length=MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL,
    )
    artifacts_truncated: bool = False
    stdio: RemoteMcpSpackTransitionStdioEvidence
    structured_result: JSON = Field(default_factory=dict)


class RemoteMcpSpackInstallTransitionEvidence(BaseModel):
    """Ordered, machine-readable evidence for a disposable-store Spack install."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.spack-fresh-install-transition.v1"] = (
        "clio-relay.spack-fresh-install-transition.v1"
    )
    cluster: str = Field(min_length=1, max_length=255)
    server_name: str = Field(min_length=1, max_length=255)
    profile: str = Field(min_length=1, max_length=64)
    requested_spec: str = Field(min_length=1, max_length=4_096)
    package_name: str = Field(min_length=1, max_length=255)
    dag_hash: str = Field(pattern=r"^[a-z0-9]{32}$")
    fresh_install_store_root: str = Field(min_length=2, max_length=4_096)
    fresh_install_configuration_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    fresh_install_configuration_manifest_path: str = Field(min_length=2, max_length=4_096)
    preinstall_configuration: RemoteMcpSpackConfigurationObservation
    postinstall_configuration: RemoteMcpSpackConfigurationObservation
    executed_spack_command_path: str | None = Field(default=None, max_length=4_096)
    executed_spack_command_relative_path: str | None = Field(default=None, max_length=1_024)
    executed_spack_command_sha256: str | None = Field(
        default=None,
        max_length=64,
    )
    executed_spack_command_size_bytes: int | None = Field(
        default=None,
        ge=0,
        le=MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
    )
    registration_revision: str | None = Field(default=None, max_length=128)
    cluster_route_revision: str | None = Field(default=None, max_length=128)
    catalog_revision: str | None = Field(default=None, max_length=128)
    server_artifact_sha256: str | None = Field(default=None, max_length=64)
    preinstall: RemoteMcpSpackTransitionCallEvidence
    install: RemoteMcpSpackTransitionCallEvidence
    postinstall: RemoteMcpSpackTransitionCallEvidence

    @model_validator(mode="after")
    def validate_transition_shape(self) -> RemoteMcpSpackInstallTransitionEvidence:
        """Reject forged phase labels or an unsafe disposable-store root."""
        if not _is_canonical_absolute_posix_path(self.fresh_install_store_root):
            raise ValueError("fresh_install_store_root must be a canonical absolute POSIX path")
        if not _is_canonical_absolute_posix_path(self.fresh_install_configuration_manifest_path):
            raise ValueError(
                "fresh_install_configuration_manifest_path must be a canonical absolute POSIX path"
            )
        command_identity = (
            self.executed_spack_command_path,
            self.executed_spack_command_relative_path,
            self.executed_spack_command_sha256,
            self.executed_spack_command_size_bytes,
        )
        if any(value is not None for value in command_identity):
            if not all(value is not None for value in command_identity):
                raise ValueError("executed Spack command identity must be complete")
            if not _is_canonical_absolute_posix_path(self.executed_spack_command_path):
                raise ValueError("executed Spack command path must be canonical and absolute")
            if not _is_canonical_relative_posix_path(self.executed_spack_command_relative_path):
                raise ValueError("executed Spack command relative path must be canonical")
            command_sha256 = cast(str, self.executed_spack_command_sha256)
            if len(command_sha256) != 64 or any(
                character not in "0123456789abcdef" for character in command_sha256
            ):
                raise ValueError("executed Spack command SHA-256 must be lowercase hexadecimal")
            if cast(int, self.executed_spack_command_size_bytes) < 1:
                raise ValueError("executed Spack command size must be positive")
            expected_path = str(
                PurePosixPath(self.fresh_install_configuration_manifest_path).parent
                / cast(str, self.executed_spack_command_relative_path)
            )
            if self.executed_spack_command_path != expected_path:
                raise ValueError(
                    "executed Spack command path must resolve from the configuration manifest"
                )
            relative_path = cast(str, self.executed_spack_command_relative_path)
            preinstall_components = [
                component
                for component in self.preinstall_configuration.components
                if component.relative_path == relative_path
            ]
            postinstall_components = [
                component
                for component in self.postinstall_configuration.components
                if component.relative_path == relative_path
            ]
            if len(preinstall_components) != 1 or len(postinstall_components) != 1:
                raise ValueError(
                    "executed Spack command must identify one preinstall and postinstall "
                    "configuration component"
                )
            if preinstall_components[0] != postinstall_components[0]:
                raise ValueError(
                    "executed Spack command configuration component must remain unchanged"
                )
            if (
                command_sha256 != preinstall_components[0].sha256
                or self.executed_spack_command_size_bytes != preinstall_components[0].size_bytes
            ):
                raise ValueError(
                    "executed Spack command SHA-256 and size must match its configuration component"
                )
        if (
            self.preinstall_configuration.phase != "preinstall"
            or self.postinstall_configuration.phase != "postinstall"
        ):
            raise ValueError("configuration observations must retain their ordered phases")
        expected_phases = (
            (self.preinstall, "preinstall", "spack_find"),
            (self.install, "install", "spack_install"),
            (self.postinstall, "postinstall", "spack_locate"),
        )
        for call, phase, tool in expected_phases:
            if call.phase != phase or call.remote_tool_name != tool:
                raise ValueError(f"{phase} evidence must represent {tool}")
        return self


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
    spack_install_transition: RemoteMcpSpackInstallTransitionEvidence | None = None

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
        if self.spack_install_transition is None and isinstance(call_job_id, str):
            call_metadata = {
                **self.call_job,
                "remote_mcp_server_name": self.server_name,
                "remote_mcp_tool_name": self.remote_tool_name,
                "virtual_alias": self.virtual_alias,
                "profile": self.profile,
            }
            result_check = next(
                (check for check in self.checks if check.name == "remote-mcp.structured-result"),
                None,
            )
            if result_check is not None:
                call_metadata["structured_result_assertion"] = result_check.evidence
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
        if self.spack_install_transition is None:
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
                            str(artifact["sha256"])
                            if isinstance(artifact.get("sha256"), str)
                            else None
                        ),
                    )
                )
        else:
            _append_spack_transition_resources(report, self.spack_install_transition)
        server_check = next(
            (check for check in self.checks if check.name == "remote-mcp.server-artifact"),
            None,
        )
        contract_check = next(
            (check for check in self.checks if check.name == "remote-mcp.spack-user-contract"),
            None,
        )
        contract_metadata: JSON = {}
        if contract_check is not None:
            contract_id = contract_check.evidence.get("declared_contract")
            contract_sha256 = contract_check.evidence.get("observed_contract_sha256")
            if isinstance(contract_id, str):
                contract_metadata["contract_id"] = contract_id
            if isinstance(contract_sha256, str):
                contract_metadata["contract_sha256"] = contract_sha256
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
                        **contract_metadata,
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


def _append_spack_transition_resources(
    report: LiveValidationReport,
    transition: RemoteMcpSpackInstallTransitionEvidence,
) -> None:
    """Append phase-scoped jobs and artifacts without duplicating the install call."""
    from clio_relay.validation_report import EvidenceReference, ValidationResource

    roles = {
        "preinstall": "spack_preinstall_find",
        "install": "spack_fresh_install",
        "postinstall": "spack_postinstall_locate",
    }
    report.resources.append(
        ValidationResource(
            kind="configuration_manifest",
            resource_id=transition.fresh_install_configuration_sha256,
            role="spack_fresh_install_configuration",
            cluster=transition.cluster,
            state="verified",
            references=[transition.fresh_install_configuration_manifest_path],
            metadata={
                "expected_sha256": transition.fresh_install_configuration_sha256,
                "preinstall": transition.preinstall_configuration.model_dump(mode="json"),
                "postinstall": transition.postinstall_configuration.model_dump(mode="json"),
            },
        )
    )
    report.artifacts.append(
        EvidenceReference(
            kind="spack_fresh_install_configuration",
            reference=transition.fresh_install_configuration_manifest_path,
            sha256=transition.fresh_install_configuration_sha256,
        )
    )
    for call in (transition.preinstall, transition.install, transition.postinstall):
        role = roles[call.phase]
        if call.job_id is not None:
            report.resources.append(
                ValidationResource(
                    kind="relay_job",
                    resource_id=call.job_id,
                    role=role,
                    cluster=transition.cluster,
                    state=call.state,
                    metadata={
                        "remote_mcp_server_name": transition.server_name,
                        "remote_mcp_tool_name": call.remote_tool_name,
                        "virtual_alias": call.virtual_alias,
                        "profile": transition.profile,
                        "arguments": call.arguments,
                        "stdio": call.stdio.model_dump(mode="json"),
                        "structured_result": call.structured_result,
                    },
                )
            )
        for artifact in call.artifacts:
            if artifact.artifact_id is None:
                continue
            artifact_role = f"{role}_{artifact.kind or 'artifact'}"
            references = [artifact.uri] if artifact.uri is not None else []
            report.resources.append(
                ValidationResource(
                    kind="artifact",
                    resource_id=artifact.artifact_id,
                    role=artifact_role,
                    cluster=transition.cluster,
                    references=references,
                    metadata={
                        **artifact.model_dump(mode="json"),
                        "transition_phase": call.phase,
                    },
                )
            )
            report.artifacts.append(
                EvidenceReference(
                    kind=artifact_role,
                    reference=(
                        artifact.uri
                        if artifact.uri is not None
                        else f"relay-artifact://{transition.cluster}/{artifact.artifact_id}"
                    ),
                    sha256=artifact.sha256,
                )
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
    cluster_route_revision: str
    registration_revision: str


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
    cluster_route_revisions: dict[str, str] = field(default_factory=lambda: dict[str, str]())
    jarvis_artifact_bindings: dict[str, str | None] = field(
        default_factory=lambda: dict[str, str | None]()
    )

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


def remote_mcp_registration_revision(registration: RemoteMcpServerConfig) -> str:
    """Hash the complete operator-controlled registration used for one route."""
    return _stable_digest({"registration": registration.model_dump(mode="json")})


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

    route_revisions = {
        cluster: cluster_route_revision(definition)
        for cluster, definition in sorted(registry.clusters.items())
    }
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
                cluster_route_revision=route_revisions[candidate.cluster],
                registration_revision=remote_mcp_registration_revision(candidate.registration),
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
            "cluster_routes": route_revisions,
            "tools": {
                alias: {
                    "namespace": tool.namespace,
                    "remote_tool": tool.remote_tool.model_dump(mode="json"),
                    "arguments_wrapped": tool.arguments_wrapped,
                    "routes": {
                        cluster: {
                            "server_name": route.server_name,
                            "registration_revision": route.registration_revision,
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
        cluster_route_revisions=route_revisions,
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
    result_expectation: RemoteMcpStructuredResultExpectation | None = None,
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
        "registration_revision": (
            remote_mcp_registration_revision(registration) if registration is not None else None
        ),
        "cluster_route_revision": (
            cluster_route_revision(definition) if definition is not None else None
        ),
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
    selected_route = (
        catalog.tools[virtual_alias].routes.get(cluster) if virtual_alias is not None else None
    )
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
                "registration_revision": (
                    selected_route.registration_revision if selected_route is not None else None
                ),
                "cluster_route_revision": (
                    selected_route.cluster_route_revision if selected_route is not None else None
                ),
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
    expected_server_artifact_digest = (
        selected_route.expected_server_artifact_digest if selected_route is not None else None
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
        and _is_sha256(expected_server_artifact_digest)
        and spec.get("expected_server_artifact_digest") == expected_server_artifact_digest
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
                "selected_route_server_artifact_digest": expected_server_artifact_digest,
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
    computed_server_artifact_digest = (
        remote_mcp_server_artifact_digest(call_server_artifact)
        if call_server_artifact is not None
        else None
    )
    server_artifact_passed = (
        call_server_artifact is not None
        and call_server_artifact.get("verified") is True
        and call_server_artifact.get("server_process_artifact_verified") is True
        and bool(call_server_artifact.get("executable"))
        and _immutable_remote_mcp_install_verified(call_server_artifact)
        and _is_sha256(call_server_artifact.get("install_artifact_sha256"))
        and call_server_artifact == discovery_server_artifact
        and computed_server_artifact_digest == expected_server_artifact_digest
        and mcp_result is not None
        and mcp_result.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and mcp_result.get("observed_server_artifact_digest") == expected_server_artifact_digest
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
                "selected_route_server_artifact_digest": expected_server_artifact_digest,
                "computed_server_artifact_digest": computed_server_artifact_digest,
                "result_expected_server_artifact_digest": (
                    mcp_result.get("expected_server_artifact_digest")
                    if mcp_result is not None
                    else None
                ),
                "result_observed_server_artifact_digest": (
                    mcp_result.get("observed_server_artifact_digest")
                    if mcp_result is not None
                    else None
                ),
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
    if result_expectation is not None:
        matching_tools = (
            [tool for tool in entry.tools if tool.name == remote_tool_name]
            if entry is not None
            else []
        )
        output_schema = matching_tools[0].output_schema if len(matching_tools) == 1 else None
        checks.append(
            build_remote_mcp_structured_result_check(
                expectation=result_expectation,
                remote_tool_name=remote_tool_name,
                arguments=spec.get("arguments", {}),
                protocol_result=protocol_result,
                output_schema=output_schema,
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


def build_remote_mcp_spack_fresh_install_transition_report(
    *,
    preinstall_report: RemoteMcpAcceptanceReport,
    install_report: RemoteMcpAcceptanceReport,
    postinstall_report: RemoteMcpAcceptanceReport,
    preinstall_protocol_result: JSON | None,
    install_protocol_result: JSON | None,
    postinstall_protocol_result: JSON | None,
    install_expectation: RemoteMcpStructuredResultExpectation,
    preinstall_configuration: RemoteMcpSpackConfigurationObservation,
    postinstall_configuration: RemoteMcpSpackConfigurationObservation,
) -> RemoteMcpAcceptanceReport:
    """Bind absent, install, and locate calls into one fail-closed Spack proof.

    The returned acceptance report retains the install call as its primary
    operation, while phase-prefixed checks and transition evidence prove that
    the package was absent immediately before a non-reusing install and was
    subsequently located strictly inside the disposable acceptance store.
    """
    store_root = install_expectation.fresh_install_store_root
    requested_spec = install_expectation.requested_spec
    configuration_sha256 = install_expectation.fresh_install_configuration_sha256
    configuration_manifest_path = install_expectation.fresh_install_configuration_manifest_path
    if (
        install_expectation.tool != "spack_install"
        or install_expectation.reuse is not False
        or requested_spec is None
        or store_root is None
        or configuration_sha256 is None
        or configuration_manifest_path is None
    ):
        raise ValueError(
            "fresh Spack transition requires a spack_install expectation with "
            "reuse=false, fresh_install_store_root, configuration SHA-256, and "
            "configuration manifest path"
        )

    configuration_check, executed_wrapper = _spack_fresh_configuration_check(
        expected_sha256=configuration_sha256,
        expected_manifest_path=configuration_manifest_path,
        preinstall=preinstall_configuration,
        postinstall=postinstall_configuration,
        install_report=install_report,
    )

    preinstall_check, preinstall_structured = _spack_preinstall_absent_check(
        report=preinstall_report,
        protocol_result=preinstall_protocol_result,
        expectation=install_expectation,
    )
    install_check, install_structured = _spack_fresh_install_check(
        report=install_report,
        protocol_result=install_protocol_result,
        expectation=install_expectation,
    )
    locate_check, locate_structured, locate_prefix = _spack_postinstall_locate_check(
        report=postinstall_report,
        protocol_result=postinstall_protocol_result,
        expectation=install_expectation,
    )
    disposable_store_passed = _is_strict_canonical_posix_descendant(
        locate_prefix,
        store_root,
    )
    disposable_store_check = RemoteMcpAcceptanceCheck(
        name="remote-mcp.spack-disposable-store",
        passed=disposable_store_passed,
        message=(
            "installed prefix is strictly inside the disposable Spack store"
            if disposable_store_passed
            else "installed prefix is not strictly inside the disposable Spack store"
        ),
        evidence={
            "fresh_install_store_root": store_root,
            "observed_prefix": _bounded_evidence_scalar(locate_prefix),
            "root_is_canonical_absolute": _is_canonical_absolute_posix_path(store_root),
            "prefix_is_strict_descendant": disposable_store_passed,
        },
    )

    identity_check, identity = _spack_transition_identity_check(
        preinstall_report=preinstall_report,
        install_report=install_report,
        postinstall_report=postinstall_report,
    )
    durable_check = _spack_transition_durable_evidence_check(
        preinstall_report=preinstall_report,
        install_report=install_report,
        postinstall_report=postinstall_report,
    )
    transition = RemoteMcpSpackInstallTransitionEvidence(
        cluster=install_report.cluster,
        server_name=install_report.server_name,
        profile=install_report.profile,
        requested_spec=requested_spec,
        package_name=install_expectation.package_name,
        dag_hash=install_expectation.dag_hash,
        fresh_install_store_root=store_root,
        fresh_install_configuration_sha256=configuration_sha256,
        fresh_install_configuration_manifest_path=configuration_manifest_path,
        preinstall_configuration=preinstall_configuration,
        postinstall_configuration=postinstall_configuration,
        executed_spack_command_path=(
            executed_wrapper["path"] if configuration_check.passed else None
        ),
        executed_spack_command_relative_path=(
            executed_wrapper["relative_path"] if configuration_check.passed else None
        ),
        executed_spack_command_sha256=(
            executed_wrapper["sha256"] if configuration_check.passed else None
        ),
        executed_spack_command_size_bytes=(
            executed_wrapper["size_bytes"] if configuration_check.passed else None
        ),
        registration_revision=identity["registration_revision"],
        cluster_route_revision=identity["cluster_route_revision"],
        catalog_revision=identity["catalog_revision"],
        server_artifact_sha256=identity["server_artifact_sha256"],
        preinstall=_spack_transition_call_evidence(
            report=preinstall_report,
            phase="preinstall",
            structured_result=preinstall_structured,
        ),
        install=_spack_transition_call_evidence(
            report=install_report,
            phase="install",
            structured_result=install_structured,
        ),
        postinstall=_spack_transition_call_evidence(
            report=postinstall_report,
            phase="postinstall",
            structured_result=locate_structured,
        ),
    )

    flattened_checks = [
        *_phase_prefixed_acceptance_checks(preinstall_report, phase="preinstall"),
        *(check.model_copy(deep=True) for check in install_report.checks),
        *_phase_prefixed_acceptance_checks(postinstall_report, phase="postinstall"),
        identity_check,
        durable_check,
        configuration_check,
        preinstall_check,
        install_check,
        locate_check,
        disposable_store_check,
    ]
    flattened_checks = _uniquely_named_acceptance_checks(flattened_checks)
    passed = all(check.passed for check in flattened_checks)
    payload = install_report.model_dump(mode="python")
    payload.update(
        {
            "passed": passed,
            "checks": flattened_checks,
            "spack_install_transition": transition,
        }
    )
    return RemoteMcpAcceptanceReport.model_validate(payload)


def _spack_fresh_configuration_check(
    *,
    expected_sha256: str,
    expected_manifest_path: str,
    preinstall: RemoteMcpSpackConfigurationObservation,
    postinstall: RemoteMcpSpackConfigurationObservation,
    install_report: RemoteMcpAcceptanceReport,
) -> tuple[RemoteMcpAcceptanceCheck, JSON]:
    """Bind independently observed wrapper/config bytes before and after installation."""
    pre_components = [component.model_dump(mode="json") for component in preinstall.components]
    post_components = [component.model_dump(mode="json") for component in postinstall.components]
    digest_matches = (
        preinstall.manifest_sha256 == expected_sha256
        and postinstall.manifest_sha256 == expected_sha256
    )
    path_matches = (
        preinstall.manifest_path == expected_manifest_path
        and postinstall.manifest_path == expected_manifest_path
    )
    components_match = pre_components == post_components
    manifest_metadata_matches = (
        preinstall.manifest_size_bytes == postinstall.manifest_size_bytes
        and preinstall.manifest_regular_file
        and postinstall.manifest_regular_file
    )
    phases_match = preinstall.phase == "preinstall" and postinstall.phase == "postinstall"
    wrapper_binding = _spack_command_configuration_binding(
        install_report=install_report,
        manifest_path=expected_manifest_path,
        preinstall=preinstall,
        postinstall=postinstall,
    )
    wrapper_matches = wrapper_binding["matches"] is True
    passed = (
        digest_matches
        and path_matches
        and components_match
        and manifest_metadata_matches
        and phases_match
        and wrapper_matches
    )
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-fresh-configuration",
            passed=passed,
            message=(
                "executed Spack wrapper and configuration bytes remained exactly bound"
                if passed
                else "executed Spack wrapper or configuration identity was not exactly bound"
            ),
            evidence={
                "expected": {
                    "manifest_path": expected_manifest_path,
                    "configuration_sha256": expected_sha256,
                },
                "preinstall": preinstall.model_dump(mode="json"),
                "postinstall": postinstall.model_dump(mode="json"),
                "digest_matches": digest_matches,
                "path_matches": path_matches,
                "components_match": components_match,
                "manifest_metadata_matches": manifest_metadata_matches,
                "phases_match": phases_match,
                "executed_spack_command": wrapper_binding,
                "wrapper_matches": wrapper_matches,
            },
        ),
        wrapper_binding,
    )


def _spack_command_configuration_binding(
    *,
    install_report: RemoteMcpAcceptanceReport,
    manifest_path: str,
    preinstall: RemoteMcpSpackConfigurationObservation,
    postinstall: RemoteMcpSpackConfigurationObservation,
) -> JSON:
    """Bind the executed ``--spack-command`` path to one observed manifest file."""
    evidence: JSON = {
        "matches": False,
        "path": None,
        "relative_path": None,
        "sha256": None,
        "size_bytes": None,
        "failures": [],
    }
    failures = cast(list[str], evidence["failures"])
    call_checks = [check for check in install_report.checks if check.name == "remote-mcp.call"]
    if len(call_checks) != 1 or not call_checks[0].passed or not install_report.passed:
        failures.append("install report does not contain one passing immutable call binding")
    spec = _as_json(install_report.call_job.get("spec")) or {}
    raw_server_args = spec.get("server_args")
    server_args = cast(list[object], raw_server_args) if isinstance(raw_server_args, list) else []
    candidates: list[str] = []
    for index, value in enumerate(server_args):
        if value == "--spack-command" and index + 1 < len(server_args):
            next_value = server_args[index + 1]
            if isinstance(next_value, str):
                candidates.append(next_value)
        elif isinstance(value, str) and value.startswith("--spack-command="):
            candidates.append(value.partition("=")[2])
    if len(candidates) != 1 or not _is_canonical_absolute_posix_path(candidates[0]):
        failures.append("install call does not contain one canonical --spack-command path")
        return evidence
    wrapper_path = candidates[0]
    manifest_parent = PurePosixPath(manifest_path).parent
    typed_wrapper = PurePosixPath(wrapper_path)
    try:
        relative = typed_wrapper.relative_to(manifest_parent)
    except ValueError:
        failures.append("executed Spack wrapper is outside the configuration manifest root")
        return evidence
    relative_path = str(relative)
    if not _is_canonical_relative_posix_path(relative_path):
        failures.append("executed Spack wrapper relative path is not canonical")
        return evidence
    pre_matches = [
        component for component in preinstall.components if component.relative_path == relative_path
    ]
    post_matches = [
        component
        for component in postinstall.components
        if component.relative_path == relative_path
    ]
    if len(pre_matches) != 1 or len(post_matches) != 1:
        failures.append("executed Spack wrapper is not one unique manifest component")
        return evidence
    pre_component = pre_matches[0]
    post_component = post_matches[0]
    component_matches = pre_component == post_component and pre_component.regular_file
    if not component_matches:
        failures.append("executed Spack wrapper bytes or regular-file identity changed")
    evidence.update(
        {
            "matches": not failures,
            "path": wrapper_path,
            "relative_path": relative_path,
            "sha256": pre_component.sha256,
            "size_bytes": pre_component.size_bytes,
        }
    )
    return evidence


def _phase_prefixed_acceptance_checks(
    report: RemoteMcpAcceptanceReport,
    *,
    phase: Literal["preinstall", "postinstall"],
) -> list[RemoteMcpAcceptanceCheck]:
    """Copy ordinary acceptance checks under one unambiguous phase namespace."""
    checks: list[RemoteMcpAcceptanceCheck] = []
    for check in report.checks:
        suffix = check.name.removeprefix("remote-mcp.")
        checks.append(
            RemoteMcpAcceptanceCheck(
                name=f"remote-mcp.{phase}.{suffix}",
                passed=check.passed,
                message=check.message,
                evidence=deepcopy(check.evidence),
            )
        )
    return checks


def _uniquely_named_acceptance_checks(
    checks: list[RemoteMcpAcceptanceCheck],
) -> list[RemoteMcpAcceptanceCheck]:
    """Preserve every assertion while giving duplicate source checks stable suffixes."""
    occurrences: dict[str, int] = {}
    result: list[RemoteMcpAcceptanceCheck] = []
    for check in checks:
        occurrence = occurrences.get(check.name, 0) + 1
        occurrences[check.name] = occurrence
        if occurrence == 1:
            result.append(check)
            continue
        result.append(
            RemoteMcpAcceptanceCheck(
                name=f"{check.name}-{occurrence}",
                passed=check.passed,
                message=check.message,
                evidence=deepcopy(check.evidence),
            )
        )
    return result


def _spack_transition_identity_check(
    *,
    preinstall_report: RemoteMcpAcceptanceReport,
    install_report: RemoteMcpAcceptanceReport,
    postinstall_report: RemoteMcpAcceptanceReport,
) -> tuple[RemoteMcpAcceptanceCheck, dict[str, str | None]]:
    """Require all phases to retain one registration, catalog, and wheel identity."""
    reports = (preinstall_report, install_report, postinstall_report)
    scopes = {(report.cluster, report.server_name, report.profile) for report in reports}
    tool_names = tuple(report.remote_tool_name for report in reports)
    reports_passed = all(
        report.passed and all(check.passed for check in report.checks) for report in reports
    )
    registration_revisions = tuple(
        _acceptance_check_string(report, "remote-mcp.register", "registration_revision")
        for report in reports
    )
    cluster_route_revisions = tuple(
        _acceptance_check_string(report, "remote-mcp.register", "cluster_route_revision")
        for report in reports
    )
    catalog_revisions = tuple(
        _acceptance_check_string(report, "remote-mcp.tools-list", "catalog_revision")
        for report in reports
    )
    server_artifacts = tuple(_acceptance_server_artifact(report) for report in reports)
    same_server_artifact = (
        all(artifact is not None for artifact in server_artifacts)
        and server_artifacts[0] == server_artifacts[1] == server_artifacts[2]
    )
    server_artifact_sha256 = (
        _stable_digest(server_artifacts[1]) if server_artifacts[1] is not None else None
    )
    revision_matches = {
        "registration": _same_nonempty_strings(registration_revisions),
        "cluster_route": _same_nonempty_strings(cluster_route_revisions),
        "catalog": _same_nonempty_strings(catalog_revisions),
    }
    expected_tools = ("spack_find", "spack_install", "spack_locate")
    passed = (
        reports_passed
        and len(scopes) == 1
        and tool_names == expected_tools
        and all(revision_matches.values())
        and same_server_artifact
    )
    identity: dict[str, str | None] = {
        "registration_revision": _common_string(registration_revisions),
        "cluster_route_revision": _common_string(cluster_route_revisions),
        "catalog_revision": _common_string(catalog_revisions),
        "server_artifact_sha256": server_artifact_sha256,
    }
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-transition-identity",
            passed=passed,
            message=(
                "all Spack phases share one passing route and verified server artifact"
                if passed
                else "Spack transition phases do not share one passing immutable route"
            ),
            evidence={
                "underlying_reports_passed": reports_passed,
                "scopes": [list(scope) for scope in sorted(scopes)],
                "tool_names": list(tool_names),
                "expected_tool_names": list(expected_tools),
                "registration_revisions": list(registration_revisions),
                "cluster_route_revisions": list(cluster_route_revisions),
                "catalog_revisions": list(catalog_revisions),
                "revision_matches": revision_matches,
                "same_server_artifact": same_server_artifact,
                "server_artifact_sha256": server_artifact_sha256,
            },
        ),
        identity,
    )


def _spack_transition_durable_evidence_check(
    *,
    preinstall_report: RemoteMcpAcceptanceReport,
    install_report: RemoteMcpAcceptanceReport,
    postinstall_report: RemoteMcpAcceptanceReport,
) -> RemoteMcpAcceptanceCheck:
    """Require distinct succeeded jobs, packaged stdio, and hashed durable artifacts."""
    reports = (preinstall_report, install_report, postinstall_report)
    required_kinds = {"stdout", "stderr", "mcp_result", "provenance"}
    jobs: list[str] = []
    phases: JSON = {}
    all_artifact_ids: list[str] = []
    passed = True
    for phase, report in zip(("preinstall", "install", "postinstall"), reports, strict=True):
        raw_job_id = report.call_job.get("job_id")
        job_id = raw_job_id if isinstance(raw_job_id, str) else None
        if job_id is not None:
            jobs.append(job_id)
        relevant_artifacts = report.artifacts[:MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL]
        artifact_kinds: set[str] = set()
        artifacts_valid = len(report.artifacts) <= MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL
        for artifact in relevant_artifacts:
            kind = artifact.get("kind")
            artifact_id = artifact.get("artifact_id")
            if isinstance(kind, str):
                artifact_kinds.add(kind)
            if isinstance(artifact_id, str):
                all_artifact_ids.append(artifact_id)
            artifacts_valid = artifacts_valid and (
                isinstance(artifact_id, str)
                and artifact.get("job_id") == job_id
                and _is_sha256(artifact.get("sha256"))
            )
        stdio_valid = (
            bool(report.mcp_stdio)
            and _stdio_initialize_passed(report.mcp_stdio)
            and report.virtual_alias is not None
            and report.virtual_alias in _stdio_listed_tool_names(report.mcp_stdio)
            and _stdio_call_job_id(report.mcp_stdio) == job_id
        )
        phase_passed = (
            job_id is not None
            and report.call_job.get("state") == "succeeded"
            and required_kinds.issubset(artifact_kinds)
            and artifacts_valid
            and stdio_valid
        )
        passed = passed and phase_passed
        phases[phase] = {
            "job_id": job_id,
            "state": report.call_job.get("state"),
            "artifact_kinds": sorted(artifact_kinds),
            "artifact_count": len(report.artifacts),
            "artifacts_valid": artifacts_valid,
            "stdio_valid": stdio_valid,
            "passed": phase_passed,
        }
    distinct_jobs = len(jobs) == 3 and len(set(jobs)) == 3
    distinct_artifacts = len(all_artifact_ids) == len(set(all_artifact_ids))
    passed = passed and distinct_jobs and distinct_artifacts
    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.spack-transition-durable-evidence",
        passed=passed,
        message=(
            "three distinct succeeded jobs retain packaged stdio and durable artifacts"
            if passed
            else "Spack transition jobs, stdio, or durable artifacts are incomplete"
        ),
        evidence={
            "required_artifact_kinds": sorted(required_kinds),
            "job_ids": jobs,
            "distinct_job_ids": distinct_jobs,
            "distinct_artifact_ids": distinct_artifacts,
            "phases": phases,
        },
    )


def _spack_preinstall_absent_check(
    *,
    report: RemoteMcpAcceptanceReport,
    protocol_result: JSON | None,
    expectation: RemoteMcpStructuredResultExpectation,
) -> tuple[RemoteMcpAcceptanceCheck, JSON]:
    """Prove an exact requested spec was absent immediately before installation."""
    structured, schema_evidence, failures = _spack_transition_structured_result(
        protocol_result,
        tool="spack_find",
    )
    arguments = _transition_call_arguments(report)
    expected_spec = cast(str, expectation.requested_spec)
    packages = structured.get("packages") if structured is not None else None
    count = structured.get("count") if structured is not None else None
    query = structured.get("query") if structured is not None else None
    if arguments.get("query") != expected_spec:
        failures.append("preinstall find call did not query the exact requested spec")
    if query != expected_spec:
        failures.append("preinstall find result query does not match the requested spec")
    if not isinstance(count, int) or isinstance(count, bool) or count != 0:
        failures.append("preinstall find result count is not zero")
    if packages != []:
        failures.append("preinstall find result packages is not an empty array")
    projection: JSON = {
        "schema_version": structured.get("schema_version") if structured is not None else None,
        "operation": structured.get("operation") if structured is not None else None,
        "query": _bounded_evidence_scalar(query),
        "count": count,
        "packages": [],
    }
    passed = not failures
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-preinstall-absent",
            passed=passed,
            message=(
                "exact requested spec was absent immediately before installation"
                if passed
                else "preinstall absence for the exact requested spec was not proven"
            ),
            evidence={
                "expected_requested_spec": expected_spec,
                "submitted_arguments": _bounded_transition_arguments(arguments, "spack_find"),
                "observed": projection,
                "output_schema": schema_evidence,
                "failures": failures,
            },
        ),
        projection,
    )


def _spack_fresh_install_check(
    *,
    report: RemoteMcpAcceptanceReport,
    protocol_result: JSON | None,
    expectation: RemoteMcpStructuredResultExpectation,
) -> tuple[RemoteMcpAcceptanceCheck, JSON]:
    """Prove one exact package identity was installed with reuse disabled."""
    structured, schema_evidence, failures = _spack_transition_structured_result(
        protocol_result,
        tool="spack_install",
    )
    arguments = _transition_call_arguments(report)
    expected_spec = cast(str, expectation.requested_spec)
    packages = (
        _spack_package_records(structured.get("packages")) if structured is not None else None
    )
    duration = structured.get("duration_seconds") if structured is not None else None
    if arguments.get("spec") != expected_spec or arguments.get("reuse") is not False:
        failures.append("install call did not submit the exact spec with reuse=false")
    if structured is None or structured.get("requested_spec") != expected_spec:
        failures.append("install result requested_spec does not match the exact submitted spec")
    if structured is None or structured.get("reuse") is not False:
        failures.append("install result does not prove reuse=false")
    if structured is None or structured.get("status") != "installed":
        failures.append("install result status is not installed")
    if (
        not isinstance(duration, (int, float))
        or isinstance(duration, bool)
        or not math.isfinite(duration)
        or duration < 0
    ):
        failures.append("install duration is not a finite non-negative number")
    package = packages[0] if packages is not None and len(packages) == 1 else None
    if package is None or not _spack_package_matches(package, expectation):
        failures.append("install result does not contain exactly one expected package identity")
    projection: JSON = {
        "schema_version": structured.get("schema_version") if structured is not None else None,
        "operation": structured.get("operation") if structured is not None else None,
        "requested_spec": _bounded_evidence_scalar(
            structured.get("requested_spec") if structured is not None else None
        ),
        "reuse": structured.get("reuse") if structured is not None else None,
        "status": _bounded_evidence_scalar(
            structured.get("status") if structured is not None else None
        ),
        "duration_seconds": duration,
        "package": _bounded_spack_package_identity(package),
        "package_count": len(packages) if packages is not None else None,
    }
    passed = not failures
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-fresh-install",
            passed=passed,
            message=(
                "exact package identity was installed with reuse disabled"
                if passed
                else "fresh non-reusing installation of the exact package was not proven"
            ),
            evidence={
                "expected": {
                    "requested_spec": expected_spec,
                    "package_name": expectation.package_name,
                    "dag_hash": expectation.dag_hash,
                    "reuse": False,
                    "status": "installed",
                },
                "submitted_arguments": _bounded_transition_arguments(arguments, "spack_install"),
                "observed": projection,
                "output_schema": schema_evidence,
                "failures": failures,
            },
        ),
        projection,
    )


def _spack_postinstall_locate_check(
    *,
    report: RemoteMcpAcceptanceReport,
    protocol_result: JSON | None,
    expectation: RemoteMcpStructuredResultExpectation,
) -> tuple[RemoteMcpAcceptanceCheck, JSON, object]:
    """Prove the exact installed DAG hash resolves to one canonical prefix."""
    structured, schema_evidence, failures = _spack_transition_structured_result(
        protocol_result,
        tool="spack_locate",
    )
    arguments = _transition_call_arguments(report)
    exact_hash_spec = f"/{expectation.dag_hash}"
    requested_spec = structured.get("requested_spec") if structured is not None else None
    load_spec = structured.get("load_spec") if structured is not None else None
    prefix = structured.get("prefix") if structured is not None else None
    package = _as_json(structured.get("package")) if structured is not None else None
    if arguments.get("spec") != exact_hash_spec:
        failures.append("postinstall locate call did not query the exact /dag_hash")
    if requested_spec != exact_hash_spec or load_spec != exact_hash_spec:
        failures.append("postinstall locate result is not bound to the exact /dag_hash")
    if package is None or not _spack_package_matches(package, expectation):
        failures.append("postinstall locate result package identity does not match")
    if not _is_canonical_absolute_posix_path(prefix):
        failures.append("postinstall locate prefix is not a canonical absolute POSIX path")
    projection: JSON = {
        "schema_version": structured.get("schema_version") if structured is not None else None,
        "operation": structured.get("operation") if structured is not None else None,
        "requested_spec": _bounded_evidence_scalar(requested_spec),
        "load_spec": _bounded_evidence_scalar(load_spec),
        "prefix": _bounded_evidence_scalar(prefix),
        "package": _bounded_spack_package_identity(package),
    }
    passed = not failures
    return (
        RemoteMcpAcceptanceCheck(
            name="remote-mcp.spack-postinstall-locate",
            passed=passed,
            message=(
                "exact installed DAG hash resolves to one canonical prefix"
                if passed
                else "postinstall locate did not prove the exact installed DAG identity"
            ),
            evidence={
                "expected": {
                    "requested_spec": exact_hash_spec,
                    "package_name": expectation.package_name,
                    "dag_hash": expectation.dag_hash,
                },
                "submitted_arguments": _bounded_transition_arguments(arguments, "spack_locate"),
                "observed": projection,
                "output_schema": schema_evidence,
                "failures": failures,
            },
        ),
        projection,
        prefix,
    )


def _spack_transition_structured_result(
    protocol_result: JSON | None,
    *,
    tool: Literal["spack_find", "spack_install", "spack_locate"],
) -> tuple[JSON | None, JSON, list[str]]:
    """Return a schema-validated transition result without retaining MCP text output."""
    failures: list[str] = []
    if protocol_result is None:
        failures.append("protocol result is missing")
        structured_value: object = None
    else:
        try:
            _require_bounded_json_structure(protocol_result, label="transition protocol result")
            _require_finite_json(protocol_result, label="transition protocol result")
        except (RecursionError, ValueError) as exc:
            failures.append(_bounded_diagnostic(str(exc)))
            structured_value = None
        else:
            if protocol_result.get("isError") is True:
                failures.append("protocol result reports isError=true")
            structured_value = protocol_result.get("structuredContent")
    structured = _as_json(structured_value)
    schema_evidence = _structured_result_schema_evidence(
        output_schema=_spack_transition_output_schema(tool),
        structured_value=structured_value,
    )
    if schema_evidence.get("structured_content_valid") is not True:
        failures.append("structuredContent does not satisfy the pinned Spack result schema")
    expected_operation = tool.removeprefix("spack_")
    if structured is None:
        failures.append("protocol result has no structuredContent object")
    else:
        if structured.get("schema_version") != "spack.mcp.result.v1":
            failures.append("structured result schema_version is not spack.mcp.result.v1")
        if structured.get("operation") != expected_operation:
            failures.append("structured result operation does not match the transition phase")
    return structured, schema_evidence, failures


def _spack_transition_output_schema(
    tool: Literal["spack_find", "spack_install", "spack_locate"],
) -> JSON:
    """Return the strict result schema pinned by the clio-kit Spack user contract."""
    nullable_string: JSON = {"anyOf": [{"type": "string"}, {"type": "null"}]}
    package_schema: JSON = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "version": deepcopy(nullable_string),
            "dag_hash": deepcopy(nullable_string),
            "compiler": deepcopy(nullable_string),
            "architecture": deepcopy(nullable_string),
        },
        "required": ["name"],
        "additionalProperties": False,
    }
    common: JSON = {
        "schema_version": {"type": "string", "const": "spack.mcp.result.v1"},
        "operation": {"type": "string", "const": tool.removeprefix("spack_")},
    }
    if tool == "spack_find":
        return {
            "type": "object",
            "properties": {
                **common,
                "query": deepcopy(nullable_string),
                "packages": {"type": "array", "items": package_schema},
                "count": {"type": "integer"},
            },
            "required": ["count"],
            "additionalProperties": False,
        }
    if tool == "spack_install":
        return {
            "type": "object",
            "properties": {
                **common,
                "requested_spec": {"type": "string"},
                "reuse": {"type": "boolean"},
                "status": {"type": "string", "const": "installed"},
                "duration_seconds": {"type": "number"},
                "packages": {"type": "array", "items": package_schema},
                "stdout_excerpt": deepcopy(nullable_string),
            },
            "required": ["requested_spec", "reuse", "duration_seconds", "packages"],
            "additionalProperties": False,
        }
    return {
        "type": "object",
        "properties": {
            **common,
            "requested_spec": {"type": "string"},
            "load_spec": {"type": "string"},
            "package": package_schema,
            "prefix": {"type": "string"},
        },
        "required": ["requested_spec", "load_spec", "package", "prefix"],
        "additionalProperties": False,
    }


def _spack_transition_call_evidence(
    *,
    report: RemoteMcpAcceptanceReport,
    phase: Literal["preinstall", "install", "postinstall"],
    structured_result: JSON,
) -> RemoteMcpSpackTransitionCallEvidence:
    """Project one ordinary acceptance report into bounded transition evidence."""
    artifacts = [
        RemoteMcpSpackTransitionArtifactEvidence(
            artifact_id=_bounded_optional_string(artifact.get("artifact_id"), 1_024),
            job_id=_bounded_optional_string(artifact.get("job_id"), 1_024),
            kind=_bounded_optional_string(artifact.get("kind"), 128),
            sha256=_bounded_optional_string(artifact.get("sha256"), 64),
            uri=_bounded_optional_string(artifact.get("uri"), 4_096),
        )
        for artifact in report.artifacts[:MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL]
    ]
    alias = report.virtual_alias
    return RemoteMcpSpackTransitionCallEvidence(
        phase=phase,
        report_passed=report.passed and all(check.passed for check in report.checks),
        cluster=report.cluster,
        server_name=report.server_name,
        profile=report.profile,
        remote_tool_name=report.remote_tool_name,
        virtual_alias=alias,
        job_id=_bounded_optional_string(report.call_job.get("job_id"), 1_024),
        state=_bounded_optional_string(report.call_job.get("state"), 128),
        arguments=_bounded_transition_arguments(
            _transition_call_arguments(report),
            report.remote_tool_name,
        ),
        artifacts=artifacts,
        artifacts_truncated=(len(report.artifacts) > MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL),
        stdio=RemoteMcpSpackTransitionStdioEvidence(
            boundary=_bounded_optional_string(report.mcp_stdio.get("boundary"), 128),
            returncode=(
                cast(int, report.mcp_stdio["returncode"])
                if isinstance(report.mcp_stdio.get("returncode"), int)
                and not isinstance(report.mcp_stdio.get("returncode"), bool)
                else None
            ),
            initialize_passed=_stdio_initialize_passed(report.mcp_stdio),
            tools_list_passed=(
                alias is not None and alias in _stdio_listed_tool_names(report.mcp_stdio)
            ),
            call_job_id=_bounded_optional_string(_stdio_call_job_id(report.mcp_stdio), 1_024),
        ),
        structured_result=structured_result,
    )


def _transition_call_arguments(report: RemoteMcpAcceptanceReport) -> JSON:
    """Return the ordinary report's MCP call arguments when structurally present."""
    spec = _as_json(report.call_job.get("spec")) or {}
    return _as_json(spec.get("arguments")) or {}


def _bounded_transition_arguments(arguments: JSON, tool: str) -> JSON:
    """Retain only operation-defining scalar arguments in transition evidence."""
    keys = {"spack_find": ("query",), "spack_install": ("spec", "reuse")}.get(
        tool,
        ("spec",),
    )
    return {key: _bounded_evidence_scalar(arguments.get(key)) for key in keys}


def _bounded_spack_package_identity(package: JSON | None) -> JSON:
    """Return bounded Spack package identity fields for durable evidence."""
    if package is None:
        return {}
    return {
        key: _bounded_evidence_scalar(package.get(key))
        for key in ("name", "version", "dag_hash", "compiler", "architecture")
    }


def _bounded_evidence_scalar(value: object) -> object:
    """Bound strings retained in acceptance evidence while preserving scalar types."""
    if isinstance(value, str):
        return _bounded_diagnostic(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _bounded_diagnostic(type(value).__name__)


def _bounded_optional_string(value: object, maximum: int) -> str | None:
    """Return a string only when it is safe to retain in a bounded evidence model."""
    return value if isinstance(value, str) and len(value) <= maximum else None


def _acceptance_check_string(
    report: RemoteMcpAcceptanceReport,
    check_name: str,
    evidence_key: str,
) -> str | None:
    """Read one bounded string from a uniquely named passing acceptance check."""
    matches = [check for check in report.checks if check.name == check_name]
    if len(matches) != 1 or not matches[0].passed:
        return None
    return _bounded_optional_string(matches[0].evidence.get(evidence_key), 128)


def _acceptance_server_artifact(report: RemoteMcpAcceptanceReport) -> JSON | None:
    """Return the exact verified call artifact from one ordinary acceptance report."""
    matches = [check for check in report.checks if check.name == "remote-mcp.server-artifact"]
    if len(matches) != 1 or not matches[0].passed:
        return None
    artifact = matches[0].evidence.get("call_server_artifact")
    return cast(JSON, artifact) if isinstance(artifact, dict) else None


def _same_nonempty_strings(values: tuple[str | None, ...]) -> bool:
    """Return whether all values are the same non-empty string."""
    return all(isinstance(value, str) and bool(value) for value in values) and len(set(values)) == 1


def _common_string(values: tuple[str | None, ...]) -> str | None:
    """Return one common non-empty value, or ``None`` when identity is ambiguous."""
    return values[0] if _same_nonempty_strings(values) else None


def _is_strict_canonical_posix_descendant(path: object, root: object) -> bool:
    """Return whether ``path`` is canonical and strictly contained by ``root``."""
    if not _is_canonical_absolute_posix_path(path) or not _is_canonical_absolute_posix_path(root):
        return False
    typed_path = PurePosixPath(cast(str, path))
    typed_root = PurePosixPath(cast(str, root))
    return typed_path != typed_root and typed_root in typed_path.parents


def build_remote_mcp_structured_result_check(
    *,
    expectation: RemoteMcpStructuredResultExpectation,
    remote_tool_name: str,
    arguments: object,
    protocol_result: JSON | None,
    output_schema: JSON | None,
) -> RemoteMcpAcceptanceCheck:
    """Validate a remote structured result against an explicit semantic contract."""
    if expectation.contract == "clio-kit-spack-user-v2":
        return _spack_structured_result_check(
            expectation=expectation,
            remote_tool_name=remote_tool_name,
            arguments=arguments,
            protocol_result=protocol_result,
            output_schema=output_schema,
        )
    raise ValueError(f"unsupported structured result contract: {expectation.contract}")


def _spack_structured_result_check(
    *,
    expectation: RemoteMcpStructuredResultExpectation,
    remote_tool_name: str,
    arguments: object,
    protocol_result: JSON | None,
    output_schema: JSON | None,
) -> RemoteMcpAcceptanceCheck:
    """Validate the exact clio-kit Spack v2 result semantics for one operation."""
    failures: list[str] = []
    typed_arguments = _as_json(arguments) or {}
    structured_value = (
        protocol_result.get("structuredContent") if protocol_result is not None else None
    )
    structured = _as_json(structured_value)
    output_schema_evidence = _structured_result_schema_evidence(
        output_schema=output_schema,
        structured_value=structured_value,
    )
    observed: JSON = {
        "structured_content_present": structured is not None,
        "schema_version": structured.get("schema_version") if structured is not None else None,
        "operation": structured.get("operation") if structured is not None else None,
    }
    if remote_tool_name != expectation.tool:
        failures.append("called tool does not match the configured result expectation")
    if output_schema_evidence["schema_present"] is not True:
        failures.append("cached tool outputSchema is absent")
    elif output_schema_evidence["schema_valid"] is not True:
        failures.append("cached tool outputSchema is invalid")
    elif output_schema_evidence["structured_content_valid"] is not True:
        failures.append("structuredContent does not satisfy the cached tool outputSchema")
    if structured is None:
        failures.append("protocol result has no structuredContent object")
    else:
        if structured.get("schema_version") != "spack.mcp.result.v1":
            failures.append("structured result schema_version is not spack.mcp.result.v1")
        expected_operation = expectation.tool.removeprefix("spack_")
        if structured.get("operation") != expected_operation:
            failures.append("structured result operation does not match the called tool")
        if expectation.tool == "spack_find":
            _validate_spack_find_result(
                structured,
                typed_arguments,
                expectation,
                failures,
                observed,
            )
        elif expectation.tool == "spack_locate":
            _validate_spack_locate_result(
                structured,
                typed_arguments,
                expectation,
                failures,
                observed,
            )
        else:
            _validate_spack_install_result(
                structured,
                typed_arguments,
                expectation,
                failures,
                observed,
            )
    passed = not failures
    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.structured-result",
        passed=passed,
        message=(
            "structured MCP result matches the configured semantic expectations"
            if passed
            else "structured MCP result does not match the configured semantic expectations"
        ),
        evidence={
            "contract": expectation.contract,
            "tool": expectation.tool,
            "expected": expectation.model_dump(mode="json"),
            "observed": observed,
            "output_schema": output_schema_evidence,
            "failures": failures,
        },
    )


def _structured_result_schema_evidence(
    *,
    output_schema: JSON | None,
    structured_value: object,
) -> JSON:
    """Validate one result against its cached schema and return bounded evidence."""
    evidence: JSON = {
        "schema_present": output_schema is not None,
        "schema_valid": False,
        "schema_sha256": None,
        "structured_content_valid": False,
        "validation_errors": [],
        "validation_errors_truncated": False,
    }
    if output_schema is None:
        return evidence
    try:
        _require_bounded_json_structure(output_schema, label="outputSchema")
        _require_finite_json(output_schema, label="outputSchema")
        _validate_json_schema(output_schema, label="outputSchema")
    except (RecursionError, ValueError) as exc:
        evidence["validation_errors"] = [_bounded_diagnostic(str(exc))]
        return evidence
    evidence["schema_sha256"] = _stable_digest(output_schema)
    evidence["schema_valid"] = True
    declared_dialect = output_schema.get("$schema")
    validator_type = (
        _JSON_SCHEMA_VALIDATORS.get(declared_dialect.rstrip("#"), Draft202012Validator)
        if isinstance(declared_dialect, str)
        else Draft202012Validator
    )
    errors: list[str] = []
    truncated = False
    try:
        validator = cast(_JsonSchemaInstanceValidator, validator_type(output_schema))
        for index, error in enumerate(validator.iter_errors(structured_value)):
            if index >= MAX_REMOTE_MCP_RESULT_SCHEMA_ERRORS:
                truncated = True
                break
            path = "/".join(str(part) for part in error.absolute_path)
            prefix = f"/{path}: " if path else ""
            errors.append(_bounded_diagnostic(f"{prefix}{error.message}"))
    except Exception as exc:  # A broken external reference must fail closed as evidence.
        errors.append(
            _bounded_diagnostic(f"outputSchema evaluation failed: {type(exc).__name__}: {exc}")
        )
    evidence["structured_content_valid"] = not errors and not truncated
    evidence["validation_errors"] = errors
    evidence["validation_errors_truncated"] = truncated
    return evidence


def _validate_spack_find_result(
    structured: JSON,
    arguments: JSON,
    expectation: RemoteMcpStructuredResultExpectation,
    failures: list[str],
    observed: JSON,
) -> None:
    """Validate a Spack find result and record bounded evidence."""
    packages = _spack_package_records(structured.get("packages"))
    count = structured.get("count")
    expected_query = arguments.get("query")
    observed.update(
        {
            "query": structured.get("query"),
            "count": count,
        }
    )
    if structured.get("query") != expected_query:
        failures.append("find result query does not match the submitted query")
    if packages is None:
        failures.append("find result packages is not an array of objects")
        return
    if not isinstance(count, int) or isinstance(count, bool) or count != len(packages):
        failures.append("find result count does not match the package array")
    _record_expected_spack_package(packages, expectation, failures, observed)


def _validate_spack_locate_result(
    structured: JSON,
    arguments: JSON,
    expectation: RemoteMcpStructuredResultExpectation,
    failures: list[str],
    observed: JSON,
) -> None:
    """Validate one unique Spack package, prefix, and canonical load spec."""
    expected_spec = expectation.requested_spec
    package = _as_json(structured.get("package"))
    prefix = structured.get("prefix")
    load_spec = structured.get("load_spec")
    expected_load_spec = f"/{expectation.dag_hash}"
    canonical_prefix = _is_canonical_absolute_posix_path(prefix)
    prefix_matches_expected = prefix == expectation.prefix
    package_matches = package is not None and _spack_package_matches(package, expectation)
    observed.update(
        {
            "requested_spec": structured.get("requested_spec"),
            "load_spec": load_spec,
            "prefix": prefix,
            "prefix_is_canonical_absolute": canonical_prefix,
            "prefix_matches_expected": prefix_matches_expected,
            "package": _spack_package_identity(package),
            "expected_package_match_count": 1 if package_matches else 0,
        }
    )
    if arguments.get("spec") != expected_spec:
        failures.append("submitted locate spec does not match the configured expectation")
    if structured.get("requested_spec") != expected_spec:
        failures.append("locate result requested_spec does not match the expectation")
    if load_spec != expected_load_spec:
        failures.append("locate result load_spec is not the canonical /dag_hash")
    if not canonical_prefix:
        failures.append("locate result prefix is not a canonical absolute POSIX path")
    if not prefix_matches_expected:
        failures.append("locate result prefix does not match the configured exact prefix")
    if not package_matches:
        failures.append("locate result package does not match the expected name and DAG hash")


def _validate_spack_install_result(
    structured: JSON,
    arguments: JSON,
    expectation: RemoteMcpStructuredResultExpectation,
    failures: list[str],
    observed: JSON,
) -> None:
    """Validate installed/reused status and the observed exact Spack identity."""
    expected_spec = expectation.requested_spec
    packages = _spack_package_records(structured.get("packages"))
    duration = structured.get("duration_seconds")
    observed.update(
        {
            "requested_spec": structured.get("requested_spec"),
            "reuse": structured.get("reuse"),
            "status": structured.get("status"),
            "duration_seconds": duration,
        }
    )
    if arguments.get("spec") != expected_spec or arguments.get("reuse") is not expectation.reuse:
        failures.append("submitted install arguments do not match the configured expectation")
    if structured.get("requested_spec") != expected_spec:
        failures.append("install result requested_spec does not match the expectation")
    if structured.get("reuse") is not expectation.reuse:
        failures.append("install result reuse does not match the expectation")
    if structured.get("status") != "installed":
        failures.append("install result does not report installed status")
    if (
        not isinstance(duration, (int, float))
        or isinstance(duration, bool)
        or not math.isfinite(duration)
        or duration < 0
    ):
        failures.append("install result duration_seconds is not a finite non-negative number")
    if packages is None:
        failures.append("install result packages is not an array of objects")
        return
    _record_expected_spack_package(packages, expectation, failures, observed)


def _record_expected_spack_package(
    packages: list[JSON],
    expectation: RemoteMcpStructuredResultExpectation,
    failures: list[str],
    observed: JSON,
) -> None:
    """Record and require one exact package identity without retaining an unbounded list."""
    matches = [package for package in packages if _spack_package_matches(package, expectation)]
    named_packages = [
        package for package in packages if package.get("name") == expectation.package_name
    ]
    named_hashes = sorted(
        {
            str(package["dag_hash"])
            for package in packages
            if package.get("name") == expectation.package_name
            and isinstance(package.get("dag_hash"), str)
        }
    )
    observed["package_count"] = len(packages)
    observed["expected_package_match_count"] = len(matches)
    observed["expected_package_name_count"] = len(named_packages)
    observed["package_hashes_for_expected_name"] = named_hashes[:20]
    if len(matches) != 1:
        failures.append("result does not contain exactly one expected package name and DAG hash")
    if named_hashes != [expectation.dag_hash]:
        failures.append("result contains an unexpected or ambiguous hash for the package name")
    if len(named_packages) != 1:
        failures.append("result does not contain one unique package record for the package name")
    if len(packages) != 1:
        failures.append("result does not contain exactly one matching root package")


def _spack_package_records(value: object) -> list[JSON] | None:
    """Return typed Spack package records only when every array item is an object."""
    if not isinstance(value, list):
        return None
    records: list[JSON] = []
    for item in cast(list[object], value):
        record = _as_json(item)
        if record is None:
            return None
        records.append(record)
    return records


def _spack_package_matches(
    package: JSON,
    expectation: RemoteMcpStructuredResultExpectation,
) -> bool:
    """Return whether one package has the exact configured stable identity."""
    return (
        package.get("name") == expectation.package_name
        and package.get("dag_hash") == expectation.dag_hash
    )


def _spack_package_identity(package: JSON | None) -> JSON:
    """Return the bounded identity fields needed in release evidence."""
    if package is None:
        return {}
    return {
        key: package.get(key) for key in ("name", "version", "dag_hash", "compiler", "architecture")
    }


def _is_canonical_absolute_posix_path(value: object) -> bool:
    """Return whether a value is a normalized absolute POSIX path without traversal."""
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or value.startswith("//")
        or value == "/"
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value


def _is_canonical_relative_posix_path(value: object) -> bool:
    """Return whether a value is a normalized, non-traversing relative POSIX path."""
    if (
        not isinstance(value, str)
        or value.startswith("/")
        or value in {"", "."}
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value


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
        and registration.contract == CLIO_KIT_SPACK_USER_CONTRACT_ID
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
            "expected_clio_kit_version": CLIO_KIT_SPACK_USER_WHEEL_VERSION,
            "observed_contract_sha256": observed_contract_digest,
        },
    )


def _scientific_catalog_user_contract_check(
    entry: RemoteMcpSchemaCacheEntry | None,
    registration: RemoteMcpServerConfig | None,
) -> RemoteMcpAcceptanceCheck:
    """Require the exact read-only scientific catalog surface approved for agents."""
    expected_names = {"scientific_dataset_describe", "scientific_dataset_search"}
    tools = {tool.name: tool for tool in entry.tools} if entry is not None else {}
    actual_names = set(tools)
    allowlisted_names: set[str] = (
        set(registration.allow_tools) if registration is not None else set()
    )
    observed_contract_digest = remote_mcp_schema_digest(list(tools.values()))

    annotation_matches: dict[str, bool] = {}
    expected_annotations = {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
    for name in expected_names:
        tool = tools.get(name)
        annotations = tool.annotations if tool is not None else None
        annotation_matches[name] = annotations is not None and all(
            annotations.get(key) is value for key, value in expected_annotations.items()
        )

    describe = tools.get("scientific_dataset_describe")
    describe_input = describe.input_schema if describe is not None else {}
    describe_input_properties = _as_json(describe_input.get("properties")) or {}
    describe_required = describe_input.get("required")
    describe_input_matches = (
        describe_input.get("type") == "object"
        and describe_input.get("additionalProperties") is False
        and set(describe_input_properties) == {"dataset_id"}
        and _as_json(describe_input_properties.get("dataset_id")) == {"type": "string"}
        and isinstance(describe_required, list)
        and cast(list[object], describe_required) == ["dataset_id"]
    )

    search = tools.get("scientific_dataset_search")
    search_input = search.input_schema if search is not None else {}
    search_input_properties = _as_json(search_input.get("properties")) or {}
    page_size = _as_json(search_input_properties.get("page_size")) or {}
    search_input_matches = (
        search_input.get("type") == "object"
        and search_input.get("additionalProperties") is False
        and set(search_input_properties)
        == {"query", "tags", "kind", "format", "page_size", "cursor"}
        and page_size == {"default": 20, "maximum": 100, "minimum": 1, "type": "integer"}
        and search_input.get("required", []) == []
    )

    describe_output = describe.output_schema if describe is not None else None
    describe_output_properties = (
        _as_json(describe_output.get("properties")) if describe_output is not None else None
    ) or {}
    dataset_schema = _as_json(describe_output_properties.get("dataset")) or {}
    dataset_properties = _as_json(dataset_schema.get("properties")) or {}
    descriptor_schema = _as_json(dataset_properties.get("descriptor")) or {}
    descriptor_properties = _as_json(descriptor_schema.get("properties")) or {}
    describe_output_matches = (
        describe_output is not None
        and describe_output.get("type") == "object"
        and describe_output.get("additionalProperties") is False
        and _as_json(describe_output_properties.get("schema_version"))
        == {
            "const": "clio-kit.scientific-dataset-description.v1",
            "default": "clio-kit.scientific-dataset-description.v1",
            "type": "string",
        }
        and descriptor_schema.get("type") == "object"
        and descriptor_schema.get("additionalProperties") is False
        and _as_json(descriptor_properties.get("schema_version"))
        == {"const": "jarvis.dataset-descriptor.v1", "type": "string"}
    )

    search_output = search.output_schema if search is not None else None
    search_output_properties = (
        _as_json(search_output.get("properties")) if search_output is not None else None
    ) or {}
    datasets_schema = _as_json(search_output_properties.get("datasets")) or {}
    dataset_item_schema = _as_json(datasets_schema.get("items")) or {}
    search_output_matches = (
        search_output is not None
        and search_output.get("type") == "object"
        and search_output.get("additionalProperties") is False
        and _as_json(search_output_properties.get("schema_version"))
        == {
            "const": "clio-kit.scientific-dataset-search.v1",
            "default": "clio-kit.scientific-dataset-search.v1",
            "type": "string",
        }
        and datasets_schema.get("type") == "array"
        and dataset_item_schema.get("type") == "object"
        and dataset_item_schema.get("additionalProperties") is False
    )

    schema_matches = {
        "scientific_dataset_describe": describe_input_matches and describe_output_matches,
        "scientific_dataset_search": search_input_matches and search_output_matches,
    }
    passed = (
        actual_names == expected_names
        and allowlisted_names == expected_names
        and registration is not None
        and registration.contract == CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID
        and "user" in registration.profiles
        and all(annotation_matches.values())
        and all(schema_matches.values())
        and observed_contract_digest == CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256
    )
    return RemoteMcpAcceptanceCheck(
        name="remote-mcp.scientific-catalog-user-contract",
        passed=passed,
        message=(
            "Scientific catalog exposes only read-only search and exact descriptor lookup"
            if passed
            else "Scientific catalog tools, allowlist, schemas, or safety annotations drifted"
        ),
        evidence={
            "expected_tool_names": sorted(expected_names),
            "remote_tool_names": sorted(actual_names),
            "allowlisted_tool_names": sorted(allowlisted_names),
            "profiles": registration.profiles if registration is not None else [],
            "declared_contract": registration.contract if registration is not None else None,
            "annotations_match": annotation_matches,
            "schemas_match": schema_matches,
            "expected_contract_sha256": CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256,
            "expected_clio_kit_version": CLIO_KIT_SPACK_USER_WHEEL_VERSION,
            "observed_contract_sha256": observed_contract_digest,
        },
    )


def _declared_contract_check(
    entry: RemoteMcpSchemaCacheEntry | None,
    registration: RemoteMcpServerConfig,
) -> RemoteMcpAcceptanceCheck:
    """Evaluate the semantic contract explicitly declared by an operator."""
    if registration.contract == CLIO_KIT_SPACK_USER_CONTRACT_ID:
        return _spack_user_contract_check(entry, registration)
    if registration.contract == CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID:
        return _scientific_catalog_user_contract_check(entry, registration)
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
    root_identifier = input_schema.get("$id")
    return (
        (isinstance(root_identifier, str) and bool(root_identifier))
        or any(key in input_schema for key in _COMPOSED_SCHEMA_KEYS)
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
        base = _bounded_base_alias(candidates[0].base_alias)
        bases.setdefault(base, []).append(identity)
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
    maximum_suffix_length = MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH - len("remote_")
    for length in range(10, min(len(identity), maximum_suffix_length) + 1):
        candidate = _alias_with_suffix(base, identity[:length])
        if candidate not in blocked:
            return candidate
    for nonce in range(1, len(blocked) + MAX_VIRTUAL_REMOTE_MCP_CANDIDATES + 2):
        suffix = hashlib.sha256(f"{identity}\0{nonce}".encode("ascii")).hexdigest()[
            :maximum_suffix_length
        ]
        candidate = f"remote_{suffix}"
        if candidate not in blocked:
            return candidate
    raise ValueError("could not assign a unique bounded remote MCP alias")


def _bounded_base_alias(base: str) -> str:
    """Bound one readable generated alias to the MCP interoperability limit."""
    if len(base) <= MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH:
        return base
    suffix = hashlib.sha256(base.encode("utf-8")).hexdigest()[:10]
    return _alias_with_suffix(base, suffix)


def _alias_with_suffix(base: str, suffix: str) -> str:
    """Append a stable suffix without exceeding the MCP tool-name limit."""
    head_length = MAX_VIRTUAL_REMOTE_MCP_ALIAS_LENGTH - len(suffix) - 1
    if head_length < 1:
        raise ValueError("remote MCP alias suffix leaves no readable prefix")
    head = base[:head_length].rstrip("_")
    if not head:
        head = "remote"[:head_length]
    return f"{head}_{suffix}"


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


def _immutable_remote_mcp_install_verified(server_artifact: JSON) -> bool:
    """Accept immutable wheel launches and wheel-backed persistent uv tools."""
    install_source = server_artifact.get("install_source")
    if install_source == "wheel":
        return True
    if install_source != "uv-tool":
        return False
    install_spec = server_artifact.get("install_spec")
    python_runtime = server_artifact.get("python_distribution_runtime")
    if (
        not isinstance(install_spec, str)
        or not install_spec.lower().endswith(".whl")
        or not isinstance(python_runtime, dict)
        or cast(JSON, python_runtime).get("runtime_closure_verified") is not True
    ):
        return False
    if server_artifact.get("nested_launcher") is not True:
        return True
    nested_runtime = server_artifact.get("nested_runtime")
    return (
        isinstance(nested_runtime, dict)
        and cast(JSON, nested_runtime).get("persistent_tool") is True
        and cast(JSON, nested_runtime).get("locked_runtime_verified") is True
    )


def _stable_digest(value: JSON) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
