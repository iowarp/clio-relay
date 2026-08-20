"""Versioned, atomically persisted remote MCP schema discovery cache.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5). This module owns
the operator-local, on-disk record of what each registered remote MCP
server's ``tools/list`` last returned:
:class:`RemoteMcpSchemaCacheEntry` (one cluster/server snapshot, with its
own bounded-freshness check) and :class:`RemoteMcpSchemaCache` (the
FileLock-backed, atomically-replaced collection of every entry --
``load``/``update_entry``/``remove_entry``/``invalidate_cluster_entries``),
the digest/fingerprint helpers that bind a cache entry to the exact
registration and execution that produced it
(:func:`remote_mcp_execution_fingerprint`,
:func:`remote_mcp_registration_revision`, :func:`remote_mcp_schema_digest`,
:func:`remote_mcp_server_artifact_digest`,
:func:`remote_mcp_server_artifact_binding_verified`), and the durable-result
artifact parser that turns one successful discovery job into a validated
cache entry (:func:`cache_entry_from_discovery_artifact`).

Every one of these nine names has a real reader elsewhere in
``remote_mcp.py``'s own catalog-assembly and admission-resolution code
(confirmed by grep before the move -- they are used far beyond this
module's own boundary, at cluster route/admission time), so
``remote_mcp.py`` imports every one of them via a plain ``from ... import``
with no unused-import risk, and that same import is the re-export several
other modules (``cli.py``, ``mcp_server.py``, ``jarvis_mcp.py``,
``jarvis_mcp_validation.py``, ``jarvis_service_runtime.py``, ``endpoint.py``)
rely on when they import these names directly from ``clio_relay.remote_mcp``.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from filelock import FileLock
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.cluster_config import (
    RemoteMcpServerConfig,
    default_registry_path,
    ensure_private_configuration_directory,
    open_private_atomic_file,
    read_bounded_configuration_bytes,
)
from clio_relay.remote_mcp_schema_validation import (
    MAX_REMOTE_MCP_JSON_DEPTH,
    _bounded_diagnostic,
    _NonFiniteJsonError,
    _reject_nonfinite_json_constant,
    _require_bounded_json_structure,
    _require_finite_json,
)
from clio_relay.remote_mcp_tool_schema import (
    RemoteMcpDiscoveryProvenance,
    RemoteMcpToolSchema,
    _immutable_remote_mcp_install_verified,
    _is_sha256,
    _parse_remote_tool,
    _server_artifact_verified,
    _stable_digest,
)

JSON = dict[str, Any]

REMOTE_MCP_CACHE_ENV = "CLIO_RELAY_REMOTE_MCP_CACHE"
REMOTE_MCP_CACHE_VERSION = 1
MAX_REMOTE_MCP_CACHE_BYTES = 16 * 1024 * 1024
MAX_REMOTE_MCP_DISCOVERY_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_REMOTE_MCP_CACHE_ENTRIES = 1_024
MAX_REMOTE_MCP_TOOLS_PER_SERVER = 2_048
REMOTE_MCP_REPLACE_ATTEMPTS = 25
REMOTE_MCP_REPLACE_RETRY_SECONDS = 0.02


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

    @classmethod
    def invalidate_cluster_entries(
        cls,
        path: Path,
        cluster: str,
    ) -> tuple[RemoteMcpSchemaCache, tuple[str, ...]]:
        """Atomically invalidate every cached server schema for one cluster."""
        ensure_private_configuration_directory(path.parent)
        with FileLock(f"{path}.lock"):
            cache = cls.load(path)
            removed_server_names = tuple(
                sorted(entry.server_name for entry in cache.entries if entry.cluster == cluster)
            )
            if not removed_server_names:
                return cache, removed_server_names
            updated = cls(entries=[entry for entry in cache.entries if entry.cluster != cluster])
            updated._write_atomic(path)
            return updated, removed_server_names

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


def _fsync_cache_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


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


def remote_mcp_server_artifact_binding_verified(
    server_artifact: object,
    *,
    expected_digest: str | None,
) -> bool:
    """Verify one immutable registered-server artifact against discovery."""
    if not isinstance(server_artifact, dict) or not _is_sha256(expected_digest):
        return False
    typed = cast(JSON, server_artifact)
    return (
        _server_artifact_verified(typed)
        and _immutable_remote_mcp_install_verified(typed)
        and _is_sha256(typed.get("install_artifact_sha256"))
        and hmac.compare_digest(
            remote_mcp_server_artifact_digest(typed),
            cast(str, expected_digest).lower(),
        )
    )


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
