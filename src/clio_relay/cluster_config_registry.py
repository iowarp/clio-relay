"""The persisted registry of locally configured cluster targets.

Split out of `cluster_config.py` (iowarp/clio-relay#231): `ClusterRegistry`
(load/save/mutate with file locking, atomic replacement, and fsync) and the
durable queue-routing revision digest that excludes fields which can change
without invalidating an existing job handle's route.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

from filelock import FileLock
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.cluster_config_io import (
    CONFIG_READ_RETRY_SECONDS,
    MAX_CLUSTER_REGISTRY_BYTES,
    _fsync_directory,
    read_bounded_configuration_bytes,
    verify_private_configuration_path,
)
from clio_relay.cluster_config_models import ClusterDefinition, _validated_cluster_label
from clio_relay.cluster_config_windows_paths import (
    ensure_private_configuration_directory,
    open_private_atomic_file,
)
from clio_relay.errors import ConfigurationError

MAX_CONFIGURED_CLUSTERS = 512
MAX_REMOTE_MCP_REGISTRATIONS = 1_024
CONFIG_REPLACE_ATTEMPTS = 25


def cluster_route_revision(definition: ClusterDefinition) -> str:
    """Return a stable digest for fields that determine durable queue routing.

    Remote MCP registrations and worker scheduling capacity can change without
    changing the SSH destination or queue location of an existing job handle.
    """
    payload = definition.model_dump(
        mode="json",
        exclude={"remote_mcp_servers", "worker_capacity"},
    )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class ClusterRegistry(BaseModel):
    """Configured cluster targets."""

    model_config = ConfigDict(extra="forbid")

    clusters: dict[str, ClusterDefinition] = Field(
        default_factory=dict,
        max_length=MAX_CONFIGURED_CLUSTERS,
    )

    @field_validator("clusters")
    @classmethod
    def _cluster_keys_match_bounded_definitions(
        cls,
        value: dict[str, ClusterDefinition],
    ) -> dict[str, ClusterDefinition]:
        for name in value:
            _validated_cluster_label(name, field="cluster registry key")
            if len(name) > 256:
                raise ValueError("cluster registry keys must not exceed 256 characters")
        mismatches = sorted(name for name, definition in value.items() if definition.name != name)
        if mismatches:
            raise ValueError(
                "cluster registry keys must match ClusterDefinition.name: " + ", ".join(mismatches)
            )
        return value

    @model_validator(mode="after")
    def _remote_mcp_registration_count_is_bounded(self) -> ClusterRegistry:
        registration_count = sum(
            len(cluster.remote_mcp_servers) for cluster in self.clusters.values()
        )
        if registration_count > MAX_REMOTE_MCP_REGISTRATIONS:
            raise ValueError(
                "cluster registry contains more than "
                f"{MAX_REMOTE_MCP_REGISTRATIONS} remote MCP registrations"
            )
        return self

    @classmethod
    def default(cls) -> ClusterRegistry:
        """Return an empty registry for explicit local cluster definitions."""
        return cls()

    @classmethod
    def load(cls, path: Path) -> ClusterRegistry:
        """Load a registry from disk, creating defaults if the file is absent.

        clio-relay#289 D2/D3: this is a read, so the parent directory gets
        the verify-with-heal-on-drift treatment
        (`verify_private_configuration_path`, zero `SetSecurityInfo` when
        already clean) rather than the unconditional-apply
        `ensure_private_configuration_directory` every write/create path
        still uses. When the directory does not exist yet (first use), it
        is a genuine bootstrap -- there is nothing to verify -- so this
        still calls `ensure_private_configuration_directory` to create and
        harden it, identically to before.
        """
        if path.parent.exists():
            verify_private_configuration_path(path.parent, directory=True)
        else:
            ensure_private_configuration_directory(path.parent)
        if not path.exists():
            with FileLock(f"{path}.lock"):
                if not path.exists():
                    cls.default()._write_atomic_unlocked(path)
        return cls.model_validate_json(
            read_bounded_configuration_bytes(path, max_bytes=MAX_CLUSTER_REGISTRY_BYTES)
        )

    def save(self, path: Path) -> None:
        """Persist the registry with locking, atomic replacement, and fsync."""
        ensure_private_configuration_directory(path.parent)
        with FileLock(f"{path}.lock"):
            validated = type(self).model_validate(self.model_dump(mode="python"))
            validated._write_atomic_unlocked(path)

    @classmethod
    def mutate(
        cls,
        path: Path,
        mutation: Callable[[ClusterRegistry], None],
    ) -> ClusterRegistry:
        """Apply a read-modify-write operation under one registry lock."""
        ensure_private_configuration_directory(path.parent)
        with FileLock(f"{path}.lock"):
            registry = (
                cls.model_validate_json(
                    read_bounded_configuration_bytes(
                        path,
                        max_bytes=MAX_CLUSTER_REGISTRY_BYTES,
                    )
                )
                if path.exists()
                else cls.default()
            )
            mutation(registry)
            validated = cls.model_validate(registry.model_dump(mode="python"))
            validated._write_atomic_unlocked(path)
            return validated

    def _write_atomic_unlocked(self, path: Path) -> None:
        """Write an atomic registry replacement while the caller holds the lock."""
        payload = (json.dumps(self.model_dump(), indent=2) + "\n").encode("utf-8")
        if len(payload) > MAX_CLUSTER_REGISTRY_BYTES:
            raise ConfigurationError(f"cluster registry exceeds {MAX_CLUSTER_REGISTRY_BYTES} bytes")
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
        try:
            with open_private_atomic_file(temporary) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            for attempt in range(CONFIG_REPLACE_ATTEMPTS):
                try:
                    os.replace(temporary, path)
                    break
                except PermissionError:
                    if attempt + 1 >= CONFIG_REPLACE_ATTEMPTS:
                        raise
                    time.sleep(CONFIG_READ_RETRY_SECONDS)
            _fsync_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)

    def require(self, name: str) -> ClusterDefinition:
        """Return a configured cluster or raise a configuration error."""
        try:
            return self.clusters[name]
        except KeyError as exc:
            raise ConfigurationError(f"cluster is not configured: {name}") from exc
