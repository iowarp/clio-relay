"""The durable last-owned-session record per cluster (iowarp/clio-relay#276 B1).

Today the only client-side session identity is env-var-only
(``CLIO_RELAY_OWNER_SESSION_ID``/``CLIO_RELAY_SESSION_GENERATION_ID``/
``CLIO_RELAY_OWNER_SESSION_CLUSTER``, ``config.py``): close the terminal that
set them and the desktop has forgotten which remote session it owns. This
module persists one durable record per cluster -- session id, generation id,
remote API port, and when it was written -- so a new process can find its way
back without the environment. The environment is not replaced: any caller
that already has a complete, matching identity (``session_attach.py``'s
``_environment_attach_target``) still uses it, exactly as before; this
registry is the fallback discovery path, not the only source.

One record per cluster, overwritten on every new owned-session bring-up
(``cli_session_start.py``) and removed on a clean, authoritatively closed
teardown (``cli_session_teardown_finalize.py``/``cli_session_teardown_
recovery.py``) -- never on ``session detach``, which deliberately leaves the
remote session running.

The on-disk shape and its lock/atomic-write discipline deliberately mirror
:class:`~clio_relay.cluster_config_registry.ClusterRegistry` byte-for-byte
(``FileLock`` + read-validate-mutate-atomic-replace-fsync): this is the same
kind of small, locally-configured, security-sensitive client state the
cluster registry already owns the pattern for, just keyed by cluster instead
of holding cluster definitions. Reusing the exact same io/Windows-ACL
primitives (``cluster_config_io.py``, ``cluster_config_windows_paths.py``)
means this file introduces no new persistence mechanism, only a new record
shape.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from filelock import FileLock
from pydantic import BaseModel, ConfigDict, Field

from clio_relay.cluster_config import default_registry_path
from clio_relay.cluster_config_io import (
    CONFIG_READ_RETRY_SECONDS,
    _fsync_directory,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    read_bounded_configuration_bytes,
)
from clio_relay.cluster_config_windows_paths import (
    ensure_private_configuration_directory,
    open_private_atomic_file,
)
from clio_relay.errors import ConfigurationError

#: Generous relative to one small per-cluster record; matches the spirit of
#: ClusterRegistry's own byte budget without borrowing its (much larger, many-
#: cluster) constant.
MAX_OWNED_SESSION_REGISTRY_BYTES = 256 * 1024
MAX_TRACKED_CLUSTER_SESSIONS = 512
CONFIG_REPLACE_ATTEMPTS = 25

OWNED_SESSION_REGISTRY_ENV = "CLIO_RELAY_OWNED_SESSION_REGISTRY"


class OwnedSessionRecord(BaseModel):
    """The durable identity of the last owned session brought up for one cluster."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1, max_length=256)
    session_id: str = Field(min_length=1, max_length=256)
    session_generation_id: str = Field(min_length=1, max_length=256)
    remote_api_port: int = Field(gt=0, le=65_535)
    created_at: str


class OwnedSessionRecordRegistry(BaseModel):
    """One durable owned-session record per cluster, keyed by cluster name."""

    model_config = ConfigDict(extra="forbid")

    sessions: dict[str, OwnedSessionRecord] = Field(
        default_factory=dict[str, OwnedSessionRecord],
        max_length=MAX_TRACKED_CLUSTER_SESSIONS,
    )

    @classmethod
    def load(cls, path: Path) -> OwnedSessionRecordRegistry:
        """Load the registry from disk, or return an empty one when absent."""
        if not path.exists():
            return cls()
        return cls.model_validate_json(
            read_bounded_configuration_bytes(path, max_bytes=MAX_OWNED_SESSION_REGISTRY_BYTES)
        )

    @classmethod
    def mutate(
        cls,
        path: Path,
        mutation: Callable[[OwnedSessionRecordRegistry], None],
    ) -> OwnedSessionRecordRegistry:
        """Apply a read-modify-write operation under one exclusive file lock."""
        ensure_private_configuration_directory(path.parent)
        with FileLock(f"{path}.lock"):
            registry = (
                cls.model_validate_json(
                    read_bounded_configuration_bytes(
                        path,
                        max_bytes=MAX_OWNED_SESSION_REGISTRY_BYTES,
                    )
                )
                if path.exists()
                else cls()
            )
            mutation(registry)
            validated = cls.model_validate(registry.model_dump(mode="python"))
            validated._write_atomic_unlocked(path)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            return validated

    def _write_atomic_unlocked(self, path: Path) -> None:
        """Write an atomic registry replacement while the caller holds the lock."""
        payload = (json.dumps(self.model_dump(), indent=2) + "\n").encode("utf-8")
        if len(payload) > MAX_OWNED_SESSION_REGISTRY_BYTES:
            raise ConfigurationError(
                f"owned session registry exceeds {MAX_OWNED_SESSION_REGISTRY_BYTES} bytes"
            )
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


def default_owned_session_record_path() -> Path:
    """Return the default durable owned-session record path.

    Sibling to the cluster registry file by default (same directory, same
    private-ACL discipline), overridable independently via
    ``CLIO_RELAY_OWNED_SESSION_REGISTRY`` for the same reasons
    ``CLIO_RELAY_CLUSTER_REGISTRY`` is independently overridable.
    """
    configured = os.getenv(OWNED_SESSION_REGISTRY_ENV)
    if configured:
        return Path(configured).expanduser().resolve()
    return default_registry_path().with_name("owned_sessions.json")


def save_owned_session_record(
    *,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    remote_api_port: int,
    path: Path | None = None,
) -> OwnedSessionRecord:
    """Persist the durable record for one cluster, overwriting any prior one.

    Called once, at successful owned-session bring-up
    (``cli_session_start.py``, when the start result is ``usable``) -- never
    speculatively, and never for a start attempt that only produced a durable
    operation handle without an attached, running API.
    """
    record = OwnedSessionRecord(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=session_generation_id,
        remote_api_port=remote_api_port,
        created_at=datetime.now(UTC).isoformat(),
    )
    target_path = path or default_owned_session_record_path()

    def _put(registry: OwnedSessionRecordRegistry) -> None:
        registry.sessions[cluster] = record

    OwnedSessionRecordRegistry.mutate(target_path, _put)
    return record


def load_owned_session_record(
    cluster: str,
    *,
    path: Path | None = None,
) -> OwnedSessionRecord | None:
    """Return the durable record for one cluster, or ``None`` when absent."""
    target_path = path or default_owned_session_record_path()
    return OwnedSessionRecordRegistry.load(target_path).sessions.get(cluster)


def clear_owned_session_record(cluster: str, *, path: Path | None = None) -> None:
    """Remove the durable record for one cluster; a no-op when none exists.

    Called only after an owned-session teardown is verified authoritatively
    closed (``closed_recovery.process_state == "already_closed"`` and its
    admission status is durably ``closed``) -- never on ``session detach``,
    which leaves the remote session running and must leave this record intact
    so a later ``session attach`` can still find it.
    """
    target_path = path or default_owned_session_record_path()
    if not target_path.exists():
        return

    def _pop(registry: OwnedSessionRecordRegistry) -> None:
        registry.sessions.pop(cluster, None)

    OwnedSessionRecordRegistry.mutate(target_path, _pop)
