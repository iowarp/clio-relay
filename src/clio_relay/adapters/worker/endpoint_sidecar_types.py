"""Shared sidecar/progress identity types and byte-budget constants.

Owner module for the plain data shapes and module-level constants that
``endpoint.py``'s ``EndpointWorker`` and its (still co-resident, being
progressively extracted per iowarp/clio-relay#231) module-level helper
functions both reach for: the filesystem-identity dataclasses pinned against
TOCTOU races (``_PackageProgressLogState``, ``_RuntimeSidecarAnchor``), the
progress/runtime sidecar schema and byte-budget constants, and the Windows
``kernel32`` constant set the ctypes-based sidecar handle primitives share. None of
these carry any behavior that reaches back into ``endpoint.py`` or any other
owner module extracted from it, so this module is a pure leaf: every other
``endpoint_*.py`` owner module (and ``endpoint.py`` itself) imports from
here, never the reverse, which is what keeps the decomposition free of
load-order circular imports.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from clio_relay.errors import RelayError


class SchedulerSubmissionUnresolvedError(RelayError):
    """An armed scheduler intent could not yet be resolved to zero or one owned job.

    Raised across ``EndpointWorker``'s (``endpoint.py``, still the composed
    facade) execution-ownership/scheduler-submission mixins.
    It lives on this pure-leaf module -- not on any one of them -- because
    every one of those owner modules raises or catches it, and a leaf is the
    only home that keeps them acyclic (iowarp/clio-relay#231, endpoint split
    slice 10). Not part of any test's ``monkeypatch.setattr`` surface, so no
    module-attribute indirection is needed for it the way ``endpoint_
    execution_lifecycle.py``'s ``EXECUTION_CLEANUP_SCAN_LIMIT`` requires.
    """


@dataclass
class _PackageProgressLogState:
    """Tail checkpoint that excludes pre-launch bytes and detects source resets."""

    path: Path
    offset: int
    identity: tuple[int, int] | None
    checkpoint_offset: int
    checkpoint_sha256: str | None


@dataclass(frozen=True, slots=True)
class _RuntimeSidecarAnchor:
    """Pinned filesystem identity for one precreated runtime sidecar."""

    device: int
    inode: int
    owner: int
    link_count: int
    mode: int
    descriptor: int | None = field(default=None, compare=False, repr=False)

    def as_metadata(self) -> dict[str, int]:
        """Return the JSON form carried only through the private broker channel."""
        return {
            "device": self.device,
            "inode": self.inode,
            "owner": self.owner,
            "link_count": self.link_count,
            "mode": self.mode,
        }


PACKAGE_PROGRESS_LOG_READ_BYTES = 1024 * 1024
PACKAGE_PROGRESS_LOG_FINAL_MAX_BYTES = 64 * 1024 * 1024
PROGRESS_SIDECAR_MAX_RECORD_BYTES = 64 * 1024
PROGRESS_SIDECAR_MAX_TOTAL_BYTES = 16 * 1024 * 1024
PROGRESS_SIDECAR_MAX_RECORDS = 10_000
PROGRESS_SIDECAR_RECORD_SCHEMA = "clio-relay.progress-sidecar-record.v1"
# One exact native JARVIS snapshot may be 4 MiB before the execution record,
# handle, sidecar envelope, and HMAC are added.
RUNTIME_SIDECAR_MAX_RECORD_BYTES = 5 * 1024 * 1024
RUNTIME_SIDECAR_MAX_TOTAL_BYTES = 64 * 1024 * 1024
RUNTIME_SIDECAR_MAX_RECORDS = 4_096
SIDECAR_DRAIN_CHUNK_BYTES = 64 * 1024
AGENT_RESULT_MAX_BYTES = 1024 * 1024
EXECUTION_CLEANUP_MAX_FOREGROUND_JOBS = 8
OUTPUT_EVENT_MAX_BYTES = 64 * 1024
EXECUTION_CLEANUP_SCHEMA = "clio-relay.execution-cleanup.v1"
EXECUTION_SIDECAR_CLEANUP_SCHEMA = "clio-relay.execution-sidecar-cleanup.v1"
EXECUTION_SIDECAR_QUARANTINE_SCHEMA = "clio-relay.execution-sidecar-quarantine.v1"
RUNTIME_SIDECAR_CHANNEL_SCHEMA = "clio-relay.runtime-sidecar-channel.v1"
EXECUTION_LAUNCH_PROTOCOL = "broker-release-after-ownership-v1"

# Windows kernel32 constants shared by the ctypes-based sidecar handle
# primitives (endpoint_windows_sidecar_handles.py). Kept underscore-
# prefixed (module-private by convention, not by enforcement) so every
# existing call site that already spells them ``_WINDOWS_...`` needs no
# rename -- only its import source changes.
_WINDOWS_DELETE = 0x00010000
_WINDOWS_FILE_READ_ATTRIBUTES = 0x00000080
_WINDOWS_FILE_SHARE_READ = 0x00000001
_WINDOWS_FILE_SHARE_WRITE = 0x00000002
_WINDOWS_OPEN_EXISTING = 3
_WINDOWS_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_WINDOWS_FILE_RENAME_INFO = 3
_WINDOWS_ERROR_FILE_NOT_FOUND = 2
_WINDOWS_ERROR_PATH_NOT_FOUND = 3
_WINDOWS_ERROR_FILE_EXISTS = 80
_WINDOWS_ERROR_ALREADY_EXISTS = 183
