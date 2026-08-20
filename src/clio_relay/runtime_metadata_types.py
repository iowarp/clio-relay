"""Schema identifiers, state vocabulary, and trust taxonomy for runtime metadata.

Extracted from ``runtime_metadata.py`` (clio-relay split/runtime-metadata-w2):
this is the dependency-free foundation every other ``runtime_metadata_*``
owner module builds on -- the wire schema-version strings JARVIS/clio-kit
producers declare, the JARVIS execution/progress state vocabularies (and the
state-machine graph that bounds legal transitions between them), the
Windows-reserved path-component set native identity validation rejects, the
:class:`RuntimeMetadataSource` trust taxonomy, and the
:class:`RuntimeMetadataIdentityConflictError` raised when authoritative
metadata would change a pinned execution identity.
"""

from __future__ import annotations

from enum import StrEnum

RUNTIME_METADATA_SCHEMA = "clio-relay.jarvis-runtime.v1"
JARVIS_RUNTIME_METADATA_SCHEMA = "jarvis.runtime.v1"
JARVIS_SCHEDULER_SUBMISSION_SCHEMA = "jarvis.scheduler.submission.v1"
JARVIS_EXECUTION_HANDLE_SCHEMA = "jarvis.execution.handle.v1"
JARVIS_EXECUTION_RECORD_SCHEMA = "jarvis.execution.record.v1"
JARVIS_EXECUTION_PROGRESS_SCHEMA = "jarvis.execution.progress.v1"
JARVIS_PROGRESS_EVENT_SCHEMA = "jarvis.progress.v1"
RUNTIME_SIDECAR_RECORD_SCHEMA = "clio-relay.runtime-sidecar-record.v1"

_JARVIS_EXECUTION_STATES = {
    "preparing",
    "scripted",
    "submitting",
    "submitted",
    "running",
    "completed",
    "failed",
    "canceled",
    "unknown",
}
_JARVIS_TERMINAL_STATES = {"scripted", "completed", "failed", "canceled"}
_JARVIS_PROGRESS_STATES = {
    "pending",
    "starting",
    "running",
    "ready",
    "completed",
    "failed",
    "canceled",
}
_WINDOWS_RESERVED_COMPONENTS = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "CLOCK$",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}
_JARVIS_REACHABLE_STATES: dict[str, set[str]] = {
    "preparing": _JARVIS_EXECUTION_STATES - {"preparing"},
    "scripted": {"running", "completed", "failed", "canceled", "unknown"},
    "submitting": {"submitted", "running", "completed", "failed", "canceled", "unknown"},
    "submitted": {"running", "completed", "failed", "canceled", "unknown"},
    "running": {"completed", "failed", "canceled", "unknown"},
    "completed": set(),
    "failed": set(),
    "canceled": set(),
    "unknown": {"submitted", "running", "completed", "failed", "canceled"},
}


class RuntimeMetadataIdentityConflictError(ValueError):
    """Raised when authoritative runtime metadata changes a pinned execution identity."""


class RuntimeMetadataSource(StrEnum):
    """Trust and compatibility source for a runtime observation."""

    JARVIS_MCP = "jarvis_mcp"
    JARVIS_SIDECAR = "jarvis_sidecar"
    RELAY_RECONCILIATION = "relay_reconciliation"
    UNTRUSTED_COMPATIBILITY = "untrusted_compatibility"
    LEGACY_STDOUT = "legacy_stdout"
