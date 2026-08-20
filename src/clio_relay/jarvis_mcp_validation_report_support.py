"""Small leaf lookups feeding ``build_jarvis_mcp_validation_report``.

Owner module for the ``jarvis_mcp_validation.py`` split (clio-relay split/
jarvis-mcp-validation): three single-caller helpers that pull one nested
field out of a larger structure for the report builder --
``_artifact_location_references`` renders one generated JARVIS artifact's
transport-neutral location as a typed reference list, and
``_spack_environment_metadata``/``_jarvis_runtime_scheduler_cluster`` reach
into the durable runtime-metadata document for the Spack environment and
JARVIS's own scheduler-native cluster (never the relay route alias). Split
out of ``jarvis_mcp_validation_report.py`` on its own to keep that file under
the 800-line new-file cap without touching the orchestrating function's body.
"""

from __future__ import annotations

from clio_relay.jarvis_mcp_validation_core import _UNBOUND_JARVIS_IDENTITY, JSON, _mapping


def _artifact_location_references(artifact: dict[str, object]) -> list[str]:
    """Render transport-neutral JARVIS artifact locations as typed references."""
    location = _mapping(artifact.get("location")) or {}
    uri = location.get("uri")
    if isinstance(uri, str) and uri:
        return [uri]
    kind = location.get("kind")
    value = location.get("value")
    if isinstance(kind, str) and kind and isinstance(value, str) and value:
        return [f"{kind}:{value}"]
    return []


def _spack_environment_metadata(runtime_metadata: JSON | None) -> JSON | None:
    details = _mapping(runtime_metadata.get("details")) if runtime_metadata else None
    runtime = _mapping(details.get("runtime_metadata")) if details else None
    runtime_details = _mapping(runtime.get("details")) if runtime else None
    return _mapping(runtime_details.get("environment")) if runtime_details else None


def _jarvis_runtime_scheduler_cluster(runtime_metadata: JSON | None) -> object:
    """Return JARVIS's scheduler-native cluster, which is not the relay route alias."""
    details = _mapping(runtime_metadata.get("details")) if runtime_metadata else None
    native_execution = _mapping(details.get("native_execution")) if details else None
    handle = _mapping(native_execution.get("execution_handle")) if native_execution else None
    return handle.get("cluster") if handle is not None else _UNBOUND_JARVIS_IDENTITY
