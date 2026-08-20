"""Structured runtime metadata emitted by JARVIS and remote MCP servers.

The relay stores this normalized contract without assuming a scheduler or an
application.  Producers may return the fields directly from an MCP tool, wrap
them in ``runtime_metadata``, or append authenticated observations to the
runtime sidecar advertised by the worker.

Owner-module re-exports (clio-relay split/runtime-metadata-w2, following the
iowarp/clio-relay#231 no-accretion decomposition doctrine). Each extracted
concern is re-imported here under its original name so every existing
``from clio_relay.runtime_metadata import X`` caller and every
``clio_relay.runtime_metadata.X`` qualified/monkeypatch access keeps
resolving unchanged -- a pure move, not a behavior change. See each owner
module's own docstring for what it owns:

* ``runtime_metadata_types.py`` -- schema-version constants, the JARVIS
  execution/progress state vocabulary and reachable-states graph, the
  Windows-reserved component set, :class:`RuntimeMetadataSource`, and
  :class:`RuntimeMetadataIdentityConflictError`.
* ``runtime_metadata_core_model.py`` -- the normalized
  :class:`JarvisRuntimeMetadata` document and its
  :class:`PackageProvenance` / :class:`TerminalRuntimeMetadata` components.
* ``runtime_metadata_coercion.py`` -- loose, best-effort payload coercion
  helpers used by the legacy JARVIS/clio-kit compatibility flow.
* ``runtime_metadata_native_validators.py`` -- strict, fail-closed field
  validators for the exact native JARVIS document family.
* ``runtime_metadata_native_documents.py`` -- the exact native JARVIS
  execution/progress document family and the clio-kit runtime projection
  validated against it.
* ``runtime_metadata_merge.py`` -- :func:`merge_runtime_metadata` and the
  identity-pinning/lifecycle-regression guards it enforces.
* ``runtime_metadata_native_normalize.py`` -- normalizing exact native
  execution documents (and their paired runtime projection) into
  :class:`JarvisRuntimeMetadata`.
* ``runtime_metadata_mcp_normalize.py`` -- decoding a persisted MCP-call
  result, including the loose legacy compatibility parse.
* ``runtime_metadata_sidecar.py`` -- the authenticated, ordered runtime
  metadata sidecar record codec.
"""

from __future__ import annotations

from clio_relay.runtime_metadata_core_model import (
    JarvisRuntimeMetadata,
    PackageProvenance,
    TerminalRuntimeMetadata,
)
from clio_relay.runtime_metadata_mcp_normalize import (
    legacy_scheduler_runtime_metadata,
    normalize_runtime_metadata,
    runtime_metadata_from_mcp_result_document,
    structured_mcp_result,
)
from clio_relay.runtime_metadata_merge import merge_runtime_metadata
from clio_relay.runtime_metadata_native_documents import (
    JarvisExecutionHandleDocument,
    JarvisExecutionProgressDocument,
    JarvisExecutionRecordDocument,
    JarvisNativeExecutionDocuments,
    JarvisPackageProgressSnapshotDocument,
    JarvisProgressEventDocument,
)
from clio_relay.runtime_metadata_native_normalize import (
    native_execution_documents,
    runtime_metadata_from_native_documents,
)
from clio_relay.runtime_metadata_sidecar import (
    runtime_metadata_from_sidecar_record,
    runtime_sidecar_record,
)
from clio_relay.runtime_metadata_types import (
    JARVIS_EXECUTION_HANDLE_SCHEMA,
    JARVIS_EXECUTION_PROGRESS_SCHEMA,
    JARVIS_EXECUTION_RECORD_SCHEMA,
    JARVIS_PROGRESS_EVENT_SCHEMA,
    JARVIS_RUNTIME_METADATA_SCHEMA,
    JARVIS_SCHEDULER_SUBMISSION_SCHEMA,
    RUNTIME_METADATA_SCHEMA,
    RUNTIME_SIDECAR_RECORD_SCHEMA,
    RuntimeMetadataIdentityConflictError,
    RuntimeMetadataSource,
)

__all__ = [
    "JARVIS_EXECUTION_HANDLE_SCHEMA",
    "JARVIS_EXECUTION_PROGRESS_SCHEMA",
    "JARVIS_EXECUTION_RECORD_SCHEMA",
    "JARVIS_PROGRESS_EVENT_SCHEMA",
    "JARVIS_RUNTIME_METADATA_SCHEMA",
    "JARVIS_SCHEDULER_SUBMISSION_SCHEMA",
    "RUNTIME_METADATA_SCHEMA",
    "RUNTIME_SIDECAR_RECORD_SCHEMA",
    "JarvisExecutionHandleDocument",
    "JarvisExecutionProgressDocument",
    "JarvisExecutionRecordDocument",
    "JarvisNativeExecutionDocuments",
    "JarvisPackageProgressSnapshotDocument",
    "JarvisProgressEventDocument",
    "JarvisRuntimeMetadata",
    "PackageProvenance",
    "RuntimeMetadataIdentityConflictError",
    "RuntimeMetadataSource",
    "TerminalRuntimeMetadata",
    "legacy_scheduler_runtime_metadata",
    "merge_runtime_metadata",
    "native_execution_documents",
    "normalize_runtime_metadata",
    "runtime_metadata_from_mcp_result_document",
    "runtime_metadata_from_native_documents",
    "runtime_metadata_from_sidecar_record",
    "runtime_sidecar_record",
    "structured_mcp_result",
]
