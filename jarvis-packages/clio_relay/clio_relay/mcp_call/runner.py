"""Minimal stdio MCP client used by relay endpoint containment and legacy JARVIS adapters."""

from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
import math
import os
import re
import secrets
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import zipfile
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from importlib import metadata
from pathlib import Path, PurePosixPath
from queue import Empty, Queue
from typing import Any, cast
from urllib.parse import unquote, urlsplit

from clio_relay.process_containment import (
    CONTAINMENT_ENV,
    nested_popen_kwargs,
    terminate_nested_process,
)

TOOLS_LIST_MAX_PAGES = 64
TOOLS_LIST_MAX_TOOLS = 10_000
TOOLS_LIST_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MCP_CALL_DEFAULT_TIMEOUT_SECONDS = 300
MCP_SERVER_TERMINATION_TIMEOUT_SECONDS = 2.0
MCP_INITIALIZE_MAX_RESPONSE_BYTES = 1024 * 1024
MCP_CALL_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MCP_SESSION_MAX_STDOUT_BYTES = 32 * 1024 * 1024
MCP_SESSION_MAX_STDERR_BYTES = 4 * 1024 * 1024
MCP_PACKAGE_PROGRESS_SCHEMA = "clio-kit.jarvis-package-progress.v1"
MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA = "clio-relay.mcp-package-progress-bridge.v1"
MCP_JARVIS_RUNTIME_SCHEMA = "jarvis.runtime.v1"
MCP_JARVIS_EXECUTION_HANDLE_SCHEMA = "jarvis.execution.handle.v1"
MCP_JARVIS_EXECUTION_RECORD_SCHEMA = "jarvis.execution.record.v1"
MCP_JARVIS_EXECUTION_PROGRESS_SCHEMA = "jarvis.execution.progress.v1"
MCP_JARVIS_PROGRESS_EVENT_SCHEMA = "jarvis.progress.v1"
MCP_JARVIS_EXECUTION_QUERY_SCHEMA = "clio-kit.jarvis-execution.v2"
MCP_JARVIS_EXECUTION_ARTIFACTS_SCHEMA = "jarvis.execution.artifacts.v1"
MCP_JARVIS_ARTIFACT_SCHEMA = "jarvis.artifact.v1"
MCP_JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA = "jarvis.execution.service-runtimes.v1"
MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA = "clio-relay.mcp-jarvis-progress-bridge.v1"
REGISTERED_JARVIS_EXECUTION_QUERY_CONTRACT = "clio-kit-jarvis-user-v3.6"
MCP_REQUEST_MAX_BYTES = 16 * 1024 * 1024
MCP_PACKAGE_PROGRESS_MAX_NOTIFICATION_BYTES = 64 * 1024
MCP_PACKAGE_PROGRESS_MAX_NOTIFICATIONS = 10_000
MCP_PACKAGE_PROGRESS_MAX_TOTAL_BYTES = 4 * 1024 * 1024
PROGRESS_SIDECAR_RECORD_SCHEMA = "clio-relay.progress-sidecar-record.v1"
_JARVIS_EXECUTION_STATES = frozenset(
    {
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
)
_JARVIS_TERMINAL_STATES = frozenset({"scripted", "completed", "failed", "canceled"})
_JARVIS_PROGRESS_STATES = frozenset(
    {"pending", "starting", "running", "ready", "completed", "failed", "canceled"}
)
_JARVIS_ARTIFACT_ROLES = frozenset(
    {"intermediate", "output", "log", "checkpoint", "provenance", "validation"}
)
_JARVIS_ARTIFACT_STATES = frozenset({"producing", "available", "finalized", "incomplete", "failed"})
_JARVIS_ARTIFACT_STRUCTURES = frozenset({"file", "directory", "collection", "stream"})
_JARVIS_ARTIFACT_OWNERSHIP = frozenset({"execution", "external", "shared"})
_JARVIS_ARTIFACT_LOCATION_KINDS = frozenset({"execution_path", "cluster_path", "external_uri"})
_JARVIS_ARTIFACT_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "package_name",
        "package_id",
        "execution_id",
        "artifact_id",
        "logical_name",
        "kind",
        "role",
        "structure",
        "ownership",
        "state",
        "revision",
        "sequence",
        "observed_at_epoch",
        "metadata",
    }
)
_JARVIS_ARTIFACT_OPTIONAL_FIELDS = frozenset(
    {"location", "media_type", "format", "size_bytes", "checksum", "message"}
)
_JARVIS_ARTIFACT_ID = re.compile(r"^art_[A-Za-z0-9_-]{22,86}$")
_JARVIS_ARTIFACT_CHECKSUM = re.compile(r"^[a-z0-9][a-z0-9_-]*:[A-Fa-f0-9]{16,256}$")
_JARVIS_ARTIFACT_MEDIA_TYPE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]*$"
)
_JARVIS_ARTIFACT_CURSOR = re.compile(r"^[A-Za-z0-9_-]+$")
_JARVIS_ARTIFACT_URI_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*$")
_JARVIS_ARTIFACT_UNSAFE_URI_SCHEMES = frozenset({"data", "file", "javascript"})
_JARVIS_ARTIFACT_MAX_PAGE_SIZE = 100
_JARVIS_ARTIFACT_DEFAULT_PAGE_SIZE = 50
_JARVIS_ARTIFACT_MAX_CURSOR_LENGTH = 1024
_JARVIS_ARTIFACT_MAX_EVENT_BYTES = 64 * 1024
_JARVIS_ARTIFACT_MAX_METADATA_BYTES = 64 * 1024
_WINDOWS_RESERVED_COMPONENTS = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "CLOCK$",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
    }
)
_JARVIS_REACHABLE_STATES: dict[str, frozenset[str]] = {
    "preparing": _JARVIS_EXECUTION_STATES - {"preparing"},
    "scripted": frozenset({"running", "completed", "failed", "canceled", "unknown"}),
    "submitting": frozenset({"submitted", "running", "completed", "failed", "canceled", "unknown"}),
    "submitted": frozenset({"running", "completed", "failed", "canceled", "unknown"}),
    "running": frozenset({"completed", "failed", "canceled", "unknown"}),
    "completed": frozenset(),
    "failed": frozenset(),
    "canceled": frozenset(),
    "unknown": frozenset({"submitted", "running", "completed", "failed", "canceled"}),
}
FILE_HASH_CHUNK_BYTES = 1024 * 1024
CLIO_KIT_WHEEL_MAX_FILES = 10_000
CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES = 1024 * 1024
CLIO_KIT_LOCK_MAX_BYTES = 16 * 1024 * 1024
CLIO_KIT_WHEEL_MAX_PROJECT_FILES = 20_000
CLIO_KIT_WHEEL_MAX_PROJECT_BYTES = 512 * 1024 * 1024
PYTHON_DISTRIBUTION_MAX_DISTRIBUTIONS = 10_000
PYTHON_DISTRIBUTION_MAX_ENTRY_POINTS = 100_000
PYTHON_DISTRIBUTION_MAX_FILES = 100_000
PYTHON_DISTRIBUTION_MAX_BYTES = 4 * 1024 * 1024 * 1024
PYTHON_TOOL_IDENTITY_MAX_BYTES = 8 * 1024 * 1024
PYTHON_TOOL_IDENTITY_TIMEOUT_SECONDS = 30
_STREAM_READ_CHARS = 64 * 1024
_TOOLS_LIST_PAGINATION_KEY = "_clioRelayPagination"
_CLIO_KIT_LOCKED_SERVER_SCHEMA = "clio-kit.locked-server.v4"
_CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY = "uv-run:materialized:frozen:no-editable:no-dev:v3"
_JARVIS_CD_LOCK_BINDING_SCHEMA = "clio-relay.jarvis-cd-lock-binding.v1"
# These values intentionally mirror clio_relay.bootstrap. A focused release test
# prevents either copy from moving independently. The JARVIS package also runs as
# a standalone repository package, where importing the installed relay bootstrap
# module is not a valid dependency boundary.
JARVIS_CD_VERSION = "1.6.0"
JARVIS_CD_WHEEL_URL = (
    "https://github.com/grc-iit/jarvis-cd/releases/download/"
    f"v{JARVIS_CD_VERSION}/jarvis_cd-{JARVIS_CD_VERSION}-py3-none-any.whl"
)
JARVIS_CD_WHEEL_SHA256 = "c4853138f3263715e806fcd794233d89f4aa58161e3c5fbab59e7f96d24f0e98"
_CLIO_KIT_RUNTIME_PROJECT_EXCLUDED_NAMES = frozenset(
    {
        ".git",
        ".coverage",
        ".DS_Store",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        ".virtualenv-app-data",
        "__pycache__",
        "dist",
        "coverage.xml",
        "htmlcov",
        "junit.xml",
        "tests",
    }
)
_RELAY_CREDENTIAL_ENV_NAMES = frozenset(
    {
        "CLIO_RELAY_API_TOKEN",
        "CLIO_RELAY_FRP_TOKEN",
        "CLIO_RELAY_PROGRESS_TOKEN",
        "CLIO_RELAY_RUNTIME_METADATA_TOKEN",
        "CLIO_RELAY_STCP_SECRET",
    }
)
_BASE_CHILD_ENV_NAMES = frozenset(
    {
        "APPDATA",
        "COMSPEC",
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "LOCALAPPDATA",
        "LOGNAME",
        "NoDefaultCurrentDirectoryInExePath",
        "PATH",
        "PATHEXT",
        "PYTHONIOENCODING",
        "PYTHONUTF8",
        "SHELL",
        "SYSTEMDRIVE",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USER",
        "USERPROFILE",
        "UV_CACHE_DIR",
        "UV_PYTHON_INSTALL_DIR",
        "UV_TOOL_DIR",
        "WINDIR",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
    }
)


class _McpProtocolFailure(RuntimeError):
    """Bounded local failure while consuming an MCP protocol session."""


class _StreamLimit:
    """Marker emitted after a child stream exceeds its capture budget."""

    __slots__ = ("message",)

    def __init__(self, message: str) -> None:
        self.message = message


class _McpProgressBridge:
    """Authenticate one MCP progress stream and append relay-sidecar records."""

    def __init__(
        self,
        *,
        path: Path,
        relay_token: str,
        expected_server_artifact_digest: str,
        observed_server_artifact_digest: str,
        expected_pipeline_id: str,
    ) -> None:
        self.path = path
        self.relay_token = relay_token
        self.expected_server_artifact_digest = expected_server_artifact_digest
        self.observed_server_artifact_digest = observed_server_artifact_digest
        self.expected_pipeline_id = expected_pipeline_id
        self.progress_token = secrets.token_urlsafe(32)
        self.notification_count = 0
        self.notification_bytes = 0
        self.last_sequence = 0
        self.sidecar_sequence = 0
        self.bound_execution_id: str | None = None
        self.bound_provider: dict[str, Any] | None = None
        self.acceptance_candidates: list[dict[str, Any]] = []
        self.native_mode: bool | None = None
        self.native_transport_sequence = 0
        self.native_execution_state: str | None = None
        self.native_execution_terminal: bool | None = None
        self.native_scripted_activation_observed = False
        self.native_package_names: dict[str, str] = {}
        self.native_package_sequences: dict[str, int] = {}
        self.native_package_event_counts: dict[str, int] = {}
        self.native_latest_candidates: dict[str, dict[str, Any]] = {}
        self.execution_validated = False

    def observe(self, message: dict[str, Any]) -> None:
        """Validate and bridge one package-progress notification immediately."""
        raw_params = message.get("params")
        if not isinstance(raw_params, dict):
            raise _McpProtocolFailure("MCP progress notification params must be an object")
        params = cast(dict[str, Any], raw_params)
        token = params.get("progressToken")
        if not isinstance(token, str) or not secrets.compare_digest(token, self.progress_token):
            raise _McpProtocolFailure("MCP progress notification token did not match")
        raw_message = params.get("message")
        if not isinstance(raw_message, str):
            raise _McpProtocolFailure("MCP package progress message must be schema-versioned JSON")
        encoded_size = len(raw_message.encode("utf-8"))
        if encoded_size > MCP_PACKAGE_PROGRESS_MAX_NOTIFICATION_BYTES:
            raise _McpProtocolFailure("MCP package progress notification exceeded its byte limit")
        self.notification_count += 1
        self.notification_bytes += encoded_size
        if self.notification_count > MCP_PACKAGE_PROGRESS_MAX_NOTIFICATIONS:
            raise _McpProtocolFailure("MCP package progress exceeded its notification limit")
        if self.notification_bytes > MCP_PACKAGE_PROGRESS_MAX_TOTAL_BYTES:
            raise _McpProtocolFailure("MCP package progress exceeded its total byte limit")
        try:
            envelope = json.loads(raw_message, object_pairs_hook=_reject_duplicate_json_keys)
        except (json.JSONDecodeError, ValueError) as exc:
            raise _McpProtocolFailure(f"MCP package progress JSON was invalid: {exc}") from exc
        typed_envelope = cast(dict[str, Any], envelope) if isinstance(envelope, dict) else None
        if (
            typed_envelope is not None
            and typed_envelope.get("schema_version") == MCP_JARVIS_EXECUTION_PROGRESS_SCHEMA
        ):
            self._observe_native_progress(typed_envelope, params=params)
            return
        if self.native_mode is True:
            raise _McpProtocolFailure("MCP progress producer changed from native to compatibility")
        self.native_mode = False
        validated = self._validated_envelope(typed_envelope, params=params)
        self._append_record(validated, execution_validated=False)
        if validated["provider_acceptance_validated"] is True:
            self.acceptance_candidates.append(validated)

    def finalize(self, structured_result: dict[str, Any] | None) -> None:
        """Bind accepted observations to the final JARVIS execution result."""
        if structured_result is None:
            if self.notification_count == 0:
                return
            raise _McpProtocolFailure(
                "MCP package progress had no structured JARVIS result for execution binding"
            )
        native_documents = _validated_native_execution_documents(structured_result)
        if native_documents is not None:
            if self.native_mode is False:
                raise _McpProtocolFailure(
                    "MCP compatibility progress result changed to native execution documents"
                )
            self._finalize_native_progress(native_documents)
            return
        if self.native_mode is True:
            raise _McpProtocolFailure(
                "MCP native progress result omitted native JARVIS execution documents"
            )
        if self.notification_count == 0:
            return
        raw_runtime = structured_result.get("runtime_metadata")
        if not isinstance(raw_runtime, dict):
            raise _McpProtocolFailure(
                "MCP package progress result omitted structured JARVIS runtime metadata"
            )
        runtime = cast(dict[str, Any], raw_runtime)
        if runtime.get("schema_version") != MCP_JARVIS_RUNTIME_SCHEMA:
            raise _McpProtocolFailure(
                "MCP package progress result omitted the JARVIS runtime producer schema"
            )
        if runtime.get("execution_id") != self.bound_execution_id:
            raise _McpProtocolFailure("MCP package progress execution id did not match the result")
        if runtime.get("pipeline_id") != self.expected_pipeline_id:
            raise _McpProtocolFailure("MCP package progress pipeline id did not match the result")
        package_name = (
            self.bound_provider.get("package_name") if self.bound_provider is not None else None
        )
        raw_provenance = runtime.get("package_provenance")
        if not isinstance(raw_provenance, list) or not any(
            isinstance(item, dict) and cast(dict[str, Any], item).get("pkg_type") == package_name
            for item in cast(list[object], raw_provenance)
        ):
            raise _McpProtocolFailure(
                "MCP package progress provider package was absent from runtime provenance"
            )
        self.execution_validated = True
        for candidate in self.acceptance_candidates:
            self._append_record(candidate, execution_validated=True)

    def result_metadata(self) -> dict[str, Any]:
        """Return non-secret progress-bridge provenance for ``mcp-result.json``."""
        if self.native_mode is True:
            return {
                "schema_version": MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA,
                "notification_count": self.notification_count,
                "notification_bytes": self.notification_bytes,
                "execution_id": self.bound_execution_id,
                "pipeline_id": self.expected_pipeline_id,
                "package_sequences": dict(sorted(self.native_package_sequences.items())),
                "expected_server_artifact_digest": self.expected_server_artifact_digest,
                "observed_server_artifact_digest": self.observed_server_artifact_digest,
                "execution_validated": self.execution_validated,
            }
        return {
            "schema_version": MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA,
            "notification_count": self.notification_count,
            "notification_bytes": self.notification_bytes,
            "execution_id": self.bound_execution_id,
            "pipeline_id": self.expected_pipeline_id,
            "provider": self.bound_provider,
            "expected_server_artifact_digest": self.expected_server_artifact_digest,
            "observed_server_artifact_digest": self.observed_server_artifact_digest,
            "execution_validated": self.execution_validated,
        }

    def _observe_native_progress(
        self,
        snapshot_value: dict[str, Any],
        *,
        params: dict[str, Any],
    ) -> None:
        """Validate one native snapshot without treating MCP progress as workload percent."""
        if self.native_mode is False:
            raise _McpProtocolFailure("MCP progress producer changed from compatibility to native")
        self.native_mode = True
        transport_value = _finite_progress_number(params.get("progress"))
        if (
            transport_value is None
            or not transport_value.is_integer()
            or int(transport_value) != self.native_transport_sequence + 1
        ):
            raise _McpProtocolFailure("MCP native progress transport sequence was not monotonic")
        self.native_transport_sequence = int(transport_value)
        snapshot = _validated_native_progress_snapshot(snapshot_value)
        if snapshot["pipeline_id"] != self.expected_pipeline_id:
            raise _McpProtocolFailure("MCP native progress pipeline id did not match the request")
        execution_id = cast(str, snapshot["execution_id"])
        if self.bound_execution_id is None:
            self.bound_execution_id = execution_id
        elif self.bound_execution_id != execution_id:
            raise _McpProtocolFailure("MCP native progress execution id changed")
        self._observe_native_execution_lifecycle(snapshot)
        packages = cast(list[dict[str, Any]], snapshot["packages"])
        package_ids = {cast(str, package["package_id"]) for package in packages}
        if not set(self.native_package_names).issubset(package_ids):
            raise _McpProtocolFailure("MCP native progress dropped a package identity")
        for package in packages:
            self._observe_native_package(
                snapshot,
                package,
                transport_sequence=self.native_transport_sequence,
            )

    def _observe_native_package(
        self,
        snapshot: dict[str, Any],
        package: dict[str, Any],
        *,
        transport_sequence: int,
    ) -> None:
        """Append a package's new latest event while recording skipped snapshot events."""
        package_id = cast(str, package["package_id"])
        package_name = cast(str, package["package_name"])
        prior_name = self.native_package_names.get(package_id)
        if prior_name is not None and prior_name != package_name:
            raise _McpProtocolFailure("MCP native package progress name changed")
        self.native_package_names[package_id] = package_name
        event_count = cast(int, package["event_count"])
        prior_count = self.native_package_event_counts.get(package_id, 0)
        if event_count < prior_count:
            raise _McpProtocolFailure("MCP native progress event count regressed")
        self.native_package_event_counts[package_id] = event_count
        latest = cast(dict[str, Any] | None, package["latest"])
        if latest is None:
            return
        event_sequence = cast(int, latest["sequence"])
        prior_sequence = self.native_package_sequences.get(package_id, -1)
        if event_sequence < prior_sequence:
            raise _McpProtocolFailure("MCP native package progress sequence regressed")
        if event_sequence == prior_sequence:
            if event_count != prior_count:
                raise _McpProtocolFailure(
                    "MCP native package progress count changed without a new event"
                )
            return
        if prior_sequence >= 0 and event_count == prior_count:
            raise _McpProtocolFailure(
                "MCP native package progress event changed without increasing its count"
            )
        candidate = {
            "snapshot": snapshot,
            "package": package,
            "event": latest,
            "transport_sequence": transport_sequence,
            "skipped_event_count": max(0, event_count - prior_count - 1),
        }
        self.native_package_sequences[package_id] = event_sequence
        self.native_latest_candidates[package_id] = candidate
        self._append_native_record(candidate, execution_validated=False)

    def _observe_native_execution_lifecycle(self, snapshot: dict[str, Any]) -> None:
        """Require each sampled execution state to be reachable without regression."""
        state = cast(str, snapshot["execution_state"])
        terminal = cast(bool, snapshot["terminal"])
        previous_state = self.native_execution_state
        previous_terminal = self.native_execution_terminal
        if previous_state is None:
            self.native_execution_state = state
            self.native_execution_terminal = terminal
            return
        if state == previous_state:
            if terminal is not previous_terminal:
                raise _McpProtocolFailure("MCP native progress terminal flag changed in place")
            return
        if state not in _JARVIS_REACHABLE_STATES[previous_state]:
            raise _McpProtocolFailure("MCP native progress execution state regressed")
        if previous_terminal is True and previous_state != "scripted":
            raise _McpProtocolFailure("MCP native terminal execution changed state")
        if previous_state == "scripted" and state != "failed":
            self.native_scripted_activation_observed = True
        self.native_execution_state = state
        self.native_execution_terminal = terminal

    def _finalize_native_progress(self, documents: dict[str, dict[str, Any]]) -> None:
        """Bind native observations to exact matching final execution documents."""
        self.native_mode = True
        handle = documents["execution_handle"]
        progress = documents["progress"]
        record = documents["execution_record"]
        execution_id = cast(str, handle["execution_id"])
        if cast(str, handle["pipeline_id"]) != self.expected_pipeline_id:
            raise _McpProtocolFailure("MCP native execution pipeline id did not match the request")
        if self.bound_execution_id is not None and self.bound_execution_id != execution_id:
            raise _McpProtocolFailure("MCP native progress execution id did not match the result")
        self.bound_execution_id = execution_id
        self._observe_native_execution_lifecycle(progress)
        if self.native_scripted_activation_observed and (
            handle["mode"] != "scheduler"
            or handle["scheduler_native_id"] is None
            or record["submitted"] is not True
        ):
            raise _McpProtocolFailure(
                "MCP native scripted execution activation lacked scheduler identity"
            )
        final_packages = {
            cast(str, package["package_id"]): package
            for package in cast(list[dict[str, Any]], progress["packages"])
        }
        if not set(self.native_package_names).issubset(final_packages):
            raise _McpProtocolFailure("MCP native final progress dropped a package identity")
        for package_id, candidate in self.native_latest_candidates.items():
            final_package = final_packages.get(package_id)
            if final_package is None:
                raise _McpProtocolFailure(
                    "MCP native progress package was absent from final result"
                )
            candidate_event = cast(dict[str, Any], candidate["event"])
            final_event = cast(dict[str, Any] | None, final_package["latest"])
            if final_event is None or cast(int, final_event["sequence"]) < cast(
                int, candidate_event["sequence"]
            ):
                raise _McpProtocolFailure("MCP native progress result regressed a package event")
            if (
                cast(int, final_event["sequence"]) == cast(int, candidate_event["sequence"])
                and final_event != candidate_event
            ):
                raise _McpProtocolFailure("MCP native progress changed an existing package event")
        final_candidates: list[tuple[str, str, int, int, dict[str, Any] | None]] = []
        for package in final_packages.values():
            package_id = cast(str, package["package_id"])
            package_name = cast(str, package["package_name"])
            previous_name = self.native_package_names.get(package_id)
            if previous_name is not None and previous_name != package_name:
                raise _McpProtocolFailure("MCP native final package progress name changed")
            latest = cast(dict[str, Any] | None, package["latest"])
            if latest is None:
                final_candidates.append((package_id, package_name, 0, -1, None))
                continue
            event_count = cast(int, package["event_count"])
            previous_count = self.native_package_event_counts.get(package_id, 0)
            if event_count < previous_count:
                raise _McpProtocolFailure("MCP native final progress event count regressed")
            previous_sequence = self.native_package_sequences.get(package_id, -1)
            final_sequence = cast(int, latest["sequence"])
            if final_sequence == previous_sequence and event_count != previous_count:
                raise _McpProtocolFailure(
                    "MCP native final progress count changed without a new event"
                )
            if (
                previous_sequence >= 0
                and final_sequence > previous_sequence
                and (event_count == previous_count)
            ):
                raise _McpProtocolFailure(
                    "MCP native final progress event changed without increasing its count"
                )
            candidate = {
                "snapshot": progress,
                "package": package,
                "event": latest,
                "transport_sequence": self.native_transport_sequence,
                "skipped_event_count": max(0, event_count - previous_count - 1),
            }
            final_candidates.append(
                (package_id, package_name, event_count, final_sequence, candidate)
            )
        self.execution_validated = True
        for package_id, package_name, event_count, final_sequence, candidate in final_candidates:
            self.native_package_names[package_id] = package_name
            self.native_package_event_counts[package_id] = event_count
            if candidate is None:
                continue
            self.native_package_sequences[package_id] = final_sequence
            self._append_native_record(candidate, execution_validated=True)

    def _append_native_record(
        self,
        candidate: dict[str, Any],
        *,
        execution_validated: bool,
    ) -> None:
        """Project one exact native event into the relay progress record transport."""
        snapshot = cast(dict[str, Any], candidate["snapshot"])
        package = cast(dict[str, Any], candidate["package"])
        event = cast(dict[str, Any], candidate["event"])
        metadata = dict(cast(dict[str, Any], event["metadata"]))
        metadata["mcp_native_progress_bridge"] = {
            "schema_version": MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA,
            "execution_id": snapshot["execution_id"],
            "pipeline_id": snapshot["pipeline_id"],
            "execution_state": snapshot["execution_state"],
            "terminal": snapshot["terminal"],
            "transport_sequence": candidate["transport_sequence"],
            "package_name": package["package_name"],
            "package_id": package["package_id"],
            "event_count": package["event_count"],
            "event_schema_version": event["schema_version"],
            "event_sequence": event["sequence"],
            "event_state": event["state"],
            "observed_at_epoch": event["observed_at_epoch"],
            "determinate": event["determinate"],
            "skipped_event_count": candidate["skipped_event_count"],
            "expected_server_artifact_digest": self.expected_server_artifact_digest,
            "observed_server_artifact_digest": self.observed_server_artifact_digest,
            "execution_validated": execution_validated,
        }
        record: dict[str, Any] = {
            "label": event["label"],
            "message": event.get("message") or event["label"],
            "metadata": metadata,
        }
        for field_name in ("current", "total", "unit"):
            if field_name in event:
                record[field_name] = event[field_name]
        self._append_progress_payload(record)

    def _validated_envelope(
        self,
        envelope: object,
        *,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(envelope, dict):
            raise _McpProtocolFailure("MCP package progress envelope must be an object")
        typed = dict(cast(dict[str, Any], envelope))
        required = {
            "schema_version",
            "execution_id",
            "pipeline_id",
            "notification_sequence",
            "source_authority",
            "provider",
            "provider_acceptance_validated",
            "record",
        }
        if set(typed) != required or typed.get("schema_version") != MCP_PACKAGE_PROGRESS_SCHEMA:
            raise _McpProtocolFailure("MCP package progress envelope schema was invalid")
        execution_id = _nonempty_bounded_text(typed.get("execution_id"), "execution_id")
        pipeline_id = _nonempty_bounded_text(typed.get("pipeline_id"), "pipeline_id")
        if pipeline_id != self.expected_pipeline_id:
            raise _McpProtocolFailure("MCP package progress pipeline id did not match the request")
        sequence = typed.get("notification_sequence")
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence != self.last_sequence + 1
        ):
            raise _McpProtocolFailure("MCP package progress sequence was not monotonic")
        self.last_sequence = sequence
        source_authority = typed.get("source_authority")
        if source_authority not in {"package_log", "jarvis_stdout_fallback"}:
            raise _McpProtocolFailure("MCP package progress source authority was invalid")
        provider = _validated_progress_provider(typed.get("provider"))
        record = _validated_progress_record(typed.get("record"))
        metadata = cast(dict[str, Any], record["metadata"])
        for key, expected in (
            ("adapter", provider["adapter"]),
            ("package_name", provider["package_name"]),
            ("package_version", provider["package_version"]),
            ("run_id", execution_id),
            ("execution_id", execution_id),
        ):
            if metadata.get(key) != expected:
                raise _McpProtocolFailure(f"MCP package progress metadata {key} did not match")
        current = _finite_progress_number(params.get("progress"))
        if current is None or current != record["current"]:
            raise _McpProtocolFailure("MCP package progress current did not match its record")
        notification_total = params.get("total")
        record_total = record.get("total")
        if notification_total is None:
            if record_total is not None:
                raise _McpProtocolFailure("MCP package progress total did not match its record")
        elif _finite_progress_number(notification_total) != record_total:
            raise _McpProtocolFailure("MCP package progress total did not match its record")
        provider_acceptance = typed.get("provider_acceptance_validated")
        if not isinstance(provider_acceptance, bool):
            raise _McpProtocolFailure("MCP package progress provider acceptance must be boolean")
        binding = {
            "execution_id": execution_id,
            "provider": provider,
        }
        if self.bound_execution_id is None:
            self.bound_execution_id = execution_id
            self.bound_provider = provider
        elif binding != {
            "execution_id": self.bound_execution_id,
            "provider": self.bound_provider,
        }:
            raise _McpProtocolFailure("MCP package progress execution or provider changed")
        typed["execution_id"] = execution_id
        typed["pipeline_id"] = pipeline_id
        typed["provider"] = provider
        typed["record"] = record
        return typed

    def _append_record(
        self,
        envelope: dict[str, Any],
        *,
        execution_validated: bool,
    ) -> None:
        record = dict(cast(dict[str, Any], envelope["record"]))
        metadata = dict(cast(dict[str, Any], record["metadata"]))
        metadata["mcp_progress_bridge"] = {
            "schema_version": MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA,
            "execution_id": envelope["execution_id"],
            "pipeline_id": envelope["pipeline_id"],
            "notification_sequence": envelope["notification_sequence"],
            "source_authority": envelope["source_authority"],
            "provider": envelope["provider"],
            "provider_acceptance_validated": envelope["provider_acceptance_validated"],
            "expected_server_artifact_digest": self.expected_server_artifact_digest,
            "observed_server_artifact_digest": self.observed_server_artifact_digest,
            "execution_validated": execution_validated,
        }
        record["metadata"] = metadata
        self._append_progress_payload(record)

    def _append_progress_payload(self, record: dict[str, Any]) -> None:
        """Sign and append one relay-shaped progress payload."""
        sequence = self.sidecar_sequence + 1
        signed = {
            "schema_version": PROGRESS_SIDECAR_RECORD_SCHEMA,
            "sequence": sequence,
            "progress": record,
        }
        canonical = json.dumps(
            signed,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        sidecar_record = {
            **signed,
            "progress_hmac": hmac.new(
                self.relay_token.encode("utf-8"),
                canonical,
                hashlib.sha256,
            ).hexdigest(),
        }
        payload = (
            json.dumps(
                sidecar_record,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        )
        if len(payload.encode("utf-8")) > MCP_PACKAGE_PROGRESS_MAX_NOTIFICATION_BYTES:
            raise _McpProtocolFailure("bridged MCP package progress exceeded its byte limit")
        _append_progress_sidecar(self.path, payload)
        self.sidecar_sequence = sequence


def _validated_progress_provider(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP package progress provider must be an object")
    typed = {str(key): item for key, item in cast(dict[object, object], value).items()}
    required = {
        "entry_point",
        "entry_point_value",
        "distribution",
        "distribution_version",
        "adapter",
        "package_name",
        "package_version",
    }
    allowed = required | {"application_profile"}
    if not required.issubset(typed) or not set(typed).issubset(allowed):
        raise _McpProtocolFailure("MCP package progress provider identity was incomplete")
    for field_name in required:
        typed[field_name] = _nonempty_bounded_text(typed[field_name], field_name)
    profile = typed.get("application_profile")
    if profile is not None:
        typed["application_profile"] = _nonempty_bounded_text(
            profile,
            "application_profile",
        )
    return typed


def _validated_progress_record(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP package progress record must be an object")
    typed = {str(key): item for key, item in cast(dict[object, object], value).items()}
    allowed = {"label", "current", "total", "unit", "message", "metadata"}
    if not {"label", "current", "metadata"}.issubset(typed) or not set(typed).issubset(allowed):
        raise _McpProtocolFailure("MCP package progress record fields were invalid")
    typed["label"] = _nonempty_bounded_text(typed["label"], "label")
    current = _finite_progress_number(typed["current"])
    if current is None:
        raise _McpProtocolFailure("MCP package progress current must be finite")
    typed["current"] = current
    if typed.get("total") is not None:
        total = _finite_progress_number(typed["total"])
        if total is None:
            raise _McpProtocolFailure("MCP package progress total must be finite")
        typed["total"] = total
    for field_name in ("unit", "message"):
        if typed.get(field_name) is not None:
            typed[field_name] = _nonempty_bounded_text(typed[field_name], field_name)
    metadata = typed.get("metadata")
    if not isinstance(metadata, dict):
        raise _McpProtocolFailure("MCP package progress metadata must be an object")
    typed["metadata"] = {
        str(key): item for key, item in cast(dict[object, object], metadata).items()
    }
    try:
        json.dumps(typed, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise _McpProtocolFailure(f"MCP package progress record was not JSON-safe: {exc}") from exc
    return typed


def _nonempty_bounded_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise _McpProtocolFailure(f"MCP package progress {field_name} was invalid")
    return value


def _finite_progress_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate producer keys before schema validation."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _is_validated_jarvis_execution_query(
    *,
    operation: str,
    tool: str | None,
    expected_server_artifact_digest: str | None,
    expected_registered_contract: str | None,
    expected_jarvis_cd_lock_binding: dict[str, str] | None,
    observed_server_artifact_digest: str | None,
    server_artifact: dict[str, Any] | None,
) -> bool:
    """Return whether this is an artifact-bound, contract-identified JARVIS query."""
    if (
        operation != "tools/call"
        or tool != "jarvis_get_execution"
        or expected_server_artifact_digest is None
        or observed_server_artifact_digest != expected_server_artifact_digest
        or server_artifact is None
        or server_artifact.get("verified") is not True
    ):
        return False
    if (
        expected_registered_contract == REGISTERED_JARVIS_EXECUTION_QUERY_CONTRACT
        and expected_jarvis_cd_lock_binding is None
    ):
        return True
    if expected_jarvis_cd_lock_binding is None or expected_registered_contract is not None:
        return False
    nested_runtime = server_artifact.get("nested_runtime")
    return (
        isinstance(nested_runtime, dict)
        and cast(dict[str, Any], nested_runtime).get("server_name") == "jarvis"
        and cast(dict[str, Any], nested_runtime).get("locked_runtime_verified") is True
    )


def _validated_jarvis_execution_query_result(
    value: dict[str, Any] | None,
    *,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Validate one unified JARVIS execution, progress, and artifact result."""
    allowed_arguments = {
        "pipeline_id",
        "execution_id",
        "include_progress",
        "include_service_runtimes",
        "artifacts",
    }
    if not set(arguments).issubset(allowed_arguments):
        raise _McpProtocolFailure("MCP JARVIS execution query contained unknown arguments")
    pipeline_id = _native_identity(arguments.get("pipeline_id"), "pipeline_id")
    execution_id = _native_identity(arguments.get("execution_id"), "execution_id")
    include_progress = arguments.get("include_progress", True)
    if not isinstance(include_progress, bool):
        raise _McpProtocolFailure("MCP JARVIS include_progress must be boolean")
    include_service_runtimes = arguments.get("include_service_runtimes", False)
    if not isinstance(include_service_runtimes, bool):
        raise _McpProtocolFailure("MCP JARVIS include_service_runtimes must be boolean")
    artifact_query = _validated_jarvis_artifact_query(arguments.get("artifacts"))
    if value is None:
        raise _McpProtocolFailure("MCP JARVIS execution query omitted its structured result")
    expected_fields = {
        "schema_version",
        "pipeline_id",
        "execution_id",
        "execution_handle",
        "execution_record",
        "runtime_metadata",
        "progress",
        "artifact_page",
        "service_runtimes",
    }
    if set(value) != expected_fields or value.get("schema_version") != (
        MCP_JARVIS_EXECUTION_QUERY_SCHEMA
    ):
        raise _McpProtocolFailure("MCP JARVIS execution query envelope was invalid")
    if value.get("pipeline_id") != pipeline_id or value.get("execution_id") != execution_id:
        raise _McpProtocolFailure("MCP JARVIS execution query identity did not match its request")
    handle = _validated_native_execution_handle(value.get("execution_handle"))
    record = _validated_native_execution_record(value.get("execution_record"))
    identity_fields = (
        "execution_id",
        "pipeline_id",
        "mode",
        "scheduler_provider",
        "scheduler_native_id",
        "cluster",
    )
    if any(handle[field] != record[field] for field in identity_fields):
        raise _McpProtocolFailure("MCP native JARVIS handle and record identities did not match")
    if record["pipeline_id"] != pipeline_id or record["execution_id"] != execution_id:
        raise _McpProtocolFailure("MCP JARVIS execution documents did not match the query")
    runtime_metadata = value.get("runtime_metadata")
    if not isinstance(runtime_metadata, dict):
        raise _McpProtocolFailure("MCP JARVIS execution runtime_metadata must be an object")
    _bounded_finite_json(
        cast(dict[str, Any], runtime_metadata),
        "JARVIS execution runtime_metadata",
        4 * 1024 * 1024,
    )

    raw_progress = value.get("progress")
    progress: dict[str, Any] | None = None
    if include_progress:
        progress = _validated_native_progress_snapshot(raw_progress)
        if (
            progress["execution_id"] != execution_id
            or progress["pipeline_id"] != pipeline_id
            or progress["execution_state"] != record["state"]
            or progress["terminal"] is not record["terminal"]
        ):
            raise _McpProtocolFailure("MCP JARVIS query progress lifecycle did not match")
    elif raw_progress is not None:
        raise _McpProtocolFailure("MCP JARVIS query returned progress after it was omitted")

    raw_service_runtimes = value.get("service_runtimes")
    service_runtime_count = 0
    if include_service_runtimes:
        if not isinstance(raw_service_runtimes, dict):
            raise _McpProtocolFailure("MCP JARVIS query omitted requested service runtimes")
        service_document = cast(dict[str, Any], raw_service_runtimes)
        expected_service_fields = {
            "schema_version",
            "execution_id",
            "pipeline_id",
            "execution_state",
            "terminal",
            "service_runtimes",
        }
        raw_services = service_document.get("service_runtimes")
        if not isinstance(raw_services, list):
            raise _McpProtocolFailure("MCP JARVIS query service runtime envelope was invalid")
        typed_services = cast(list[object], raw_services)
        if (
            set(service_document) != expected_service_fields
            or service_document.get("schema_version")
            != MCP_JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA
            or service_document.get("execution_id") != execution_id
            or service_document.get("pipeline_id") != pipeline_id
            or service_document.get("execution_state") != record["state"]
            or service_document.get("terminal") is not record["terminal"]
            or len(typed_services) > 4_096
            or not all(isinstance(item, dict) for item in typed_services)
        ):
            raise _McpProtocolFailure("MCP JARVIS query service runtime envelope was invalid")
        _bounded_finite_json(
            service_document,
            "JARVIS execution service runtimes",
            4 * 1024 * 1024,
        )
        service_runtime_count = len(typed_services)
    elif raw_service_runtimes is not None:
        raise _McpProtocolFailure(
            "MCP JARVIS query returned service runtimes after they were omitted"
        )

    raw_artifact_page = value.get("artifact_page")
    artifact_page: dict[str, Any] | None = None
    if artifact_query is None:
        if raw_artifact_page is not None:
            raise _McpProtocolFailure(
                "MCP JARVIS query returned artifacts without an artifact request"
            )
    else:
        artifact_page = _validated_jarvis_artifact_page(
            raw_artifact_page,
            query=artifact_query,
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            execution_state=cast(str, record["state"]),
            terminal=cast(bool, record["terminal"]),
        )
    return {
        "schema_version": "clio-relay.jarvis-execution-query-validation.v1",
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "include_progress": include_progress,
        "progress_included": progress is not None,
        "include_service_runtimes": include_service_runtimes,
        "service_runtimes_included": raw_service_runtimes is not None,
        "service_runtime_count": service_runtime_count,
        "artifacts_requested": artifact_query is not None,
        "artifact_filters": artifact_query or {},
        "returned_artifact_count": (
            artifact_page["returned_artifact_count"] if artifact_page is not None else 0
        ),
        "next_cursor_present": (
            artifact_page is not None and artifact_page["next_cursor"] is not None
        ),
    }


def _validated_jarvis_artifact_query(value: object) -> dict[str, Any] | None:
    """Validate the bounded artifact selector before trusting its response page."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP JARVIS artifacts query must be an object or null")
    typed = dict(cast(dict[str, Any], value))
    allowed = {"package_id", "role", "state", "artifact_id", "page_size", "cursor"}
    if not set(typed).issubset(allowed):
        raise _McpProtocolFailure("MCP JARVIS artifact query contained unknown filters")
    for field_name, maximum in (("package_id", 256), ("artifact_id", 90)):
        field_value = typed.get(field_name)
        if field_value is not None:
            _jarvis_artifact_text(field_value, field_name, maximum=maximum)
    artifact_id = typed.get("artifact_id")
    if artifact_id is not None and _JARVIS_ARTIFACT_ID.fullmatch(cast(str, artifact_id)) is None:
        raise _McpProtocolFailure("MCP JARVIS artifact_id filter was invalid")
    role = typed.get("role")
    if role is not None and role not in _JARVIS_ARTIFACT_ROLES:
        raise _McpProtocolFailure("MCP JARVIS artifact role filter was invalid")
    state = typed.get("state")
    if state is not None and state not in _JARVIS_ARTIFACT_STATES:
        raise _McpProtocolFailure("MCP JARVIS artifact state filter was invalid")
    page_size = typed.get("page_size", _JARVIS_ARTIFACT_DEFAULT_PAGE_SIZE)
    if (
        isinstance(page_size, bool)
        or not isinstance(page_size, int)
        or not 1 <= page_size <= _JARVIS_ARTIFACT_MAX_PAGE_SIZE
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact page_size was invalid")
    cursor = typed.get("cursor")
    if cursor is not None and (
        not isinstance(cursor, str)
        or not cursor
        or len(cursor) > _JARVIS_ARTIFACT_MAX_CURSOR_LENGTH
        or _JARVIS_ARTIFACT_CURSOR.fullmatch(cursor) is None
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact cursor was invalid")
    return {
        "package_id": typed.get("package_id"),
        "role": role,
        "state": state,
        "artifact_id": artifact_id,
        "page_size": page_size,
        "cursor": cursor,
    }


def _validated_jarvis_artifact_page(
    value: object,
    *,
    query: dict[str, Any],
    pipeline_id: str,
    execution_id: str,
    execution_state: str,
    terminal: bool,
) -> dict[str, Any]:
    """Validate identity, lifecycle, counts, filters, and cursor bounds for one page."""
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP JARVIS artifact_page must be an object")
    typed = dict(cast(dict[str, Any], value))
    expected = {
        "producer_schema_version",
        "pipeline_id",
        "execution_id",
        "execution_state",
        "terminal",
        "artifacts",
        "matching_artifact_count",
        "returned_artifact_count",
        "next_cursor",
    }
    if set(typed) != expected or typed.get("producer_schema_version") != (
        MCP_JARVIS_EXECUTION_ARTIFACTS_SCHEMA
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact page schema was invalid")
    if typed.get("pipeline_id") != pipeline_id or typed.get("execution_id") != execution_id:
        raise _McpProtocolFailure("MCP JARVIS artifact page identity did not match")
    if typed.get("execution_state") != execution_state or typed.get("terminal") is not terminal:
        raise _McpProtocolFailure("MCP JARVIS artifact page lifecycle did not match")
    raw_artifacts = typed.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise _McpProtocolFailure("MCP JARVIS artifact page entries must be an array")
    artifact_items = cast(list[object], raw_artifacts)
    page_size = cast(int, query["page_size"])
    if len(artifact_items) > page_size:
        raise _McpProtocolFailure("MCP JARVIS artifact page exceeded the requested page_size")
    seen_ids: set[str] = set()
    artifacts = [
        _validated_jarvis_artifact_event(
            item,
            execution_id=execution_id,
            query=query,
            seen_ids=seen_ids,
        )
        for item in artifact_items
    ]
    returned = typed.get("returned_artifact_count")
    matching = typed.get("matching_artifact_count")
    if (
        isinstance(returned, bool)
        or not isinstance(returned, int)
        or returned != len(artifacts)
        or isinstance(matching, bool)
        or not isinstance(matching, int)
        or matching < returned
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact page counts did not match")
    if query.get("artifact_id") is not None and (matching > 1 or returned > 1):
        raise _McpProtocolFailure("MCP JARVIS exact artifact filter returned multiple matches")
    next_cursor = typed.get("next_cursor")
    if next_cursor is not None and (
        not artifacts
        or not isinstance(next_cursor, str)
        or not next_cursor
        or len(next_cursor) > _JARVIS_ARTIFACT_MAX_CURSOR_LENGTH
        or _JARVIS_ARTIFACT_CURSOR.fullmatch(next_cursor) is None
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact next_cursor was invalid")
    if query.get("artifact_id") is not None and next_cursor is not None:
        raise _McpProtocolFailure("MCP JARVIS exact artifact filter unexpectedly paginated")
    typed["artifacts"] = artifacts
    return typed


def _validated_jarvis_artifact_event(
    value: object,
    *,
    execution_id: str,
    query: dict[str, Any],
    seen_ids: set[str],
) -> dict[str, Any]:
    """Validate one generated artifact and require it to satisfy the request filters."""
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP JARVIS artifact entry must be an object")
    typed = dict(cast(dict[str, Any], value))
    if (
        not _JARVIS_ARTIFACT_REQUIRED_FIELDS.issubset(typed)
        or not set(typed).issubset(
            _JARVIS_ARTIFACT_REQUIRED_FIELDS | _JARVIS_ARTIFACT_OPTIONAL_FIELDS
        )
        or typed.get("schema_version") != MCP_JARVIS_ARTIFACT_SCHEMA
        or typed.get("execution_id") != execution_id
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact entry schema or identity was invalid")
    for field_name in ("package_name", "package_id", "logical_name", "kind"):
        _jarvis_artifact_text(typed.get(field_name), field_name, maximum=256)
    artifact_id = typed.get("artifact_id")
    if (
        not isinstance(artifact_id, str)
        or _JARVIS_ARTIFACT_ID.fullmatch(artifact_id) is None
        or artifact_id in seen_ids
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact identity was invalid")
    seen_ids.add(artifact_id)
    allowed_fields = {
        "role": _JARVIS_ARTIFACT_ROLES,
        "state": _JARVIS_ARTIFACT_STATES,
        "structure": _JARVIS_ARTIFACT_STRUCTURES,
        "ownership": _JARVIS_ARTIFACT_OWNERSHIP,
    }
    for field_name, allowed in allowed_fields.items():
        if typed.get(field_name) not in allowed:
            raise _McpProtocolFailure(f"MCP JARVIS artifact {field_name} was invalid")
    for field_name in ("revision", "sequence"):
        item = typed.get(field_name)
        if isinstance(item, bool) or not isinstance(item, int) or item < 1:
            raise _McpProtocolFailure(f"MCP JARVIS artifact {field_name} was invalid")
    observed = _finite_progress_number(typed.get("observed_at_epoch"))
    if observed is None or observed < 0:
        raise _McpProtocolFailure("MCP JARVIS artifact observation time was invalid")
    metadata_value = typed.get("metadata")
    if not isinstance(metadata_value, dict):
        raise _McpProtocolFailure("MCP JARVIS artifact metadata was invalid")
    _bounded_finite_json(
        cast(dict[str, Any], metadata_value),
        "JARVIS artifact metadata",
        _JARVIS_ARTIFACT_MAX_METADATA_BYTES,
    )
    _validate_jarvis_artifact_location(typed)
    _validate_jarvis_artifact_optional_fields(typed)
    _bounded_finite_json(typed, "JARVIS artifact entry", _JARVIS_ARTIFACT_MAX_EVENT_BYTES)
    for field_name in ("package_id", "role", "state", "artifact_id"):
        expected = query.get(field_name)
        if expected is not None and typed.get(field_name) != expected:
            raise _McpProtocolFailure(
                f"MCP JARVIS artifact did not satisfy the {field_name} filter"
            )
    return typed


def _validate_jarvis_artifact_location(value: dict[str, Any]) -> None:
    """Validate transport-neutral location and ownership semantics."""
    location = value.get("location")
    if location is None:
        if value["state"] in {"available", "finalized"}:
            raise _McpProtocolFailure("MCP JARVIS available artifact omitted its location")
        return
    if not isinstance(location, dict) or set(cast(dict[object, object], location)) != {
        "kind",
        "value",
    }:
        raise _McpProtocolFailure("MCP JARVIS artifact location was invalid")
    typed_location = cast(dict[str, Any], location)
    kind = typed_location.get("kind")
    if kind not in _JARVIS_ARTIFACT_LOCATION_KINDS:
        raise _McpProtocolFailure("MCP JARVIS artifact location kind was invalid")
    rendered = _jarvis_artifact_text(
        typed_location.get("value"),
        "location",
        maximum=4096,
    )
    if kind == "execution_path":
        path = PurePosixPath(rendered)
        if (
            "\\" in rendered
            or path.is_absolute()
            or rendered.startswith("/")
            or rendered.endswith("/")
            or "//" in rendered
            or any(part in {"", ".", ".."} for part in path.parts)
            or (bool(path.parts) and ":" in path.parts[0])
            or path.as_posix() != rendered
        ):
            raise _McpProtocolFailure("MCP JARVIS execution artifact path was invalid")
    elif kind == "cluster_path":
        path = PurePosixPath(rendered)
        if (
            "\\" in rendered
            or not path.is_absolute()
            or not rendered.startswith("/")
            or rendered == "/"
            or rendered.endswith("/")
            or "//" in rendered
            or any(part in {"", ".", ".."} for part in path.parts[1:])
            or path.as_posix() != rendered
        ):
            raise _McpProtocolFailure("MCP JARVIS cluster artifact path was invalid")
    else:
        try:
            parsed = urlsplit(rendered)
            has_user_info = parsed.username is not None or parsed.password is not None
        except ValueError as exc:
            raise _McpProtocolFailure("MCP JARVIS external artifact URI was invalid") from exc
        scheme = parsed.scheme.lower()
        if (
            not scheme
            or _JARVIS_ARTIFACT_URI_SCHEME.fullmatch(scheme) is None
            or len(scheme) == 1
            or scheme in _JARVIS_ARTIFACT_UNSAFE_URI_SCHEMES
            or has_user_info
            or (scheme in {"gs", "http", "https", "s3"} and not parsed.netloc)
        ):
            raise _McpProtocolFailure("MCP JARVIS external artifact URI was invalid")
    if (kind == "execution_path") is not (value["ownership"] == "execution"):
        raise _McpProtocolFailure("MCP JARVIS artifact location ownership was invalid")


def _validate_jarvis_artifact_optional_fields(value: dict[str, Any]) -> None:
    """Validate optional generated-artifact metadata fields."""
    for field_name, maximum in (("format", 256), ("message", 4096)):
        if field_name in value:
            _jarvis_artifact_text(value[field_name], field_name, maximum=maximum)
    media_type = value.get("media_type")
    if media_type is not None and (
        not isinstance(media_type, str) or _JARVIS_ARTIFACT_MEDIA_TYPE.fullmatch(media_type) is None
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact media_type was invalid")
    if "size_bytes" in value:
        size = value["size_bytes"]
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise _McpProtocolFailure("MCP JARVIS artifact size_bytes was invalid")
    checksum = value.get("checksum")
    if checksum is not None and (
        not isinstance(checksum, str) or _JARVIS_ARTIFACT_CHECKSUM.fullmatch(checksum) is None
    ):
        raise _McpProtocolFailure("MCP JARVIS artifact checksum was invalid")


def _jarvis_artifact_text(value: object, field_name: str, *, maximum: int) -> str:
    """Return one bounded nonblank artifact field without control characters."""
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise _McpProtocolFailure(f"MCP JARVIS artifact {field_name} was invalid")
    return value


def _validated_native_execution_documents(
    value: dict[str, Any],
) -> dict[str, dict[str, Any]] | None:
    """Validate an exact native JARVIS handle/record/progress result envelope."""
    keys = {"execution_handle", "execution_record", "progress"}
    present = keys & set(value)
    if not present:
        return None
    if present != keys:
        raise _McpProtocolFailure("MCP native JARVIS result omitted execution documents")
    handle = _validated_native_execution_handle(value["execution_handle"])
    record = _validated_native_execution_record(value["execution_record"])
    progress = _validated_native_progress_snapshot(value["progress"])
    identity_fields = (
        "execution_id",
        "pipeline_id",
        "mode",
        "scheduler_provider",
        "scheduler_native_id",
        "cluster",
    )
    if any(handle[field] != record[field] for field in identity_fields):
        raise _McpProtocolFailure("MCP native JARVIS handle and record identities did not match")
    if (
        progress["execution_id"] != record["execution_id"]
        or progress["pipeline_id"] != record["pipeline_id"]
        or progress["execution_state"] != record["state"]
        or progress["terminal"] is not record["terminal"]
    ):
        raise _McpProtocolFailure("MCP native JARVIS record and progress did not match")
    return {
        "execution_handle": handle,
        "execution_record": record,
        "progress": progress,
    }


def _validated_native_execution_handle(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP native JARVIS execution_handle must be an object")
    typed = dict(cast(dict[str, Any], value))
    expected = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "mode",
        "scheduler_provider",
        "scheduler_native_id",
        "cluster",
    }
    if set(typed) != expected or typed.get("schema_version") != MCP_JARVIS_EXECUTION_HANDLE_SCHEMA:
        raise _McpProtocolFailure("MCP native JARVIS execution_handle schema was invalid")
    _native_identity(typed.get("execution_id"), "execution_id")
    _native_identity(typed.get("pipeline_id"), "pipeline_id")
    mode = typed.get("mode")
    if mode not in {"direct", "scheduler"}:
        raise _McpProtocolFailure("MCP native JARVIS execution mode was invalid")
    for field_name in ("scheduler_provider", "scheduler_native_id", "cluster"):
        field_value = typed.get(field_name)
        if field_value is not None:
            _native_text(field_value, field_name)
    if mode == "direct" and any(
        typed.get(field_name) is not None
        for field_name in ("scheduler_provider", "scheduler_native_id", "cluster")
    ):
        raise _McpProtocolFailure("MCP native direct execution claimed scheduler identity")
    if mode == "scheduler" and typed.get("scheduler_provider") is None:
        raise _McpProtocolFailure("MCP native scheduler execution omitted its provider")
    if typed.get("scheduler_provider") == "slurm":
        native_id = typed.get("scheduler_native_id")
        cluster = typed.get("cluster")
        if native_id is not None and (
            len(cast(str, native_id)) > 64
            or not cast(str, native_id).isascii()
            or not cast(str, native_id).isdigit()
        ):
            raise _McpProtocolFailure("MCP native SLURM identity was invalid")
        if cluster is not None and (
            len(cast(str, cluster)) > 255
            or any(
                not (character.isascii() and (character.isalnum() or character in "._-"))
                for character in cast(str, cluster)
            )
        ):
            raise _McpProtocolFailure("MCP native SLURM cluster was invalid")
    return typed


def _validated_native_execution_record(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP native JARVIS execution_record must be an object")
    typed = dict(cast(dict[str, Any], value))
    expected = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "pipeline_name",
        "mode",
        "scheduler_provider",
        "scheduler_native_id",
        "cluster",
        "state",
        "submitted",
        "terminal",
        "created_at",
        "updated_at",
        "return_code",
        "error",
        "metadata",
    }
    if set(typed) != expected or typed.get("schema_version") != MCP_JARVIS_EXECUTION_RECORD_SCHEMA:
        raise _McpProtocolFailure("MCP native JARVIS execution_record schema was invalid")
    handle_projection = {
        "schema_version": MCP_JARVIS_EXECUTION_HANDLE_SCHEMA,
        **{
            key: typed[key]
            for key in (
                "execution_id",
                "pipeline_id",
                "mode",
                "scheduler_provider",
                "scheduler_native_id",
                "cluster",
            )
        },
    }
    _validated_native_execution_handle(handle_projection)
    if typed.get("pipeline_name") != typed.get("pipeline_id"):
        raise _McpProtocolFailure("MCP native JARVIS pipeline identity did not match")
    state = typed.get("state")
    if state not in _JARVIS_EXECUTION_STATES:
        raise _McpProtocolFailure("MCP native JARVIS execution state was invalid")
    submitted = typed.get("submitted")
    terminal = typed.get("terminal")
    if not isinstance(submitted, bool) or not isinstance(terminal, bool):
        raise _McpProtocolFailure("MCP native JARVIS lifecycle flags must be boolean")
    if terminal and state not in _JARVIS_TERMINAL_STATES:
        raise _McpProtocolFailure("MCP native terminal execution state was invalid")
    if state in {"completed", "failed", "canceled"} and terminal is not True:
        raise _McpProtocolFailure("MCP native terminal state omitted terminal=true")
    return_code = typed.get("return_code")
    if return_code is not None and (
        isinstance(return_code, bool) or not isinstance(return_code, int)
    ):
        raise _McpProtocolFailure("MCP native JARVIS return_code was invalid")
    if state == "completed" and return_code != 0:
        raise _McpProtocolFailure("MCP native completed execution requires return_code=0")
    if state == "failed" and (return_code is None or return_code == 0):
        raise _McpProtocolFailure("MCP native failed execution requires a nonzero return_code")
    _native_timestamp(typed.get("created_at"), "created_at")
    _native_timestamp(typed.get("updated_at"), "updated_at")
    error = typed.get("error")
    if error is not None:
        _native_text(error, "error", maximum=16_384, allow_newlines=True)
    metadata_value = typed.get("metadata")
    if not isinstance(metadata_value, dict):
        raise _McpProtocolFailure("MCP native JARVIS execution metadata must be an object")
    metadata_document = cast(dict[str, Any], metadata_value)
    _bounded_finite_json(metadata_document, "native JARVIS execution metadata", 48_000)
    native_id = typed.get("scheduler_native_id")
    raw_submission = metadata_document.get("submission")
    if raw_submission is None:
        if native_id is not None or submitted is True:
            raise _McpProtocolFailure("MCP native scheduler identity omitted submission proof")
        return typed
    if not isinstance(raw_submission, dict):
        raise _McpProtocolFailure("MCP native scheduler submission proof must be an object")
    if typed["mode"] != "scheduler":
        raise _McpProtocolFailure("MCP native direct execution carried scheduler submission proof")
    submission_document = cast(dict[str, Any], raw_submission)
    submission_submitted = submission_document.get("submitted")
    if (
        submission_document.get("schema_version") != "jarvis.scheduler.submission.v1"
        or submission_document.get("execution_id") != typed.get("execution_id")
        or submission_document.get("provider") != typed.get("scheduler_provider")
        or submission_document.get("scheduler_job_id") != native_id
        or submission_document.get("scheduler_cluster") != typed.get("cluster")
        or not isinstance(submission_submitted, bool)
        or submission_submitted is not submitted
    ):
        raise _McpProtocolFailure("MCP native scheduler submission proof did not match")
    identity_source = submission_document.get("identity_source")
    if native_id is not None and (
        identity_source != "scheduler_submit_api" or submission_submitted is not True
    ):
        raise _McpProtocolFailure("MCP native scheduler submission identity was not authoritative")
    if native_id is None and identity_source is not None:
        raise _McpProtocolFailure("MCP native scheduler submission source claimed no identity")
    for field_name in (
        "script_path",
        "hostfile_path",
        "pipeline_snapshot_path",
        "pipeline_input_path",
        "execution_root_path",
        "output_path",
        "error_path",
    ):
        field_value = submission_document.get(field_name)
        if field_value is not None:
            _native_text(field_value, field_name, maximum=16_384)
    return typed


def _validated_native_progress_snapshot(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP native JARVIS progress must be an object")
    typed = dict(cast(dict[str, Any], value))
    expected = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "execution_state",
        "terminal",
        "packages",
    }
    if (
        set(typed) != expected
        or typed.get("schema_version") != MCP_JARVIS_EXECUTION_PROGRESS_SCHEMA
    ):
        raise _McpProtocolFailure("MCP native JARVIS progress snapshot schema was invalid")
    execution_id = _native_identity(typed.get("execution_id"), "execution_id")
    _native_identity(typed.get("pipeline_id"), "pipeline_id")
    if typed.get("execution_state") not in _JARVIS_EXECUTION_STATES:
        raise _McpProtocolFailure("MCP native JARVIS progress state was invalid")
    terminal = typed.get("terminal")
    if not isinstance(terminal, bool):
        raise _McpProtocolFailure("MCP native JARVIS progress terminal flag was invalid")
    if terminal and typed["execution_state"] not in _JARVIS_TERMINAL_STATES:
        raise _McpProtocolFailure("MCP native JARVIS terminal progress state was invalid")
    if typed["execution_state"] in {"completed", "failed", "canceled"} and not terminal:
        raise _McpProtocolFailure("MCP native JARVIS terminal progress omitted terminal=true")
    raw_packages = typed.get("packages")
    if not isinstance(raw_packages, list):
        raise _McpProtocolFailure("MCP native JARVIS progress packages must be an array")
    packages: list[dict[str, Any]] = []
    package_ids: set[str] = set()
    for raw_package in cast(list[object], raw_packages):
        if not isinstance(raw_package, dict):
            raise _McpProtocolFailure("MCP native JARVIS package progress must be an object")
        package = dict(cast(dict[str, Any], raw_package))
        if set(package) != {"package_id", "package_name", "event_count", "latest"}:
            raise _McpProtocolFailure("MCP native JARVIS package progress fields were invalid")
        package_id = _native_text(package.get("package_id"), "package_id", maximum=256)
        package_name = _native_text(package.get("package_name"), "package_name", maximum=256)
        event_count = package.get("event_count")
        if isinstance(event_count, bool) or not isinstance(event_count, int) or event_count < 0:
            raise _McpProtocolFailure("MCP native JARVIS event_count was invalid")
        if package_id in package_ids:
            raise _McpProtocolFailure("MCP native JARVIS progress repeated a package_id")
        package_ids.add(package_id)
        latest_value = package.get("latest")
        latest = None if latest_value is None else _validated_native_progress_event(latest_value)
        if (event_count == 0) is not (latest is None):
            raise _McpProtocolFailure("MCP native JARVIS event_count did not match latest")
        if latest is not None and (
            latest["package_id"] != package_id
            or latest["package_name"] != package_name
            or latest["execution_id"] != execution_id
        ):
            raise _McpProtocolFailure("MCP native JARVIS package event identity did not match")
        package["latest"] = latest
        packages.append(package)
    typed["packages"] = packages
    return typed


def _validated_native_progress_event(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP native JARVIS progress event must be an object")
    typed = dict(cast(dict[str, Any], value))
    required = {
        "schema_version",
        "package_name",
        "package_id",
        "execution_id",
        "label",
        "state",
        "sequence",
        "observed_at_epoch",
        "determinate",
        "metadata",
    }
    optional = {"current", "total", "unit", "message"}
    if (
        not required.issubset(typed)
        or not set(typed).issubset(required | optional)
        or typed.get("schema_version") != MCP_JARVIS_PROGRESS_EVENT_SCHEMA
    ):
        raise _McpProtocolFailure("MCP native JARVIS progress event schema was invalid")
    for field_name in ("package_name", "package_id", "execution_id", "label"):
        _native_text(typed.get(field_name), field_name, maximum=256)
    if typed.get("state") not in _JARVIS_PROGRESS_STATES:
        raise _McpProtocolFailure("MCP native JARVIS progress event state was invalid")
    sequence = typed.get("sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        raise _McpProtocolFailure("MCP native JARVIS progress event sequence was invalid")
    observed = _finite_progress_number(typed.get("observed_at_epoch"))
    if observed is None or observed < 0:
        raise _McpProtocolFailure("MCP native JARVIS progress timestamp was invalid")
    raw_current = typed.get("current")
    raw_total = typed.get("total")
    current = None if raw_current is None else _finite_progress_number(raw_current)
    total = None if raw_total is None else _finite_progress_number(raw_total)
    if raw_current is not None and (current is None or current < 0):
        raise _McpProtocolFailure("MCP native JARVIS progress current was invalid")
    if raw_total is not None and (
        total is None or total <= 0 or current is None or current > total
    ):
        raise _McpProtocolFailure("MCP native JARVIS progress total was invalid")
    if typed.get("determinate") is not (current is not None and total is not None):
        raise _McpProtocolFailure("MCP native JARVIS determinate flag was invalid")
    if typed.get("unit") is not None:
        _native_text(typed.get("unit"), "unit", maximum=256)
    if typed.get("message") is not None:
        _native_text(typed.get("message"), "message")
    metadata_value = typed.get("metadata")
    if not isinstance(metadata_value, dict):
        raise _McpProtocolFailure("MCP native JARVIS progress metadata must be an object")
    _bounded_finite_json(
        cast(dict[str, Any], metadata_value),
        "native JARVIS progress metadata",
        48_000,
    )
    return typed


def _native_identity(value: object, field_name: str) -> str:
    rendered = _native_text(value, field_name, maximum=128)
    reserved_stem = rendered.split(".", 1)[0].upper()
    if (
        not rendered[0].isalnum()
        or rendered.endswith(".")
        or reserved_stem in _WINDOWS_RESERVED_COMPONENTS
        or any(
            not (character.isascii() and (character.isalnum() or character in "._-"))
            for character in rendered
        )
    ):
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} was not portable")
    return rendered


def _native_text(
    value: object,
    field_name: str,
    *,
    maximum: int = 4096,
    allow_newlines: bool = False,
) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum:
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} was invalid")
    allowed_controls: set[str] = {"\n", "\r", "\t"} if allow_newlines else set()
    if any(
        (ord(character) < 32 and character not in allowed_controls) or ord(character) == 127
        for character in value
    ):
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} contained controls")
    return value


def _native_timestamp(value: object, field_name: str) -> str:
    rendered = _native_text(value, field_name, maximum=64)
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(rendered.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} was invalid") from exc
    if parsed.tzinfo is None:
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} omitted timezone")
    return rendered


def _bounded_finite_json(value: object, label: str, maximum: int) -> None:
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise _McpProtocolFailure(f"MCP {label} was not finite JSON") from exc
    if len(payload) > maximum:
        raise _McpProtocolFailure(f"MCP {label} exceeded its byte limit")


def _append_progress_sidecar(path: Path, payload: str) -> None:
    encoded = payload.encode("utf-8")
    flags = (
        os.O_WRONLY
        | os.O_APPEND
        | int(getattr(os, "O_BINARY", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        os.set_inheritable(descriptor, False)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise _McpProtocolFailure("relay progress sidecar is not a regular file")
        if opened.st_nlink != 1:
            raise _McpProtocolFailure("relay progress sidecar hardlink count changed")
        if os.name != "nt" and (
            opened.st_uid != os.getuid() or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise _McpProtocolFailure("relay progress sidecar ownership or mode changed")
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise _McpProtocolFailure("relay progress sidecar append made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


_StreamEvent = str | _StreamLimit | None
_SignalHandler = Callable[[int, Any], None] | int | None


def run_mcp_call_from_params(params: dict[str, Any]) -> int:
    """Run one MCP tools/call or tools/list request and write mcp-result.json."""
    server = _required_str(params, "server")
    server_args = _str_list(params.get("server_args", []), key="server_args")
    env_from = _environment_references(params.get("env_from", {}))
    expected_server_artifact_digest = _optional_sha256(
        params.get("expected_server_artifact_digest"),
        key="expected_server_artifact_digest",
    )
    expected_registered_contract = _optional_str(params.get("expected_registered_contract"))
    expected_jarvis_cd_lock_binding = _jarvis_cd_lock_expectation(
        params.get("expected_jarvis_cd_lock_binding")
    )
    operation = _operation(params.get("operation", "tools/call"))
    tool = _optional_str(params.get("tool"))
    arguments = _object(params.get("arguments", {}))
    jarvis_input_manifest = _jarvis_input_manifest(
        params.get("jarvis_input_manifest"),
        operation=operation,
        tool=tool,
        arguments=arguments,
        expected_registered_contract=expected_registered_contract,
        expected_jarvis_cd_lock_binding=expected_jarvis_cd_lock_binding,
    )
    if operation == "tools/call" and tool is None:
        raise ValueError("tool is required for tools/call")
    if operation == "tools/list" and (tool is not None or arguments):
        raise ValueError("tools/list does not accept tool or arguments")
    timeout = _optional_int(params.get("timeout_seconds"))
    if timeout is None:
        timeout = MCP_CALL_DEFAULT_TIMEOUT_SECONDS
    started_at = time.time()
    result_path = Path.cwd() / "mcp-result.json"
    progress_bridge: _McpProgressBridge | None = None
    server_artifact: dict[str, Any] | None = None
    observed_server_artifact_digest: str | None = None
    execution_artifact: dict[str, Any] | None = None
    result_validation: dict[str, Any] | None = None
    try:
        server_artifact = (
            _server_artifact_identity(
                server,
                server_args,
                verify_relay_jarvis_cd_lock=True,
            )
            if expected_jarvis_cd_lock_binding is not None
            else _server_artifact_identity(server, server_args)
        )
        _reject_verified_runtime_environment_remap(
            server_artifact=server_artifact,
            env_from=env_from,
        )
        command = [
            _server_artifact_launch_executable(server_artifact),
            *server_args,
        ]
        observed_server_artifact_digest = _server_artifact_digest(server_artifact)
        if expected_jarvis_cd_lock_binding is not None:
            _require_locked_jarvis_cd_binding(
                server_artifact,
                expected=expected_jarvis_cd_lock_binding,
            )
        if expected_server_artifact_digest is not None:
            if server_artifact.get("verified") is not True:
                raise ValueError("MCP server artifact is not verified before launch")
            if observed_server_artifact_digest != expected_server_artifact_digest:
                raise ValueError(
                    "MCP server artifact changed after discovery; refusing tools/call launch"
                )
        progress_bridge = _package_progress_bridge_from_invocation(
            operation=operation,
            tool=tool,
            arguments=arguments,
            expected_server_artifact_digest=expected_server_artifact_digest,
            expected_registered_contract=expected_registered_contract,
            expected_jarvis_cd_lock_binding=expected_jarvis_cd_lock_binding,
            observed_server_artifact_digest=observed_server_artifact_digest,
            server_artifact=server_artifact,
        )
        with _prepared_mcp_launch(
            command,
            server_args=server_args,
            server_artifact=server_artifact,
        ) as prepared:
            launch_command, execution_artifact = prepared
            if (
                operation == "tools/call"
                and progress_bridge is None
                and jarvis_input_manifest is None
            ):
                process = _run_mcp_session(
                    launch_command,
                    tool=tool,
                    arguments=arguments,
                    timeout=timeout,
                    env_from=env_from,
                )
            elif operation == "tools/call" and jarvis_input_manifest is None:
                process = _run_mcp_session(
                    launch_command,
                    tool=tool,
                    arguments=arguments,
                    timeout=timeout,
                    env_from=env_from,
                    progress_bridge=progress_bridge,
                )
            elif operation == "tools/call":
                process = _run_mcp_session(
                    launch_command,
                    tool=tool,
                    arguments=arguments,
                    timeout=timeout,
                    env_from=env_from,
                    progress_bridge=progress_bridge,
                    jarvis_input_manifest=jarvis_input_manifest,
                )
            else:
                process = _run_mcp_session(
                    launch_command,
                    tool=None,
                    arguments={},
                    timeout=timeout,
                    operation=operation,
                    env_from=env_from,
                )
        returncode = process.returncode
        timed_out = False
        protocol_error = _protocol_error(process.stdout, operation=operation)
        if protocol_error is not None:
            returncode = 1
        else:
            protocol_result = _response_result(
                str(process.stdout or ""),
                response_id=_response_id(operation),
            )
            structured_result = _structured_result(protocol_result, operation=operation)
            try:
                if _is_validated_jarvis_execution_query(
                    operation=operation,
                    tool=tool,
                    expected_server_artifact_digest=expected_server_artifact_digest,
                    expected_registered_contract=expected_registered_contract,
                    expected_jarvis_cd_lock_binding=expected_jarvis_cd_lock_binding,
                    observed_server_artifact_digest=observed_server_artifact_digest,
                    server_artifact=server_artifact,
                ):
                    result_validation = _validated_jarvis_execution_query_result(
                        structured_result,
                        arguments=arguments,
                    )
                if progress_bridge is not None:
                    progress_bridge.finalize(structured_result)
            except _McpProtocolFailure as exc:
                returncode = 1
                protocol_error = str(exc)
    except subprocess.TimeoutExpired as exc:
        process = subprocess.CompletedProcess(
            args=[_resolve_executable(server), *server_args],
            returncode=124,
            stdout=_text_output(exc.stdout),
            stderr=_text_output(exc.stderr),
        )
        returncode = 124
        timed_out = True
        protocol_error = None
    except (OSError, ValueError) as exc:
        process = subprocess.CompletedProcess(
            args=[_resolve_executable(server), *server_args],
            returncode=1,
            stdout="",
            stderr=str(exc),
        )
        returncode = 1
        timed_out = False
        protocol_error = f"MCP server launch failed: {exc}"
    _write_mcp_result(
        result_path=result_path,
        server=server,
        server_args=server_args,
        env_from=env_from,
        expected_server_artifact_digest=expected_server_artifact_digest,
        expected_registered_contract=expected_registered_contract,
        expected_jarvis_cd_lock_binding=expected_jarvis_cd_lock_binding,
        server_artifact=server_artifact,
        observed_server_artifact_digest=observed_server_artifact_digest,
        execution_artifact=execution_artifact,
        operation=operation,
        tool=tool,
        arguments=arguments,
        jarvis_input_manifest=jarvis_input_manifest,
        returncode=returncode,
        stdout=str(process.stdout or ""),
        stderr=str(process.stderr or ""),
        started_at=started_at,
        timed_out=timed_out,
        protocol_error=protocol_error,
        progress_bridge=(
            progress_bridge.result_metadata() if progress_bridge is not None else None
        ),
        result_validation=result_validation,
    )
    return returncode


def _package_progress_bridge_from_invocation(
    *,
    operation: str,
    tool: str | None,
    arguments: dict[str, Any],
    expected_server_artifact_digest: str | None,
    expected_registered_contract: str | None,
    expected_jarvis_cd_lock_binding: dict[str, str] | None,
    observed_server_artifact_digest: str,
    server_artifact: dict[str, Any],
) -> _McpProgressBridge | None:
    """Create a private bridge only for a recognized artifact-bound JARVIS call."""
    progress_path = os.environ.get("CLIO_RELAY_PROGRESS_FILE")
    relay_token = os.environ.get("CLIO_RELAY_PROGRESS_TOKEN")
    if progress_path is None and relay_token is None:
        return None
    if progress_path is None or relay_token is None or not relay_token:
        raise ValueError("relay progress sidecar path and token must be configured together")
    if operation != "tools/call" or tool != "jarvis_run":
        return None
    registered_route = (
        expected_registered_contract == REGISTERED_JARVIS_EXECUTION_QUERY_CONTRACT
        and expected_jarvis_cd_lock_binding is None
    )
    built_in_route = (
        expected_registered_contract is None and expected_jarvis_cd_lock_binding is not None
    )
    if (
        expected_server_artifact_digest is None
        or not (registered_route or built_in_route)
        or observed_server_artifact_digest != expected_server_artifact_digest
        or server_artifact.get("verified") is not True
    ):
        return None
    pipeline_id = arguments.get("pipeline_id")
    if not isinstance(pipeline_id, str) or not pipeline_id:
        raise ValueError("artifact-bound jarvis_run progress requires pipeline_id")
    return _McpProgressBridge(
        path=Path(progress_path).expanduser(),
        relay_token=relay_token,
        expected_server_artifact_digest=expected_server_artifact_digest,
        observed_server_artifact_digest=observed_server_artifact_digest,
        expected_pipeline_id=pipeline_id,
    )


def _initialize_message() -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": "clio-relay-mcp-init",
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "clio-relay", "version": _package_version()},
        },
    }


def _initialized_message() -> dict[str, Any]:
    return {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}


def _call_message(
    *,
    tool: str,
    arguments: dict[str, Any],
    progress_token: str | None = None,
    response_id: str = "clio-relay-mcp-call",
) -> dict[str, Any]:
    params: dict[str, Any] = {"name": tool, "arguments": arguments}
    if progress_token is not None:
        params["_meta"] = {"progressToken": progress_token}
    return {
        "jsonrpc": "2.0",
        "id": response_id,
        "method": "tools/call",
        "params": params,
    }


def _tools_list_message(
    *, cursor: str | None = None, response_id: str = "clio-relay-mcp-tools-list"
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if cursor is not None:
        params["cursor"] = cursor
    return {
        "jsonrpc": "2.0",
        "id": response_id,
        "method": "tools/list",
        "params": params,
    }


def _package_version() -> str:
    try:
        return metadata.version("clio-relay")
    except metadata.PackageNotFoundError:
        return "0+unknown"


def _decoded_json_object(value: str) -> dict[str, Any] | None:
    """Decode a JSON object without leaking decoder ``Unknown`` types."""
    try:
        decoded: object = json.loads(value)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, dict):
        return None
    return cast(dict[str, Any], decoded)


def _text_output(value: str | bytes | None) -> str:
    """Normalize subprocess timeout output from text or byte mode."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _protocol_error(stdout: str, *, operation: str = "tools/call") -> str | None:
    response_id = _response_id(operation)
    response_seen = False
    for line in stdout.splitlines():
        if not line.strip():
            continue
        message = _decoded_json_object(line)
        if message is None:
            continue
        message_id = message.get("id")
        matching_id = message_id == response_id or (
            operation == "tools/list"
            and isinstance(message_id, str)
            and message_id.startswith(f"{response_id}-page-")
        )
        if not matching_id:
            continue
        response_seen = True
        error = message.get("error")
        if error is not None:
            return json.dumps(error, sort_keys=True)
        result = message.get("result")
        if operation == "tools/call" and isinstance(result, dict):
            typed_result = cast(dict[str, Any], result)
            if typed_result.get("isError") is True:
                return "tools/call returned isError=true"
    if not response_seen:
        return f"missing {operation} response"
    return None


def _response_id(operation: str) -> str:
    if operation == "tools/call":
        return "clio-relay-mcp-call"
    if operation == "tools/list":
        return "clio-relay-mcp-tools-list"
    raise ValueError(f"unsupported MCP operation: {operation}")


def _response_result(stdout: str, *, response_id: str) -> dict[str, Any] | None:
    matched: dict[str, Any] | None = None
    for line in stdout.splitlines():
        if not line.strip():
            continue
        message = _decoded_json_object(line)
        if message is None or message.get("id") != response_id:
            continue
        result = message.get("result")
        matched = cast(dict[str, Any], result) if isinstance(result, dict) else None
    return matched


def _structured_result(
    protocol_result: dict[str, Any] | None,
    *,
    operation: str,
) -> dict[str, Any] | None:
    if operation != "tools/call" or protocol_result is None:
        return None
    structured = protocol_result.get("structuredContent")
    if isinstance(structured, dict):
        return cast(dict[str, Any], structured)
    content = protocol_result.get("content")
    if not isinstance(content, list):
        return None
    for raw_item in cast(list[object], content):
        if not isinstance(raw_item, dict):
            continue
        item = cast(dict[str, Any], raw_item)
        if item.get("type") != "text":
            continue
        text = item.get("text")
        if not isinstance(text, str):
            continue
        decoded = _decoded_json_object(text)
        if decoded is not None:
            return decoded
    return None


def _write_mcp_result(
    *,
    result_path: Path,
    server: str,
    server_args: list[str],
    env_from: dict[str, str],
    expected_server_artifact_digest: str | None,
    expected_registered_contract: str | None,
    expected_jarvis_cd_lock_binding: dict[str, str] | None,
    server_artifact: dict[str, Any] | None,
    observed_server_artifact_digest: str | None,
    execution_artifact: dict[str, Any] | None,
    operation: str,
    tool: str | None,
    arguments: dict[str, Any],
    jarvis_input_manifest: dict[str, Any] | None,
    returncode: int,
    stdout: str,
    stderr: str,
    started_at: float,
    timed_out: bool,
    protocol_error: str | None,
    progress_bridge: dict[str, Any] | None,
    result_validation: dict[str, Any] | None,
) -> None:
    finished_at = time.time()
    protocol_result = _response_result(stdout, response_id=_response_id(operation))
    pagination: dict[str, Any] | None = None
    if protocol_result is not None and isinstance(
        protocol_result.get(_TOOLS_LIST_PAGINATION_KEY), dict
    ):
        protocol_result = dict(protocol_result)
        pagination = protocol_result.pop(_TOOLS_LIST_PAGINATION_KEY)
    initialize_result = _response_result(stdout, response_id="clio-relay-mcp-init")
    protocol_version = (
        initialize_result.get("protocolVersion") if initialize_result is not None else None
    )
    server_info: object = (
        initialize_result.get("serverInfo", {}) if initialize_result is not None else {}
    )
    if server_artifact is None:
        server_artifact = (
            _server_artifact_identity(
                server,
                server_args,
                verify_relay_jarvis_cd_lock=True,
            )
            if expected_jarvis_cd_lock_binding is not None
            else _server_artifact_identity(server, server_args)
        )
    if observed_server_artifact_digest is None:
        observed_server_artifact_digest = _server_artifact_digest(server_artifact)
    result_document: dict[str, Any] = {
        "server": server,
        "server_args": server_args,
        "env_from": env_from,
        "operation": operation,
        "tool": tool,
        "arguments": arguments,
        "input_reconciliation": jarvis_input_manifest,
        "protocol_result": protocol_result,
        "structured_result": _structured_result(protocol_result, operation=operation),
        "protocol_version": protocol_version,
        "server_info": server_info,
        "server_artifact": server_artifact,
        "server_execution_artifact": execution_artifact,
        "expected_server_artifact_digest": expected_server_artifact_digest,
        "observed_server_artifact_digest": observed_server_artifact_digest,
        "pagination": pagination,
        "returncode": returncode,
        "stdout": stdout,
        "stderr": stderr,
        "timed_out": timed_out,
        "protocol_error": protocol_error,
        "package_progress_bridge": progress_bridge,
        "result_validation": result_validation,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": finished_at - started_at,
    }
    if expected_registered_contract is not None:
        result_document["expected_registered_contract"] = expected_registered_contract
    if expected_jarvis_cd_lock_binding is not None:
        result_document["expected_jarvis_cd_lock_binding"] = expected_jarvis_cd_lock_binding
    result_path.write_text(
        json.dumps(
            result_document,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


@contextmanager
def _prepared_mcp_launch(
    command: list[str],
    *,
    server_args: list[str],
    server_artifact: dict[str, Any],
) -> Generator[tuple[list[str], dict[str, Any] | None]]:
    """Launch an exact wheel only through a private verified byte snapshot."""
    wheel_identity = _wheel_install_input_identity(server_artifact)
    if wheel_identity is None:
        yield command, None
        return
    install_spec = server_artifact.get("install_spec")
    if not isinstance(install_spec, str):
        raise ValueError("exact MCP wheel install specification is unavailable")
    from_indexes = [
        index
        for index, argument in enumerate(server_args[:-1])
        if argument == "--from" and server_args[index + 1] == install_spec
    ]
    if len(from_indexes) != 1:
        raise ValueError("exact MCP wheel has no unique --from launch argument")
    source_path = Path(cast(str, wheel_identity["path"]))
    expected_sha256 = cast(str, wheel_identity["sha256"])
    expected_size = cast(int, wheel_identity["size_bytes"])
    private_root = Path(tempfile.mkdtemp(prefix="clio-relay-mcp-wheel-"))
    snapshot_path = private_root / source_path.name
    source_stream: Any = None
    snapshot_stream: Any = None
    source_identity: tuple[int, int, int, int] | None = None
    snapshot_identity: tuple[int, int, int, int] | None = None
    directory_identity: tuple[int, int, int, int] | None = None
    posix_parent_descriptor: int | None = None
    posix_directory_descriptor: int | None = None
    windows_directory_handle: int | None = None
    windows_snapshot_handle: int | None = None
    evidence: dict[str, Any] = {
        "schema_version": "clio-relay.mcp-execution-artifact.v1",
        "source_path": str(source_path),
        "source_sha256": expected_sha256,
        "source_size_bytes": expected_size,
        "private_snapshot": True,
        "snapshot_sha256": None,
        "snapshot_size_bytes": None,
        "snapshot_verified_before_launch": False,
        "snapshot_verified_after_launch": False,
        "source_verified_after_launch": False,
        "cleanup_verified": False,
    }
    body_failure: BaseException | None = None
    security_failures: list[str] = []
    try:
        directory_identity = _private_directory_identity(private_root, writable=True)
        if os.name == "nt":
            windows_directory_handle = _open_windows_snapshot_cleanup_handle(
                private_root,
                expected_inode=directory_identity[1],
                directory=True,
            )
        else:
            posix_parent_descriptor, posix_directory_descriptor = (
                _open_posix_snapshot_cleanup_descriptors(private_root)
            )
        source_stream = source_path.open("rb")
        source_identity = _verified_stream_identity(
            source_stream,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            label="source MCP wheel",
        )
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | int(getattr(os, "O_BINARY", 0))
            | int(getattr(os, "O_CLOEXEC", 0))
            | int(getattr(os, "O_NOFOLLOW", 0))
        )
        descriptor = os.open(snapshot_path, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as destination:
                source_stream.seek(0)
                while chunk := source_stream.read(FILE_HASH_CHUNK_BYTES):
                    destination.write(chunk)
                destination.flush()
                os.fsync(destination.fileno())
        finally:
            os.close(descriptor)
        if os.name != "nt":
            os.chmod(snapshot_path, 0o400)
            os.chmod(private_root, 0o500)
        directory_identity = _private_directory_identity(private_root, writable=False)
        snapshot_stream = snapshot_path.open("rb")
        snapshot_identity = _verified_stream_identity(
            snapshot_stream,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            label="private MCP wheel snapshot",
        )
        if not _private_snapshot_permissions_safe(snapshot_stream, snapshot_path):
            raise ValueError("private MCP wheel snapshot permissions are unsafe")
        if not _path_matches_identity(snapshot_path, snapshot_identity):
            raise ValueError("private MCP wheel snapshot path changed before launch")
        evidence.update(
            {
                "snapshot_sha256": expected_sha256,
                "snapshot_size_bytes": expected_size,
                "snapshot_verified_before_launch": True,
            }
        )
        snapshot_args = list(server_args)
        snapshot_args[from_indexes[0] + 1] = str(snapshot_path)
        launch_command = [command[0], *snapshot_args]
        try:
            yield launch_command, evidence
        except BaseException as exc:
            body_failure = exc
        if not _stream_still_matches(
            source_stream,
            identity=source_identity,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
        ):
            security_failures.append("source MCP wheel descriptor changed during launch")
        elif not _path_matches_identity(source_path, source_identity):
            security_failures.append("source MCP wheel path changed during launch")
        else:
            evidence["source_verified_after_launch"] = True
        if not _stream_still_matches(
            snapshot_stream,
            identity=snapshot_identity,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
        ):
            security_failures.append("private MCP wheel snapshot changed during launch")
        elif not _private_snapshot_permissions_safe(snapshot_stream, snapshot_path):
            security_failures.append("private MCP wheel snapshot permissions changed")
        elif not _path_matches_identity(snapshot_path, snapshot_identity):
            security_failures.append("private MCP wheel snapshot path changed during launch")
        elif not _private_directory_still_matches(
            private_root,
            directory_identity,
        ):
            security_failures.append("private MCP wheel directory changed during launch")
        else:
            evidence["snapshot_verified_after_launch"] = True
    finally:
        posix_snapshot_descriptor = (
            snapshot_stream.fileno() if os.name != "nt" and snapshot_stream is not None else None
        )
        if os.name == "nt" and snapshot_stream is not None:
            snapshot_stream.close()
        if source_stream is not None:
            source_stream.close()
        try:
            cleanup_error = _remove_private_snapshot(
                private_root,
                snapshot_path=snapshot_path,
                directory_identity=directory_identity,
                snapshot_identity=snapshot_identity,
                posix_parent_descriptor=posix_parent_descriptor,
                posix_directory_descriptor=posix_directory_descriptor,
                posix_snapshot_descriptor=posix_snapshot_descriptor,
                windows_directory_handle=windows_directory_handle,
                windows_snapshot_handle=windows_snapshot_handle,
            )
        finally:
            if os.name != "nt" and snapshot_stream is not None:
                snapshot_stream.close()
        evidence["cleanup_verified"] = cleanup_error is None
        if cleanup_error is not None:
            security_failures.append(cleanup_error)
    if security_failures:
        raise ValueError("; ".join(security_failures)) from body_failure
    if body_failure is not None:
        raise body_failure


def _wheel_install_input_identity(
    server_artifact: dict[str, Any],
) -> dict[str, Any] | None:
    """Return the unique exact wheel input recorded in release provenance."""
    if server_artifact.get("install_source") != "wheel":
        return None
    install_spec = server_artifact.get("install_spec")
    raw_inputs = server_artifact.get("input_files")
    if not isinstance(install_spec, str) or not isinstance(raw_inputs, list):
        raise ValueError("exact MCP wheel provenance is incomplete")
    try:
        resolved = str(Path(install_spec).expanduser().resolve(strict=True))
    except OSError as exc:
        raise ValueError("exact MCP wheel disappeared before launch") from exc
    matches = [
        cast(dict[str, Any], item)
        for item in cast(list[object], raw_inputs)
        if isinstance(item, dict) and cast(dict[str, Any], item).get("path") == resolved
    ]
    if len(matches) != 1:
        raise ValueError("exact MCP wheel has no unique recorded input identity")
    identity = matches[0]
    if (
        not isinstance(identity.get("sha256"), str)
        or not isinstance(identity.get("size_bytes"), int)
        or identity.get("size_bytes", -1) < 0
    ):
        raise ValueError("exact MCP wheel input identity is incomplete")
    return identity


def _file_descriptor_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    """Return the stable fields used to bind an open regular artifact."""
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def _verified_stream_identity(
    stream: Any,
    *,
    expected_sha256: str,
    expected_size: int,
    label: str,
) -> tuple[int, int, int, int]:
    """Verify one held regular stream against its expected exact bytes."""
    opened = os.fstat(stream.fileno())
    if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
        raise ValueError(f"{label} size or type did not match release provenance")
    identity = _file_descriptor_identity(opened)
    digest = hashlib.sha256()
    stream.seek(0)
    while chunk := stream.read(FILE_HASH_CHUNK_BYTES):
        digest.update(chunk)
    after = os.fstat(stream.fileno())
    if _file_descriptor_identity(after) != identity or not hmac.compare_digest(
        digest.hexdigest(),
        expected_sha256,
    ):
        raise ValueError(f"{label} bytes changed during verification")
    stream.seek(0)
    return identity


def _stream_still_matches(
    stream: Any,
    *,
    identity: tuple[int, int, int, int],
    expected_sha256: str,
    expected_size: int,
) -> bool:
    """Revalidate a held stream after the nested child exits."""
    try:
        return (
            _verified_stream_identity(
                stream,
                expected_sha256=expected_sha256,
                expected_size=expected_size,
                label="held MCP wheel",
            )
            == identity
        )
    except (OSError, ValueError):
        return False


def _path_matches_identity(path: Path, identity: tuple[int, int, int, int]) -> bool:
    """Return whether a path still names the held regular artifact."""
    try:
        observed = path.lstat()
    except OSError:
        return False
    return stat.S_ISREG(observed.st_mode) and _file_descriptor_identity(observed) == identity


def _private_snapshot_permissions_safe(stream: Any, path: Path) -> bool:
    """Return whether the held snapshot remains a private single-link regular file."""
    try:
        opened = os.fstat(stream.fileno())
        observed = path.lstat()
    except OSError:
        return False
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or opened.st_nlink != 1
        or observed.st_nlink != 1
    ):
        return False
    return os.name == "nt" or (
        opened.st_uid == os.getuid()
        and observed.st_uid == os.getuid()
        and stat.S_IMODE(opened.st_mode) == 0o400
        and stat.S_IMODE(observed.st_mode) == 0o400
    )


def _private_directory_identity(
    path: Path,
    *,
    writable: bool,
) -> tuple[int, int, int, int]:
    """Validate one private real snapshot directory and return its identity."""
    observed = path.lstat()
    expected_mode = 0o700 if writable else 0o500
    if not stat.S_ISDIR(observed.st_mode) or path.is_symlink():
        raise ValueError("private MCP wheel directory is not a real directory")
    if os.name != "nt" and (
        observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) != expected_mode
    ):
        raise ValueError("private MCP wheel directory ownership or mode is unsafe")
    return _file_descriptor_identity(observed)


def _private_directory_still_matches(
    path: Path,
    identity: tuple[int, int, int, int],
) -> bool:
    """Revalidate the private snapshot directory after execution."""
    try:
        observed = _private_directory_identity(path, writable=False)
    except (OSError, ValueError):
        return False
    return observed[:2] == identity[:2]


def _open_posix_snapshot_cleanup_descriptors(path: Path) -> tuple[int, int]:
    """Hold the snapshot parent and exact directory without following links."""
    directory_flags = (
        os.O_RDONLY
        | int(getattr(os, "O_DIRECTORY", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
    )
    parent_descriptor = os.open(path.parent, directory_flags)
    try:
        directory_descriptor = os.open(
            path.name,
            directory_flags,
            dir_fd=parent_descriptor,
        )
    except BaseException:
        os.close(parent_descriptor)
        raise
    try:
        opened = os.fstat(directory_descriptor)
        observed = path.lstat()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or (opened.st_dev, opened.st_ino) != (observed.st_dev, observed.st_ino)
        ):
            raise ValueError("private MCP wheel directory changed while opening cleanup handles")
    except BaseException:
        os.close(directory_descriptor)
        os.close(parent_descriptor)
        raise
    return parent_descriptor, directory_descriptor


_WINDOWS_DELETE = 0x00010000
_WINDOWS_FILE_LIST_DIRECTORY = 0x00000001
_WINDOWS_FILE_READ_ATTRIBUTES = 0x00000080
_WINDOWS_FILE_SHARE_READ = 0x00000001
_WINDOWS_FILE_SHARE_WRITE = 0x00000002
_WINDOWS_OPEN_EXISTING = 3
_WINDOWS_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_WINDOWS_FILE_DISPOSITION_INFO = 4


def _open_windows_snapshot_cleanup_handle(
    path: Path,
    *,
    expected_inode: int,
    directory: bool,
) -> int:
    """Open one exact Windows cleanup entry without permitting substitution."""
    if os.name != "nt":
        raise RuntimeError("Windows snapshot cleanup handles require Windows")
    import ctypes
    from ctypes import wintypes

    desired_access = _WINDOWS_DELETE | _WINDOWS_FILE_READ_ATTRIBUTES
    flags = _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT
    if directory:
        desired_access |= _WINDOWS_FILE_LIST_DIRECTORY
        flags |= _WINDOWS_FILE_FLAG_BACKUP_SEMANTICS
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    raw_handle = kernel32.CreateFileW(
        str(path),
        desired_access,
        _WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        None,
        _WINDOWS_OPEN_EXISTING,
        flags,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if raw_handle == invalid_handle:
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error), path)
    handle = int(raw_handle)
    try:
        attributes, inode, links = _windows_snapshot_handle_information(handle, path)
        is_directory = bool(attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY)
        is_reparse = bool(attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT)
        if (
            expected_inode <= 0
            or inode != expected_inode
            or is_directory != directory
            or is_reparse
            or (not directory and links != 1)
        ):
            raise ValueError(f"Windows snapshot cleanup entry changed while opening: {path}")
        return handle
    except BaseException:
        _close_windows_snapshot_cleanup_handle(handle)
        raise


def _windows_snapshot_handle_information(
    handle: int,
    path: Path,
) -> tuple[int, int, int]:
    """Return attributes, stable identity, and links for a Windows handle."""
    if os.name != "nt":
        raise RuntimeError("Windows snapshot handle inspection requires Windows")
    import ctypes
    from ctypes import wintypes

    class _ByHandleFileInformation(ctypes.Structure):
        _fields_ = [
            ("file_attributes", wintypes.DWORD),
            ("creation_time", wintypes.FILETIME),
            ("last_access_time", wintypes.FILETIME),
            ("last_write_time", wintypes.FILETIME),
            ("volume_serial_number", wintypes.DWORD),
            ("file_size_high", wintypes.DWORD),
            ("file_size_low", wintypes.DWORD),
            ("number_of_links", wintypes.DWORD),
            ("file_index_high", wintypes.DWORD),
            ("file_index_low", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    ]
    kernel32.GetFileInformationByHandle.restype = wintypes.BOOL
    information = _ByHandleFileInformation()
    if not kernel32.GetFileInformationByHandle(handle, ctypes.byref(information)):
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error), path)
    inode = (int(information.file_index_high) << 32) | int(information.file_index_low)
    return (
        int(information.file_attributes),
        inode,
        int(information.number_of_links),
    )


def _mark_windows_snapshot_handle_for_delete(handle: int, path: Path) -> None:
    """Mark one exact Windows cleanup handle for deletion on close."""
    if os.name != "nt":
        raise RuntimeError("Windows snapshot handle deletion requires Windows")
    import ctypes
    from ctypes import wintypes

    class _FileDispositionInformation(ctypes.Structure):
        _fields_ = [("delete_file", wintypes.BOOL)]

    disposition = _FileDispositionInformation(delete_file=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.SetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    kernel32.SetFileInformationByHandle.restype = wintypes.BOOL
    if not kernel32.SetFileInformationByHandle(
        handle,
        _WINDOWS_FILE_DISPOSITION_INFO,
        ctypes.byref(disposition),
        ctypes.sizeof(disposition),
    ):
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error), path)


def _close_windows_snapshot_cleanup_handle(handle: int) -> None:
    """Close a Windows cleanup handle."""
    if os.name != "nt":
        raise RuntimeError("Windows snapshot handle cleanup requires Windows")
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.CloseHandle(handle)


def _remove_private_snapshot(
    path: Path,
    *,
    snapshot_path: Path,
    directory_identity: tuple[int, int, int, int] | None,
    snapshot_identity: tuple[int, int, int, int] | None,
    posix_parent_descriptor: int | None,
    posix_directory_descriptor: int | None,
    posix_snapshot_descriptor: int | None,
    windows_directory_handle: int | None,
    windows_snapshot_handle: int | None,
) -> str | None:
    """Delete the exact held snapshot file and directory without path recursion."""
    if os.name == "nt":
        return _remove_windows_private_snapshot(
            path,
            snapshot_path=snapshot_path,
            directory_identity=directory_identity,
            snapshot_identity=snapshot_identity,
            directory_handle=windows_directory_handle,
            snapshot_handle=windows_snapshot_handle,
        )
    return _remove_posix_private_snapshot(
        path,
        snapshot_path=snapshot_path,
        directory_identity=directory_identity,
        snapshot_identity=snapshot_identity,
        parent_descriptor=posix_parent_descriptor,
        directory_descriptor=posix_directory_descriptor,
        snapshot_descriptor=posix_snapshot_descriptor,
    )


def _remove_posix_private_snapshot(
    path: Path,
    *,
    snapshot_path: Path,
    directory_identity: tuple[int, int, int, int] | None,
    snapshot_identity: tuple[int, int, int, int] | None,
    parent_descriptor: int | None,
    directory_descriptor: int | None,
    snapshot_descriptor: int | None,
) -> str | None:
    """Delete a POSIX snapshot through held parent and directory descriptors."""
    if parent_descriptor is None or directory_descriptor is None or directory_identity is None:
        for descriptor in (directory_descriptor, parent_descriptor):
            if descriptor is not None:
                os.close(descriptor)
        return "private MCP wheel snapshot has no complete POSIX cleanup handles"
    try:
        held_directory = os.fstat(directory_descriptor)
        if (
            not stat.S_ISDIR(held_directory.st_mode)
            or _file_descriptor_identity(held_directory)[:2] != directory_identity[:2]
        ):
            return "private MCP wheel snapshot directory handle changed before cleanup"
        _posix_fchmod(directory_descriptor, 0o700)
        entries = set(os.listdir(directory_descriptor))
        expected_entries: set[str] = (
            {snapshot_path.name} if snapshot_identity is not None else set()
        )
        unexpected_entries = entries - expected_entries
        if snapshot_identity is not None:
            if snapshot_descriptor is None:
                return "private MCP wheel snapshot has no held POSIX file descriptor"
            held_snapshot = os.fstat(snapshot_descriptor)
            if (
                not stat.S_ISREG(held_snapshot.st_mode)
                or held_snapshot.st_nlink != 1
                or _file_descriptor_identity(held_snapshot)[:2] != snapshot_identity[:2]
            ):
                return "private MCP wheel snapshot held file changed before cleanup"
            if snapshot_path.name not in entries:
                return "private MCP wheel snapshot file disappeared before cleanup"
            observed_snapshot = os.stat(
                snapshot_path.name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(observed_snapshot.st_mode)
                or _file_descriptor_identity(observed_snapshot)[:2] != snapshot_identity[:2]
            ):
                return "private MCP wheel snapshot file changed before cleanup"
            os.unlink(snapshot_path.name, dir_fd=directory_descriptor)
            os.fsync(directory_descriptor)
            unlinked_snapshot = os.fstat(snapshot_descriptor)
            if (
                _file_descriptor_identity(unlinked_snapshot)[:2] != snapshot_identity[:2]
                or unlinked_snapshot.st_nlink != 0
            ):
                return "private MCP wheel snapshot held file remained linked after cleanup"
        if unexpected_entries:
            return "private MCP wheel snapshot directory contains unexpected entries"
        if os.listdir(directory_descriptor):
            return "private MCP wheel snapshot directory was not empty after file cleanup"
        observed_path = os.stat(
            path.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISDIR(observed_path.st_mode)
            or _file_descriptor_identity(observed_path)[:2] != directory_identity[:2]
        ):
            return "private MCP wheel snapshot directory path changed before cleanup"
        os.rmdir(path.name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
        if os.fstat(directory_descriptor).st_nlink != 0:
            return "private MCP wheel snapshot original directory remained after cleanup"
        return None
    except OSError as exc:
        return f"private MCP wheel snapshot cleanup failed: {exc}"
    finally:
        os.close(directory_descriptor)
        os.close(parent_descriptor)


def _posix_fchmod(descriptor: int, mode: int) -> None:
    """Call POSIX fchmod without exposing the platform-specific attribute to Pyright."""
    fchmod = cast(Callable[[int, int], None], getattr(os, "fchmod"))  # noqa: B009
    fchmod(descriptor, mode)


def _remove_windows_private_snapshot(
    path: Path,
    *,
    snapshot_path: Path,
    directory_identity: tuple[int, int, int, int] | None,
    snapshot_identity: tuple[int, int, int, int] | None,
    directory_handle: int | None,
    snapshot_handle: int | None,
) -> str | None:
    """Delete the exact Windows snapshot file and directory by retained handles."""
    if directory_handle is None or directory_identity is None:
        if snapshot_handle is not None:
            _close_windows_snapshot_cleanup_handle(snapshot_handle)
        if directory_handle is not None:
            _close_windows_snapshot_cleanup_handle(directory_handle)
        return "private MCP wheel snapshot has no complete Windows cleanup handles"
    active_directory_handle: int | None = directory_handle
    active_snapshot_handle: int | None = snapshot_handle
    try:
        directory_attributes, directory_inode, _ = _windows_snapshot_handle_information(
            directory_handle,
            path,
        )
        if (
            directory_inode != directory_identity[1]
            or not directory_attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY
            or directory_attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT
        ):
            return "private MCP wheel snapshot directory handle changed before cleanup"
        if snapshot_identity is not None:
            if snapshot_handle is None:
                snapshot_handle = _open_windows_snapshot_cleanup_handle(
                    snapshot_path,
                    expected_inode=snapshot_identity[1],
                    directory=False,
                )
                active_snapshot_handle = snapshot_handle
            snapshot_attributes, snapshot_inode, links = _windows_snapshot_handle_information(
                snapshot_handle,
                snapshot_path,
            )
            if (
                snapshot_inode != snapshot_identity[1]
                or snapshot_attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY
                or snapshot_attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT
                or links != 1
            ):
                return "private MCP wheel snapshot file handle changed before cleanup"
            _mark_windows_snapshot_handle_for_delete(snapshot_handle, snapshot_path)
            _close_windows_snapshot_cleanup_handle(snapshot_handle)
            active_snapshot_handle = None
            if snapshot_path.exists():
                return "private MCP wheel snapshot file remained after handle deletion"
        _mark_windows_snapshot_handle_for_delete(directory_handle, path)
        _close_windows_snapshot_cleanup_handle(directory_handle)
        active_directory_handle = None
        if path.exists():
            return "private MCP wheel snapshot directory remained after handle deletion"
        return None
    except OSError as exc:
        return f"private MCP wheel snapshot cleanup failed: {exc}"
    finally:
        if active_snapshot_handle is not None:
            _close_windows_snapshot_cleanup_handle(active_snapshot_handle)
        if active_directory_handle is not None:
            _close_windows_snapshot_cleanup_handle(active_directory_handle)


def mcp_server_artifact_identity(
    server: str,
    server_args: list[str],
    *,
    verify_relay_jarvis_cd_lock: bool = False,
) -> dict[str, Any]:
    """Return machine-readable launch identity for one stdio MCP server."""
    return _server_artifact_identity(
        server,
        server_args,
        verify_relay_jarvis_cd_lock=verify_relay_jarvis_cd_lock,
    )


def _server_artifact_identity(
    server: str,
    server_args: list[str],
    *,
    verify_relay_jarvis_cd_lock: bool = False,
) -> dict[str, Any]:
    """Describe the executable and immutable package inputs used for one MCP server."""
    resolved_executable = Path(_resolve_executable(server)).expanduser()
    executable = _file_identity(resolved_executable)
    install_spec: str | None = None
    for index, argument in enumerate(server_args[:-1]):
        if argument == "--from":
            install_spec = server_args[index + 1]
            break
    input_files: list[dict[str, Any]] = []
    for argument in server_args:
        identity = _file_identity(Path(argument).expanduser())
        if identity is not None and identity not in input_files:
            input_files.append(identity)
    install_source = _install_spec_source(install_spec)
    resolved_install_spec = (
        str(Path(install_spec).expanduser().resolve()) if install_spec is not None else None
    )
    install_artifact = next(
        (item for item in input_files if item["path"] == resolved_install_spec),
        None,
    )
    python_distribution_runtime = (
        _python_console_distribution_identity(resolved_executable)
        if install_spec is None and executable is not None
        else None
    )
    runtime_launcher_identity = (
        python_distribution_runtime.get("external_launcher_identity")
        if python_distribution_runtime is not None
        else None
    )
    runtime_launcher_verified = (
        executable is not None
        and isinstance(runtime_launcher_identity, dict)
        and cast(dict[str, Any], runtime_launcher_identity) == executable
    )
    if (
        python_distribution_runtime is not None
        and python_distribution_runtime.get("runtime_closure_verified") is True
        and not runtime_launcher_verified
    ):
        python_distribution_runtime["runtime_closure_verified"] = False
        python_distribution_runtime["error"] = (
            "direct server executable changed during Python runtime inspection"
        )
    direct_runtime_verified = (
        python_distribution_runtime is not None
        and python_distribution_runtime.get("runtime_closure_verified") is True
        and runtime_launcher_verified
    )
    direct_install_artifact = _direct_distribution_source_identity(python_distribution_runtime)
    if direct_install_artifact is not None and direct_install_artifact not in input_files:
        input_files.append(direct_install_artifact)
    recorded_install_spec = (
        install_spec
        if install_spec is not None
        else (str(direct_install_artifact["path"]) if direct_install_artifact is not None else None)
    )
    recorded_install_source = (
        install_source
        if install_spec is not None
        else ("uv-tool" if direct_install_artifact is not None else None)
    )
    recorded_install_artifact = install_artifact or direct_install_artifact
    launcher_artifact_verified = executable is not None and (
        (install_spec is None and direct_runtime_verified)
        or (install_spec is not None and install_artifact is not None)
    )
    nested_server_name = _nested_clio_kit_server_name(
        server_args,
        python_distribution_runtime=python_distribution_runtime,
    )
    nested_launcher = nested_server_name is not None
    nested_runtime = (
        (
            _locked_clio_kit_runtime_identity(
                install_artifact,
                server_name=nested_server_name,
                resolved_executable=resolved_executable,
                verify_relay_jarvis_cd_lock=verify_relay_jarvis_cd_lock,
            )
            if install_artifact is not None
            else _installed_clio_kit_runtime_identity(
                python_distribution_runtime,
                server_name=nested_server_name,
                resolved_executable=resolved_executable,
                verify_relay_jarvis_cd_lock=verify_relay_jarvis_cd_lock,
            )
        )
        if nested_server_name is not None
        else None
    )
    nested_runtime_verified = (
        nested_runtime is not None and nested_runtime.get("locked_runtime_verified") is True
    )
    server_process_artifact_verified = launcher_artifact_verified and (
        not nested_launcher or nested_runtime_verified
    )
    return {
        "requested_command": server,
        "resolved_executable": str(resolved_executable),
        "executable": executable,
        "install_spec": recorded_install_spec,
        "install_source": recorded_install_source,
        "install_artifact_sha256": (
            recorded_install_artifact.get("sha256")
            if recorded_install_artifact is not None
            else None
        ),
        "input_files": input_files,
        "launcher_artifact_verified": launcher_artifact_verified,
        "python_distribution_runtime": python_distribution_runtime,
        "nested_launcher": nested_launcher,
        "nested_runtime": nested_runtime,
        "server_process_artifact_verified": server_process_artifact_verified,
        "identity_error": (
            "clio-kit mcp-server child source, lock, or uv runtime is not bound to its "
            "persistent tool distribution"
            if nested_launcher and not nested_runtime_verified
            else (
                "direct server executable is not bound to a verified Python entry-point "
                "distribution RECORD closure"
                if install_spec is None and not direct_runtime_verified
                else None
            )
        ),
        "verified": server_process_artifact_verified,
    }


def _python_console_distribution_identity(executable: Path) -> dict[str, Any]:
    """Bind a direct Python console launcher to its complete installed wheel RECORD."""
    evidence: dict[str, Any] = {
        "schema_version": "clio-relay.python-distribution-runtime.v1",
        "distribution": None,
        "distribution_version": None,
        "entry_point": None,
        "entry_point_value": None,
        "record_sha256": None,
        "runtime_closure_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "runtime_closure_verified": False,
        "direct_url": None,
        "provider_interpreter": None,
        "external_launcher_identity": None,
        "contract_source_path": None,
        "server_lock_paths": {},
        "error": None,
    }
    try:
        resolved_executable = executable.resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"could not resolve direct server executable: {exc}"
        return evidence
    launcher_identity = _file_identity(resolved_executable)
    if launcher_identity is None:
        evidence["error"] = "direct server executable has no stable file identity"
        return evidence
    evidence["external_launcher_identity"] = launcher_identity
    command_name = (
        resolved_executable.stem
        if resolved_executable.suffix.casefold() == ".exe"
        else resolved_executable.name
    )
    matches: list[tuple[metadata.Distribution, metadata.EntryPoint]] = []
    distribution_count = 0
    entry_point_count = 0
    try:
        distributions = metadata.distributions()
        for distribution in distributions:
            distribution_count += 1
            if distribution_count > PYTHON_DISTRIBUTION_MAX_DISTRIBUTIONS:
                evidence["error"] = "installed Python distribution count exceeded its limit"
                return evidence
            files = distribution.files
            if files is None or not _distribution_contains_executable(
                distribution,
                files,
                resolved_executable,
            ):
                continue
            for entry_point in distribution.entry_points:
                entry_point_count += 1
                if entry_point_count > PYTHON_DISTRIBUTION_MAX_ENTRY_POINTS:
                    evidence["error"] = "installed Python entry-point count exceeded its limit"
                    return evidence
                if entry_point.group == "console_scripts" and entry_point.name == command_name:
                    matches.append((distribution, entry_point))
    except (OSError, TypeError, ValueError) as exc:
        evidence["error"] = f"could not inspect installed Python distributions: {exc}"
        return evidence
    if len(matches) != 1:
        return _external_python_console_distribution_identity(
            resolved_executable,
            command_name=command_name,
        )
    distribution, entry_point = matches[0]
    evidence.update(
        {
            "distribution": distribution.metadata.get("Name"),
            "distribution_version": distribution.version,
            "entry_point": entry_point.name,
            "entry_point_value": entry_point.value,
            "provider_interpreter": sys.executable,
        }
    )
    files = distribution.files or []
    for member in files:
        normalized = str(member).replace("\\", "/")
        path = str(Path(str(distribution.locate_file(member))).resolve())
        if normalized.endswith("clio_kit/__init__.py"):
            evidence["contract_source_path"] = path
        match = re.search(r"clio-kit-mcp-servers/([^/]+)/uv\.lock$", normalized)
        if match is not None:
            cast(dict[str, str], evidence["server_lock_paths"])[match.group(1)] = path
    direct_url = _distribution_direct_url(distribution)
    evidence["direct_url"] = direct_url
    if direct_url is not None:
        directory = direct_url.get("dir_info")
        typed_directory = cast(dict[str, Any], directory) if isinstance(directory, dict) else {}
        if typed_directory.get("editable") is True:
            evidence["error"] = "editable Python distributions have no immutable runtime closure"
            return evidence
    closure = _verify_distribution_record_closure(distribution)
    evidence.update(closure)
    return evidence


def _direct_distribution_source_identity(
    runtime: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Return the retained wheel behind one verified persistent tool install."""
    direct_url = runtime.get("direct_url") if runtime is not None else None
    if not isinstance(direct_url, dict):
        return None
    url = cast(dict[str, Any], direct_url).get("url")
    if not isinstance(url, str):
        return None
    parsed = urlsplit(url)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        return None
    source_value = unquote(parsed.path)
    if os.name == "nt" and re.fullmatch(r"/[A-Za-z]:/.*", source_value):
        source_value = source_value[1:]
    source = Path(source_value)
    if source.suffix.lower() != ".whl":
        return None
    return _file_identity(source)


def _persistent_tool_launcher_shebang(payload: bytes, *, executable_name: str) -> str:
    """Return the provider shebang from a script or Windows uv trampoline."""
    script = payload
    if not payload.startswith(b"#!"):
        if Path(executable_name).suffix.casefold() != ".exe":
            raise ValueError("persistent tool launcher has no Python shebang")
        with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
            candidates = [
                member for member in archive.infolist() if member.filename == "__main__.py"
            ]
            if len(candidates) != 1 or not _zip_member_is_regular(candidates[0]):
                raise ValueError("Windows persistent tool launcher has no unique __main__.py")
            if candidates[0].flag_bits & 0x1:
                raise ValueError("Windows persistent tool launcher script is encrypted")
            script = _read_bounded_zip_member(
                archive,
                candidates[0].filename,
                max_bytes=CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES,
            )
    lines = script.split(b"\n", 3)
    first_line = lines[0]
    if len(first_line) > 4096:
        raise ValueError("persistent tool launcher shebang exceeded its byte limit")
    shebang = first_line.decode("utf-8", errors="strict").rstrip("\r")
    if not shebang.startswith("#!") or not shebang[2:]:
        raise ValueError("persistent tool launcher has no direct Python interpreter shebang")
    if "\x00" in shebang:
        raise ValueError("persistent tool launcher shebang contains a null byte")
    if shebang == "#!/bin/sh":
        if len(lines) < 3 or any(len(line) > 4096 for line in lines[1:3]):
            raise ValueError("persistent uv shell trampoline is incomplete or oversized")
        execution_line = lines[1].decode("utf-8", errors="strict").rstrip("\r")
        closing_line = lines[2].decode("utf-8", errors="strict").rstrip("\r")
        try:
            execution = shlex.split(execution_line, posix=True)
        except ValueError as exc:
            raise ValueError("persistent uv shell trampoline has invalid quoting") from exc
        if len(execution) != 4 or execution[0] != "exec" or execution[2:] != ["$0", "$@"]:
            raise ValueError("persistent uv shell trampoline has an unsupported exec contract")
        provider = execution[1]
        quoted_provider = "'" + provider.replace("'", "'\"'\"'") + "'"
        canonical_execution_line = f"'''exec' {quoted_provider} \"$0\" \"$@\""
        if (
            not provider.startswith("/")
            or "\x00" in provider
            or execution_line != canonical_execution_line
            or closing_line != "' '''"
        ):
            raise ValueError("persistent uv shell trampoline has an invalid provider contract")
        return f"#!{provider}"
    return shebang


def _external_python_console_distribution_identity(
    executable: Path,
    *,
    command_name: str,
) -> dict[str, Any]:
    """Verify a console script installed in an isolated persistent tool environment."""
    evidence: dict[str, Any] = {
        "schema_version": "clio-relay.python-distribution-runtime.v1",
        "distribution": None,
        "distribution_version": None,
        "entry_point": None,
        "entry_point_value": None,
        "record_sha256": None,
        "runtime_closure_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "runtime_closure_verified": False,
        "direct_url": None,
        "provider_interpreter": None,
        "provider_interpreter_identity": None,
        "external_launcher_identity": None,
        "distribution_console_script": None,
        "launcher_copy_verified": False,
        "contract_source_path": None,
        "server_lock_paths": {},
        "error": None,
    }
    launcher_snapshot = _bounded_regular_file_snapshot(
        executable,
        max_bytes=PYTHON_TOOL_IDENTITY_MAX_BYTES,
    )
    if launcher_snapshot is None:
        evidence["error"] = "persistent tool launcher is not one stable bounded file"
        return evidence
    launcher_bytes, launcher_descriptor = launcher_snapshot
    launcher_sha256 = hashlib.sha256(launcher_bytes).hexdigest()
    evidence["external_launcher_identity"] = {
        "path": str(executable),
        "filename": executable.name,
        "sha256": launcher_sha256,
        "size_bytes": len(launcher_bytes),
    }
    try:
        shebang = _persistent_tool_launcher_shebang(
            launcher_bytes,
            executable_name=executable.name,
        )
    except (
        NotImplementedError,
        RuntimeError,
        UnicodeDecodeError,
        ValueError,
        zipfile.BadZipFile,
    ) as exc:
        evidence["error"] = f"could not read persistent tool launcher: {exc}"
        return evidence
    if not shebang.startswith("#!") or not shebang[2:]:
        evidence["error"] = "persistent tool launcher has no direct Python interpreter shebang"
        return evidence
    provider_launcher = Path(shebang[2:]).expanduser()
    if not provider_launcher.is_absolute():
        evidence["error"] = "persistent tool provider interpreter path is not absolute"
        return evidence
    try:
        provider_launcher_identity = _file_descriptor_identity(provider_launcher.lstat())
        provider = provider_launcher.resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"persistent tool provider interpreter is unavailable: {exc}"
        return evidence
    provider_identity = _file_identity(provider)
    if provider_identity is None:
        evidence["error"] = "persistent tool provider interpreter has no file identity"
        return evidence
    probe = r"""
import hashlib
import json
import sys
from importlib import metadata
from pathlib import Path

command_name = sys.argv[1]
external_launcher_sha256 = sys.argv[2]
matches = []
distribution_count = 0
entry_point_count = 0
candidate_names = {command_name.casefold(), f"{command_name}.exe".casefold()}
for distribution in metadata.distributions():
    distribution_count += 1
    if distribution_count > 10_000:
        raise SystemExit("installed Python distribution count exceeded its limit")
    files = distribution.files or []
    if len(files) > 100_000:
        raise SystemExit("installed Python distribution file count exceeded its limit")
    entry_points = []
    for entry_point in distribution.entry_points:
        entry_point_count += 1
        if entry_point_count > 100_000:
            raise SystemExit("installed Python entry-point count exceeded its limit")
        if entry_point.group == "console_scripts" and entry_point.name == command_name:
            entry_points.append(entry_point)
    if len(entry_points) != 1:
        continue
    for launcher_member in files:
        located_launcher = Path(str(distribution.locate_file(launcher_member))).resolve()
        if located_launcher.name.casefold() not in candidate_names:
            continue
        launcher_stat = located_launcher.stat()
        if not located_launcher.is_file() or not 1 <= launcher_stat.st_size <= 8 * 1024 * 1024:
            continue
        launcher_hash = hashlib.sha256()
        with located_launcher.open("rb") as launcher_stream:
            while launcher_chunk := launcher_stream.read(1024 * 1024):
                launcher_hash.update(launcher_chunk)
        launcher_after = located_launcher.stat()
        launcher_key = (
            launcher_stat.st_dev,
            launcher_stat.st_ino,
            launcher_stat.st_size,
            launcher_stat.st_mtime_ns,
        )
        if (
            launcher_after.st_dev,
            launcher_after.st_ino,
            launcher_after.st_size,
            launcher_after.st_mtime_ns,
        ) != launcher_key:
            raise SystemExit("persistent tool RECORD-owned launcher changed during inspection")
        if launcher_hash.hexdigest() != external_launcher_sha256:
            continue
        entry_point = entry_points[0]
        direct_url_text = distribution.read_text("direct_url.json")
        direct_url = json.loads(direct_url_text) if direct_url_text else None
        serialized_files = []
        contract_source_path = None
        server_lock_paths = {}
        for member in files:
            normalized = str(member).replace("\\", "/")
            located = str(Path(str(distribution.locate_file(member))).resolve())
            member_hash = member.hash
            serialized_files.append({
                "name": normalized,
                "path": located,
                "hash_mode": member_hash.mode if member_hash is not None else None,
                "hash_value": member_hash.value if member_hash is not None else None,
                "size": member.size,
            })
            if normalized.endswith("clio_kit/__init__.py"):
                contract_source_path = located
            marker = "clio-kit-mcp-servers/"
            if marker in normalized and normalized.endswith("/uv.lock"):
                server_name = normalized.split(marker, 1)[1].split("/", 1)[0]
                server_lock_paths[server_name] = located
        matches.append({
            "executable": sys.executable,
            "distribution_console_script": str(located_launcher),
            "distribution_console_script_sha256": launcher_hash.hexdigest(),
            "distribution": distribution.metadata.get("Name"),
            "distribution_version": distribution.version,
            "entry_point": entry_point.name,
            "entry_point_value": entry_point.value,
            "direct_url": direct_url,
            "files": serialized_files,
            "contract_source_path": contract_source_path,
            "server_lock_paths": server_lock_paths,
        })
print(json.dumps({"matches": matches}, sort_keys=True))
"""
    try:
        completed = subprocess.run(
            [str(provider_launcher), "-I", "-c", probe, command_name, launcher_sha256],
            check=False,
            capture_output=True,
            text=True,
            timeout=PYTHON_TOOL_IDENTITY_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
        evidence["error"] = f"persistent tool distribution probe failed: {exc}"
        return evidence
    try:
        launcher_after = _file_descriptor_identity(provider_launcher.lstat())
        provider_after = provider_launcher.resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"persistent tool provider interpreter changed: {exc}"
        return evidence
    if (
        launcher_after != provider_launcher_identity
        or provider_after != provider
        or _file_identity(provider) != provider_identity
    ):
        evidence["error"] = "persistent tool provider interpreter changed during inspection"
        return evidence
    stdout_bytes = completed.stdout.encode("utf-8")
    if (
        completed.returncode != 0
        or not stdout_bytes
        or len(stdout_bytes) > PYTHON_TOOL_IDENTITY_MAX_BYTES
    ):
        evidence["error"] = "persistent tool distribution probe returned no bounded evidence"
        return evidence
    try:
        decoded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        evidence["error"] = f"persistent tool distribution probe returned invalid JSON: {exc}"
        return evidence
    decoded_mapping = cast(dict[str, object], decoded) if isinstance(decoded, dict) else {}
    raw_matches: object = decoded_mapping.get("matches")
    if not isinstance(raw_matches, list):
        evidence["error"] = (
            "persistent tool launcher has no unique installed console-script distribution"
        )
        return evidence
    matches = cast(list[object], raw_matches)
    if len(matches) != 1 or not isinstance(matches[0], dict):
        evidence["error"] = (
            "persistent tool launcher has no unique installed console-script distribution"
        )
        return evidence
    identity = cast(dict[str, Any], matches[0])
    direct_url = identity.get("direct_url")
    if isinstance(direct_url, dict):
        dir_info = cast(dict[str, Any], direct_url).get("dir_info")
        if isinstance(dir_info, dict) and cast(dict[str, Any], dir_info).get("editable") is True:
            evidence["error"] = "editable Python distributions have no immutable runtime closure"
            return evidence
    raw_files = identity.get("files")
    if not isinstance(raw_files, list):
        evidence["error"] = "persistent tool distribution omitted its RECORD closure"
        return evidence
    try:
        observed_provider = Path(str(identity.get("executable"))).resolve(strict=True)
    except OSError:
        observed_provider = Path("__unverified_provider__")
    if observed_provider != provider:
        evidence["error"] = "persistent tool probe executed under the wrong interpreter"
        return evidence
    console_script_value = identity.get("distribution_console_script")
    if not isinstance(console_script_value, str):
        evidence["error"] = "persistent tool distribution omitted its RECORD-owned launcher"
        return evidence
    try:
        console_script = Path(console_script_value).resolve(strict=True)
        provider_environment_bin = provider_launcher.parent.resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"persistent tool RECORD-owned launcher is unavailable: {exc}"
        return evidence
    console_script_snapshot = _bounded_regular_file_snapshot(
        console_script,
        max_bytes=PYTHON_TOOL_IDENTITY_MAX_BYTES,
    )
    if console_script_snapshot is None:
        evidence["error"] = "persistent tool RECORD-owned launcher is not one bounded file"
        return evidence
    console_script_bytes, console_script_descriptor = console_script_snapshot
    console_script_sha256 = hashlib.sha256(console_script_bytes).hexdigest()
    if (
        console_script.parent != provider_environment_bin
        or identity.get("distribution_console_script_sha256") != launcher_sha256
        or not hmac.compare_digest(console_script_sha256, launcher_sha256)
        or not hmac.compare_digest(console_script_bytes, launcher_bytes)
    ):
        evidence["error"] = (
            "persistent tool launcher does not match its RECORD-owned console script"
        )
        return evidence
    closure = _verify_external_distribution_record_closure(cast(list[object], raw_files))
    launcher_after = _bounded_regular_file_snapshot(
        executable,
        max_bytes=PYTHON_TOOL_IDENTITY_MAX_BYTES,
    )
    console_script_after = _bounded_regular_file_snapshot(
        console_script,
        max_bytes=PYTHON_TOOL_IDENTITY_MAX_BYTES,
    )
    if (
        launcher_after is None
        or console_script_after is None
        or launcher_after[1] != launcher_descriptor
        or console_script_after[1] != console_script_descriptor
        or not hmac.compare_digest(launcher_after[0], launcher_bytes)
        or not hmac.compare_digest(console_script_after[0], console_script_bytes)
    ):
        evidence["error"] = "persistent tool launcher changed during inspection"
        return evidence
    evidence.update(
        {
            "distribution": identity.get("distribution"),
            "distribution_version": identity.get("distribution_version"),
            "entry_point": identity.get("entry_point"),
            "entry_point_value": identity.get("entry_point_value"),
            "direct_url": identity.get("direct_url"),
            "provider_interpreter": str(provider),
            "provider_interpreter_identity": provider_identity,
            "distribution_console_script": {
                "path": str(console_script),
                "filename": console_script.name,
                "sha256": console_script_sha256,
                "size_bytes": len(console_script_bytes),
            },
            "launcher_copy_verified": True,
            "contract_source_path": identity.get("contract_source_path"),
            "server_lock_paths": identity.get("server_lock_paths", {}),
            **closure,
        }
    )
    return evidence


def _verify_external_distribution_record_closure(
    raw_files: list[object],
) -> dict[str, Any]:
    """Verify the RECORD closure described by an isolated tool interpreter."""
    failure: dict[str, Any] = {
        "record_sha256": None,
        "runtime_closure_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "runtime_closure_verified": False,
        "error": None,
    }
    if not raw_files or len(raw_files) > PYTHON_DISTRIBUTION_MAX_FILES:
        failure["error"] = "persistent tool RECORD file list was missing or exceeded its limit"
        return failure
    names: set[str] = set()
    record_paths: list[Path] = []
    closure_inputs: list[tuple[str, int, str]] = []
    total_bytes = 0
    for item in raw_files:
        if not isinstance(item, dict):
            failure["error"] = "persistent tool RECORD entry was not an object"
            return failure
        member = cast(dict[str, Any], item)
        name = member.get("name")
        path = member.get("path")
        if not isinstance(name, str) or not name or name in names or not isinstance(path, str):
            failure["error"] = "persistent tool RECORD contained an invalid or duplicate path"
            return failure
        names.add(name)
        member_path = Path(path)
        if name.endswith(".dist-info/RECORD"):
            record_paths.append(member_path)
            continue
        size = member.get("size")
        if (
            member.get("hash_mode") != "sha256"
            or not isinstance(member.get("hash_value"), str)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
        ):
            failure["error"] = f"persistent tool RECORD entry was not SHA-256 bound: {name}"
            return failure
        total_bytes += size
        if total_bytes > PYTHON_DISTRIBUTION_MAX_BYTES:
            failure["error"] = "persistent tool RECORD byte total exceeded its limit"
            return failure
        actual = _record_bound_sha256(member_path, expected_size=size)
        expected = _urlsafe_sha256_digest(cast(str, member["hash_value"]))
        if actual is None or expected is None or not hmac.compare_digest(actual, expected):
            failure["error"] = f"persistent tool distribution file hash mismatch: {name}"
            return failure
        closure_inputs.append((name, size, actual))
    if len(record_paths) != 1:
        failure["error"] = "persistent tool distribution had no unique RECORD file"
        return failure
    try:
        record_size = record_paths[0].lstat().st_size
    except OSError:
        record_size = -1
    record_sha256 = _record_bound_sha256(record_paths[0], expected_size=record_size)
    if record_sha256 is None:
        failure["error"] = "persistent tool RECORD file was missing"
        return failure
    closure_hash = hashlib.sha256()
    for name, size, digest in sorted(closure_inputs):
        encoded = name.encode("utf-8")
        closure_hash.update(len(encoded).to_bytes(8, "big"))
        closure_hash.update(encoded)
        closure_hash.update(size.to_bytes(8, "big"))
        closure_hash.update(bytes.fromhex(digest))
    closure_hash.update(bytes.fromhex(record_sha256))
    return {
        "record_sha256": record_sha256,
        "runtime_closure_sha256": closure_hash.hexdigest(),
        "runtime_file_count": len(closure_inputs),
        "runtime_bytes": total_bytes,
        "runtime_closure_verified": True,
        "error": None,
    }


def _distribution_contains_executable(
    distribution: metadata.Distribution,
    files: list[metadata.PackagePath],
    executable: Path,
) -> bool:
    """Return whether a distribution RECORD owns the exact console launcher path."""
    for member in files:
        try:
            candidate = Path(str(distribution.locate_file(member))).resolve(strict=True)
        except OSError:
            continue
        if candidate == executable:
            return True
    return False


def _distribution_direct_url(distribution: metadata.Distribution) -> dict[str, Any] | None:
    """Read PEP 610 provenance without trusting malformed metadata."""
    try:
        raw = distribution.read_text("direct_url.json")
    except (OSError, UnicodeDecodeError):
        return None
    if raw is None:
        return None
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return cast(dict[str, Any], decoded) if isinstance(decoded, dict) else None


def _verify_distribution_record_closure(
    distribution: metadata.Distribution,
) -> dict[str, Any]:
    """Verify every installed wheel file against RECORD and digest the exact closure."""
    files = distribution.files
    failure: dict[str, Any] = {
        "record_sha256": None,
        "runtime_closure_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "runtime_closure_verified": False,
        "error": None,
    }
    if files is None or not files or len(files) > PYTHON_DISTRIBUTION_MAX_FILES:
        failure["error"] = "Python distribution RECORD file list was missing or exceeded its limit"
        return failure
    normalized_names: set[str] = set()
    record_members: list[metadata.PackagePath] = []
    total_bytes = 0
    closure_inputs: list[tuple[str, int, str]] = []
    for member in files:
        normalized = str(member).replace("\\", "/")
        if normalized in normalized_names:
            failure["error"] = "Python distribution RECORD contained duplicate paths"
            return failure
        normalized_names.add(normalized)
        if normalized.endswith(".dist-info/RECORD"):
            record_members.append(member)
            continue
        expected_hash = member.hash
        expected_size = member.size
        if (
            expected_hash is None
            or expected_hash.mode != "sha256"
            or expected_size is None
            or expected_size < 0
        ):
            failure["error"] = (
                f"Python distribution RECORD entry was not SHA-256 bound: {normalized}"
            )
            return failure
        total_bytes += expected_size
        if total_bytes > PYTHON_DISTRIBUTION_MAX_BYTES:
            failure["error"] = "Python distribution RECORD byte total exceeded its limit"
            return failure
        path = Path(str(distribution.locate_file(member)))
        actual_hash = _record_bound_sha256(path, expected_size=expected_size)
        if actual_hash is None:
            failure["error"] = f"Python distribution file was missing or unstable: {normalized}"
            return failure
        expected_digest = _urlsafe_sha256_digest(expected_hash.value)
        if expected_digest is None or not hmac.compare_digest(actual_hash, expected_digest):
            failure["error"] = f"Python distribution RECORD hash mismatch: {normalized}"
            return failure
        closure_inputs.append((normalized, expected_size, actual_hash))
    if len(record_members) != 1:
        failure["error"] = "Python distribution had no unique RECORD file"
        return failure
    record_path = Path(str(distribution.locate_file(record_members[0])))
    try:
        record_size = record_path.lstat().st_size
    except OSError:
        record_size = -1
    record_sha256 = _record_bound_sha256(record_path, expected_size=record_size)
    if record_sha256 is None:
        failure["error"] = "Python distribution RECORD file was missing"
        return failure
    closure_hash = hashlib.sha256()
    for normalized, size_bytes, digest in sorted(closure_inputs):
        encoded = normalized.encode("utf-8")
        closure_hash.update(len(encoded).to_bytes(8, "big"))
        closure_hash.update(encoded)
        closure_hash.update(size_bytes.to_bytes(8, "big"))
        closure_hash.update(bytes.fromhex(digest))
    closure_hash.update(bytes.fromhex(record_sha256))
    return {
        "record_sha256": record_sha256,
        "runtime_closure_sha256": closure_hash.hexdigest(),
        "runtime_file_count": len(closure_inputs),
        "runtime_bytes": total_bytes,
        "runtime_closure_verified": True,
        "error": None,
    }


def _record_bound_sha256(path: Path, *, expected_size: int) -> str | None:
    """Hash one non-link regular distribution file and reject path replacement races."""
    try:
        before = path.lstat()
    except OSError:
        return None
    attributes = getattr(before, "st_file_attributes", 0)
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if (
        stat.S_ISLNK(before.st_mode)
        or not stat.S_ISREG(before.st_mode)
        or (reparse and attributes & reparse)
        or before.st_size != expected_size
    ):
        return None
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            ) != identity or not stat.S_ISREG(opened.st_mode):
                return None
            while chunk := stream.read(FILE_HASH_CHUNK_BYTES):
                digest.update(chunk)
    except OSError:
        return None
    try:
        after = path.lstat()
    except OSError:
        return None
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
        return None
    return digest.hexdigest()


def _urlsafe_sha256_digest(value: str) -> str | None:
    """Decode an unpadded wheel RECORD SHA-256 value to lowercase hex."""
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, TypeError):
        return None
    return decoded.hex() if len(decoded) == hashlib.sha256().digest_size else None


def _server_artifact_digest(server_artifact: dict[str, Any]) -> str:
    """Return the canonical discovery/execution artifact binding digest."""
    return hashlib.sha256(
        json.dumps(
            {"server_artifact": server_artifact},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _reject_verified_runtime_environment_remap(
    *,
    server_artifact: dict[str, Any],
    env_from: dict[str, str],
) -> None:
    """Keep a locked clio-kit child on the uv resolution identity just verified."""
    python_runtime = server_artifact.get("python_distribution_runtime")
    python_runtime_verified = (
        isinstance(python_runtime, dict)
        and cast(dict[str, Any], python_runtime).get("runtime_closure_verified") is True
    )
    nested_runtime = server_artifact.get("nested_runtime")
    nested_runtime_verified = (
        isinstance(nested_runtime, dict)
        and cast(dict[str, Any], nested_runtime).get("locked_runtime_verified") is True
    )
    if not python_runtime_verified and not nested_runtime_verified:
        return
    fixed_names = {
        "home",
        "homedrive",
        "homepath",
        "nodefaultcurrentdirectoryinexepath",
        "path",
        "pathext",
        "userprofile",
        "virtual_env",
        "xdg_cache_home",
        "xdg_config_home",
        "xdg_data_home",
        "xdg_state_home",
    }
    forbidden = sorted(
        child_name
        for child_name in env_from
        if (
            (
                python_runtime_verified
                and (
                    child_name.casefold() == "__pyvenv_launcher__"
                    or child_name.casefold().startswith("python")
                )
            )
            or (
                (python_runtime_verified or nested_runtime_verified)
                and (
                    child_name.casefold() in {"libpath", "shlib_path"}
                    or child_name.casefold().startswith(("dyld_", "ld_"))
                )
            )
            or (
                nested_runtime_verified
                and (
                    child_name.casefold() in fixed_names
                    or child_name.casefold().startswith(("clio_kit_", "python", "uv_"))
                )
            )
        )
    )
    if forbidden:
        raise ValueError(
            "verified MCP runtime cannot remap interpreter, native loader, or uv "
            "resolution environment through env_from"
        )


def _server_artifact_launch_executable(server_artifact: dict[str, Any]) -> str:
    """Return the exact executable path captured by server artifact inspection."""
    executable = server_artifact.get("executable")
    if isinstance(executable, dict):
        path = cast(dict[str, Any], executable).get("path")
        if isinstance(path, str) and path:
            return path
    if server_artifact.get("verified") is True:
        raise ValueError("verified MCP server artifact omitted its executable path")
    resolved = server_artifact.get("resolved_executable")
    if not isinstance(resolved, str) or not resolved:
        raise ValueError("MCP server artifact omitted its resolved executable")
    return resolved


def _nested_clio_kit_server_name(
    server_args: list[str],
    *,
    python_distribution_runtime: dict[str, Any] | None,
) -> str | None:
    """Return the embedded server selected through clio-kit's child launcher."""
    for index, argument in enumerate(server_args[:-1]):
        if argument != "--from":
            continue
        command = server_args[index + 2 :]
        if (
            len(command) >= 3
            and command[0] == "clio-kit"
            and command[1] == "mcp-server"
            and command[2]
        ):
            return command[2]
        return None
    if (
        len(server_args) >= 2
        and server_args[0] == "mcp-server"
        and bool(server_args[1])
        and python_distribution_runtime is not None
        and str(python_distribution_runtime.get("distribution", "")).lower().replace("_", "-")
        == "clio-kit"
        and python_distribution_runtime.get("entry_point") == "clio-kit"
        and python_distribution_runtime.get("runtime_closure_verified") is True
    ):
        return server_args[1]
    return None


def _installed_clio_kit_runtime_identity(
    distribution_runtime: dict[str, Any] | None,
    *,
    server_name: str,
    resolved_executable: Path,
    verify_relay_jarvis_cd_lock: bool,
) -> dict[str, Any]:
    """Verify clio-kit's locked child launcher from a persistent tool environment."""
    uv_identity = _file_identity(Path(_resolve_executable("uv")).expanduser())
    evidence: dict[str, Any] = {
        "schema_version": _CLIO_KIT_LOCKED_SERVER_SCHEMA,
        "server_name": server_name,
        "runtime_policy": _CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY,
        "project_sha256": None,
        "lock_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "contract_source_verified": False,
        "uv_executable": uv_identity,
        "persistent_tool": True,
        "locked_runtime_verified": False,
        "error": None,
    }
    if (
        distribution_runtime is None
        or str(distribution_runtime.get("distribution", "")).lower().replace("_", "-") != "clio-kit"
        or distribution_runtime.get("entry_point") != "clio-kit"
        or distribution_runtime.get("runtime_closure_verified") is not True
    ):
        evidence["error"] = "persistent clio-kit distribution closure is unverified"
        return evidence
    source_value = distribution_runtime.get("contract_source_path")
    lock_paths = distribution_runtime.get("server_lock_paths")
    lock_value = (
        cast(dict[str, Any], lock_paths).get(server_name) if isinstance(lock_paths, dict) else None
    )
    if not isinstance(source_value, str) or not isinstance(lock_value, str):
        evidence["error"] = "persistent clio-kit tool omitted launcher or server lock files"
        return evidence
    source_path = Path(source_value)
    lock_path = Path(lock_value)
    source = _bounded_regular_file_bytes(
        source_path,
        max_bytes=CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES,
    )
    lock = _bounded_regular_file_bytes(
        lock_path,
        max_bytes=CLIO_KIT_LOCK_MAX_BYTES,
    )
    lock_identity = _file_identity(lock_path)
    if source is None or lock is None or lock_identity is None:
        evidence["error"] = "persistent clio-kit launcher or lock file is unavailable"
        return evidence
    try:
        launcher_source = source.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        evidence["error"] = "persistent clio-kit launcher source is not UTF-8"
        return evidence
    contract_source_verified = all(
        marker in launcher_source
        for marker in (
            f'LOCKED_SERVER_LAUNCH_SCHEMA = "{_CLIO_KIT_LOCKED_SERVER_SCHEMA}"',
            f'_LOCKED_SERVER_RUNTIME_POLICY = "{_CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY}"',
            '"--no-dev"',
            '"--no-editable"',
            '"--frozen"',
            "locked_server_project_identity",
            "materialize_locked_server_project",
            "UV_PROJECT_ENVIRONMENT",
        )
    )
    project_sha256 = distribution_runtime.get("runtime_closure_sha256")
    runtime_file_count = distribution_runtime.get("runtime_file_count")
    runtime_bytes = distribution_runtime.get("runtime_bytes")
    jarvis_cd_lock_binding = (
        _jarvis_cd_lock_binding(lock)
        if server_name == "jarvis" and verify_relay_jarvis_cd_lock
        else None
    )
    locked = (
        contract_source_verified
        and uv_identity is not None
        and isinstance(project_sha256, str)
        and _is_sha256_text(project_sha256)
        and isinstance(runtime_file_count, int)
        and not isinstance(runtime_file_count, bool)
        and runtime_file_count > 0
        and isinstance(runtime_bytes, int)
        and not isinstance(runtime_bytes, bool)
        and runtime_bytes > 0
    )
    evidence.update(
        {
            "project_sha256": project_sha256,
            "lock_sha256": lock_identity.get("sha256"),
            "runtime_file_count": runtime_file_count,
            "runtime_bytes": runtime_bytes,
            "contract_source_verified": contract_source_verified,
            "locked_runtime_verified": locked,
            "error": (
                None
                if locked
                else "persistent clio-kit launcher contract, lock, or uv executable is unverified"
            ),
        }
    )
    if jarvis_cd_lock_binding is not None:
        evidence["jarvis_cd_lock_binding"] = jarvis_cd_lock_binding
    return evidence


def _is_sha256_text(value: object) -> bool:
    """Return whether a value is one lowercase hexadecimal SHA-256 digest."""
    return (
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def _bounded_regular_file_bytes(path: Path, *, max_bytes: int) -> bytes | None:
    """Read one stable non-link regular file under an explicit byte limit."""
    snapshot = _bounded_regular_file_snapshot(path, max_bytes=max_bytes)
    return snapshot[0] if snapshot is not None else None


def _bounded_regular_file_snapshot(
    path: Path,
    *,
    max_bytes: int,
) -> tuple[bytes, tuple[int, int, int, int]] | None:
    """Read one stable regular file and return the descriptor identity read."""
    try:
        before = path.lstat()
    except OSError:
        return None
    if (
        stat.S_ISLNK(before.st_mode)
        or not stat.S_ISREG(before.st_mode)
        or before.st_size < 0
        or before.st_size > max_bytes
    ):
        return None
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            ) != identity:
                return None
            payload = stream.read(max_bytes + 1)
    except OSError:
        return None
    if len(payload) != before.st_size:
        return None
    try:
        after = path.lstat()
    except OSError:
        return None
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
        return None
    return payload, identity


def _locked_clio_kit_runtime_identity(
    install_artifact: dict[str, Any] | None,
    *,
    server_name: str,
    resolved_executable: Path,
    verify_relay_jarvis_cd_lock: bool,
) -> dict[str, Any]:
    """Verify the locked embedded project selected by a clio-kit wheel."""
    wheel_path = (
        Path(str(install_artifact["path"]))
        if install_artifact is not None and isinstance(install_artifact.get("path"), str)
        else None
    )
    uv_identity = _file_identity(Path(_resolve_executable("uv")).expanduser())
    evidence: dict[str, Any] = {
        "schema_version": _CLIO_KIT_LOCKED_SERVER_SCHEMA,
        "server_name": server_name,
        "runtime_policy": _CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY,
        "project_sha256": None,
        "lock_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "contract_source_verified": False,
        "uv_executable": uv_identity,
        "locked_runtime_verified": False,
        "error": None,
    }
    if wheel_path is None or wheel_path.suffix.lower() != ".whl":
        evidence["error"] = "nested clio-kit runtime requires an exact wheel file"
        return evidence
    try:
        with _verified_wheel_archive(wheel_path, install_artifact) as wheel:
            members = _validated_wheel_members(wheel)
            launcher = members.get("clio_kit/__init__.py")
            if launcher is None or not _zip_member_is_regular(launcher):
                raise ValueError("clio-kit wheel has no unique launcher source")
            launcher_source = _read_bounded_zip_member(
                wheel,
                launcher.filename,
                max_bytes=CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES,
            ).decode("utf-8", errors="strict")
            contract_source_verified = all(
                marker in launcher_source
                for marker in (
                    f'LOCKED_SERVER_LAUNCH_SCHEMA = "{_CLIO_KIT_LOCKED_SERVER_SCHEMA}"',
                    (f'_LOCKED_SERVER_RUNTIME_POLICY = "{_CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY}"'),
                    '"--no-dev"',
                    '"--no-editable"',
                    '"--frozen"',
                    "locked_server_project_identity",
                    "materialize_locked_server_project",
                    "UV_PROJECT_ENVIRONMENT",
                )
            )
            suffix = f"/clio-kit-mcp-servers/{server_name}/uv.lock"
            lock_names = [
                name
                for name in members
                if name.endswith(suffix) or name == f"clio-kit-mcp-servers/{server_name}/uv.lock"
            ]
            if len(lock_names) != 1:
                raise ValueError("clio-kit wheel has no unique embedded server lock")
            lock_name = lock_names[0]
            prefix = lock_name[: -len("uv.lock")]
            inputs = _clio_kit_runtime_project_members(
                members,
                prefix=prefix,
                server_name=server_name,
            )
            digest = hashlib.sha256()
            policy = _CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY.encode("utf-8")
            digest.update(len(policy).to_bytes(8, "big"))
            digest.update(policy)
            digest.update(len(inputs).to_bytes(8, "big"))
            project_bytes = 0
            lock_sha256: str | None = None
            lock_content: bytes | None = None
            for relative, member in inputs:
                encoded = relative.encode("utf-8")
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
                content_digest = hashlib.sha256()
                content_length = 0
                for chunk in _bounded_zip_member_chunks(
                    wheel,
                    member.filename,
                    max_bytes=CLIO_KIT_WHEEL_MAX_PROJECT_BYTES,
                ):
                    project_bytes += len(chunk)
                    if project_bytes > CLIO_KIT_WHEEL_MAX_PROJECT_BYTES:
                        raise ValueError("clio-kit embedded project exceeded its byte limit")
                    content_length += len(chunk)
                    content_digest.update(chunk)
                digest.update(content_length.to_bytes(8, "big"))
                digest.update(content_digest.digest())
                if relative == "uv.lock":
                    lock_sha256 = content_digest.hexdigest()
                    lock_content = _read_bounded_zip_member(
                        wheel,
                        member.filename,
                        max_bytes=CLIO_KIT_LOCK_MAX_BYTES,
                    )
            if lock_sha256 is None:
                raise ValueError("clio-kit embedded server project has no lock digest")
            if lock_content is None:
                raise ValueError("clio-kit embedded server project has no readable lock")
    except (
        NotImplementedError,
        OSError,
        RuntimeError,
        UnicodeDecodeError,
        ValueError,
        zipfile.BadZipFile,
    ) as exc:
        evidence["error"] = f"could not verify locked clio-kit runtime: {exc}"
        return evidence
    jarvis_cd_lock_binding = (
        _jarvis_cd_lock_binding(lock_content)
        if server_name == "jarvis" and verify_relay_jarvis_cd_lock
        else None
    )
    locked = contract_source_verified and uv_identity is not None
    evidence.update(
        {
            "project_sha256": digest.hexdigest(),
            "lock_sha256": lock_sha256,
            "runtime_file_count": len(inputs),
            "runtime_bytes": project_bytes,
            "contract_source_verified": contract_source_verified,
            "locked_runtime_verified": locked,
            "error": (
                None
                if locked
                else "clio-kit locked launcher contract or uv executable is unverified"
            ),
        }
    )
    if jarvis_cd_lock_binding is not None:
        evidence["jarvis_cd_lock_binding"] = jarvis_cd_lock_binding
    return evidence


def _jarvis_cd_lock_binding(lock_content: bytes) -> dict[str, Any]:
    """Verify the unique JARVIS-CD wheel selected by one embedded uv lock."""
    evidence: dict[str, Any] = {
        "schema_version": _JARVIS_CD_LOCK_BINDING_SCHEMA,
        "dependency": "jarvis-cd",
        "expected_version": JARVIS_CD_VERSION,
        "expected_url": JARVIS_CD_WHEEL_URL,
        "expected_sha256": JARVIS_CD_WHEEL_SHA256,
        "jarvis_mcp_package_entry_count": 0,
        "resolved_dependency_entry_count": 0,
        "observed_resolved_dependency_entries": [],
        "metadata_requirement_entry_count": 0,
        "observed_metadata_requirement_entries": [],
        "observed_metadata_requirement_urls": [],
        "package_entry_count": 0,
        "wheel_entry_count": 0,
        "observed_version": None,
        "observed_source_url": None,
        "observed_wheel_url": None,
        "observed_wheel_sha256": None,
        "verified": False,
        "error": None,
    }
    try:
        document = tomllib.loads(lock_content.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        evidence["error"] = f"clio-kit JARVIS uv.lock is invalid: {exc}"
        return evidence
    raw_packages = document.get("package")
    if not isinstance(raw_packages, list):
        evidence["error"] = "clio-kit JARVIS uv.lock omitted package records"
        return evidence
    package_records = [
        cast(dict[str, Any], value)
        for value in cast(list[object], raw_packages)
        if isinstance(value, dict)
    ]
    jarvis_mcp_packages = [
        value
        for value in package_records
        if _normalized_distribution_name(value.get("name")) == "jarvis-mcp"
    ]
    evidence["jarvis_mcp_package_entry_count"] = len(jarvis_mcp_packages)
    if len(jarvis_mcp_packages) != 1:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock must contain exactly one jarvis-mcp package record"
        )
        return evidence
    raw_dependencies = jarvis_mcp_packages[0].get("dependencies")
    dependencies = (
        cast(list[object], raw_dependencies) if isinstance(raw_dependencies, list) else []
    )
    jarvis_cd_dependencies = [
        cast(dict[str, Any], value)
        for value in dependencies
        if isinstance(value, dict)
        and _normalized_distribution_name(cast(dict[str, Any], value).get("name")) == "jarvis-cd"
    ]
    evidence["resolved_dependency_entry_count"] = len(jarvis_cd_dependencies)
    evidence["observed_resolved_dependency_entries"] = [
        _lock_entry_evidence(value, expected_fields=("name",)) for value in jarvis_cd_dependencies
    ]
    if len(jarvis_cd_dependencies) != 1:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp must resolve exactly one direct "
            "jarvis-cd dependency"
        )
        return evidence
    if jarvis_cd_dependencies[0] != {"name": "jarvis-cd"}:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp resolved jarvis-cd dependency must be unconditional"
        )
        return evidence
    raw_metadata = jarvis_mcp_packages[0].get("metadata")
    metadata_value = cast(dict[str, Any], raw_metadata) if isinstance(raw_metadata, dict) else None
    raw_requirements = metadata_value.get("requires-dist") if metadata_value is not None else None
    requirements = (
        cast(list[object], raw_requirements) if isinstance(raw_requirements, list) else []
    )
    jarvis_cd_requirements = [
        cast(dict[str, Any], value)
        for value in requirements
        if isinstance(value, dict)
        and _normalized_distribution_name(cast(dict[str, Any], value).get("name")) == "jarvis-cd"
    ]
    evidence["metadata_requirement_entry_count"] = len(jarvis_cd_requirements)
    evidence["observed_metadata_requirement_entries"] = [
        _lock_entry_evidence(value, expected_fields=("name", "url"))
        for value in jarvis_cd_requirements
    ]
    evidence["observed_metadata_requirement_urls"] = [
        _safe_observed_lock_text(value.get("url")) for value in jarvis_cd_requirements
    ]
    if len(jarvis_cd_requirements) != 1:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp metadata must contain exactly one "
            "jarvis-cd requirement"
        )
        return evidence
    if jarvis_cd_requirements[0].get("url") != JARVIS_CD_WHEEL_URL:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp metadata jarvis-cd URL does not match relay pin"
        )
        return evidence
    if jarvis_cd_requirements[0] != {
        "name": "jarvis-cd",
        "url": JARVIS_CD_WHEEL_URL,
    }:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-mcp metadata jarvis-cd requirement "
            "must be an unconditional direct URL"
        )
        return evidence
    packages = [
        value
        for value in package_records
        if _normalized_distribution_name(value.get("name")) == "jarvis-cd"
    ]
    evidence["package_entry_count"] = len(packages)
    if len(packages) != 1:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock must contain exactly one jarvis-cd package record"
        )
        return evidence
    package = packages[0]
    version = package.get("version")
    evidence["observed_version"] = _safe_observed_lock_text(version)
    raw_source = package.get("source")
    source = cast(dict[str, Any], raw_source) if isinstance(raw_source, dict) else None
    source_url = source.get("url") if source is not None else None
    evidence["observed_source_url"] = _safe_observed_lock_text(source_url)
    raw_wheels = package.get("wheels")
    wheels = cast(list[object], raw_wheels) if isinstance(raw_wheels, list) else []
    evidence["wheel_entry_count"] = len(wheels)
    if len(wheels) == 1 and isinstance(wheels[0], dict):
        wheel = cast(dict[str, Any], wheels[0])
        wheel_url = wheel.get("url")
        wheel_hash = wheel.get("hash")
        evidence["observed_wheel_url"] = _safe_observed_lock_text(wheel_url)
        if isinstance(wheel_hash, str) and wheel_hash.startswith("sha256:"):
            evidence["observed_wheel_sha256"] = wheel_hash.removeprefix("sha256:")
    if version != JARVIS_CD_VERSION:
        evidence["error"] = "clio-kit JARVIS uv.lock jarvis-cd version does not match relay pin"
        return evidence
    if not isinstance(source_url, str) or source_url != JARVIS_CD_WHEEL_URL:
        evidence["error"] = "clio-kit JARVIS uv.lock jarvis-cd source URL does not match relay pin"
        return evidence
    if len(wheels) != 1 or not isinstance(wheels[0], dict):
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-cd must contain exactly one wheel record"
        )
        return evidence
    wheel_url = evidence["observed_wheel_url"]
    if wheel_url != source_url:
        evidence["error"] = "clio-kit JARVIS uv.lock jarvis-cd source and wheel URLs do not match"
        return evidence
    if wheel_url != JARVIS_CD_WHEEL_URL:
        evidence["error"] = "clio-kit JARVIS uv.lock jarvis-cd wheel URL does not match relay pin"
        return evidence
    wheel_sha256 = evidence["observed_wheel_sha256"]
    if wheel_sha256 != JARVIS_CD_WHEEL_SHA256:
        evidence["error"] = (
            "clio-kit JARVIS uv.lock jarvis-cd wheel SHA-256 does not match relay pin"
        )
        return evidence
    evidence["verified"] = True
    return evidence


def _lock_entry_evidence(
    value: dict[str, Any],
    *,
    expected_fields: tuple[str, ...],
) -> dict[str, Any]:
    """Project one TOML table into bounded, always-JSON-safe lock evidence."""
    evidence: dict[str, Any] = {}
    for field_name in expected_fields:
        if field_name in value:
            evidence[field_name] = _safe_observed_lock_text(value[field_name])
    unexpected_fields = sorted(set(value).difference(expected_fields))
    if unexpected_fields:
        evidence["unexpected_field_count"] = len(unexpected_fields)
        evidence["unexpected_fields"] = unexpected_fields[:32]
    return evidence


def _safe_observed_lock_text(value: object) -> str | None:
    """Keep expected lock text verbatim and safely identify every other TOML type."""
    if value is None or isinstance(value, str):
        return value
    return f"<invalid TOML {type(value).__name__}>"


def _normalized_distribution_name(value: object) -> str | None:
    """Return the normalized distribution name used by Python package metadata."""
    if not isinstance(value, str) or not value:
        return None
    return re.sub(r"[-_.]+", "-", value).lower()


def _require_locked_jarvis_cd_binding(
    server_artifact: dict[str, Any],
    *,
    expected: dict[str, str],
) -> None:
    """Refuse the built-in locked clio-kit JARVIS server when its JARVIS pin drifts."""
    raw_runtime = server_artifact.get("nested_runtime")
    if not isinstance(raw_runtime, dict):
        raise ValueError("built-in JARVIS MCP server omitted locked clio-kit runtime evidence")
    runtime = cast(dict[str, Any], raw_runtime)
    if runtime.get("server_name") != "jarvis":
        raise ValueError("built-in JARVIS MCP server did not select the locked jarvis runtime")
    raw_binding = runtime.get("jarvis_cd_lock_binding")
    binding = cast(dict[str, Any], raw_binding) if isinstance(raw_binding, dict) else None
    if (
        server_artifact.get("verified") is True
        and runtime.get("schema_version") == "clio-kit.locked-server.v4"
        and runtime.get("locked_runtime_verified") is True
        and binding is not None
        and binding.get("schema_version") == _JARVIS_CD_LOCK_BINDING_SCHEMA
        and binding.get("dependency") == "jarvis-cd"
        and binding.get("verified") is True
        and binding.get("error") is None
        and binding.get("expected_version") == expected["version"]
        and binding.get("expected_url") == expected["url"]
        and binding.get("expected_sha256") == expected["sha256"]
        and binding.get("observed_version") == expected["version"]
        and binding.get("observed_source_url") == expected["url"]
        and binding.get("observed_wheel_url") == expected["url"]
        and binding.get("observed_wheel_sha256") == expected["sha256"]
        and binding.get("resolved_dependency_entry_count") == 1
        and binding.get("observed_resolved_dependency_entries") == [{"name": "jarvis-cd"}]
        and binding.get("jarvis_mcp_package_entry_count") == 1
        and binding.get("metadata_requirement_entry_count") == 1
        and binding.get("observed_metadata_requirement_entries")
        == [{"name": "jarvis-cd", "url": expected["url"]}]
        and binding.get("observed_metadata_requirement_urls") == [expected["url"]]
        and binding.get("package_entry_count") == 1
        and binding.get("wheel_entry_count") == 1
    ):
        return
    if server_artifact.get("verified") is not True:
        reason = (
            server_artifact.get("identity_error")
            or server_artifact.get("error")
            or "outer MCP server artifact did not verify"
        )
    elif runtime.get("schema_version") != "clio-kit.locked-server.v4":
        reason = "locked clio-kit runtime schema did not verify"
    elif runtime.get("locked_runtime_verified") is not True:
        reason = runtime.get("error") or "locked clio-kit launcher/runtime did not verify"
    elif binding is None:
        reason = "JARVIS-CD lock binding evidence is missing"
    else:
        reason = binding.get("error") or "JARVIS-CD lock binding evidence did not match"
    raise ValueError(
        f"built-in locked clio-kit JARVIS MCP has an unverified jarvis-cd dependency: {reason}"
    )


def _jarvis_cd_lock_expectation(value: object) -> dict[str, str] | None:
    """Validate the explicit built-in JARVIS dependency expectation from the relay spec."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("expected_jarvis_cd_lock_binding must be an object")
    typed = cast(dict[object, object], value)
    expected = {
        "schema_version": "clio-relay.jarvis-cd-lock-expectation.v1",
        "version": JARVIS_CD_VERSION,
        "url": JARVIS_CD_WHEEL_URL,
        "sha256": JARVIS_CD_WHEEL_SHA256,
    }
    if typed != expected:
        raise ValueError("expected_jarvis_cd_lock_binding does not match the relay release pin")
    return expected


@contextmanager
def _verified_wheel_archive(
    path: Path,
    artifact: dict[str, Any] | None,
) -> Generator[zipfile.ZipFile]:
    """Open the exact hashed regular wheel and reject replacement during inspection."""
    expected_sha256 = artifact.get("sha256") if artifact is not None else None
    expected_size = artifact.get("size_bytes") if artifact is not None else None
    if not isinstance(expected_sha256, str) or not isinstance(expected_size, int):
        raise ValueError("clio-kit wheel identity is incomplete")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
            raise ValueError("clio-kit wheel changed before runtime verification")
        identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        digest = hashlib.sha256()
        while chunk := stream.read(FILE_HASH_CHUNK_BYTES):
            digest.update(chunk)
        if not hmac.compare_digest(digest.hexdigest(), expected_sha256):
            raise ValueError("clio-kit wheel changed before runtime verification")
        stream.seek(0)
        with zipfile.ZipFile(stream) as archive:
            yield archive
        after = os.fstat(stream.fileno())
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
            raise ValueError("clio-kit wheel changed during runtime verification")


def _validated_wheel_members(archive: zipfile.ZipFile) -> dict[str, zipfile.ZipInfo]:
    """Return unique, normalized wheel members after bounded path validation."""
    infos = archive.infolist()
    if len(infos) > CLIO_KIT_WHEEL_MAX_FILES:
        raise ValueError("clio-kit wheel exceeded its file-count limit")
    members: dict[str, zipfile.ZipInfo] = {}
    for info in infos:
        name = info.filename
        if info.flag_bits & 0x1:
            raise ValueError("clio-kit wheel contains an encrypted member")
        if not name or "\x00" in name or "\\" in name:
            raise ValueError("clio-kit wheel contains an unsafe member path")
        path_text = name[:-1] if info.is_dir() else name
        path = PurePosixPath(path_text)
        first_part = path.parts[0] if path.parts else ""
        if (
            not path_text
            or path_text.startswith("/")
            or path.as_posix() != path_text
            or any(part in {"", ".", ".."} for part in path.parts)
            or (len(first_part) >= 2 and first_part[1] == ":")
        ):
            raise ValueError(f"clio-kit wheel contains an unsafe member path: {name}")
        if name in members:
            raise ValueError("clio-kit wheel contains duplicate member names")
        members[name] = info
    return members


def _clio_kit_runtime_project_members(
    members: dict[str, zipfile.ZipInfo],
    *,
    prefix: str,
    server_name: str,
) -> list[tuple[str, zipfile.ZipInfo]]:
    """Select the exact bounded project file set used by clio-kit's v4 launcher."""
    inputs: list[tuple[str, zipfile.ZipInfo]] = []
    relative_files: set[str] = set()
    casefolded_files: set[str] = set()
    declared_bytes = 0
    for name, member in members.items():
        if not name.startswith(prefix) or name == prefix:
            continue
        relative = name[len(prefix) :]
        relative_path = PurePosixPath(relative.rstrip("/"))
        if any(part in _CLIO_KIT_RUNTIME_PROJECT_EXCLUDED_NAMES for part in relative_path.parts):
            continue
        if member.is_dir():
            if not _zip_member_is_directory(member):
                raise ValueError(
                    f"clio-kit embedded server project contains a non-directory: {relative}"
                )
            continue
        if not _zip_member_is_regular(member):
            raise ValueError(
                f"clio-kit embedded server project contains a non-regular file: {relative}"
            )
        if relative in relative_files or relative.casefold() in casefolded_files:
            raise ValueError("clio-kit embedded server project contains colliding paths")
        relative_files.add(relative)
        casefolded_files.add(relative.casefold())
        declared_bytes += member.file_size
        inputs.append((relative, member))
        if (
            len(inputs) > CLIO_KIT_WHEEL_MAX_PROJECT_FILES
            or declared_bytes > CLIO_KIT_WHEEL_MAX_PROJECT_BYTES
        ):
            raise ValueError("clio-kit embedded project exceeded its materialization bound")
    for relative in relative_files:
        parent = PurePosixPath(relative).parent
        while parent != PurePosixPath("."):
            if parent.as_posix() in relative_files:
                raise ValueError("clio-kit embedded server project contains colliding paths")
            parent = parent.parent
    if not {"pyproject.toml", "uv.lock"}.issubset(relative_files):
        raise ValueError(f"clio-kit embedded server project is incomplete: {server_name}")
    return sorted(inputs, key=lambda item: item[0])


def _zip_member_is_regular(member: zipfile.ZipInfo) -> bool:
    """Return whether one ZIP member represents a regular file."""
    if member.is_dir():
        return False
    file_type = stat.S_IFMT((member.external_attr >> 16) & 0xFFFF)
    return file_type in {0, stat.S_IFREG}


def _zip_member_is_directory(member: zipfile.ZipInfo) -> bool:
    """Return whether one ZIP directory entry has a compatible file mode."""
    if not member.is_dir():
        return False
    file_type = stat.S_IFMT((member.external_attr >> 16) & 0xFFFF)
    return file_type in {0, stat.S_IFDIR}


def _file_identity(path: Path) -> dict[str, Any] | None:
    try:
        resolved = path.resolve(strict=True)
        if not resolved.is_file():
            return None
        digest = _sha256_file(resolved)
        size_bytes = resolved.stat().st_size
    except OSError:
        return None
    return {
        "path": str(resolved),
        "filename": resolved.name,
        "sha256": digest,
        "size_bytes": size_bytes,
    }


def _sha256_file(path: Path) -> str:
    """Hash one file with fixed memory use."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(FILE_HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _read_bounded_zip_member(
    archive: zipfile.ZipFile,
    name: str,
    *,
    max_bytes: int,
) -> bytes:
    """Read one small wheel member after enforcing its decompressed limit."""
    return b"".join(_bounded_zip_member_chunks(archive, name, max_bytes=max_bytes))


def _bounded_zip_member_chunks(
    archive: zipfile.ZipFile,
    name: str,
    *,
    max_bytes: int,
) -> Iterator[bytes]:
    """Read a wheel member in bounded chunks and reject decompression growth."""
    info = archive.getinfo(name)
    if info.file_size > max_bytes:
        raise ValueError(f"wheel member exceeded its byte limit: {name}")
    observed = 0
    with archive.open(info, "r") as stream:
        while chunk := stream.read(min(FILE_HASH_CHUNK_BYTES, max_bytes - observed + 1)):
            observed += len(chunk)
            if observed > max_bytes:
                raise ValueError(f"wheel member exceeded its byte limit: {name}")
            yield chunk
    if observed != info.file_size:
        raise ValueError(f"wheel member size did not match its directory record: {name}")


def _install_spec_source(install_spec: str | None) -> str | None:
    if install_spec is None:
        return None
    candidate = Path(install_spec).expanduser()
    if candidate.suffix.lower() == ".whl" and candidate.is_file():
        return "wheel"
    package, separator, version = install_spec.rpartition("==")
    if separator and package and version and not any(char.isspace() for char in install_spec):
        return "pypi"
    return "unverified"


def _run_mcp_session(
    command: list[str],
    *,
    tool: str | None,
    arguments: dict[str, Any],
    timeout: int | None,
    operation: str = "tools/call",
    env_from: dict[str, str] | None = None,
    progress_bridge: _McpProgressBridge | None = None,
    jarvis_input_manifest: dict[str, Any] | None = None,
) -> subprocess.CompletedProcess[str]:
    process = _open_process(command, env_from=env_from or {})
    previous_handlers = _install_parent_termination_handlers(process)
    stdout_queue: Queue[_StreamEvent] = Queue()
    stderr_queue: Queue[_StreamEvent] = Queue()
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    stdout_thread = _start_reader(
        process.stdout,
        stdout_queue,
        stream_name="stdout",
        max_bytes=MCP_SESSION_MAX_STDOUT_BYTES,
    )
    stderr_thread = _start_reader(
        process.stderr,
        stderr_queue,
        stream_name="stderr",
        max_bytes=MCP_SESSION_MAX_STDERR_BYTES,
    )
    started_at = time.monotonic()
    deadline = None if timeout is None else started_at + timeout
    try:
        _write_message(process, _initialize_message())
        _wait_for_response(
            stdout_queue,
            "clio-relay-mcp-init",
            stdout_lines,
            process=process,
            deadline=deadline,
            command=command,
            response_bytes=[0],
            max_response_bytes=MCP_INITIALIZE_MAX_RESPONSE_BYTES,
            response_label="initialize",
        )
        _write_message(process, _initialized_message())
        if operation == "tools/call":
            if jarvis_input_manifest is not None:
                _run_jarvis_input_reconciliation(
                    process,
                    stdout_queue,
                    stdout_lines,
                    manifest=jarvis_input_manifest,
                    deadline=deadline,
                    command=command,
                )
            request = _call_message(
                tool=_required_optional_str(tool, "tool"),
                arguments=arguments,
                progress_token=(
                    progress_bridge.progress_token if progress_bridge is not None else None
                ),
            )
            _write_message(process, request)
            _wait_for_response(
                stdout_queue,
                _response_id(operation),
                stdout_lines,
                process=process,
                deadline=deadline,
                command=command,
                response_bytes=[0],
                max_response_bytes=MCP_CALL_MAX_RESPONSE_BYTES,
                response_label="tools/call",
                notification_handler=(
                    progress_bridge.observe if progress_bridge is not None else None
                ),
            )
        else:
            _run_bounded_tools_list(
                process,
                stdout_queue,
                stdout_lines,
                deadline=deadline,
                command=command,
            )
        if process.stdin is not None:
            process.stdin.close()
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        process.wait(timeout=remaining)
    except _McpProtocolFailure as exc:
        stdout_lines.append(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": _response_id(operation),
                    "error": {"code": -32000, "message": str(exc)},
                },
                separators=(",", ":"),
            )
            + "\n"
        )
        if process.stdin is not None:
            process.stdin.close()
        _terminate_process_tree(process)
    except subprocess.TimeoutExpired as exc:
        _terminate_process_tree(process)
        _drain_available(stdout_queue, stdout_lines)
        _drain_available(stderr_queue, stderr_lines)
        raise subprocess.TimeoutExpired(
            command,
            timeout if timeout is not None else 0,
            output="".join(stdout_lines) or exc.output,
            stderr="".join(stderr_lines) or exc.stderr,
        ) from exc
    finally:
        _restore_parent_termination_handlers(previous_handlers)
        if process.poll() is None:
            _terminate_process_tree(process)
        _join_reader(stdout_thread, stdout_queue, stdout_lines)
        _join_reader(stderr_thread, stderr_queue, stderr_lines)
    return subprocess.CompletedProcess(
        command,
        process.returncode if process.returncode is not None else 0,
        stdout="".join(stdout_lines),
        stderr="".join(stderr_lines),
    )


def _run_jarvis_input_reconciliation(
    process: subprocess.Popen[str],
    stdout_queue: Queue[_StreamEvent],
    stdout_lines: list[str],
    *,
    manifest: dict[str, Any],
    deadline: float | None,
    command: list[str],
) -> None:
    """Materialize an admitted input manifest before the final jarvis_run call."""
    route = cast(dict[str, Any], manifest["route"])
    pipeline_id = cast(str, route["pipeline_id"])
    configs: dict[str, dict[str, str]] = {}
    for raw_resolution in cast(list[dict[str, Any]], manifest["resolutions"]):
        binding = cast(dict[str, Any], raw_resolution["binding"])
        step_id = cast(str, binding["step_id"])
        setting = cast(str, binding["canonical_setting"])
        remote_path = cast(str, binding["remote_path"])
        step_config = configs.setdefault(step_id, {})
        if setting in step_config:
            raise _McpProtocolFailure(
                "JARVIS input manifest repeated one step setting during materialization"
            )
        step_config[setting] = remote_path
    response_bytes = [0]
    for index, step_id in enumerate(sorted(configs), start=1):
        response_id = f"clio-relay-mcp-input-reconcile-{index}"
        _write_message(
            process,
            _call_message(
                tool="jarvis_edit_step",
                arguments={
                    "pipeline_id": pipeline_id,
                    "step_id": step_id,
                    "config": configs[step_id],
                    "operation": "edit",
                },
                response_id=response_id,
            ),
        )
        response = _wait_for_response(
            stdout_queue,
            response_id,
            stdout_lines,
            process=process,
            deadline=deadline,
            command=command,
            response_bytes=response_bytes,
            max_response_bytes=MCP_CALL_MAX_RESPONSE_BYTES,
            response_label="JARVIS input reconciliation",
        )
        if response.get("error") is not None:
            raise _McpProtocolFailure(f"JARVIS input reconciliation failed for step {step_id}")
        result = response.get("result")
        if not isinstance(result, dict) or cast(dict[str, Any], result).get("isError") is True:
            raise _McpProtocolFailure(
                f"JARVIS input reconciliation was rejected for step {step_id}"
            )


def _run_bounded_tools_list(
    process: subprocess.Popen[str],
    stdout_queue: Queue[_StreamEvent],
    stdout_lines: list[str],
    *,
    deadline: float | None,
    command: list[str],
) -> None:
    """Consume all tools/list pages within fixed resource limits."""
    tools_by_name: dict[str, dict[str, Any]] = {}
    seen_cursors: set[str] = set()
    response_bytes = [0]
    cursor: str | None = None
    pages = 0
    while True:
        if pages >= TOOLS_LIST_MAX_PAGES:
            raise _McpProtocolFailure(
                f"tools/list exceeded maximum page count {TOOLS_LIST_MAX_PAGES}"
            )
        response_id = (
            "clio-relay-mcp-tools-list"
            if pages == 0
            else f"clio-relay-mcp-tools-list-page-{pages + 1}"
        )
        _write_message(
            process,
            _tools_list_message(cursor=cursor, response_id=response_id),
        )
        response = _wait_for_response(
            stdout_queue,
            response_id,
            stdout_lines,
            process=process,
            deadline=deadline,
            command=command,
            response_bytes=response_bytes,
            max_response_bytes=TOOLS_LIST_MAX_RESPONSE_BYTES,
            response_label="tools/list",
        )
        pages += 1
        if response.get("error") is not None:
            return
        result = response.get("result")
        if not isinstance(result, dict):
            raise _McpProtocolFailure("tools/list response result must be an object")
        typed_result = cast(dict[str, Any], result)
        raw_tools = typed_result.get("tools")
        if not isinstance(raw_tools, list):
            raise _McpProtocolFailure("tools/list response must contain a tools array")
        for raw_value in cast(list[object], raw_tools):
            if not isinstance(raw_value, dict):
                raise _McpProtocolFailure("tools/list entries must be objects")
            value = cast(dict[str, Any], raw_value)
            name = value.get("name")
            if not isinstance(name, str) or not name:
                raise _McpProtocolFailure("tools/list entries must have non-empty names")
            existing = tools_by_name.get(name)
            if existing is not None:
                if existing != value:
                    raise _McpProtocolFailure(
                        f"tools/list returned conflicting definitions for tool {name}"
                    )
                continue
            tools_by_name[name] = value
            if len(tools_by_name) > TOOLS_LIST_MAX_TOOLS:
                raise _McpProtocolFailure(
                    f"tools/list exceeded maximum tool count {TOOLS_LIST_MAX_TOOLS}"
                )
        next_cursor = typed_result.get("nextCursor")
        if next_cursor is None:
            break
        if not isinstance(next_cursor, str):
            raise _McpProtocolFailure("tools/list nextCursor must be a string")
        if next_cursor in seen_cursors:
            raise _McpProtocolFailure("tools/list returned a repeated nextCursor")
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    aggregate = {
        "jsonrpc": "2.0",
        "id": "clio-relay-mcp-tools-list",
        "result": {
            "tools": list(tools_by_name.values()),
            _TOOLS_LIST_PAGINATION_KEY: {
                "pages": pages,
                "tools": len(tools_by_name),
                "response_bytes": response_bytes[0],
                "limits": {
                    "max_pages": TOOLS_LIST_MAX_PAGES,
                    "max_tools": TOOLS_LIST_MAX_TOOLS,
                    "max_response_bytes": TOOLS_LIST_MAX_RESPONSE_BYTES,
                },
            },
        },
    }
    stdout_lines.append(json.dumps(aggregate, separators=(",", ":")) + "\n")


def _write_message(process: subprocess.Popen[str], message: dict[str, Any]) -> None:
    if process.stdin is None:
        raise RuntimeError("MCP server stdin is not available")
    process.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    process.stdin.flush()


def _wait_for_response(
    queue: Queue[_StreamEvent],
    response_id: str,
    lines: list[str],
    *,
    process: subprocess.Popen[str],
    deadline: float | None,
    command: list[str],
    response_bytes: list[int] | None = None,
    max_response_bytes: int | None = None,
    response_label: str = "MCP response",
    notification_handler: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    while True:
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise subprocess.TimeoutExpired(command, timeout=0, output="".join(lines))
        try:
            line = queue.get(timeout=0.2 if remaining is None else min(0.2, remaining))
        except Empty:
            continue
        if line is None:
            returncode = process.poll()
            if returncode is None:
                try:
                    returncode = process.wait(timeout=0.2)
                except subprocess.TimeoutExpired:
                    returncode = None
            detail = f" with return code {returncode}" if returncode is not None else ""
            raise _McpProtocolFailure(
                f"MCP server stdout closed before response {response_id}{detail}"
            )
        if isinstance(line, _StreamLimit):
            raise _McpProtocolFailure(line.message)
        lines.append(line)
        if response_bytes is not None:
            response_bytes[0] += len(line.encode("utf-8"))
            if max_response_bytes is not None and response_bytes[0] > max_response_bytes:
                raise _McpProtocolFailure(
                    f"{response_label} exceeded maximum response size {max_response_bytes} bytes"
                )
        message = _decoded_json_object(line)
        if message is None:
            continue
        if notification_handler is not None and message.get("method") == "notifications/progress":
            notification_handler(message)
        if message.get("id") == response_id:
            return message


def _start_reader(
    stream: Any,
    queue: Queue[_StreamEvent],
    *,
    stream_name: str,
    max_bytes: int,
) -> threading.Thread:
    def read_stream() -> None:
        captured_bytes = 0
        pending = ""
        limit_reported = False
        try:
            if stream is not None:
                while True:
                    fragment = stream.readline(_STREAM_READ_CHARS)
                    if fragment == "":
                        break
                    if limit_reported:
                        continue
                    captured_bytes += len(fragment.encode("utf-8"))
                    if captured_bytes > max_bytes:
                        queue.put(
                            _StreamLimit(
                                f"MCP server {stream_name} exceeded maximum capture size "
                                f"{max_bytes} bytes"
                            )
                        )
                        pending = ""
                        limit_reported = True
                        continue
                    pending += fragment
                    if fragment.endswith("\n"):
                        queue.put(pending)
                        pending = ""
                if pending and not limit_reported:
                    queue.put(pending)
        finally:
            queue.put(None)

    thread = threading.Thread(target=read_stream, daemon=True)
    thread.start()
    return thread


def _join_reader(
    thread: threading.Thread,
    queue: Queue[_StreamEvent],
    lines: list[str],
) -> None:
    thread.join(timeout=1)
    _drain_available(queue, lines)


def _drain_available(queue: Queue[_StreamEvent], lines: list[str]) -> None:
    while True:
        try:
            line = queue.get_nowait()
        except Empty:
            return
        if isinstance(line, _StreamLimit):
            lines.append(f"\n[{line.message}]\n")
        elif line is not None:
            lines.append(line)


def _open_process(
    command: list[str], *, env_from: dict[str, str] | None = None
) -> subprocess.Popen[str]:
    child_env = _child_env(env_from) if env_from else _scrubbed_env()
    return subprocess.Popen(
        command,
        env=child_env,
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **nested_popen_kwargs(child_env),
    )


def _resolve_executable(executable: str) -> str:
    """Resolve executables commonly installed into user-local cluster paths."""
    resolved = shutil.which(executable)
    if resolved is not None:
        return resolved
    if executable in {"uv", "uvx"}:
        user_local_executable = Path.home() / ".local" / "bin" / executable
        if user_local_executable.exists():
            return str(user_local_executable)
    return executable


def _terminate_process_tree(process: subprocess.Popen[str]) -> None:
    terminate_nested_process(
        process,
        timeout_seconds=MCP_SERVER_TERMINATION_TIMEOUT_SECONDS,
    )


def _install_parent_termination_handlers(
    process: subprocess.Popen[str],
) -> dict[int, _SignalHandler]:
    """Ensure outer JARVIS termination cleans the separately-owned MCP group."""
    # Python signal handlers are process-global and may only be installed by the
    # main interpreter thread. Durable JARVIS executions invoke package start
    # hooks from a worker thread, where the session's ``finally`` block and the
    # relay containment boundary own child cleanup instead.
    if threading.current_thread() is not threading.main_thread():
        return {}
    previous: dict[int, _SignalHandler] = {}
    terminating = False

    def terminate(signum: int, _frame: Any) -> None:
        nonlocal terminating
        if terminating:
            return
        terminating = True
        _terminate_process_tree(process)
        raise SystemExit(128 + signum)

    signals: list[int] = [int(signal.SIGTERM), int(signal.SIGINT)]
    if os.name == "nt" and hasattr(signal, "SIGBREAK"):
        signals.append(int(vars(signal)["SIGBREAK"]))
    try:
        for signum in signals:
            previous[int(signum)] = signal.getsignal(signum)
            signal.signal(signum, terminate)
    except ValueError:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        return {}
    return previous


def _restore_parent_termination_handlers(previous: dict[int, _SignalHandler]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _child_env(env_from: dict[str, str]) -> dict[str, str]:
    """Build a minimal child environment plus explicit named references."""
    env = {name: os.environ[name] for name in _BASE_CHILD_ENV_NAMES if name in os.environ}
    if CONTAINMENT_ENV in os.environ:
        env[CONTAINMENT_ENV] = os.environ[CONTAINMENT_ENV]
    for child_name, source_name in env_from.items():
        _validate_environment_reference(child_name, source_name)
        try:
            env[child_name] = os.environ[source_name]
        except KeyError as exc:
            raise ValueError(f"MCP env_from source is not set: {source_name}") from exc
    return env


def _scrubbed_env() -> dict[str, str]:
    """Compatibility alias for the minimal environment without explicit references."""
    return _child_env({})


def _environment_references(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError("env_from must be a string object")
    references: dict[str, str] = {}
    for child_name, source_name in cast(dict[object, object], value).items():
        if not isinstance(child_name, str) or not isinstance(source_name, str):
            raise ValueError("env_from must be a string object")
        _validate_environment_reference(child_name, source_name)
        references[child_name] = source_name
    return references


def _validate_environment_reference(child_name: str, source_name: str) -> None:
    if not _valid_environment_name(child_name) or not _valid_environment_name(source_name):
        raise ValueError("MCP env_from keys and values must be environment names")
    forbidden = {
        name
        for name in (child_name, source_name)
        if name in _RELAY_CREDENTIAL_ENV_NAMES
        or (
            name.startswith("CLIO_RELAY_") and (name.endswith("_TOKEN") or name.endswith("_SECRET"))
        )
    }
    if forbidden:
        credential = sorted(forbidden)[0]
        raise ValueError(f"MCP env_from cannot expose relay credential {credential}")


def _valid_environment_name(value: str) -> bool:
    return (
        bool(value)
        and (value[0].isalpha() or value[0] == "_")
        and all(character.isalnum() or character == "_" for character in value)
    )


def _required_str(params: dict[str, Any], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def _required_optional_str(value: str | None, key: str) -> str:
    if value is None or not value:
        raise ValueError(f"{key} is required")
    return value


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError("tool must be a non-empty string")
    return value


def _operation(value: Any) -> str:
    if not isinstance(value, str) or value not in {"tools/call", "tools/list"}:
        raise ValueError("operation must be tools/call or tools/list")
    return value


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("arguments must be an object")
    return cast(dict[str, Any], value)


def _jarvis_input_manifest(
    value: Any,
    *,
    operation: str,
    tool: str | None,
    arguments: dict[str, Any],
    expected_registered_contract: str | None,
    expected_jarvis_cd_lock_binding: dict[str, str] | None,
) -> dict[str, Any] | None:
    """Validate one relay-owned resolved-input manifest before MCP launch."""
    if value is None:
        return None
    if (
        operation != "tools/call"
        or tool != "jarvis_run"
        or expected_registered_contract != REGISTERED_JARVIS_EXECUTION_QUERY_CONTRACT
        or expected_jarvis_cd_lock_binding is not None
        or not isinstance(value, dict)
    ):
        raise ValueError("JARVIS input manifest requires the registered jarvis_run contract")
    manifest = cast(dict[str, Any], value)
    expected_fields = {
        "schema_version",
        "route",
        "route_sha256",
        "idempotency_key",
        "resolutions",
        "artifact_uses",
        "manifest_sha256",
        "created_at",
        "document_sha256",
    }
    if (
        set(manifest) != expected_fields
        or manifest.get("schema_version") != "clio-relay.jarvis-run-input-manifest.v1"
    ):
        raise ValueError("JARVIS input manifest fields are invalid")
    route = manifest.get("route")
    expected_route_fields = {
        "schema_version",
        "cluster",
        "server_name",
        "contract",
        "cluster_route_revision",
        "registration_revision",
        "expected_server_artifact_digest",
        "pipeline_id",
        "owner_session_id",
        "owner_session_generation_id",
    }
    if not isinstance(route, dict):
        raise ValueError("JARVIS input manifest route is invalid")
    typed_route = cast(dict[str, Any], route)
    if (
        set(typed_route) != expected_route_fields
        or typed_route.get("schema_version") != "clio-relay.jarvis-pipeline-input-route.v1"
        or typed_route.get("contract") != REGISTERED_JARVIS_EXECUTION_QUERY_CONTRACT
        or typed_route.get("pipeline_id") != arguments.get("pipeline_id")
    ):
        raise ValueError("JARVIS input manifest route is invalid")
    route_sha256 = _canonical_json_sha256(typed_route)
    if manifest.get("route_sha256") != route_sha256:
        raise ValueError("JARVIS input manifest route checksum is invalid")
    if not isinstance(manifest.get("idempotency_key"), str) or not manifest["idempotency_key"]:
        raise ValueError("JARVIS input manifest idempotency key is invalid")
    raw_resolutions = manifest.get("resolutions")
    if not isinstance(raw_resolutions, list):
        raise ValueError("JARVIS input manifest resolutions are invalid")
    typed_resolutions = cast(list[object], raw_resolutions)
    if not typed_resolutions or len(typed_resolutions) > 1_000:
        raise ValueError("JARVIS input manifest resolutions are invalid")
    resolutions: list[dict[str, Any]] = []
    identities: list[tuple[str, str]] = []
    expected_uses: list[dict[str, Any]] = []
    for raw_resolution in typed_resolutions:
        if not isinstance(raw_resolution, dict):
            raise ValueError("JARVIS input manifest resolution fields are invalid")
        resolution = cast(dict[str, Any], raw_resolution)
        if set(resolution) != {
            "binding",
            "disposition",
            "previous_sha256",
        }:
            raise ValueError("JARVIS input manifest resolution fields are invalid")
        binding = resolution.get("binding")
        if not isinstance(binding, dict):
            raise ValueError("JARVIS input manifest binding fields are invalid")
        typed_binding = cast(dict[str, Any], binding)
        if set(typed_binding) != {
            "step_id",
            "canonical_setting",
            "accepted_names",
            "workspace_relative_path",
            "logical_name",
            "size_bytes",
            "sha256",
            "remote_path",
            "artifact_use",
        }:
            raise ValueError("JARVIS input manifest binding fields are invalid")
        step_id = typed_binding.get("step_id")
        setting = typed_binding.get("canonical_setting")
        accepted_names = typed_binding.get("accepted_names")
        relative_path = typed_binding.get("workspace_relative_path")
        remote_path = typed_binding.get("remote_path")
        sha256 = typed_binding.get("sha256")
        artifact_use = typed_binding.get("artifact_use")
        typed_accepted_names = (
            cast(list[object], accepted_names) if isinstance(accepted_names, list) else []
        )
        typed_artifact_use = (
            cast(dict[str, Any], artifact_use) if isinstance(artifact_use, dict) else {}
        )
        if (
            not isinstance(step_id, str)
            or not step_id
            or not isinstance(setting, str)
            or not setting
            or not isinstance(accepted_names, list)
            or not accepted_names
            or accepted_names[0] != setting
            or not all(isinstance(item, str) and item for item in typed_accepted_names)
            or len(typed_accepted_names) != len(set(cast(list[str], typed_accepted_names)))
            or not isinstance(relative_path, str)
            or not relative_path
            or "\\" in relative_path
            or relative_path.startswith("/")
            or any(part in {"", ".", ".."} for part in relative_path.split("/"))
            or not isinstance(remote_path, str)
            or not remote_path.startswith("/")
            or remote_path.startswith("//")
            or not _is_sha256(sha256)
            or not isinstance(artifact_use, dict)
            or typed_artifact_use.get("sha256") != sha256
        ):
            raise ValueError("JARVIS input manifest binding identity is invalid")
        previous_sha256 = resolution.get("previous_sha256")
        disposition = resolution.get("disposition")
        if not _is_sha256(previous_sha256) or (
            (disposition == "reused") != (previous_sha256 == sha256)
        ):
            raise ValueError("JARVIS input manifest resolution disposition is invalid")
        identities.append((step_id, setting))
        expected_uses.append(typed_artifact_use)
        resolutions.append(resolution)
    if identities != sorted(identities) or len(identities) != len(set(identities)):
        raise ValueError("JARVIS input manifest binding identities are not canonical")
    expected_uses.sort(key=lambda item: (str(item.get("artifact_id")), str(item.get("sha256"))))
    if manifest.get("artifact_uses") != expected_uses:
        raise ValueError("JARVIS input manifest artifact uses do not match its bindings")
    expected_manifest_sha256 = _canonical_json_sha256(
        {
            "route_sha256": route_sha256,
            "idempotency_key": manifest["idempotency_key"],
            "resolutions": resolutions,
            "artifact_uses": expected_uses,
        }
    )
    if manifest.get("manifest_sha256") != expected_manifest_sha256:
        raise ValueError("JARVIS input manifest resolution checksum is invalid")
    document = dict(manifest)
    document.pop("document_sha256")
    if manifest.get("document_sha256") != _canonical_json_sha256(document):
        raise ValueError("JARVIS input manifest document checksum is invalid")
    if (
        len(
            json.dumps(
                manifest,
                allow_nan=False,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        > 1 * 1024 * 1024
    ):
        raise ValueError("JARVIS input manifest exceeded its byte limit")
    return manifest


def _is_sha256(value: object) -> bool:
    """Return whether a value is one canonical lowercase SHA-256 digest."""
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _canonical_json_sha256(value: object) -> str:
    """Return the SHA-256 of one finite canonical JSON value."""
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _str_list(value: Any, *, key: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{key} must be a string array")
    items = cast(list[object], value)
    if not all(isinstance(item, str) for item in items):
        raise ValueError(f"{key} must be a string array")
    return [item for item in items if isinstance(item, str)]


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_sha256(value: Any, *, key: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a SHA-256 string")
    normalized = value.lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{key} must be a SHA-256 string")
    return normalized


def run_mcp_call_request_file(request_path: Path) -> int:
    """Execute one bounded request document and mirror its captured streams.

    The endpoint worker uses this entry point directly under relay-owned process
    containment.  The JARVIS package continues to call
    :func:`run_mcp_call_from_params` for compatibility with already registered
    repositories.
    """
    with request_path.open("rb") as stream:
        payload = stream.read(MCP_REQUEST_MAX_BYTES + 1)
    if len(payload) > MCP_REQUEST_MAX_BYTES:
        raise ValueError(f"MCP request exceeds the {MCP_REQUEST_MAX_BYTES}-byte endpoint limit")
    decoded = json.loads(payload, object_pairs_hook=_reject_duplicate_json_keys)
    if not isinstance(decoded, dict):
        raise ValueError("MCP request document must be an object")
    return_code = run_mcp_call_from_params(cast(dict[str, Any], decoded))
    result_path = Path.cwd() / "mcp-result.json"
    try:
        result = json.loads(
            result_path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return return_code
    if not isinstance(result, dict):
        return return_code
    typed_result = cast(dict[str, Any], result)
    stdout = typed_result.get("stdout")
    stderr = typed_result.get("stderr")
    if isinstance(stdout, str) and stdout:
        print(stdout, end="")
    if isinstance(stderr, str) and stderr:
        print(stderr, end="", file=sys.stderr)
    return return_code


def main(argv: list[str] | None = None) -> int:
    """Run one endpoint-owned MCP request document from the command line."""
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        print("usage: python runner.py REQUEST.json", file=sys.stderr)
        return 2
    try:
        return run_mcp_call_request_file(Path(arguments[0]))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"invalid MCP request: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
