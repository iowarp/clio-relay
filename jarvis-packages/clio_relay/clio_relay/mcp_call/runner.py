"""Minimal stdio MCP client used by the relay MCP-call JARVIS package."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import secrets
import shutil
import signal
import stat
import subprocess
import tempfile
import threading
import time
import zipfile
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from importlib import metadata
from pathlib import Path, PurePosixPath
from queue import Empty, Queue
from typing import Any, cast

from clio_relay.process_containment import (
    CONTAINMENT_ENV,
    nested_popen_kwargs,
    terminate_nested_process,
)

TOOLS_LIST_MAX_PAGES = 64
TOOLS_LIST_MAX_TOOLS = 10_000
TOOLS_LIST_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MCP_CALL_DEFAULT_TIMEOUT_SECONDS = 300
MCP_INITIALIZE_MAX_RESPONSE_BYTES = 1024 * 1024
MCP_CALL_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MCP_SESSION_MAX_STDOUT_BYTES = 32 * 1024 * 1024
MCP_SESSION_MAX_STDERR_BYTES = 4 * 1024 * 1024
MCP_PACKAGE_PROGRESS_SCHEMA = "clio-kit.jarvis-package-progress.v1"
MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA = "clio-relay.mcp-package-progress-bridge.v1"
MCP_JARVIS_RUNTIME_SCHEMA = "jarvis.runtime.v1"
MCP_PACKAGE_PROGRESS_MAX_NOTIFICATION_BYTES = 64 * 1024
MCP_PACKAGE_PROGRESS_MAX_NOTIFICATIONS = 10_000
MCP_PACKAGE_PROGRESS_MAX_TOTAL_BYTES = 4 * 1024 * 1024
PROGRESS_SIDECAR_RECORD_SCHEMA = "clio-relay.progress-sidecar-record.v1"
FILE_HASH_CHUNK_BYTES = 1024 * 1024
CLIO_KIT_WHEEL_MAX_FILES = 10_000
CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES = 1024 * 1024
CLIO_KIT_WHEEL_MAX_PROJECT_FILES = 20_000
CLIO_KIT_WHEEL_MAX_PROJECT_BYTES = 512 * 1024 * 1024
PYTHON_DISTRIBUTION_MAX_DISTRIBUTIONS = 10_000
PYTHON_DISTRIBUTION_MAX_ENTRY_POINTS = 100_000
PYTHON_DISTRIBUTION_MAX_FILES = 100_000
PYTHON_DISTRIBUTION_MAX_BYTES = 4 * 1024 * 1024 * 1024
_STREAM_READ_CHARS = 64 * 1024
_TOOLS_LIST_PAGINATION_KEY = "_clioRelayPagination"
_CLIO_KIT_LOCKED_SERVER_SCHEMA = "clio-kit.locked-server.v4"
_CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY = "uv-run:materialized:frozen:no-editable:no-dev:v3"
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
            envelope = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            raise _McpProtocolFailure(f"MCP package progress JSON was invalid: {exc}") from exc
        validated = self._validated_envelope(envelope, params=params)
        self._append_record(validated, execution_validated=False)
        if validated["provider_acceptance_validated"] is True:
            self.acceptance_candidates.append(validated)

    def finalize(self, structured_result: dict[str, Any] | None) -> None:
        """Bind accepted observations to the final JARVIS execution result."""
        if self.notification_count == 0:
            return
        if structured_result is None:
            raise _McpProtocolFailure(
                "MCP package progress had no structured JARVIS result for execution binding"
            )
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
    operation = _operation(params.get("operation", "tools/call"))
    tool = _optional_str(params.get("tool"))
    arguments = _object(params.get("arguments", {}))
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
    try:
        command = [_resolve_executable(server), *server_args]
        server_artifact = _server_artifact_identity(server, server_args)
        observed_server_artifact_digest = _server_artifact_digest(server_artifact)
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
            observed_server_artifact_digest=observed_server_artifact_digest,
            server_artifact=server_artifact,
        )
        with _prepared_mcp_launch(
            command,
            server_args=server_args,
            server_artifact=server_artifact,
        ) as prepared:
            launch_command, execution_artifact = prepared
            if operation == "tools/call" and progress_bridge is None:
                process = _run_mcp_session(
                    launch_command,
                    tool=tool,
                    arguments=arguments,
                    timeout=timeout,
                    env_from=env_from,
                )
            elif operation == "tools/call":
                process = _run_mcp_session(
                    launch_command,
                    tool=tool,
                    arguments=arguments,
                    timeout=timeout,
                    env_from=env_from,
                    progress_bridge=progress_bridge,
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
        elif progress_bridge is not None:
            protocol_result = _response_result(
                str(process.stdout or ""),
                response_id=_response_id(operation),
            )
            try:
                progress_bridge.finalize(_structured_result(protocol_result, operation=operation))
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
        server_artifact=server_artifact,
        observed_server_artifact_digest=observed_server_artifact_digest,
        execution_artifact=execution_artifact,
        operation=operation,
        tool=tool,
        arguments=arguments,
        returncode=returncode,
        stdout=str(process.stdout or ""),
        stderr=str(process.stderr or ""),
        started_at=started_at,
        timed_out=timed_out,
        protocol_error=protocol_error,
        progress_bridge=(
            progress_bridge.result_metadata() if progress_bridge is not None else None
        ),
    )
    return returncode


def _package_progress_bridge_from_invocation(
    *,
    operation: str,
    tool: str | None,
    arguments: dict[str, Any],
    expected_server_artifact_digest: str | None,
    observed_server_artifact_digest: str,
    server_artifact: dict[str, Any],
) -> _McpProgressBridge | None:
    """Create a private bridge only for an artifact-bound locked JARVIS call."""
    progress_path = os.environ.get("CLIO_RELAY_PROGRESS_FILE")
    relay_token = os.environ.get("CLIO_RELAY_PROGRESS_TOKEN")
    if progress_path is None and relay_token is None:
        return None
    if progress_path is None or relay_token is None or not relay_token:
        raise ValueError("relay progress sidecar path and token must be configured together")
    if operation != "tools/call" or tool != "jarvis_run":
        return None
    raw_nested_runtime = server_artifact.get("nested_runtime")
    nested_runtime = (
        cast(dict[str, Any], raw_nested_runtime) if isinstance(raw_nested_runtime, dict) else None
    )
    if (
        expected_server_artifact_digest is None
        or observed_server_artifact_digest != expected_server_artifact_digest
        or server_artifact.get("verified") is not True
        or nested_runtime is None
        or nested_runtime.get("server_name") != "jarvis"
        or nested_runtime.get("locked_runtime_verified") is not True
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
) -> dict[str, Any]:
    params: dict[str, Any] = {"name": tool, "arguments": arguments}
    if progress_token is not None:
        params["_meta"] = {"progressToken": progress_token}
    return {
        "jsonrpc": "2.0",
        "id": "clio-relay-mcp-call",
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
    server_artifact: dict[str, Any] | None,
    observed_server_artifact_digest: str | None,
    execution_artifact: dict[str, Any] | None,
    operation: str,
    tool: str | None,
    arguments: dict[str, Any],
    returncode: int,
    stdout: str,
    stderr: str,
    started_at: float,
    timed_out: bool,
    protocol_error: str | None,
    progress_bridge: dict[str, Any] | None,
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
        server_artifact = _server_artifact_identity(server, server_args)
    if observed_server_artifact_digest is None:
        observed_server_artifact_digest = _server_artifact_digest(server_artifact)
    result_path.write_text(
        json.dumps(
            {
                "server": server,
                "server_args": server_args,
                "env_from": env_from,
                "operation": operation,
                "tool": tool,
                "arguments": arguments,
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
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_seconds": finished_at - started_at,
            },
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


def _server_artifact_identity(server: str, server_args: list[str]) -> dict[str, Any]:
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
    direct_runtime_verified = (
        python_distribution_runtime is not None
        and python_distribution_runtime.get("runtime_closure_verified") is True
    )
    launcher_artifact_verified = executable is not None and (
        (install_spec is None and direct_runtime_verified)
        or (install_spec is not None and install_artifact is not None)
    )
    nested_server_name = _nested_clio_kit_server_name(server_args)
    nested_launcher = nested_server_name is not None
    nested_runtime = (
        _locked_clio_kit_runtime_identity(
            install_artifact,
            server_name=nested_server_name,
            resolved_executable=resolved_executable,
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
        "install_spec": install_spec,
        "install_source": install_source,
        "install_artifact_sha256": (
            install_artifact.get("sha256") if install_artifact is not None else None
        ),
        "input_files": input_files,
        "launcher_artifact_verified": launcher_artifact_verified,
        "python_distribution_runtime": python_distribution_runtime,
        "nested_launcher": nested_launcher,
        "nested_runtime": nested_runtime,
        "server_process_artifact_verified": server_process_artifact_verified,
        "identity_error": (
            "clio-kit mcp-server child source, lock, or uv runtime is not bound to the "
            "outer clio-kit wheel"
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
        "error": None,
    }
    try:
        resolved_executable = executable.resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"could not resolve direct server executable: {exc}"
        return evidence
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
        evidence["error"] = (
            "direct server executable has no unique installed console-script distribution"
        )
        return evidence
    distribution, entry_point = matches[0]
    evidence.update(
        {
            "distribution": distribution.metadata.get("Name"),
            "distribution_version": distribution.version,
            "entry_point": entry_point.name,
            "entry_point_value": entry_point.value,
        }
    )
    direct_url = _distribution_direct_url(distribution)
    if direct_url is not None:
        directory = direct_url.get("dir_info")
        typed_directory = cast(dict[str, Any], directory) if isinstance(directory, dict) else {}
        if typed_directory.get("editable") is True:
            evidence["error"] = "editable Python distributions have no immutable runtime closure"
            return evidence
    closure = _verify_distribution_record_closure(distribution)
    evidence.update(closure)
    return evidence


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


def _nested_clio_kit_server_name(server_args: list[str]) -> str | None:
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
    return None


def _locked_clio_kit_runtime_identity(
    install_artifact: dict[str, Any] | None,
    *,
    server_name: str,
    resolved_executable: Path,
) -> dict[str, Any]:
    """Verify the locked embedded project selected by a clio-kit wheel."""
    wheel_path = (
        Path(str(install_artifact["path"]))
        if install_artifact is not None and isinstance(install_artifact.get("path"), str)
        else None
    )
    uv_name = "uv.exe" if resolved_executable.suffix.lower() == ".exe" else "uv"
    uv_identity = _file_identity(resolved_executable.with_name(uv_name))
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
            if lock_sha256 is None:
                raise ValueError("clio-kit embedded server project has no lock digest")
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
    evidence.update(
        {
            "project_sha256": digest.hexdigest(),
            "lock_sha256": lock_sha256,
            "runtime_file_count": len(inputs),
            "runtime_bytes": project_bytes,
            "contract_source_verified": contract_source_verified,
            "locked_runtime_verified": contract_source_verified and uv_identity is not None,
            "error": (
                None
                if contract_source_verified and uv_identity is not None
                else "clio-kit locked launcher contract or uv executable is unverified"
            ),
        }
    )
    return evidence


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
    if executable == "uvx":
        user_local_uvx = Path.home() / ".local" / "bin" / "uvx"
        if user_local_uvx.exists():
            return str(user_local_uvx)
    return executable


def _terminate_process_tree(process: subprocess.Popen[str]) -> None:
    terminate_nested_process(process)


def _install_parent_termination_handlers(
    process: subprocess.Popen[str],
) -> dict[int, _SignalHandler]:
    """Ensure outer JARVIS termination cleans the separately-owned MCP group."""
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
