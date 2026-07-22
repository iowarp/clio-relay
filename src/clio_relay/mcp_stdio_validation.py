"""Packaged stdio MCP boundary exercised by release acceptance commands."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import stat
import subprocess
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic
from typing import Any, BinaryIO, cast

from clio_relay import __version__
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_ID,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_user_contract,
    jarvis_user_contract_digest,
    virtual_jarvis_tool_definitions,
)
from clio_relay.mcp_server import USER_MCP_TOOL_NAMES, static_mcp_tool_names
from clio_relay.process_containment import (
    OwnedProcessSpawnError,
    broker_child_environment_payload,
    ensure_owned_process_tree_empty,
    owner_environment,
    release_owned_process,
    spawn_owned_process,
    terminate_owned_process,
)

JSON = dict[str, Any]
_INITIALIZE_ID = "clio-relay-validation-initialize"
_TOOLS_LIST_ID = "clio-relay-validation-tools-list"
_TOOLS_CALL_ID = "clio-relay-validation-tools-call"
_VALIDATION_EXECUTABLE_ENV = "CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE"
_EXPECTED_PROTOCOL_VERSION = "2024-11-05"
_MAX_STDIN_BYTES = 4 * 1024 * 1024
_MAX_STDOUT_BYTES = 4 * 1024 * 1024
_MAX_STDERR_BYTES = 256 * 1024
_MAX_EXECUTABLE_BYTES = 128 * 1024 * 1024
_STREAM_READ_BYTES = 64 * 1024
_PROCESS_POLL_SECONDS = 0.02
_DIAGNOSTIC_BYTES = 4_096
_SENSITIVE_DIAGNOSTIC = re.compile(
    r"(?i)\b(authorization|bearer|capability|credential|password|secret|token)"
    r"(\s*[:=]\s*)([^\s,;]+)"
)
_BEARER_DIAGNOSTIC = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_SENSITIVE_ENVIRONMENT_NAME = re.compile(
    r"(?i)(authorization|auth|bearer|capability|credential|key|password|secret|token)"
)
_PACKAGED_BASE_ENVIRONMENT_NAMES = frozenset(
    {
        "APPDATA",
        "COMSPEC",
        "DBUS_SESSION_BUS_ADDRESS",
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "LOCALAPPDATA",
        "LOGNAME",
        "PATH",
        "PATHEXT",
        "PROGRAMDATA",
        "SYSTEMDRIVE",
        "SYSTEMROOT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "TZ",
        "USER",
        "USERPROFILE",
        "WINDIR",
        "XDG_RUNTIME_DIR",
        "CLIO_RELAY_CLI_MODE",
        "CLIO_RELAY_CLUSTER_REGISTRY",
        "CLIO_RELAY_CORE_DIR",
        "CLIO_RELAY_REMOTE_MCP_CACHE",
        "CLIO_RELAY_SPOOL_DIR",
        "CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_JOB",
        "CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_STREAM",
        "CLIO_RELAY_STORAGE_CORE_HIGH_WATER_BYTES",
        "CLIO_RELAY_STORAGE_JOB_CORE_ALLOWANCE_BYTES",
        "CLIO_RELAY_STORAGE_JOB_RESULT_ALLOWANCE_BYTES",
        "CLIO_RELAY_STORAGE_LOCK_TIMEOUT_SECONDS",
        "CLIO_RELAY_STORAGE_MAX_JOB_RESERVATION_BYTES",
        "CLIO_RELAY_STORAGE_MAX_LEDGER_BYTES",
        "CLIO_RELAY_STORAGE_MAX_RESERVATIONS",
        "CLIO_RELAY_STORAGE_MAX_SCAN_ACCOUNTED_BYTES",
        "CLIO_RELAY_STORAGE_MAX_SCAN_DEPTH",
        "CLIO_RELAY_STORAGE_MAX_SCAN_ENTRIES",
        "CLIO_RELAY_STORAGE_MINIMUM_FREE_BYTES",
        "CLIO_RELAY_STORAGE_RUNTIME_CHECK_INTERVAL_SECONDS",
        "CLIO_RELAY_STORAGE_SPOOL_HIGH_WATER_BYTES",
        "CLIO_RELAY_STORAGE_TOTAL_HIGH_WATER_BYTES",
    }
)


@dataclass(frozen=True)
class PackagedMcpStdioSession:
    """Machine evidence captured from one packaged MCP stdio process."""

    command: tuple[str, ...]
    returncode: int
    initialize_response: JSON
    tools_list_response: JSON
    tools_call_response: JSON
    transcript_sha256: str
    stderr_sha256: str
    stderr_excerpt: str
    configured_executable: str | None = None
    canonical_executable: str | None = None
    executable_sha256: str | None = None
    server_info_sha256: str | None = None
    tools_list_sha256: str | None = None
    called_tool_schema_sha256: str | None = None
    jarvis_virtual_tools_sha256: str | None = None
    called_tool_name: str | None = None
    containment_mode: str | None = None
    containment_enforceable: bool = False

    def evidence(self) -> JSON:
        """Return bounded JSON evidence suitable for validation reports."""
        initialize_result = _mapping(self.initialize_response.get("result")) or {}
        server_info = _mapping(initialize_result.get("serverInfo")) or {}
        tools_result = _mapping(self.tools_list_response.get("result")) or {}
        raw_tools = tools_result.get("tools")
        tools = cast(list[object], raw_tools) if isinstance(raw_tools, list) else []
        tool_names: list[str] = []
        for raw_tool in tools:
            if not isinstance(raw_tool, dict):
                continue
            tool = cast(JSON, raw_tool)
            name = tool.get("name")
            if isinstance(name, str):
                tool_names.append(name)
        tool_names.sort()
        call_job_id = _safe_call_job_id(self.tools_call_response)
        projection: JSON = {
            "schema_version": "clio-relay.packaged-mcp-stdio-evidence.v1",
            "boundary": "packaged_clio_relay_mcp_server_stdio",
            "command": list(self.command),
            "configured_executable": self.configured_executable,
            "canonical_executable": self.canonical_executable,
            "executable_sha256": self.executable_sha256,
            "returncode": self.returncode,
            "protocol_version": initialize_result.get("protocolVersion"),
            "server_name": server_info.get("name"),
            "server_version": server_info.get("version"),
            "server_info_sha256": self.server_info_sha256,
            "tool_names": tool_names,
            "tools_list_sha256": self.tools_list_sha256,
            "called_tool_name": self.called_tool_name,
            "called_tool_schema_sha256": self.called_tool_schema_sha256,
            "call_job_id": call_job_id,
            "jarvis_contract_id": CLIO_KIT_JARVIS_USER_CONTRACT_ID,
            "jarvis_contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
            "jarvis_virtual_tools_sha256": self.jarvis_virtual_tools_sha256,
            "containment_mode": self.containment_mode,
            "containment_enforceable": self.containment_enforceable,
        }
        projection["protocol_evidence_sha256"] = _canonical_digest(projection)
        return projection


@dataclass(frozen=True)
class _ExecutableIdentity:
    """Stable identity for the exact launcher selected before process creation."""

    configured_path: Path
    canonical_path: Path
    configured_lstat: tuple[int, int, int, int]
    target_stat: tuple[int, int, int, int]
    sha256: str


@dataclass
class _BoundedPipeCapture:
    """Thread-safe bounded bytes captured from one child pipe."""

    label: str
    maximum_bytes: int
    content: bytearray = field(default_factory=bytearray)
    overflow: bool = False
    error: BaseException | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def append(self, chunk: bytes) -> bool:
        """Append within the cap and return whether the stream overflowed."""
        with self.lock:
            remaining = self.maximum_bytes - len(self.content)
            if remaining > 0:
                self.content.extend(chunk[:remaining])
            if len(chunk) > remaining:
                self.overflow = True
            return self.overflow

    def snapshot(self) -> tuple[bytes, bool, BaseException | None]:
        """Return an immutable capture snapshot."""
        with self.lock:
            return bytes(self.content), self.overflow, self.error


def run_packaged_mcp_stdio_session(
    *,
    profile: str,
    tool: str,
    arguments: JSON,
    timeout_seconds: float = 60,
    extra_environment: Mapping[str, str] | None = None,
    require_enforceable_containment: bool = False,
) -> PackagedMcpStdioSession:
    """Initialize, list, and call through the exact installed relay executable."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise RelayError("packaged MCP stdio validation timeout must be finite and positive")
    normalized_profile = _normalize_validation_profile(profile)
    executable = _resolve_packaged_executable()
    command = (str(executable.canonical_path), "mcp-server", "--profile", normalized_profile)
    messages: tuple[JSON, ...] = (
        {
            "jsonrpc": "2.0",
            "id": _INITIALIZE_ID,
            "method": "initialize",
            "params": {
                "protocolVersion": _EXPECTED_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "clio-relay-validation", "version": "1.0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": _TOOLS_LIST_ID,
            "method": "tools/list",
            "params": {},
        },
        {
            "jsonrpc": "2.0",
            "id": _TOOLS_CALL_ID,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        },
    )
    try:
        session_input = "".join(
            json.dumps(
                message,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for message in messages
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RelayError("packaged MCP stdio request was not finite JSON") from exc
    stdout, stderr, returncode, containment = _run_bounded_process(
        command,
        session_input=session_input,
        timeout_seconds=timeout_seconds,
        extra_environment=extra_environment,
        require_enforceable_containment=require_enforceable_containment,
        staged_mcp=True,
        called_tool=tool,
        profile=normalized_profile,
    )
    _verify_executable_unchanged(executable)
    responses = _responses_by_id(stdout)
    missing = [
        response_id
        for response_id in (_INITIALIZE_ID, _TOOLS_LIST_ID, _TOOLS_CALL_ID)
        if response_id not in responses
    ]
    if returncode != 0 or missing:
        detail = _sanitized_diagnostic(
            stderr,
            forbidden_values=(extra_environment.values() if extra_environment else ()),
        )
        raise RelayError(
            "packaged MCP stdio validation returned an incomplete transcript: "
            f"returncode={returncode} missing={missing} stderr={detail!r}"
        )
    initialize = responses[_INITIALIZE_ID]
    tools_list = responses[_TOOLS_LIST_ID]
    server_info, tools, called_tool = _validate_protocol_contract(
        initialize_response=initialize,
        tools_list_response=tools_list,
        called_tool=tool,
        profile=normalized_profile,
    )
    jarvis_virtual_tools_sha256 = _validate_pinned_jarvis_contract(tools)
    return PackagedMcpStdioSession(
        command=command,
        returncode=returncode,
        initialize_response=initialize,
        tools_list_response=tools_list,
        tools_call_response=responses[_TOOLS_CALL_ID],
        transcript_sha256=hashlib.sha256(stdout).hexdigest(),
        stderr_sha256=hashlib.sha256(stderr).hexdigest(),
        stderr_excerpt=_sanitized_diagnostic(
            stderr,
            forbidden_values=(extra_environment.values() if extra_environment else ()),
        ),
        configured_executable=str(executable.configured_path),
        canonical_executable=str(executable.canonical_path),
        executable_sha256=executable.sha256,
        server_info_sha256=_canonical_digest(server_info),
        tools_list_sha256=_tools_digest(tools),
        called_tool_schema_sha256=_canonical_digest(called_tool),
        jarvis_virtual_tools_sha256=jarvis_virtual_tools_sha256,
        called_tool_name=tool,
        containment_mode=cast(str | None, containment.get("mode")),
        containment_enforceable=containment.get("enforceable") is True,
    )


def _normalize_validation_profile(profile: str) -> str:
    """Normalize the exact aliases accepted by the packaged MCP command."""
    normalized = profile.strip().lower()
    if normalized in {"", "user", "agent"}:
        return "user"
    if normalized in {"admin", "operator", "all"}:
        return normalized
    raise RelayError("packaged MCP validation profile was unsupported")


def _resolve_packaged_executable() -> _ExecutableIdentity:
    configured = os.environ.get(_VALIDATION_EXECUTABLE_ENV)
    selected = configured if configured is not None else shutil.which("clio-relay")
    if selected is None:
        raise RelayError(
            "packaged clio-relay executable is unavailable; install the exact wheel as a "
            "persistent uv tool before running validation"
        )
    configured_path = Path(selected).expanduser()
    if configured is not None and not configured_path.is_absolute():
        raise RelayError(f"{_VALIDATION_EXECUTABLE_ENV} must name an absolute executable path")
    configured_path = Path(os.path.abspath(configured_path))
    try:
        configured_lstat = configured_path.lstat()
        canonical_path = configured_path.resolve(strict=True)
        target_stat = canonical_path.stat()
    except OSError as exc:
        raise RelayError("configured packaged clio-relay executable could not be verified") from exc
    if not stat.S_ISREG(target_stat.st_mode):
        raise RelayError("configured packaged clio-relay executable is not a regular file")
    if os.name != "nt" and not os.access(canonical_path, os.X_OK):
        raise RelayError("configured packaged clio-relay executable is not executable")
    if target_stat.st_size > _MAX_EXECUTABLE_BYTES:
        raise RelayError("configured packaged clio-relay executable exceeded its byte limit")
    return _ExecutableIdentity(
        configured_path=configured_path,
        canonical_path=canonical_path,
        configured_lstat=_stat_identity(configured_lstat),
        target_stat=_stat_identity(target_stat),
        sha256=_hash_regular_file(canonical_path, expected=_stat_identity(target_stat)),
    )


def _verify_executable_unchanged(executable: _ExecutableIdentity) -> None:
    try:
        configured_lstat = _stat_identity(executable.configured_path.lstat())
        canonical_path = executable.configured_path.resolve(strict=True)
        target_stat = _stat_identity(canonical_path.stat())
    except OSError as exc:
        raise RelayError("packaged clio-relay executable changed during validation") from exc
    if (
        configured_lstat != executable.configured_lstat
        or canonical_path != executable.canonical_path
        or target_stat != executable.target_stat
        or _hash_regular_file(canonical_path, expected=target_stat) != executable.sha256
    ):
        raise RelayError("packaged clio-relay executable changed during validation")


def _run_bounded_process(
    command: tuple[str, ...],
    *,
    session_input: bytes,
    timeout_seconds: float,
    extra_environment: Mapping[str, str] | None,
    require_enforceable_containment: bool = False,
    staged_mcp: bool = False,
    called_tool: str | None = None,
    profile: str | None = None,
) -> tuple[bytes, bytes, int, JSON]:
    if len(session_input) > _MAX_STDIN_BYTES:
        raise RelayError("packaged MCP stdio validation input exceeded its byte limit")
    deadline = monotonic() + timeout_seconds
    launch_environment = _packaged_launch_environment()
    explicit_environment = _validated_extra_environment(extra_environment)
    child_environment = {**launch_environment, **explicit_environment}
    inherited_private_values = {
        value
        for name, value in child_environment.items()
        if len(value) >= 8 and _SENSITIVE_ENVIRONMENT_NAME.search(name)
    }
    explicit_private_values = set(explicit_environment.values())
    private_values = frozenset(inherited_private_values | explicit_private_values)
    containment: JSON = {}
    if staged_mcp and (called_tool is None or profile is None):
        raise RelayError("staged packaged MCP validation omitted its contract identity")

    def record_containment(_process_id: int, metadata: dict[str, object]) -> None:
        containment.update(metadata)

    try:
        process = cast(
            subprocess.Popen[bytes],
            cast(
                object,
                spawn_owned_process(
                    list(command),
                    on_ready=record_containment,
                    credential_payload=(
                        broker_child_environment_payload(explicit_environment)
                        if explicit_environment and os.name != "nt"
                        else None
                    ),
                    target_environment=(
                        explicit_environment if explicit_environment and os.name == "nt" else None
                    ),
                    stdin_payload=None if staged_mcp else session_input,
                    interactive_stdin=staged_mcp,
                    startup_timeout_seconds=max(0.001, deadline - monotonic()),
                    require_enforceable=require_enforceable_containment,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=launch_environment,
                ),
            ),
        )
    except OwnedProcessSpawnError as exc:
        if monotonic() >= deadline:
            raise ObservationTimeoutError(
                "packaged MCP stdio validation exceeded its total wall-clock deadline"
            ) from None
        cleanup_errors = ",".join(exc.cleanup_errors) or "none"
        raise RelayError(
            "packaged MCP stdio validation could not start with verified cleanup: "
            f"pid={exc.process_id} mode={exc.mode} "
            f"cleanup_verified={exc.cleanup_verified} cleanup_errors={cleanup_errors}"
        ) from None
    except (OSError, RuntimeError) as exc:
        if monotonic() >= deadline:
            raise ObservationTimeoutError(
                "packaged MCP stdio validation exceeded its total wall-clock deadline"
            ) from None
        raise RelayError(
            f"packaged MCP stdio validation could not start: {type(exc).__name__}"
        ) from None
    if process.stdout is None or process.stderr is None:
        try:
            _terminate_bounded_process(process)
        finally:
            release_owned_process(cast(subprocess.Popen[str], cast(object, process)))
        raise RelayError("packaged MCP stdio validation did not create isolated pipes")

    stdout_capture = _BoundedPipeCapture("stdout", _MAX_STDOUT_BYTES)
    stderr_capture = _BoundedPipeCapture("stderr", _MAX_STDERR_BYTES)
    activity = threading.Event()
    readers = (
        threading.Thread(
            target=_capture_pipe,
            args=(process.stdout, stdout_capture, activity),
            daemon=True,
            name="clio-relay-mcp-stdout",
        ),
        threading.Thread(
            target=_capture_pipe,
            args=(process.stderr, stderr_capture, activity),
            daemon=True,
            name="clio-relay-mcp-stderr",
        ),
    )
    for reader in readers:
        reader.start()

    failure: str | None = None
    deadline_expired = False
    if staged_mcp:
        try:
            _exchange_staged_mcp(
                process,
                session_input=session_input,
                stdout_capture=stdout_capture,
                stderr_capture=stderr_capture,
                activity=activity,
                deadline=deadline,
                private_values=private_values,
                called_tool=cast(str, called_tool),
                profile=cast(str, profile),
            )
        except ObservationTimeoutError as exc:
            failure = str(exc)
            deadline_expired = True
        except RelayError as exc:
            failure = str(exc)
    while failure is None and process.poll() is None:
        stdout_snapshot = stdout_capture.snapshot()
        stderr_snapshot = stderr_capture.snapshot()
        if stdout_snapshot[1] or stderr_snapshot[1]:
            streams = [
                label
                for label, snapshot in (
                    ("stdout", stdout_snapshot),
                    ("stderr", stderr_snapshot),
                )
                if snapshot[1]
            ]
            failure = f"packaged MCP stdio validation exceeded its {'/'.join(streams)} byte limit"
            break
        if stdout_snapshot[2] is not None or stderr_snapshot[2] is not None:
            failure = "packaged MCP stdio validation could not read its child pipes"
            break
        remaining = deadline - monotonic()
        if remaining <= 0:
            failure = "packaged MCP stdio validation exceeded its total wall-clock deadline"
            deadline_expired = True
            break
        activity.wait(min(_PROCESS_POLL_SECONDS, remaining))
        activity.clear()

    containment_error: RuntimeError | None = None
    terminated_for_failure = False
    if failure is not None:
        try:
            _terminate_bounded_process(process)
            terminated_for_failure = True
        except RuntimeError as exc:
            containment_error = exc
            failure = "packaged MCP child process containment could not be verified"
            deadline_expired = False
    join_deadline = (
        deadline if failure is None else min(deadline, monotonic() + _PROCESS_POLL_SECONDS)
    )
    for reader in readers:
        reader.join(max(0.0, join_deadline - monotonic()))
    stdout, stdout_overflow, stdout_error = stdout_capture.snapshot()
    stderr, stderr_overflow, stderr_error = stderr_capture.snapshot()
    if failure is None and monotonic() >= deadline:
        failure = "packaged MCP stdio validation exceeded its total wall-clock deadline"
        deadline_expired = True
    if failure is None and (stdout_overflow or stderr_overflow):
        streams = [
            label
            for label, overflow in (("stdout", stdout_overflow), ("stderr", stderr_overflow))
            if overflow
        ]
        failure = f"packaged MCP stdio validation exceeded its {'/'.join(streams)} byte limit"
    if failure is None and (stdout_error is not None or stderr_error is not None):
        failure = "packaged MCP stdio validation could not read its child pipes"
    if failure is None and any(reader.is_alive() for reader in readers):
        failure = "packaged MCP stdio validation exceeded its total wall-clock deadline"
        deadline_expired = True
    if failure is None and any(
        value and value.encode("utf-8") in payload
        for value in private_values
        for payload in (stdout, stderr)
    ):
        failure = "packaged MCP child emitted a child-only secret"
    try:
        if failure is None:
            ensure_owned_process_tree_empty(cast(subprocess.Popen[str], cast(object, process)))
        elif not terminated_for_failure:
            _terminate_bounded_process(process)
    except RuntimeError as exc:
        containment_error = exc
        failure = "packaged MCP child process containment could not be verified"
        deadline_expired = False
    finally:
        try:
            release_owned_process(cast(subprocess.Popen[str], cast(object, process)))
        except RuntimeError as exc:
            containment_error = exc
            failure = "packaged MCP child process containment could not be released"
            deadline_expired = False
    # Provider cleanup is part of a successful acceptance run. Safety cleanup may
    # finish after the deadline, but an over-budget run is never accepted.
    if failure is None and monotonic() >= deadline:
        failure = "packaged MCP stdio validation exceeded its total wall-clock deadline"
        deadline_expired = True
    if failure is not None:
        detail = _sanitized_diagnostic(stderr, forbidden_values=private_values)
        cause = "" if containment_error is None else f" cause={type(containment_error).__name__}"
        error_type = ObservationTimeoutError if deadline_expired else RelayError
        raise error_type(
            f"{failure}; stdout_bytes={len(stdout)} stderr_bytes={len(stderr)} "
            f"stderr={detail!r}{cause}"
        ) from None
    return stdout, stderr, process.returncode, containment


def _exchange_staged_mcp(
    process: subprocess.Popen[bytes],
    *,
    session_input: bytes,
    stdout_capture: _BoundedPipeCapture,
    stderr_capture: _BoundedPipeCapture,
    activity: threading.Event,
    deadline: float,
    private_values: frozenset[str],
    called_tool: str,
    profile: str,
) -> None:
    """Perform the MCP initialization lifecycle in protocol order under one deadline."""
    if not session_input.endswith(b"\n"):
        raise RelayError("packaged MCP staged request omitted its final LF")
    frames = session_input[:-1].split(b"\n")
    if len(frames) != 4:
        raise RelayError("packaged MCP staged request did not contain its exact lifecycle")
    initialize_frame, initialized_frame, list_frame, call_frame = (
        frame + b"\n" for frame in frames
    )
    _write_mcp_frame(process, initialize_frame, deadline=deadline)
    initialize = _await_mcp_response(
        process,
        response_id=_INITIALIZE_ID,
        allowed_response_ids={_INITIALIZE_ID},
        stdout_capture=stdout_capture,
        stderr_capture=stderr_capture,
        activity=activity,
        deadline=deadline,
        private_values=private_values,
    )
    _validate_initialize_contract(initialize)
    _write_mcp_frame(process, initialized_frame, deadline=deadline)
    _write_mcp_frame(process, list_frame, deadline=deadline)
    tools_list = _await_mcp_response(
        process,
        response_id=_TOOLS_LIST_ID,
        allowed_response_ids={_INITIALIZE_ID, _TOOLS_LIST_ID},
        stdout_capture=stdout_capture,
        stderr_capture=stderr_capture,
        activity=activity,
        deadline=deadline,
        private_values=private_values,
    )
    _validate_tools_contract(
        tools_list,
        called_tool=called_tool,
        profile=profile,
    )
    _write_mcp_frame(process, call_frame, deadline=deadline)
    call_response = _await_mcp_response(
        process,
        response_id=_TOOLS_CALL_ID,
        allowed_response_ids={_INITIALIZE_ID, _TOOLS_LIST_ID, _TOOLS_CALL_ID},
        stdout_capture=stdout_capture,
        stderr_capture=stderr_capture,
        activity=activity,
        deadline=deadline,
        private_values=private_values,
    )
    _validated_call_structured_content(call_response)
    if process.stdin is None:
        raise RelayError("packaged MCP staged request lost its stdin pipe")
    try:
        process.stdin.close()
    except OSError:
        raise RelayError("packaged MCP staged request could not close its stdin pipe") from None
    process.stdin = None


def _write_mcp_frame(
    process: subprocess.Popen[bytes],
    frame: bytes,
    *,
    deadline: float,
) -> None:
    """Write one complete request frame without allowing pipe backpressure to escape deadline."""
    if process.stdin is None:
        raise RelayError("packaged MCP staged request lost its stdin pipe")
    input_pipe = process.stdin
    completed = threading.Event()
    errors: list[BaseException] = []

    def write() -> None:
        try:
            view = memoryview(frame)
            while view:
                written = os.write(input_pipe.fileno(), view)
                if written <= 0:
                    raise OSError("request write made no progress")
                view = view[written:]
        except BaseException as exc:
            errors.append(exc)
        finally:
            completed.set()

    writer = threading.Thread(
        target=write,
        daemon=True,
        name=f"clio-relay-mcp-request-{process.pid}",
    )
    writer.start()
    if not completed.wait(max(0.0, deadline - monotonic())):
        raise ObservationTimeoutError(
            "packaged MCP stdio validation exceeded its total wall-clock deadline"
        )
    if errors:
        raise RelayError("packaged MCP stdio validation could not write its request pipe")


def _await_mcp_response(
    process: subprocess.Popen[bytes],
    *,
    response_id: str,
    allowed_response_ids: set[str],
    stdout_capture: _BoundedPipeCapture,
    stderr_capture: _BoundedPipeCapture,
    activity: threading.Event,
    deadline: float,
    private_values: frozenset[str],
) -> JSON:
    """Wait for one correlated response while continuously enforcing all stream bounds."""
    while True:
        stdout, stdout_overflow, stdout_error = stdout_capture.snapshot()
        stderr, stderr_overflow, stderr_error = stderr_capture.snapshot()
        if stdout_overflow or stderr_overflow:
            streams = [
                label
                for label, overflow in (("stdout", stdout_overflow), ("stderr", stderr_overflow))
                if overflow
            ]
            raise RelayError(
                f"packaged MCP stdio validation exceeded its {'/'.join(streams)} byte limit"
            )
        if stdout_error is not None or stderr_error is not None:
            raise RelayError("packaged MCP stdio validation could not read its child pipes")
        if any(
            value and value.encode("utf-8") in payload
            for value in private_values
            for payload in (stdout, stderr)
        ):
            raise RelayError("packaged MCP child emitted a child-only secret")
        complete_boundary = stdout.rfind(b"\n")
        if complete_boundary >= 0:
            responses = _responses_by_id(
                stdout[: complete_boundary + 1],
                allowed_ids=allowed_response_ids,
            )
            if response_id in responses:
                return responses[response_id]
        if process.poll() is not None:
            raise RelayError(f"packaged MCP exited before correlated response {response_id}")
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise ObservationTimeoutError(
                "packaged MCP stdio validation exceeded its total wall-clock deadline"
            )
        activity.wait(min(_PROCESS_POLL_SECONDS, remaining))
        activity.clear()


def _packaged_launch_environment() -> dict[str, str]:
    """Build a least-privilege broker environment without ambient credentials."""
    selected = {
        name: value
        for name, value in os.environ.items()
        if name.upper() in _PACKAGED_BASE_ENVIRONMENT_NAMES
        and _SENSITIVE_ENVIRONMENT_NAME.search(name) is None
    }
    return owner_environment(selected)


def _validated_extra_environment(
    extra_environment: Mapping[str, str] | None,
) -> dict[str, str]:
    """Validate explicit target-only values before one-shot broker transport."""
    environment: dict[str, str] = {}
    if extra_environment is None:
        return environment
    for name, value in extra_environment.items():
        if not name or "=" in name or "\x00" in name or "\x00" in value:
            raise RelayError("packaged MCP child environment contained an invalid entry")
        environment[name] = value
    return environment


def _capture_pipe(
    stream: BinaryIO,
    capture: _BoundedPipeCapture,
    activity: threading.Event,
) -> None:
    try:
        while True:
            chunk = os.read(stream.fileno(), _STREAM_READ_BYTES)
            if not chunk:
                return
            overflow = capture.append(chunk)
            activity.set()
            if overflow:
                return
    except OSError as exc:
        with capture.lock:
            capture.error = exc
        activity.set()


def _terminate_bounded_process(process: subprocess.Popen[bytes]) -> None:
    terminate_owned_process(cast(subprocess.Popen[str], cast(object, process)))


def _responses_by_id(
    stdout: bytes,
    *,
    allowed_ids: set[str] | None = None,
) -> dict[str, JSON]:
    if not stdout or not stdout.endswith(b"\n"):
        raise RelayError("packaged MCP stdio transcript omitted its final LF frame boundary")
    frames = stdout[:-1].split(b"\n")
    if any(not frame for frame in frames):
        raise RelayError("packaged MCP stdio transcript contained a blank frame")
    accepted_ids = allowed_ids or {_INITIALIZE_ID, _TOOLS_LIST_ID, _TOOLS_CALL_ID}
    responses: dict[str, JSON] = {}
    for frame in frames:
        decoded = decode_strict_json(frame, label="packaged MCP stdio transcript frame")
        if not isinstance(decoded, dict):
            raise RelayError("packaged MCP stdio transcript contained a non-object message")
        response = cast(JSON, decoded)
        response_id = response.get("id")
        if not isinstance(response_id, str):
            if response_id is None and (
                response.get("jsonrpc") == "2.0"
                and isinstance(response.get("method"), str)
                and bool(response.get("method"))
                and set(response) <= {"jsonrpc", "method", "params"}
                and ("params" not in response or isinstance(response.get("params"), dict))
            ):
                continue
            raise RelayError("packaged MCP stdio transcript contained an uncorrelated message")
        if response_id not in accepted_ids:
            raise RelayError("packaged MCP stdio transcript contained an unknown response id")
        if response_id in responses:
            raise RelayError("packaged MCP stdio transcript repeated a response id")
        if response.get("jsonrpc") != "2.0":
            raise RelayError("packaged MCP stdio transcript used an unexpected JSON-RPC version")
        has_result = "result" in response
        has_error = "error" in response
        if (
            has_result == has_error
            or "method" in response
            or set(response) - {"jsonrpc", "id", "result", "error"}
        ):
            raise RelayError("packaged MCP stdio transcript contained an invalid response envelope")
        if has_error:
            error = _mapping(response.get("error"))
            if (
                error is None
                or set(error) - {"code", "message", "data"}
                or not isinstance(error.get("code"), int)
                or isinstance(error.get("code"), bool)
                or not isinstance(error.get("message"), str)
            ):
                raise RelayError("packaged MCP stdio transcript contained an invalid error object")
        responses[response_id] = response
    return responses


def _validate_protocol_contract(
    *,
    initialize_response: JSON,
    tools_list_response: JSON,
    called_tool: str,
    profile: str,
) -> tuple[JSON, list[JSON], JSON]:
    server_info = _validate_initialize_contract(initialize_response)
    tools, selected = _validate_tools_contract(
        tools_list_response,
        called_tool=called_tool,
        profile=profile,
    )
    return server_info, tools, selected


def _validate_initialize_contract(initialize_response: JSON) -> JSON:
    """Validate the exact packaged relay initialization contract before activation."""
    initialize_result = _required_result(initialize_response, label="initialize")
    if set(initialize_result) != {"protocolVersion", "capabilities", "serverInfo"}:
        raise RelayError("packaged MCP initialize result contained unexpected fields")
    if initialize_result.get("protocolVersion") != _EXPECTED_PROTOCOL_VERSION:
        raise RelayError("packaged MCP initialize protocol version did not match")
    if initialize_result.get("capabilities") != {"tools": {}}:
        raise RelayError("packaged MCP initialize capabilities did not match")
    server_info = _mapping(initialize_result.get("serverInfo"))
    if server_info is None or server_info.get("name") != "clio-relay":
        raise RelayError("packaged MCP initialize serverInfo name did not match")
    if server_info.get("version") != __version__:
        raise RelayError(
            "packaged MCP initialize serverInfo version did not match the running distribution"
        )
    if set(server_info) != {"name", "version"}:
        raise RelayError("packaged MCP initialize serverInfo contained unexpected fields")
    return server_info


def _validate_tools_contract(
    tools_list_response: JSON,
    *,
    called_tool: str,
    profile: str,
) -> tuple[list[JSON], JSON]:
    """Validate tools/list before issuing the selected tools/call request."""
    tools_result = _required_result(tools_list_response, label="tools/list")
    if set(tools_result) - {"tools", "nextCursor", "_meta"}:
        raise RelayError("packaged MCP tools/list result contained unexpected fields")
    metadata = _mapping(tools_result.get("_meta"))
    if metadata is not None and (
        set(metadata)
        != {
            "clio-relay/remote-mcp-catalog-revision",
            "clio-relay/profile",
        }
        or metadata.get("clio-relay/profile") != profile
        or not isinstance(metadata.get("clio-relay/remote-mcp-catalog-revision"), str)
        or re.fullmatch(
            r"[0-9a-f]{64}",
            cast(str, metadata.get("clio-relay/remote-mcp-catalog-revision")),
        )
        is None
    ):
        raise RelayError("packaged MCP tools/list metadata did not match its exact contract")
    if tools_result.get("nextCursor") is not None:
        raise RelayError("packaged MCP tools/list was paginated and therefore incomplete")
    raw_tools = tools_result.get("tools")
    if not isinstance(raw_tools, list):
        raise RelayError("packaged MCP tools/list omitted its tools array")
    tools: list[JSON] = []
    names: set[str] = set()
    selected: JSON | None = None
    for raw_tool in cast(list[object], raw_tools):
        if not isinstance(raw_tool, dict):
            raise RelayError("packaged MCP tools/list contained a non-object tool")
        definition = cast(JSON, raw_tool)
        name = definition.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise RelayError("packaged MCP tools/list contained an invalid or duplicate tool name")
        if not isinstance(definition.get("description"), str) or not isinstance(
            definition.get("inputSchema"), dict
        ):
            raise RelayError(f"packaged MCP tool {name} omitted its exact agent-facing schema")
        names.add(name)
        tools.append(definition)
        if name == called_tool:
            selected = definition
    if selected is None:
        raise RelayError(f"packaged MCP did not advertise required tool {called_tool}")
    if profile == "user":
        forbidden_static = (
            static_mcp_tool_names() - USER_MCP_TOOL_NAMES - set(jarvis_user_contract())
        )
        leaked = sorted(names & forbidden_static)
        if leaked:
            raise RelayError("packaged user MCP exposed static administrative tools")
    _canonical_digest({"tools": tools})
    return tools, selected


def _validate_pinned_jarvis_contract(tools: list[JSON]) -> str | None:
    if jarvis_user_contract_digest() != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256:
        raise RelayError("bundled clio-kit JARVIS contract digest did not match its pin")
    pinned_names = set(jarvis_user_contract())
    actual = {cast(str, tool["name"]): tool for tool in tools if tool.get("name") in pinned_names}
    # Built-in JARVIS tools are advertised only when this profile has a verified
    # JARVIS route. Generic remote-MCP acceptance must therefore permit the exact
    # empty surface. Once any built-in JARVIS tool is exposed, however, the whole
    # pinned contract remains mandatory so a partial or mixed-version surface can
    # never pass release validation.
    if not actual:
        return None
    if set(actual) != pinned_names:
        raise RelayError("packaged MCP tools/list omitted part of the pinned JARVIS contract")
    cluster_enums: list[tuple[str, ...]] = []
    for name in sorted(pinned_names):
        input_schema = _mapping(actual[name].get("inputSchema")) or {}
        properties = _mapping(input_schema.get("properties")) or {}
        cluster_schema = _mapping(properties.get("cluster")) or {}
        raw_enum = cluster_schema.get("enum")
        enum_items = cast(list[object], raw_enum) if isinstance(raw_enum, list) else []
        if not isinstance(raw_enum, list) or not all(
            isinstance(item, str) and item for item in enum_items
        ):
            raise RelayError("packaged JARVIS tools omitted their configured cluster enum")
        enum = tuple(cast(list[str], raw_enum))
        if list(enum) != sorted(set(enum)):
            raise RelayError("packaged JARVIS tools exposed an invalid configured cluster enum")
        cluster_enums.append(enum)
    if len(set(cluster_enums)) != 1:
        raise RelayError("packaged JARVIS tools disagreed about configured cluster targets")
    clusters = list(cluster_enums[0])
    expected = {
        cast(str, definition["name"]): definition
        for definition in virtual_jarvis_tool_definitions(clusters=clusters)
    }
    if actual != expected:
        raise RelayError("packaged MCP JARVIS v3.6 agent-facing schema did not match its pin")
    return _tools_digest([actual[name] for name in sorted(actual)])


def _required_result(response: JSON, *, label: str) -> JSON:
    if "error" in response:
        raise RelayError(f"packaged MCP {label} returned a JSON-RPC error")
    result = _mapping(response.get("result"))
    if result is None:
        raise RelayError(f"packaged MCP {label} omitted its result object")
    return result


def _safe_call_job_id(response: JSON) -> str | None:
    """Project only one bounded non-secret relay job identifier from a call result."""
    try:
        structured = _validated_call_structured_content(response)
    except RelayError:
        return None
    candidate = structured.get("job_id")
    if (
        isinstance(candidate, str)
        and len(candidate) <= 1_024
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", candidate) is not None
    ):
        return candidate
    return None


def _validated_call_structured_content(response: JSON) -> JSON:
    """Validate one exact successful MCP result before projecting any durable identifier."""
    result = _required_result(response, label="tools/call")
    if set(result) != {"content", "structuredContent", "isError"}:
        raise RelayError("packaged MCP tools/call result contained unexpected fields")
    if result.get("isError") is not False:
        raise RelayError("packaged MCP tools/call reported an error")
    raw_content = result.get("content")
    structured = _mapping(result.get("structuredContent"))
    if not isinstance(raw_content, list) or structured is None:
        raise RelayError("packaged MCP tools/call omitted its exact structured result")
    content = cast(list[object], raw_content)
    if len(content) != 1 or not isinstance(content[0], dict):
        raise RelayError("packaged MCP tools/call returned invalid text content")
    item = cast(JSON, content[0])
    if set(item) != {"type", "text"}:
        raise RelayError("packaged MCP tools/call returned invalid text content")
    text = item.get("text")
    if item.get("type") != "text" or not isinstance(text, str):
        raise RelayError("packaged MCP tools/call returned invalid text content")
    if decode_strict_json(text, label="packaged MCP tools/call text") != structured:
        raise RelayError("packaged MCP tools/call text and structured content differed")
    return structured


def _tools_digest(tools: list[JSON]) -> str:
    ordered = sorted(tools, key=lambda definition: cast(str, definition.get("name")))
    return _canonical_digest({"tools": ordered})


def _canonical_digest(value: object) -> str:
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RelayError("packaged MCP contract was not canonical JSON") from exc
    return hashlib.sha256(payload).hexdigest()


def _hash_regular_file(path: Path, *, expected: tuple[int, int, int, int]) -> str:
    digest = hashlib.sha256()
    remaining = expected[2]
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if _stat_identity(opened) != expected or not stat.S_ISREG(opened.st_mode):
                raise RelayError("configured packaged clio-relay executable identity changed")
            while remaining > 0:
                chunk = stream.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise RelayError("configured packaged clio-relay executable identity changed")
                digest.update(chunk)
                remaining -= len(chunk)
            if stream.read(1):
                raise RelayError("configured packaged clio-relay executable identity changed")
        final_identity = _stat_identity(path.stat())
    except OSError as exc:
        raise RelayError("configured packaged clio-relay executable could not be read") from exc
    if final_identity != expected:
        raise RelayError("configured packaged clio-relay executable identity changed")
    return digest.hexdigest()


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (int(value.st_dev), int(value.st_ino), int(value.st_size), int(value.st_mtime_ns))


def _sanitized_diagnostic(
    stderr: bytes,
    *,
    forbidden_values: Iterable[str] = (),
) -> str:
    decoded = stderr.decode("utf-8", errors="replace")
    printable = "".join(
        character if character in "\n\r\t" or character.isprintable() else "?"
        for character in decoded
    )
    redacted = printable
    sensitive_values = {
        value
        for name, value in os.environ.items()
        if len(value) >= 8 and _SENSITIVE_ENVIRONMENT_NAME.search(name)
    }
    sensitive_values.update(value for value in forbidden_values if value)
    for value in sorted(sensitive_values, key=len, reverse=True):
        redacted = redacted.replace(value, "[redacted]")
    redacted = _BEARER_DIAGNOSTIC.sub("Bearer [redacted]", redacted)
    redacted = _SENSITIVE_DIAGNOSTIC.sub(r"\1\2[redacted]", redacted)
    bounded = bounded_error_detail(redacted)
    if bounded is None:
        return ""
    encoded = bounded.encode("utf-8")
    if len(encoded) <= _DIAGNOSTIC_BYTES:
        return bounded
    return encoded[:_DIAGNOSTIC_BYTES].decode("utf-8", errors="ignore")


def decode_strict_json(payload: bytes | str, *, label: str) -> object:
    """Decode duplicate-free UTF-8 JSON and reject every non-finite number."""
    failed = False
    try:
        text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        decoded = cast(
            object,
            json.loads(
                text,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_nonfinite_json,
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
        failed = True
        decoded = None
    if failed:
        raise RelayError(f"{label} contained invalid JSON") from None
    _reject_nested_nonfinite_json(decoded, label=label)
    return decoded


def _reject_nested_nonfinite_json(value: object, *, label: str) -> None:
    stack: list[tuple[object, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > 64:
            raise RelayError(f"{label} exceeded the JSON nesting limit")
        if isinstance(current, float) and not math.isfinite(current):
            raise RelayError(f"{label} contained a non-finite JSON number")
        if isinstance(current, dict):
            stack.extend(
                (nested, depth + 1) for nested in cast(dict[object, object], current).values()
            )
        elif isinstance(current, list):
            stack.extend((nested, depth + 1) for nested in cast(list[object], current))


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> JSON:
    result: JSON = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_nonfinite_json(value: str) -> None:
    raise ValueError(f"non-finite JSON value: {value}")


def _mapping(value: object) -> JSON | None:
    return cast(JSON, value) if isinstance(value, dict) else None
