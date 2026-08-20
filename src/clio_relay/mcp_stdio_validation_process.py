"""Bounded, contained child-process spawn and the staged MCP handshake exchange.

Extracted from :mod:`clio_relay.mcp_stdio_validation` (file-size
decomposition; see ``scripts/check_file_size.py``). ``_run_bounded_process``
(spawn under verified containment, enforce the stdin/stdout/stderr byte caps
and the total wall-clock deadline, detect a child-emitted secret, and verify
the owned process tree is empty on the way out) and the staged handshake it
drives when ``staged_mcp=True`` (``_exchange_staged_mcp``/``_write_mcp_frame``/
``_await_mcp_response``, which wait for ``initialize`` before sending
``notifications/initialized``/``tools/list``/``tools/call`` in protocol
order) stay in ONE module rather than split further: they share the same
``_BoundedPipeCapture`` pair and ``threading.Event`` across every read, and
must all observe the identical ``deadline``/``monotonic()`` semantics several
tests assert on directly, so separating them would mean passing that shared
mutable state across a module boundary for no real cohesion benefit.

Every name this module reads as a bare global that ``tests/test_mcp_stdio_
validation.py`` monkeypatches while calling ``_run_bounded_process`` directly
(``monotonic``, ``spawn_owned_process``, ``_capture_pipe``,
``_terminate_bounded_process``, ``release_owned_process``,
``_MAX_STDOUT_BYTES``, ``_MAX_STDERR_BYTES``) is bound in THIS module's own
namespace -- imported names included -- so the test patches this module
object, not the facade, for those seven attributes (the facade's own
``run_packaged_mcp_stdio_session`` calls ``_run_bounded_process`` unchanged,
and reaches the same patched bindings through the ordinary call). The module
logger is pinned to the pre-split name ``clio_relay.mcp_stdio_validation``
(rather than ``__name__``) because ``test_packaged_stdio_timeout_keeps_
child_stderr_off_public_error`` asserts on it via
``caplog.at_level(..., logger="clio_relay.mcp_stdio_validation")``.
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
from collections.abc import Mapping
from time import monotonic
from typing import Any, cast

from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.mcp_stdio_validation_contract import (
    _INITIALIZE_ID,
    _TOOLS_CALL_ID,
    _TOOLS_LIST_ID,
    _responses_by_id,
    _validate_initialize_contract,
    _validate_tools_contract,
    _validated_call_structured_content,
)
from clio_relay.mcp_stdio_validation_process_io import (
    _BoundedPipeCapture,
    _capture_pipe,
    _packaged_launch_environment,
    _terminate_bounded_process,
    _validated_extra_environment,
)
from clio_relay.mcp_stdio_validation_support import (
    _SENSITIVE_ENVIRONMENT_NAME,
    _sanitized_diagnostic,
)
from clio_relay.process_containment import (
    OwnedProcessSpawnError,
    broker_child_environment_payload,
    ensure_owned_process_tree_empty,
    release_owned_process,
    spawn_owned_process,
)

JSON = dict[str, Any]
logger = logging.getLogger("clio_relay.mcp_stdio_validation")

_MAX_STDIN_BYTES = 4 * 1024 * 1024
_MAX_STDOUT_BYTES = 4 * 1024 * 1024
_MAX_STDERR_BYTES = 256 * 1024
_PROCESS_POLL_SECONDS = 0.02


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
        if deadline_expired:
            logger.warning(
                "packaged MCP timeout: command=clio-relay-mcp-server "
                "phase=stdio_validation timeout_seconds=%s stdout_bytes=%s "
                "stderr_bytes=%s stderr=%r%s",
                timeout_seconds,
                len(stdout),
                len(stderr),
                detail,
                cause,
            )
            raise ObservationTimeoutError(
                "packaged clio-relay mcp-server timed out during stdio validation "
                f"after {timeout_seconds:g} seconds"
            ) from None
        raise RelayError(
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
