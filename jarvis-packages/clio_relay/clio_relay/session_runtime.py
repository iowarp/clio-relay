"""Run one bounded MCP stdio session: launch, handshake, call/list, teardown.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3).
``_run_mcp_session`` is both directly monkeypatched on the ``runner`` facade
(``tests/test_mcp_call_runner.py`` calls ``runner._run_mcp_session(...)``
directly in several tests) and calls several *other* individually-monkeypatched
names itself: ``_open_process``, ``_install_parent_termination_handlers``,
``_restore_parent_termination_handlers``, and the ``TOOLS_LIST_MAX_PAGES`` /
``TOOLS_LIST_MAX_TOOLS`` / ``TOOLS_LIST_MAX_RESPONSE_BYTES`` /
``MCP_CALL_MAX_RESPONSE_BYTES`` bounds (``tests/test_mcp_call_runner.py``
monkeypatches these constants directly on ``runner`` too, e.g.
``monkeypatch.setattr(runner, "TOOLS_LIST_MAX_PAGES", 1)``). All of those calls
and constant reads go through ``_facade()`` -- a deferred, call-time attribute
lookup on the ``clio_relay.mcp_call.runner`` module object -- instead of a
plain top-level import, so a monkeypatch applied to the facade's own attribute
is observed here too. See :mod:`clio_relay.clio_kit_runtime_identity`
for the same pattern applied to a smaller module.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from queue import Empty, Queue
from typing import Any, cast

from clio_relay._mcp_call_runner_facade import facade as _facade
from clio_relay.constants import (
    _CLIO_KIT_CACHE_EVENT_SCHEMA,
    _CLIO_KIT_LOCKED_SERVER_SCHEMA,
    _CLIO_KIT_POST_BUILD_EVENTS,
    _CLIO_KIT_REQUEST_ENV_OVERRIDES,
    _JARVIS_MCP_SPACK_COMMAND_CHILD_ENV,
    _RELAY_JARVIS_SPACK_COMMAND_ENV,
    _TOOLS_LIST_PAGINATION_KEY,
    MCP_INITIALIZE_MAX_RESPONSE_BYTES,
    MCP_SESSION_MAX_STDERR_BYTES,
    MCP_SESSION_MAX_STDOUT_BYTES,
)
from clio_relay.params_and_manifest import _required_optional_str
from clio_relay.progress_bridge import _McpProgressBridge
from clio_relay.protocol_messages import (
    _call_message,
    _decoded_json_object,
    _initialize_message,
    _initialized_message,
    _McpProtocolFailure,
    _response_id,
    _StreamEvent,
    _StreamLimit,
    _tools_list_message,
)
from clio_relay.stdio_io import (
    _drain_available,
    _join_reader,
    _start_reader,
    _wait_for_response,
    _write_message,
)


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
    wait_for_locked_launcher: bool = False,
) -> subprocess.CompletedProcess[str]:
    overrides = _child_environment_overrides(wait_for_locked_launcher=wait_for_locked_launcher)
    process = _facade()._open_process(
        command,
        env_from=env_from or {},
        environment_overrides=overrides or None,
    )
    previous_handlers = _facade()._install_parent_termination_handlers(process)
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
        if wait_for_locked_launcher:
            _wait_for_locked_launcher_readiness(
                stderr_queue,
                stderr_lines,
                process=process,
                deadline=deadline,
                command=command,
            )
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
                max_response_bytes=_facade().MCP_CALL_MAX_RESPONSE_BYTES,
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
        _facade()._terminate_process_tree(process)
    except subprocess.TimeoutExpired as exc:
        _facade()._terminate_process_tree(process)
        _drain_available(stdout_queue, stdout_lines)
        _drain_available(stderr_queue, stderr_lines)
        raise subprocess.TimeoutExpired(
            command,
            timeout if timeout is not None else 0,
            output="".join(stdout_lines) or exc.output,
            stderr="".join(stderr_lines) or exc.stderr,
        ) from exc
    finally:
        _facade()._restore_parent_termination_handlers(previous_handlers)
        if process.poll() is None:
            _facade()._terminate_process_tree(process)
        _join_reader(stdout_thread, stdout_queue, stdout_lines)
        _join_reader(stderr_thread, stderr_queue, stderr_lines)
    return subprocess.CompletedProcess(
        command,
        process.returncode if process.returncode is not None else 0,
        stdout="".join(stdout_lines),
        stderr="".join(stderr_lines),
    )


def _requires_locked_launcher_readiness(server_artifact: dict[str, Any]) -> bool:
    """Return whether a verified nested clio-kit launcher needs its stdin gate."""
    raw_runtime = server_artifact.get("nested_runtime")
    runtime = cast(dict[str, Any], raw_runtime) if isinstance(raw_runtime, dict) else None
    return bool(
        server_artifact.get("verified") is True
        and server_artifact.get("nested_launcher") is True
        and runtime is not None
        and runtime.get("schema_version") == _CLIO_KIT_LOCKED_SERVER_SCHEMA
        and runtime.get("locked_runtime_verified") is True
    )


def _wait_for_locked_launcher_readiness(
    queue: Queue[_StreamEvent],
    lines: list[str],
    *,
    process: subprocess.Popen[str],
    deadline: float | None,
    command: list[str],
) -> None:
    """Withhold MCP stdin until the verified clio-kit launcher finishes its build."""
    while True:
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise subprocess.TimeoutExpired(command, timeout=0)
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
                "verified clio-kit launcher stderr closed before its post-build "
                f"readiness event{detail}"
            )
        if isinstance(line, _StreamLimit):
            raise _McpProtocolFailure(line.message)
        lines.append(line)
        message = _decoded_json_object(line)
        if (
            message is not None
            and message.get("schema_version") == _CLIO_KIT_CACHE_EVENT_SCHEMA
            and message.get("event") in _CLIO_KIT_POST_BUILD_EVENTS
        ):
            return


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
            max_response_bytes=_facade().MCP_CALL_MAX_RESPONSE_BYTES,
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
        if pages >= _facade().TOOLS_LIST_MAX_PAGES:
            raise _McpProtocolFailure(
                f"tools/list exceeded maximum page count {_facade().TOOLS_LIST_MAX_PAGES}"
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
            max_response_bytes=_facade().TOOLS_LIST_MAX_RESPONSE_BYTES,
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
            if len(tools_by_name) > _facade().TOOLS_LIST_MAX_TOOLS:
                raise _McpProtocolFailure(
                    f"tools/list exceeded maximum tool count {_facade().TOOLS_LIST_MAX_TOOLS}"
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
                    "max_pages": _facade().TOOLS_LIST_MAX_PAGES,
                    "max_tools": _facade().TOOLS_LIST_MAX_TOOLS,
                    "max_response_bytes": _facade().TOOLS_LIST_MAX_RESPONSE_BYTES,
                },
            },
        },
    }
    stdout_lines.append(json.dumps(aggregate, separators=(",", ":")) + "\n")


def _child_environment_overrides(*, wait_for_locked_launcher: bool) -> dict[str, str]:
    """Return every relay-owned value applied on top of the referenced child env."""
    overrides: dict[str, str] = {}
    if wait_for_locked_launcher:
        overrides.update(_CLIO_KIT_REQUEST_ENV_OVERRIDES)
    overrides.update(_relay_composed_run_environment())
    return overrides


def _relay_composed_run_environment() -> dict[str, str]:
    """Map the relay-composed site Spack identity onto the JARVIS child variable.

    Returns:
        The child overrides, empty when the relay composed no site identity for
        this call. The relay publishes the variable only for a JARVIS run whose
        cluster registered a Spack executable, so an unregistered cluster keeps
        the previous child environment exactly.
    """
    composed = os.environ.get(_RELAY_JARVIS_SPACK_COMMAND_ENV)
    if not composed:
        return {}
    return {_JARVIS_MCP_SPACK_COMMAND_CHILD_ENV: composed}
