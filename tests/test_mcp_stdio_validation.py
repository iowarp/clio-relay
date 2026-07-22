from __future__ import annotations

import copy
import hashlib
import json
import os
import shlex
import shutil
import sys
import time
import traceback
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pytest import MonkeyPatch

from clio_relay import __version__
from clio_relay import mcp_stdio_validation as mcp_stdio_validation_module
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_ID,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    JARVIS_MCP_CACHE_SERVER_NAME,
    jarvis_user_contract,
    virtual_jarvis_tool_definitions,
)
from clio_relay.mcp_stdio_validation import run_packaged_mcp_stdio_session
from clio_relay.models import McpCallSpec, deterministic_jarvis_execution_id
from clio_relay.process_containment import OwnedProcessSpawnError
from clio_relay.remote_mcp import (
    RemoteMcpDiscoveryProvenance,
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    RemoteMcpToolSchema,
    remote_mcp_server_artifact_digest,
)
from tests.jarvis_mcp_fakes import verified_jarvis_server_artifact

JSON = dict[str, Any]


def _canonical_digest(value: object) -> str:
    payload = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_fake_executable(tmp_path: Path, program: str) -> Path:
    runner = tmp_path / "fake_mcp_runner.py"
    runner.write_text(program, encoding="utf-8")
    if os.name == "nt":
        launcher = tmp_path / "clio-relay.cmd"
        launcher.write_text(
            f'@echo off\r\n"{sys.executable}" "{runner}" %*\r\n',
            encoding="utf-8",
        )
    else:
        launcher = tmp_path / "clio-relay"
        launcher.write_text(
            f'#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(runner))} "$@"\n',
            encoding="utf-8",
        )
        launcher.chmod(0o755)
    return launcher.resolve()


def _fake_transcript(*, version: str = __version__, tools: list[JSON] | None = None) -> bytes:
    selected_tools = (
        tools if tools is not None else virtual_jarvis_tool_definitions(clusters=["alpha"])
    )
    responses: tuple[JSON, ...] = (
        {
            "jsonrpc": "2.0",
            "id": "clio-relay-validation-initialize",
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "clio-relay", "version": version},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": "clio-relay-validation-tools-list",
            "result": {"tools": selected_tools},
        },
        {
            "jsonrpc": "2.0",
            "id": "clio-relay-validation-tools-call",
            "result": {
                "content": [{"type": "text", "text": '{"ok":true}'}],
                "structuredContent": {"ok": True},
                "isError": False,
            },
        },
    )
    return b"".join(
        json.dumps(item, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
        for item in responses
    )


def _staged_fake_program(
    transcript: bytes,
    *,
    prefix: str = "",
    consume_environment: bool = False,
) -> str:
    """Build a fake server that responds only after each required MCP lifecycle request."""
    responses = transcript.splitlines(keepends=True)
    assert len(responses) == 3
    environment_setup = (
        "from clio_relay.process_containment import consume_broker_child_environment\n"
        "consume_broker_child_environment()\n"
        if consume_environment
        else ""
    )
    return (
        "import json\nimport sys\n"
        + environment_setup
        + prefix
        + f"responses = {responses!r}\n"
        + "expected = [\n"
        + "    ('initialize', 'clio-relay-validation-initialize', 0),\n"
        + "    ('notifications/initialized', None, None),\n"
        + "    ('tools/list', 'clio-relay-validation-tools-list', 1),\n"
        + "    ('tools/call', 'clio-relay-validation-tools-call', 2),\n"
        + "]\n"
        + "for method, request_id, response_index in expected:\n"
        + "    line = sys.stdin.buffer.readline()\n"
        + "    if not line:\n        raise SystemExit(81)\n"
        + "    request = json.loads(line)\n"
        + "    if request.get('method') != method or request.get('id') != request_id:\n"
        + "        raise SystemExit(82)\n"
        + "    if response_index is not None:\n"
        + "        sys.stdout.buffer.write(responses[response_index])\n"
        + "        sys.stdout.buffer.flush()\n"
    )


def test_packaged_stdio_session_initializes_lists_and_calls_virtual_jarvis(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    monkeypatch.setenv("CLIO_RELAY_CORE_DIR", str(tmp_path / "core"))
    monkeypatch.setenv("CLIO_RELAY_SPOOL_DIR", str(tmp_path / "spool"))
    cache_path = tmp_path / ".clio-relay" / "remote-mcp-cache.json"
    monkeypatch.setenv("CLIO_RELAY_REMOTE_MCP_CACHE", str(cache_path))
    ClusterRegistry(clusters={"alpha": ClusterDefinition(name="alpha", ssh_host="localhost")}).save(
        tmp_path / ".clio-relay" / "clusters.json"
    )
    server_artifact = verified_jarvis_server_artifact()
    contract = jarvis_user_contract()
    now = datetime.now(UTC)
    RemoteMcpSchemaCache.update_entry(
        cache_path,
        RemoteMcpSchemaCacheEntry(
            cluster="alpha",
            server_name=JARVIS_MCP_CACHE_SERVER_NAME,
            execution_fingerprint="fixture",
            discovered_at=now,
            expires_at=now + timedelta(hours=1),
            schema_digest=CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
            tools=[
                RemoteMcpToolSchema(
                    name=name,
                    description=str(definition["description"]),
                    input_schema=definition["inputSchema"],
                    output_schema=definition["outputSchema"],
                    annotations=definition["annotations"],
                )
                for name, definition in contract.items()
            ],
            provenance=RemoteMcpDiscoveryProvenance(
                discovery_job_id="job-discovery",
                artifact_id="artifact-discovery",
                artifact_sha256="b" * 64,
                server_artifact=server_artifact,
            ),
        ),
    )
    packaged_executable = shutil.which("clio-relay")
    assert packaged_executable is not None
    wrong_path = tmp_path / "wrong-path"
    wrong_path.mkdir()
    wrong_executable = _write_fake_executable(
        wrong_path,
        "import sys\nsys.stdin.buffer.read()\nraise SystemExit(97)\n",
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", packaged_executable)
    monkeypatch.setenv("PATH", str(wrong_path) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("CLIO_RELAY_STORAGE_MINIMUM_FREE_BYTES", "0")
    monkeypatch.setenv("CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_STREAM", "1024")
    monkeypatch.setenv("CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_JOB", "1024")
    monkeypatch.setenv("CLIO_RELAY_STORAGE_JOB_CORE_ALLOWANCE_BYTES", "1024")
    monkeypatch.setenv("CLIO_RELAY_STORAGE_JOB_RESULT_ALLOWANCE_BYTES", "1024")

    session = run_packaged_mcp_stdio_session(
        profile="user",
        tool="jarvis_run",
        arguments={"cluster": "alpha", "pipeline_id": "stdio-acceptance"},
    )

    initialize = session.initialize_response["result"]
    listed = session.tools_list_response["result"]["tools"]
    assert "result" in session.tools_call_response, session.evidence()
    call = session.tools_call_response["result"]["structuredContent"]
    job = ClioCoreQueue(tmp_path / "core").get_job(call["job_id"])
    assert initialize["serverInfo"]["name"] == "clio-relay"
    assert "jarvis_run" in {tool["name"] for tool in listed}
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.expected_server_artifact_digest == remote_mcp_server_artifact_digest(
        server_artifact
    )
    assert job.spec.tool == "jarvis_run"
    execution_id = deterministic_jarvis_execution_id(
        cluster=job.cluster,
        idempotency_key=job.idempotency_key,
        job_id=job.job_id,
    )
    assert job.spec.arguments == {
        "pipeline_id": "stdio-acceptance",
        "execution_id": execution_id,
    }
    assert session.evidence()["boundary"] == "packaged_clio_relay_mcp_server_stdio"
    assert session.transcript_sha256
    assert session.command[0] == str(Path(packaged_executable).resolve())
    assert session.command[0] != str(wrong_executable)
    assert session.configured_executable == str(Path(packaged_executable).absolute())
    assert session.canonical_executable == str(Path(packaged_executable).resolve())
    assert session.executable_sha256 is not None and len(session.executable_sha256) == 64
    assert session.server_info_sha256 is not None and len(session.server_info_sha256) == 64
    assert session.tools_list_sha256 is not None and len(session.tools_list_sha256) == 64
    assert session.called_tool_schema_sha256 is not None
    assert session.jarvis_virtual_tools_sha256 is not None
    ordered_tools = sorted(listed, key=lambda definition: definition["name"])
    called_tool = next(definition for definition in listed if definition["name"] == "jarvis_run")
    jarvis_tools = [
        definition for definition in ordered_tools if definition["name"] in jarvis_user_contract()
    ]
    assert session.server_info_sha256 == _canonical_digest(initialize["serverInfo"])
    assert session.tools_list_sha256 == _canonical_digest({"tools": ordered_tools})
    assert session.called_tool_schema_sha256 == _canonical_digest(called_tool)
    assert session.jarvis_virtual_tools_sha256 == _canonical_digest({"tools": jarvis_tools})
    evidence = session.evidence()
    assert evidence["jarvis_contract_id"] == CLIO_KIT_JARVIS_USER_CONTRACT_ID
    assert evidence["jarvis_contract_sha256"] == CLIO_KIT_JARVIS_USER_CONTRACT_SHA256
    assert isinstance(evidence["containment_enforceable"], bool)
    assert evidence["called_tool_name"] == "jarvis_run"
    assert evidence["call_job_id"] == call["job_id"]
    assert "protocol_evidence_sha256" in evidence
    for forbidden_key in (
        "initialize_response",
        "tools_list_response",
        "tools_call_response",
        "transcript_sha256",
        "stderr_sha256",
        "stderr_excerpt",
    ):
        assert forbidden_key not in evidence
    capability = "unknown-one-time-capability-value"
    session.tools_call_response["result"]["structuredContent"]["capability"] = capability
    serialized_evidence = json.dumps(session.evidence(), sort_keys=True)
    assert "capability" not in serialized_evidence
    assert capability not in serialized_evidence


def test_packaged_stdio_session_rejects_unverified_configured_executable(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE",
        str(tmp_path / "missing-clio-relay"),
    )
    with pytest.raises(RelayError, match="could not be verified"):
        run_packaged_mcp_stdio_session(profile="user", tool="jarvis_run", arguments={})


def test_packaged_stdio_session_kills_output_flood_and_sanitizes_diagnostics(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    executable = _write_fake_executable(
        tmp_path,
        """
import sys

sys.stdin.buffer.readline()
sys.stderr.write("token=top-secret-value\\nraw=top-secret-value\\n")
sys.stderr.flush()
sys.stdout.buffer.write(b"x" * 65536)
sys.stdout.buffer.flush()
""".lstrip(),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))
    monkeypatch.setenv("MCP_TEST_TOKEN", "top-secret-value")
    monkeypatch.setattr(mcp_stdio_validation_module, "_MAX_STDOUT_BYTES", 1_024)

    with pytest.raises(RelayError, match="stdout byte limit") as captured:
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={},
            timeout_seconds=2,
        )
    diagnostic = str(captured.value)
    assert "top-secret-value" not in diagnostic
    assert "token=[redacted]" in diagnostic
    assert len(diagnostic.encode("utf-8")) < 8_192


@pytest.mark.parametrize("timeout_seconds", [0.0, -1.0, float("nan"), float("inf")])
def test_packaged_stdio_session_rejects_nonfinite_or_nonpositive_deadline(
    timeout_seconds: float,
) -> None:
    with pytest.raises(RelayError, match="finite and positive"):
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={},
            timeout_seconds=timeout_seconds,
        )


def test_packaged_stdio_session_kills_stderr_flood_with_bounded_diagnostic(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    executable = _write_fake_executable(
        tmp_path,
        """
import sys

sys.stdin.buffer.readline()
sys.stderr.buffer.write(b"y" * 65536)
sys.stderr.buffer.flush()
""".lstrip(),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))
    monkeypatch.setattr(mcp_stdio_validation_module, "_MAX_STDERR_BYTES", 1_024)

    with pytest.raises(RelayError, match="stderr byte limit") as captured:
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={},
            timeout_seconds=2,
        )
    diagnostic = str(captured.value)
    assert "stderr_bytes=1024" in diagnostic
    assert len(diagnostic.encode("utf-8")) < 8_192


def test_packaged_stdio_session_kills_process_group_at_total_deadline(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    marker = tmp_path / "escaped-child.txt"
    child = (
        "import pathlib,time;time.sleep(0.5);"
        f"pathlib.Path({str(marker)!r}).write_text('escaped',encoding='utf-8')"
    )
    executable = _write_fake_executable(
        tmp_path,
        f"""
import subprocess
import sys
import time

sys.stdin.buffer.read()
subprocess.Popen([sys.executable, "-c", {child!r}])
time.sleep(10)
""".lstrip(),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))

    started = time.monotonic()
    with pytest.raises(ObservationTimeoutError, match="total wall-clock deadline"):
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={},
            timeout_seconds=0.1,
        )
    assert time.monotonic() - started < 3
    time.sleep(0.7)
    assert not marker.exists()


def test_packaged_stdio_rejects_root_that_becomes_terminal_after_deadline(
    monkeypatch: MonkeyPatch,
) -> None:
    """A terminal poll cannot bypass the absolute deadline after the loop condition."""
    private = cast(Any, mcp_stdio_validation_module)
    clock_calls = 0

    def clock() -> float:
        nonlocal clock_calls
        clock_calls += 1
        return 0.0 if clock_calls <= 2 else 2.0

    process = SimpleNamespace(
        pid=8124,
        stdout=BytesIO(b""),
        stderr=BytesIO(b""),
        returncode=0,
        poll=lambda: 0,
    )

    def spawn(
        _command: list[str],
        *,
        on_ready: Any,
        **_kwargs: object,
    ) -> object:
        on_ready(
            process.pid,
            {"mode": "windows_job_object", "enforceable": True},
        )
        return process

    monkeypatch.setattr(mcp_stdio_validation_module, "monotonic", clock)
    monkeypatch.setattr(mcp_stdio_validation_module, "spawn_owned_process", spawn)

    def ignore_process(_process: object) -> None:
        return None

    monkeypatch.setattr(mcp_stdio_validation_module, "_terminate_bounded_process", ignore_process)
    monkeypatch.setattr(mcp_stdio_validation_module, "release_owned_process", ignore_process)

    with pytest.raises(RelayError, match="total wall-clock deadline"):
        private._run_bounded_process(
            ("clio-relay",),
            session_input=b"{}\n",
            timeout_seconds=1.0,
            extra_environment=None,
        )


def test_packaged_stdio_session_rejects_server_version_mismatch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    transcript = _fake_transcript(version="0.0.0")
    executable = _write_fake_executable(
        tmp_path,
        _staged_fake_program(transcript),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))

    with pytest.raises(RelayError, match="serverInfo version did not match"):
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={},
            timeout_seconds=2,
        )


def test_packaged_stdio_session_uses_staged_mcp_lifecycle(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The client waits for initialize before activation, list, and call requests."""
    responses = _fake_transcript().splitlines(keepends=True)
    program = f"""
import json
import queue
import sys
import threading

requests = queue.Queue()

def read_requests():
    for _ in range(4):
        line = sys.stdin.buffer.readline()
        if not line:
            return
        requests.put(json.loads(line))

reader = threading.Thread(target=read_requests)
reader.start()
initialize = requests.get(timeout=2)
if initialize.get("method") != "initialize":
    raise SystemExit(84)
try:
    requests.get(timeout=0.1)
except queue.Empty:
    pass
else:
    raise SystemExit(85)
responses = {responses!r}
sys.stdout.buffer.write(responses[0])
sys.stdout.buffer.flush()
initialized = requests.get(timeout=2)
tools_list = requests.get(timeout=2)
if initialized.get("method") != "notifications/initialized":
    raise SystemExit(86)
if tools_list.get("method") != "tools/list":
    raise SystemExit(87)
sys.stdout.buffer.write(responses[1])
sys.stdout.buffer.flush()
tools_call = requests.get(timeout=2)
if tools_call.get("method") != "tools/call":
    raise SystemExit(88)
sys.stdout.buffer.write(responses[2])
sys.stdout.buffer.flush()
reader.join(timeout=2)
if reader.is_alive():
    raise SystemExit(89)
""".lstrip()
    executable = _write_fake_executable(tmp_path, program)
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))

    session = run_packaged_mcp_stdio_session(
        profile="user",
        tool="jarvis_run",
        arguments={},
        timeout_seconds=5,
    )

    assert session.returncode == 0


def test_packaged_stdio_rejects_unexpected_initialize_secret(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Unknown server fields cannot flow into any downstream validation report."""
    secret = "unexpected-initialize-secret"
    transcript = _fake_transcript().replace(
        b'"serverInfo":{"name":"clio-relay",',
        f'"serverInfo":{{"capability":"{secret}","name":"clio-relay",'.encode(),
    )
    executable = _write_fake_executable(tmp_path, _staged_fake_program(transcript))
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))

    with pytest.raises(RelayError, match="serverInfo contained unexpected fields") as failure:
        run_packaged_mcp_stdio_session(profile="user", tool="jarvis_run", arguments={})
    assert secret not in str(failure.value)


def test_packaged_stdio_session_requires_exact_initialize_capabilities(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    transcript = _fake_transcript().replace(
        b'"capabilities":{"tools":{}}',
        b'"capabilities":{}',
    )
    executable = _write_fake_executable(
        tmp_path,
        _staged_fake_program(transcript),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))

    with pytest.raises(RelayError, match="initialize capabilities did not match"):
        run_packaged_mcp_stdio_session(profile="user", tool="jarvis_run", arguments={})


def test_packaged_stdio_session_rejects_jarvis_schema_mismatch(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    tools = copy.deepcopy(virtual_jarvis_tool_definitions(clusters=["alpha"]))
    jarvis_run = next(tool for tool in tools if tool["name"] == "jarvis_run")
    jarvis_run["inputSchema"]["properties"]["unreviewed_control"] = {"type": "boolean"}
    transcript = _fake_transcript(tools=tools)
    executable = _write_fake_executable(
        tmp_path,
        _staged_fake_program(transcript),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))

    with pytest.raises(RelayError, match="JARVIS v3.6 agent-facing schema did not match"):
        run_packaged_mcp_stdio_session(profile="user", tool="jarvis_run", arguments={})


def test_packaged_stdio_session_large_duplex_exchange_obeys_absolute_deadline(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Broker stdin pumping cannot deadlock output produced before the child reads."""
    executable = _write_fake_executable(
        tmp_path,
        "import sys\nsys.stdout.buffer.write(b'x' * 1048576)\nsys.stdout.buffer.flush()\n"
        "sys.stdin.buffer.read()\n",
    )
    private = cast(Any, mcp_stdio_validation_module)
    started = time.monotonic()
    with pytest.raises(RelayError, match="total wall-clock deadline"):
        private._run_bounded_process(
            (str(executable),),
            session_input=b"y" * 1_048_576,
            timeout_seconds=0.1,
            extra_environment=None,
        )
    assert time.monotonic() - started < 2


def test_packaged_stdio_session_rejects_emitted_environment_secrets(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """An explicitly delivered child credential fails without diagnostic disclosure."""
    environment_name = "CHILD_ONLY_SECRET"
    secret = "neutral-secret-value=with-padding"
    transcript = _fake_transcript()
    program = _staged_fake_program(
        transcript,
        consume_environment=True,
        prefix=(
            "import os\n"
            f"sys.stderr.write('neutral-label:' + os.environ[{environment_name!r}])\n"
            "sys.stderr.flush()\n"
        ),
    )
    executable = _write_fake_executable(tmp_path, program)
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))
    monkeypatch.delenv(environment_name, raising=False)
    with pytest.raises(RelayError, match="emitted a child-only secret") as captured:
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={},
            extra_environment={environment_name: secret},
        )
    assert secret not in str(captured.value)
    assert "neutral-secret-value" not in str(captured.value)


def test_packaged_stdio_child_does_not_inherit_ambient_credentials(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A packaged child cannot read an unrelated credential from its parent's environment."""
    secret = "ambient-token-that-must-not-reach-the-child"
    transcript = _fake_transcript()
    executable = _write_fake_executable(
        tmp_path,
        _staged_fake_program(
            transcript,
            prefix=(
                "import os\nif 'INHERITED_API_TOKEN' in os.environ:\n    raise SystemExit(83)\n"
            ),
        ),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))
    monkeypatch.setenv("INHERITED_API_TOKEN", secret)

    session = run_packaged_mcp_stdio_session(
        profile="user",
        tool="jarvis_run",
        arguments={},
    )

    assert session.returncode == 0
    assert secret not in json.dumps(session.evidence())


def test_packaged_user_profile_rejects_static_admin_tool_for_agent_alias(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    tools = virtual_jarvis_tool_definitions(clusters=["alpha"])
    tools.append(
        {
            "name": "relay_submit_mcp_call",
            "description": "Administrative submission primitive.",
            "inputSchema": {"type": "object"},
        }
    )
    transcript = _fake_transcript(tools=tools)
    executable = _write_fake_executable(
        tmp_path,
        _staged_fake_program(transcript),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))
    with pytest.raises(RelayError, match="static administrative tools"):
        run_packaged_mcp_stdio_session(profile="agent", tool="jarvis_run", arguments={})


def test_packaged_stdio_rejects_nonfinite_outbound_and_inbound_json(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    marker = tmp_path / "started.txt"
    executable = _write_fake_executable(
        tmp_path,
        f"import pathlib\npathlib.Path({str(marker)!r}).write_text('started')\n",
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))
    with pytest.raises(RelayError, match="request was not finite JSON"):
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={"value": float("nan")},
        )
    assert not marker.exists()

    transcript = _fake_transcript().replace(
        b'"structuredContent":{"ok":true}',
        b'"structuredContent":{"value":1e9999}',
    )
    executable = _write_fake_executable(
        tmp_path,
        _staged_fake_program(transcript),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))
    with pytest.raises(RelayError, match="non-finite JSON number"):
        run_packaged_mcp_stdio_session(profile="user", tool="jarvis_run", arguments={})


def test_packaged_stdio_rejects_error_result_even_when_it_contains_job_id(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """An error-shaped tools/call result can never contribute a durable job handle."""
    transcript = (
        _fake_transcript()
        .replace(b'"isError":false', b'"isError":true')
        .replace(b'{\\"ok\\":true}', b'{\\"job_id\\":\\"job-danger\\"}')
        .replace(b'"structuredContent":{"ok":true}', b'"structuredContent":{"job_id":"job-danger"}')
    )
    executable = _write_fake_executable(tmp_path, _staged_fake_program(transcript))
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))

    with pytest.raises(RelayError, match="tools/call reported an error"):
        run_packaged_mcp_stdio_session(profile="user", tool="jarvis_run", arguments={})


def test_packaged_stdio_byte_framing_preserves_unicode_and_rejects_ambiguity() -> None:
    private = cast(Any, mcp_stdio_validation_module)
    unicode_frame = (
        b'{"jsonrpc":"2.0","id":"clio-relay-validation-initialize",'
        b'"result":{"note":"before\xe2\x80\xa8after"}}\n'
    )
    parsed = private._responses_by_id(unicode_frame)
    assert parsed["clio-relay-validation-initialize"]["result"]["note"] == "before\u2028after"
    notification = b'{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info"}}\n'
    assert private._responses_by_id(notification + unicode_frame) == parsed
    with pytest.raises(RelayError, match="final LF"):
        private._responses_by_id(unicode_frame[:-1])
    with pytest.raises(RelayError, match="blank frame"):
        private._responses_by_id(unicode_frame + b"\n")
    unknown = b'{"jsonrpc":"2.0","id":"unknown","result":{}}\n'
    with pytest.raises(RelayError, match="unknown response id"):
        private._responses_by_id(unknown)
    invalid_notification = b'{"jsonrpc":"2.0","method":"notifications/message","result":{}}\n'
    with pytest.raises(RelayError, match="uncorrelated message"):
        private._responses_by_id(invalid_notification)
    invalid_responses = (
        b'{"jsonrpc":"2.0","id":"clio-relay-validation-initialize"}\n',
        b'{"jsonrpc":"2.0","id":"clio-relay-validation-initialize","result":{},"error":{}}\n',
        b'{"jsonrpc":"2.0","id":"clio-relay-validation-initialize",'
        b'"method":"server/request","result":{}}\n',
    )
    for invalid_response in invalid_responses:
        with pytest.raises(RelayError, match="invalid response envelope"):
            private._responses_by_id(invalid_response)
    secret = "untrusted-frame-secret-value"
    malformed = f'{{"secret":"{secret}"'.encode() + b"\n"
    with pytest.raises(RelayError, match="invalid JSON") as malformed_error:
        private._responses_by_id(malformed)
    assert malformed_error.value.__cause__ is None
    assert malformed_error.value.__context__ is None
    assert secret not in "".join(traceback.format_exception(malformed_error.value))
    deeply_nested = (b"[" * 1_000) + b"0" + (b"]" * 1_000)
    with pytest.raises(RelayError) as depth_error:
        private.decode_strict_json(deeply_nested, label="deep child frame")
    assert depth_error.value.__cause__ is None
    assert not isinstance(depth_error.value, RecursionError)


def test_packaged_stdio_rejects_and_kills_orphan_after_valid_root_exit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A valid transcript cannot hide a detached descendant in the owned scope."""
    marker = tmp_path / "orphan-survived.txt"
    child = (
        "import pathlib,time;time.sleep(0.5);"
        f"pathlib.Path({str(marker)!r}).write_text('survived',encoding='utf-8')"
    )
    transcript = _fake_transcript()
    executable = _write_fake_executable(
        tmp_path,
        _staged_fake_program(
            transcript,
            prefix=(
                "import subprocess\n"
                "subprocess.Popen(\n"
                f"    [sys.executable, '-c', {child!r}],\n"
                "    stdin=subprocess.DEVNULL,\n"
                "    stdout=subprocess.DEVNULL,\n"
                "    stderr=subprocess.DEVNULL,\n"
                ")\n"
            ),
        ),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))
    with pytest.raises(RelayError, match="containment could not be verified"):
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={},
            timeout_seconds=2,
        )
    time.sleep(0.7)
    assert not marker.exists()


def test_packaged_stdio_kills_orphan_holding_pipe_after_root_exit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A descendant-held pipe is bounded and terminated after its root exits."""
    marker = tmp_path / "pipe-orphan-survived.txt"
    child = (
        "import pathlib,time;time.sleep(0.5);"
        f"pathlib.Path({str(marker)!r}).write_text('survived',encoding='utf-8')"
    )
    executable = _write_fake_executable(
        tmp_path,
        f"""
import subprocess
import sys

sys.stdin.buffer.read()
subprocess.Popen([sys.executable, "-c", {child!r}])
""".lstrip(),
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))
    with pytest.raises(RelayError, match="total wall-clock deadline"):
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={},
            timeout_seconds=0.1,
        )
    time.sleep(0.7)
    assert not marker.exists()


def test_secret_bearing_session_requires_enforceable_containment_before_spawn(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Unsupported containment fails before a secret-bearing executable can start."""
    marker = tmp_path / "must-not-start.txt"
    executable = _write_fake_executable(
        tmp_path,
        f"import pathlib\npathlib.Path({str(marker)!r}).write_text('started')\n",
    )
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))

    def unsupported_containment(**_kwargs: object) -> dict[str, object]:
        return {
            "mode": "cooperative_process_group",
            "enforceable": False,
            "reason": "test provider unavailable",
        }

    monkeypatch.setattr(
        "clio_relay.process_containment.containment_capability",
        unsupported_containment,
    )
    with pytest.raises(RelayError, match="could not start"):
        run_packaged_mcp_stdio_session(
            profile="user",
            tool="jarvis_run",
            arguments={},
            extra_environment={"CLIO_RELAY_FRP_TOKEN": "secret-value"},
            require_enforceable_containment=True,
        )
    assert not marker.exists()


def test_packaged_stdio_surfaces_safe_failed_startup_cleanup_evidence(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A failed owned spawn exposes bounded cleanup state without exception chaining."""
    executable = _write_fake_executable(tmp_path, "raise SystemExit(0)\n")
    monkeypatch.setenv("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE", str(executable))

    def fail_spawn(*_args: object, **_kwargs: object) -> object:
        raise OwnedProcessSpawnError(
            process_id=8123,
            mode="windows_job_object",
            cleanup_errors=["owned spawn termination failed: RuntimeError"],
            cause=RuntimeError("private startup detail"),
        )

    monkeypatch.setattr(mcp_stdio_validation_module, "spawn_owned_process", fail_spawn)
    with pytest.raises(RelayError, match="cleanup_verified=False") as failure:
        run_packaged_mcp_stdio_session(profile="user", tool="jarvis_run", arguments={})
    assert "pid=8123" in str(failure.value)
    assert "private startup detail" not in str(failure.value)
    assert failure.value.__cause__ is None
