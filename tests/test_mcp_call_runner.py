from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, cast

from pytest import MonkeyPatch


class McpCallRunnerModule(Protocol):
    def run_mcp_call_from_params(self, params: dict[str, object]) -> int:
        """Run a remote MCP call from serialized parameters."""
        ...


def test_mcp_call_runner_initializes_before_tool_call(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    captured: dict[str, str] = {}

    def fake_run(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
    ) -> subprocess.CompletedProcess[str]:
        del timeout
        captured["command"] = json.dumps(command)
        captured["tool"] = tool
        captured["arguments"] = json.dumps(arguments)
        stdout = "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-init", "result": {}}),
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": "clio-relay-mcp-call",
                        "result": {"content": [{"type": "text", "text": "ok"}]},
                    }
                ),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {"server": "science-mcp", "tool": "inspect", "arguments": {"path": "x"}}
    )
    messages = [
        json.loads(line)
        for line in cast(Any, runner)
        ._render_session_input(tool="inspect", arguments={"path": "x"})
        .splitlines()
    ]
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert json.loads(captured["command"]) == ["science-mcp"]
    assert captured["tool"] == "inspect"
    assert json.loads(captured["arguments"]) == {"path": "x"}
    assert [message.get("method") for message in messages] == [
        "initialize",
        "notifications/initialized",
        "tools/call",
    ]
    assert messages[2]["params"]["arguments"] == {"path": "x"}
    assert result["returncode"] == 0
    assert result["timed_out"] is False
    assert result["protocol_error"] is None


def test_mcp_call_runner_supports_server_arguments(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    captured: dict[str, list[str]] = {}

    def fake_run(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
    ) -> subprocess.CompletedProcess[str]:
        del tool, arguments, timeout
        captured["command"] = command
        stdout = "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-init", "result": {}}),
                json.dumps({"jsonrpc": "2.0", "id": "clio-relay-mcp-call", "result": {}}),
            ]
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {
            "server": "uvx",
            "server_args": [
                "--from",
                "clio-kit==2.2.6",
                "clio-kit",
                "mcp-server",
                "jarvis",
            ],
            "tool": "jarvis_describe",
        }
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert captured["command"][-5:] == [
        "--from",
        "clio-kit==2.2.6",
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]
    assert result["server"] == "uvx"
    assert result["server_args"] == [
        "--from",
        "clio-kit==2.2.6",
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]


def test_mcp_call_runner_records_protocol_errors(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)

    def fake_run(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
    ) -> subprocess.CompletedProcess[str]:
        del tool, arguments, timeout
        stdout = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "clio-relay-mcp-call",
                "error": {"code": -32603, "message": "tool failed"},
            }
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {"server": "science-mcp", "tool": "inspect"}
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 1
    assert result["returncode"] == 1
    assert "tool failed" in result["protocol_error"]


def test_mcp_call_runner_writes_result_on_timeout(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)

    def fake_run(
        command: list[str],
        *,
        tool: str,
        arguments: dict[str, object],
        timeout: int | None,
    ) -> subprocess.CompletedProcess[str]:
        del tool, arguments
        raise subprocess.TimeoutExpired(command, timeout=1, output="partial", stderr="late")

    monkeypatch.setattr(cast(Any, runner), "_run_mcp_session", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {"server": "science-mcp", "tool": "inspect", "timeout_seconds": 1}
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 124
    assert result["returncode"] == 124
    assert result["timed_out"] is True
    assert result["stdout"] == "partial"
    assert result["stderr"] == "late"


def test_mcp_call_runner_scrubs_progress_env_from_server(
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.setenv("CLIO_RELAY_PROGRESS_FILE", "forbidden")
    monkeypatch.setenv("CLIO_RELAY_PROGRESS_TOKEN", "forbidden-token")

    scrubbed = cast(Any, runner)._scrubbed_env()

    assert "CLIO_RELAY_PROGRESS_FILE" not in scrubbed
    assert "CLIO_RELAY_PROGRESS_TOKEN" not in scrubbed


def _load_runner() -> ModuleType:
    path = (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "mcp_call"
        / "runner.py"
    )
    spec = importlib.util.spec_from_file_location("clio_relay_mcp_call_runner", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load MCP call runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
