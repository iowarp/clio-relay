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

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["command"] = json.dumps(command)
        captured["input"] = str(kwargs["input"])
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

    monkeypatch.setattr(cast(Any, runner).subprocess, "run", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {"server": "science-mcp", "tool": "inspect", "arguments": {"path": "x"}}
    )
    messages = [json.loads(line) for line in captured["input"].splitlines()]
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 0
    assert json.loads(captured["command"]) == ["science-mcp"]
    assert [message.get("method") for message in messages] == [
        "initialize",
        "notifications/initialized",
        "tools/call",
    ]
    assert messages[2]["params"]["arguments"] == {"path": "x"}
    assert result["returncode"] == 0
    assert result["timed_out"] is False
    assert result["protocol_error"] is None


def test_mcp_call_runner_records_protocol_errors(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)

    def fake_run(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        stdout = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "clio-relay-mcp-call",
                "error": {"code": -32603, "message": "tool failed"},
            }
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(cast(Any, runner).subprocess, "run", fake_run)

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

    def fake_run(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(command, timeout=1, output="partial", stderr="late")

    monkeypatch.setattr(cast(Any, runner).subprocess, "run", fake_run)

    return_code = cast(McpCallRunnerModule, runner).run_mcp_call_from_params(
        {"server": "science-mcp", "tool": "inspect", "timeout_seconds": 1}
    )
    result = json.loads((tmp_path / "mcp-result.json").read_text(encoding="utf-8"))

    assert return_code == 124
    assert result["returncode"] == 124
    assert result["timed_out"] is True
    assert result["stdout"] == "partial"
    assert result["stderr"] == "late"


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
