from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, cast

from pytest import MonkeyPatch


class RemoteAgentRunnerModule(Protocol):
    def run_remote_agent_from_params(self, params: dict[str, object]) -> int:
        """Run a remote-agent task from serialized parameters."""
        ...


def test_exec_adapter_runs_configured_agent_with_templates(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    agent_script = tmp_path / "agent.py"
    output_path = tmp_path / "args.json"
    prompt_path = tmp_path / "prompt.md"
    mcp_path = tmp_path / "mcp.toml"
    agent_script.write_text(
        (
            "import json, sys\n"
            "from pathlib import Path\n"
            f"Path({str(output_path)!r}).write_text(json.dumps(sys.argv[1:]))\n"
        ),
        encoding="utf-8",
    )
    prompt_path.write_text("do the work", encoding="utf-8")
    mcp_path.write_text("[mcp_servers.local]\ncommand = 'python'\n", encoding="utf-8")

    return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
        {
            "agent_bin": "python",
            "agent_adapter": "exec",
            "agent_args": [
                str(agent_script),
                "--prompt",
                "{prompt}",
                "--mcp",
                "{mcp_config_path}",
                "--model",
                "{model}",
            ],
            "prompt_path": str(prompt_path),
            "mcp_config_path": str(mcp_path),
            "model": "configured-model",
        }
    )

    assert return_code == 0
    result = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))
    assert result["adapter"] == "exec"
    assert result["agent_bin"] == "python"
    assert result["returncode"] == 0
    assert result["prompt_path"] == str(prompt_path)
    assert result["mcp_config_path"] == str(mcp_path)
    assert json.loads(output_path.read_text(encoding="utf-8")) == [
        "--prompt",
        "do the work",
        "--mcp",
        str(mcp_path),
        "--model",
        "configured-model",
    ]


def test_codex_adapter_disables_interactive_approvals(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("use the tool", encoding="utf-8")
    captured: dict[str, list[str]] = {}

    def fake_run(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cast(Any, runner).subprocess, "run", fake_run)

    return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
        {
            "agent_bin": "codex",
            "agent_adapter": "codex",
            "prompt_path": str(prompt_path),
        }
    )

    assert return_code == 0
    result = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))
    assert result["adapter"] == "codex"
    assert result["returncode"] == 0
    assert captured["command"][:4] == [
        "codex",
        "--dangerously-bypass-approvals-and-sandbox",
        "exec",
        "--json",
    ]


def test_agent_timeout_writes_structured_result(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("run too long", encoding="utf-8")

    def fake_run(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(command, timeout=1)

    monkeypatch.setattr(cast(Any, runner).subprocess, "run", fake_run)

    return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
        {
            "agent_bin": "agent",
            "agent_adapter": "exec",
            "prompt_path": str(prompt_path),
            "timeout_seconds": 1,
        }
    )

    result = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))

    assert return_code == 124
    assert result["returncode"] == 124
    assert result["timed_out"] is True


def _load_runner() -> ModuleType:
    path = (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "remote_agent"
        / "runner.py"
    )
    spec = importlib.util.spec_from_file_location("clio_relay_remote_agent_runner", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load remote agent runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
