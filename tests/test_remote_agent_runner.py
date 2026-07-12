from __future__ import annotations

import importlib.util
import json
import os
import stat
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
            "import json, os, sys\n"
            "from pathlib import Path\n"
            f"Path({str(output_path)!r}).write_text(json.dumps("
            "{'args': sys.argv[1:], "
            "'progress_file': os.environ.get('CLIO_RELAY_PROGRESS_FILE'), "
            "'progress_token': os.environ.get('CLIO_RELAY_PROGRESS_TOKEN'), "
            "'runtime_file': os.environ.get('CLIO_RELAY_RUNTIME_METADATA_FILE'), "
            "'runtime_token': os.environ.get('CLIO_RELAY_RUNTIME_METADATA_TOKEN'), "
            "'api_token': os.environ.get('CLIO_RELAY_API_TOKEN'), "
            "'frp_token': os.environ.get('CLIO_RELAY_FRP_TOKEN'), "
            "'stcp_secret': os.environ.get('CLIO_RELAY_STCP_SECRET'), "
            "'owner_token': os.environ.get('CLIO_RELAY_SESSION_OWNER_TOKEN')}))\n"
        ),
        encoding="utf-8",
    )
    prompt_path.write_text("do the work", encoding="utf-8")
    mcp_path.write_text("[mcp_servers.local]\ncommand = 'python'\n", encoding="utf-8")

    monkeypatch.setenv("CLIO_RELAY_PROGRESS_FILE", "forbidden")
    monkeypatch.setenv("CLIO_RELAY_PROGRESS_TOKEN", "forbidden-token")
    monkeypatch.setenv("CLIO_RELAY_RUNTIME_METADATA_FILE", "forbidden-runtime")
    monkeypatch.setenv("CLIO_RELAY_RUNTIME_METADATA_TOKEN", "forbidden-runtime-token")
    monkeypatch.setenv("CLIO_RELAY_API_TOKEN", "forbidden-api-token")
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "forbidden-frp-token")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "forbidden-stcp-secret")
    monkeypatch.setenv("CLIO_RELAY_SESSION_OWNER_TOKEN", "forbidden-owner-token")

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
            "context": {"source_event_seq": 9, "match_groups": {"step": "50"}},
        }
    )

    assert return_code == 0
    result = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))
    assert result["adapter"] == "exec"
    assert result["agent_bin"] == "python"
    assert result["returncode"] == 0
    assert result["prompt_path"] == str(prompt_path)
    assert result["mcp_config_path"] == str(mcp_path)
    expected_prompt = (
        "do the work\n\n"
        "Relay monitor context:\n"
        '{\n  "match_groups": {\n    "step": "50"\n  },\n  "source_event_seq": 9\n}\n'
    )
    captured_agent = json.loads(output_path.read_text(encoding="utf-8"))
    assert captured_agent["args"] == [
        "--prompt",
        expected_prompt,
        "--mcp",
        str(mcp_path),
        "--model",
        "configured-model",
    ]
    assert captured_agent["progress_file"] is None
    assert captured_agent["progress_token"] is None
    assert captured_agent["runtime_file"] is None
    assert captured_agent["runtime_token"] is None
    assert captured_agent["api_token"] is None
    assert captured_agent["frp_token"] is None
    assert captured_agent["stcp_secret"] is None
    assert captured_agent["owner_token"] is None


def test_codex_adapter_disables_interactive_approvals(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("use the tool", encoding="utf-8")
    captured: dict[str, list[str]] = {}

    def fake_run(
        command: list[str],
        *,
        cwd: Path | None,
        timeout: int | None,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, timeout
        captured["command"] = command
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cast(Any, runner), "_run_process", fake_run)

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


def test_codex_adapter_uses_private_ephemeral_mcp_profile(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    codex_home = tmp_path / "codex-home"
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("use the private tool", encoding="utf-8")
    mcp_path = tmp_path / "mcp.toml"
    mcp_document = "[mcp_servers.private]\ncommand = 'private-server'\n"
    mcp_path.write_text(mcp_document, encoding="utf-8")
    observed_profile: Path | None = None

    def fake_run(
        command: list[str],
        *,
        cwd: Path | None,
        timeout: int | None,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, timeout
        nonlocal observed_profile
        profile_name = command[command.index("--profile") + 1]
        observed_profile = codex_home / f"{profile_name}.config.toml"
        assert observed_profile.read_text(encoding="utf-8") == mcp_document
        if os.name != "nt":
            assert stat.S_IMODE(observed_profile.stat().st_mode) == 0o600
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(cast(Any, runner), "_run_process", fake_run)

    return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
        {
            "agent_bin": "codex",
            "agent_adapter": "codex",
            "prompt_path": str(prompt_path),
            "mcp_config_path": str(mcp_path),
        }
    )

    assert return_code == 0
    assert observed_profile is not None
    assert not observed_profile.exists()


def test_agent_timeout_writes_structured_result(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("run too long", encoding="utf-8")

    def fake_run(
        command: list[str],
        *,
        cwd: Path | None,
        timeout: int | None,
    ) -> subprocess.CompletedProcess[str]:
        del cwd
        raise subprocess.TimeoutExpired(command, timeout=1)

    monkeypatch.setattr(cast(Any, runner), "_run_process", fake_run)

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
    assert result["error_type"] == "TimeoutExpired"


def test_missing_agent_binary_writes_structured_result(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("run", encoding="utf-8")

    def fake_run(
        command: list[str],
        *,
        cwd: Path | None,
        timeout: int | None,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, timeout
        raise FileNotFoundError(command[0])

    monkeypatch.setattr(cast(Any, runner), "_run_process", fake_run)

    return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
        {
            "agent_bin": "missing-agent",
            "agent_adapter": "exec",
            "prompt_path": str(prompt_path),
        }
    )

    result = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))

    assert return_code == 127
    assert result["returncode"] == 127
    assert result["error_type"] == "FileNotFoundError"
    assert result["agent_bin"] == "missing-agent"


def test_invalid_agent_setup_writes_structured_result(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)

    return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
        {
            "agent_bin": "agent",
            "agent_adapter": "missing-adapter",
            "prompt_path": str(tmp_path / "missing-prompt.md"),
        }
    )

    result = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))

    assert return_code == 2
    assert result["returncode"] == 2
    assert result["error_type"] == "FileNotFoundError"
    assert result["prompt_path"].endswith("missing-prompt.md")


def test_agent_rejects_oversized_prompt_without_launching(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    prompt_path = tmp_path / "prompt.md"
    with prompt_path.open("wb") as stream:
        stream.truncate(4 * 1_048_576 + 1)

    def forbidden_run(
        command: list[str],
        *,
        cwd: Path | None,
        timeout: int | None,
    ) -> subprocess.CompletedProcess[str]:
        del command, cwd, timeout
        raise AssertionError("oversized prompt must fail before agent launch")

    monkeypatch.setattr(cast(Any, runner), "_run_process", forbidden_run)

    return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
        {
            "agent_bin": "agent",
            "agent_adapter": "exec",
            "prompt_path": str(prompt_path),
        }
    )

    result = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))
    assert return_code == 2
    assert result["error_type"] == "ValueError"
    assert "byte limit" in result["error_message"]


def test_codex_adapter_rejects_oversized_profile_without_leaving_secrets(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    codex_home = tmp_path / "codex-home"
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("use the tool", encoding="utf-8")
    mcp_path = tmp_path / "mcp.toml"
    with mcp_path.open("wb") as stream:
        stream.truncate(1_048_576 + 1)

    return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
        {
            "agent_bin": "codex",
            "agent_adapter": "codex",
            "prompt_path": str(prompt_path),
            "mcp_config_path": str(mcp_path),
        }
    )

    result = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))
    assert return_code == 2
    assert result["error_type"] == "ValueError"
    assert "byte limit" in result["error_message"]
    assert not list(codex_home.glob("clio-relay-agent-*.config.toml"))


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
