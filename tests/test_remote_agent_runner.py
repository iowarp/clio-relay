from __future__ import annotations

import importlib.util
import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, cast

from pytest import MonkeyPatch

from clio_relay.models import ArtifactRef


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
            "relay_job_id": "job_00000000000000000000000000000000",
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


def test_exec_adapter_mints_converged_final_answer_projection(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    monkeypatch.chdir(tmp_path)
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("finish the child turn", encoding="utf-8")
    agent_script = tmp_path / "agent.py"
    agent_script.write_text(
        (
            "import sys\n"
            "from pathlib import Path\n"
            "Path(sys.argv[1]).write_text('durable final answer\\n', encoding='utf-8')\n"
        ),
        encoding="utf-8",
    )

    return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
        {
            "relay_job_id": "job_11111111111111111111111111111111",
            "agent_bin": sys.executable,
            "agent_adapter": "exec",
            "agent_args": [str(agent_script), "{output_path}"],
            "prompt_path": str(prompt_path),
        }
    )

    result = json.loads((tmp_path / "agent-result.json").read_text(encoding="utf-8"))
    projection = cast(dict[str, Any], result["final_answer_artifact_ref"])
    artifact = ArtifactRef(**projection)
    answer_bytes = (tmp_path / "agent-last-message.txt").read_bytes()

    assert return_code == 0
    assert artifact.job_id == "job_11111111111111111111111111111111"
    assert artifact.kind == "report"
    assert artifact.uri == (tmp_path / "agent-last-message.txt").as_uri()
    assert artifact.size_bytes == len(answer_bytes)
    assert artifact.sha256 is not None
    assert artifact.metadata["mechanism"] == "model"
    assert artifact.metadata["clio.provenance.v1"] == {
        "job_id": artifact.job_id,
        "uri": artifact.uri,
        "size_bytes": artifact.size_bytes,
        "created_at": artifact.created_at.isoformat().replace("+00:00", "Z"),
    }


def test_gact_adapter_matches_exec_terminal_shape(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    runner = _load_runner()
    assert cast(Any, runner)._gact_command(
        agent_bin="uv",
        agent_args=[],
        prompt_text="answer through clio",
    ) == [
        "uv",
        "run",
        "--no-sync",
        "clio-agent",
        "--query",
        "answer through clio",
        "--json",
    ]
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text("answer through clio", encoding="utf-8")
    mcp_path = tmp_path / "mcp.yaml"
    mcp_document = "mcp_servers:\n  relay:\n    command: clio-relay\n"
    mcp_path.write_text(mcp_document, encoding="utf-8")
    fake_clio = tmp_path / "fake_clio.py"
    fake_clio.write_text(
        (
            "import json, os, sys\n"
            "assert sys.argv[1:3] == ['--query', 'answer through clio']\n"
            "assert sys.argv[3] == '--json'\n"
            "assert os.environ['CLIO_LM_MODEL'] == 'claude-native-model'\n"
            "from pathlib import Path\n"
            "mcp = Path(os.environ['XDG_CONFIG_HOME']) / 'clio-agent' / 'mcp.yaml'\n"
            f"assert mcp.read_text(encoding='utf-8') == {mcp_document!r}\n"
            "print(json.dumps({'answer': 'gact final answer', 'error_info': None}))\n"
        ),
        encoding="utf-8",
    )

    terminal_shapes: dict[str, set[str]] = {}
    for adapter in ("exec", "gact"):
        run_dir = tmp_path / adapter
        run_dir.mkdir()
        monkeypatch.chdir(run_dir)
        if adapter == "exec":
            exec_script = run_dir / "exec_agent.py"
            exec_script.write_text(
                (
                    "import sys\n"
                    "from pathlib import Path\n"
                    "Path(sys.argv[1]).write_text('exec final answer', encoding='utf-8')\n"
                ),
                encoding="utf-8",
            )
            agent_args = [str(exec_script), "{output_path}"]
        else:
            agent_args = [str(fake_clio)]
        return_code = cast(RemoteAgentRunnerModule, runner).run_remote_agent_from_params(
            {
                "relay_job_id": "job_22222222222222222222222222222222",
                "agent_bin": sys.executable,
                "agent_adapter": adapter,
                "agent_args": agent_args,
                "prompt_path": str(prompt_path),
                "model": "claude-native-model",
                **({"mcp_config_path": str(mcp_path)} if adapter == "gact" else {}),
            }
        )
        result = json.loads((run_dir / "agent-result.json").read_text(encoding="utf-8"))
        assert return_code == 0
        assert result["returncode"] == 0
        assert result["last_message_path"] == str(run_dir / "agent-last-message.txt")
        assert "final_answer_artifact_ref" in result
        terminal_shapes[adapter] = set(result)

    assert (tmp_path / "gact" / "agent-last-message.txt").read_text(encoding="utf-8") == (
        "gact final answer"
    )
    assert terminal_shapes["gact"] == terminal_shapes["exec"]


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
