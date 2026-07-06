from __future__ import annotations

from pathlib import Path

import yaml

from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import JarvisRunSpec, McpCallSpec, RemoteAgentTaskSpec


def test_bounded_command_yaml_generation() -> None:
    provider = JarvisCdProvider()
    rendered = provider.render_bounded_command_yaml(
        JarvisRunSpec(command=["python", "-V"], env={"A": "B"}, timeout_seconds=30)
    )
    document = yaml.safe_load(rendered)

    package = document["pkgs"][0]
    assert package["pkg_type"] == "clio_relay.bounded_command"
    assert package["command"] == ["python", "-V"]
    assert package["env"] == {"A": "B"}
    assert package["timeout_seconds"] == 30


def test_codex_task_yaml_generation(tmp_path: Path) -> None:
    provider = JarvisCdProvider(codex_bin="/opt/codex/bin/codex")
    rendered = provider.render_codex_task_yaml(
        RemoteAgentTaskSpec(
            prompt_path=tmp_path / "prompt.md",
            mcp_config_path=tmp_path / "mcp.json",
        )
    )
    document = yaml.safe_load(rendered)

    package = document["pkgs"][0]
    assert package["pkg_type"] == "clio_relay.codex_agent"
    assert package["codex_bin"] == "/opt/codex/bin/codex"
    assert package["prompt_path"].endswith("prompt.md")


def test_mcp_call_yaml_generation() -> None:
    provider = JarvisCdProvider()
    rendered = provider.render_mcp_call_yaml(
        McpCallSpec(server="science", tool="inspect", arguments={"path": "x"})
    )
    document = yaml.safe_load(rendered)

    package = document["pkgs"][0]
    assert package["pkg_type"] == "clio_relay.mcp_call"
    assert package["server"] == "science"
    assert package["tool"] == "inspect"
    assert package["arguments"] == {"path": "x"}
