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

    package = document["packages"][0]
    assert package["name"] == "clio-relay.bounded-command"
    assert package["parameters"]["command"] == ["python", "-V"]
    assert package["parameters"]["env"] == {"A": "B"}
    assert package["parameters"]["timeout_seconds"] == 30


def test_codex_task_yaml_generation(tmp_path: Path) -> None:
    provider = JarvisCdProvider(codex_bin="/opt/codex/bin/codex")
    rendered = provider.render_codex_task_yaml(
        RemoteAgentTaskSpec(
            prompt_path=tmp_path / "prompt.md",
            mcp_config_path=tmp_path / "mcp.json",
        )
    )
    document = yaml.safe_load(rendered)

    package = document["packages"][0]
    assert package["name"] == "clio-relay.codex-agent"
    assert package["parameters"]["codex_bin"] == "/opt/codex/bin/codex"
    assert package["parameters"]["prompt_path"].endswith("prompt.md")


def test_mcp_call_yaml_generation() -> None:
    provider = JarvisCdProvider()
    rendered = provider.render_mcp_call_yaml(
        McpCallSpec(server="science", tool="inspect", arguments={"path": "x"})
    )
    document = yaml.safe_load(rendered)

    package = document["packages"][0]
    assert package["name"] == "clio-relay.mcp-call"
    assert package["parameters"]["server"] == "science"
    assert package["parameters"]["tool"] == "inspect"
    assert package["parameters"]["arguments"] == {"path": "x"}
