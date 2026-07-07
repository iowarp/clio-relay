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
    assert "progress" not in package


def test_remote_agent_task_yaml_generation(tmp_path: Path) -> None:
    provider = JarvisCdProvider(
        agent_bin="/opt/agent/bin/current-agent",
        agent_adapter="exec",
        agent_args=["--prompt", "{prompt_path}"],
    )
    rendered = provider.render_remote_agent_task_yaml(
        RemoteAgentTaskSpec(
            prompt_path=str(tmp_path / "prompt.md"),
            mcp_config_path=str(tmp_path / "mcp.json"),
            context={"source_event_seq": 7, "match_groups": {"step": "50"}},
        )
    )
    document = yaml.safe_load(rendered)

    package = document["pkgs"][0]
    assert package["pkg_type"] == "clio_relay.remote_agent"
    assert package["agent_bin"] == "/opt/agent/bin/current-agent"
    assert package["agent_adapter"] == "exec"
    assert package["agent_args"] == ["--prompt", "{prompt_path}"]
    assert package["prompt_path"].endswith("prompt.md")
    assert package["context"] == {"source_event_seq": 7, "match_groups": {"step": "50"}}


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


def test_unscheduled_pipeline_uses_direct_run_command(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        yaml.safe_dump({"name": "direct", "pkgs": []}),
        encoding="utf-8",
    )
    provider = JarvisCdProvider(jarvis_bin="/opt/jarvis/bin/jarvis")

    assert provider.pipeline_command(pipeline) == [
        "/opt/jarvis/bin/jarvis",
        "ppl",
        "run",
        "yaml",
        str(pipeline),
    ]


def test_scheduled_pipeline_uses_waiting_scheduler_runner(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        yaml.safe_dump(
            {
                "name": "scheduled",
                "scheduler": {"name": "slurm", "exclusive": True},
                "pkgs": [],
            }
        ),
        encoding="utf-8",
    )
    bin_dir = tmp_path / "opt" / "jarvis" / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "python").write_text("", encoding="utf-8")
    provider = JarvisCdProvider(jarvis_bin=str(bin_dir / "jarvis"))

    command = provider.pipeline_command(pipeline)

    assert command[0] == str(bin_dir / "python")
    assert command[1] == "-c"
    assert "sbatch" in command[2]
    assert "--parsable" in command[2]
    assert "scheduler_job_id=" in command[2]
    assert command[3] == str(pipeline)


def test_scheduled_pipeline_test_config_uses_scheduler_runner(tmp_path: Path) -> None:
    pipeline = tmp_path / "pipeline-test.yaml"
    pipeline.write_text(
        yaml.safe_dump(
            {
                "config": {
                    "name": "scheduled-test",
                    "scheduler": {"name": "slurm", "nodes": 2},
                    "pkgs": [],
                },
                "vars": {"case": [1]},
            }
        ),
        encoding="utf-8",
    )
    provider = JarvisCdProvider(jarvis_bin="jarvis")

    command = provider.pipeline_command(pipeline)

    assert command[1] == "-c"
    assert "load_yaml_auto" in command[2]
    assert command[3] == str(pipeline)


def test_scheduled_pipeline_uses_wrapper_shebang_when_sibling_python_is_missing(
    tmp_path: Path,
) -> None:
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        yaml.safe_dump(
            {
                "name": "scheduled",
                "scheduler": {"name": "slurm"},
                "pkgs": [],
            }
        ),
        encoding="utf-8",
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    jarvis = bin_dir / "jarvis"
    jarvis.write_text(
        "#!/opt/clio-relay/jarvis-venv/bin/python\nprint('jarvis')\n",
        encoding="utf-8",
    )
    provider = JarvisCdProvider(jarvis_bin=str(jarvis))

    command = provider.pipeline_command(pipeline)

    assert command[0] == "/opt/clio-relay/jarvis-venv/bin/python"
