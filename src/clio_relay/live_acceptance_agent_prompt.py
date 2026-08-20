"""Generated agent-child prompt staging for live acceptance.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of writing
one deterministic MCP-only prompt for the acceptance run's agent-child job,
staged alongside its own pipeline YAML on the remote host.
"""

from __future__ import annotations

from pathlib import Path

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.live_acceptance_models import CommandRunner
from clio_relay.live_acceptance_remote_io import (
    _remote_shell,
    _remote_write_file,
    _stage_acceptance_files,
)


def _write_generated_agent_prompt(
    definition: ClusterDefinition,
    *,
    cluster: str,
    run_id: str,
    child_yaml: Path,
    runner: CommandRunner,
) -> str:
    remote_home = _remote_home(definition.ssh_host, runner=runner)
    remote_prompt = f"{remote_home}/.local/share/clio-relay/live-tests/{run_id}/agent-prompt.md"
    idempotency_key = f"live-test:{cluster}:{run_id}:agent-child"
    child_pipeline_yaml = _stage_acceptance_files(
        definition,
        jarvis_yaml=child_yaml,
        pipeline_yaml_text=child_yaml.read_text(encoding="utf-8"),
        run_id=f"{run_id}-agent-child",
        runner=runner,
    )
    prompt = _generated_agent_prompt(
        cluster=cluster,
        idempotency_key=idempotency_key,
        pipeline_yaml=child_pipeline_yaml,
    )
    _remote_write_file(
        definition.ssh_host,
        remote_prompt,
        prompt.encode("utf-8"),
        runner=runner,
    )
    return remote_prompt


def _remote_home(ssh_host: str, *, runner: CommandRunner) -> str:
    home = _remote_shell(ssh_host, 'printf "%s" "$HOME"', runner=runner).strip()
    if not home.startswith("/"):
        raise RelayError(f"remote HOME did not resolve to an absolute path: {home}")
    return home


def _generated_agent_prompt(
    *,
    cluster: str,
    idempotency_key: str,
    pipeline_yaml: str,
) -> str:
    return (
        "Use only the MCP tool named relay_submit_jarvis_pipeline. "
        "Do not use shell commands.\n\n"
        "Call relay_submit_jarvis_pipeline with:\n"
        f"- cluster: {cluster}\n"
        f"- idempotency_key: {idempotency_key}\n"
        "- pipeline_yaml: the exact YAML below\n\n"
        "After the tool returns, respond with only the relay job id.\n\n"
        "```yaml\n"
        f"{pipeline_yaml.rstrip()}\n"
        "```\n"
    )
