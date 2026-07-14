from __future__ import annotations

import subprocess
from collections.abc import Callable
from typing import Protocol, cast

import pytest

import clio_relay.live_acceptance as live_acceptance
import clio_relay.service_runtime as service_runtime
import clio_relay.transport_probe as transport_probe
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.deployment import render_endpoint_user_service
from clio_relay.doctor import run_cluster_doctor
from clio_relay.errors import ConfigurationError
from clio_relay.remote_cli import remote_env
from clio_relay.remote_values import (
    render_remote_shell_path,
    render_remote_shell_value,
    render_systemd_remote_path,
    render_systemd_remote_value,
)


class _RemoteRenderer(Protocol):
    def __call__(self, value: str, *, field: str) -> str:
        """Render one configured remote value."""
        ...


def _configured_definition() -> ClusterDefinition:
    return ClusterDefinition(
        name="custom",
        ssh_host="cluster.example.test",
        core_dir="$HOME/core dir/o'hare$(touch /tmp/core)`id`$CORE_SUFFIX",
        spool_dir="/srv/$HOME/spool dir/$(touch /tmp/spool)`id`$SPOOL_SUFFIX",
        jarvis_bin="$HOME/bin/jarvis $(touch /tmp/jarvis)`id`$JARVIS_SUFFIX",
        frpc_bin="/opt/$HOME/frpc $(touch /tmp/frpc)`id`$FRPC_SUFFIX",
        agent_bin="$HOME/bin/agent $(touch /tmp/agent)`id`$AGENT_SUFFIX",
    )


def test_remote_value_contract_expands_only_exact_leading_home() -> None:
    """Shell and systemd renderers preserve every non-leading expansion token literally."""
    leading = "$HOME/state dir/o'hare$(touch /tmp/x)`id`$VALUE_SUFFIX"
    absolute = "/srv/$HOME/state dir/o'hare$(touch /tmp/y)`id`$VALUE_SUFFIX"

    assert render_remote_shell_value(leading, field="value") == (
        '"$HOME/state dir/o\'hare\\$(touch /tmp/x)\\`id\\`\\$VALUE_SUFFIX"'
    )
    assert render_remote_shell_value(absolute, field="value") == (
        '"/srv/\\$HOME/state dir/o\'hare\\$(touch /tmp/y)\\`id\\`\\$VALUE_SUFFIX"'
    )
    assert render_systemd_remote_value(leading, field="value") == (
        "%h/state dir/o'hare$(touch /tmp/x)`id`$VALUE_SUFFIX"
    )
    assert render_systemd_remote_value(absolute, field="value") == absolute


@pytest.mark.parametrize("value", ["relative/path", "$USER/path", "$HOME-other/path", ""])
def test_remote_path_contract_requires_an_unambiguous_absolute_path(value: str) -> None:
    """State paths cannot depend on an unspecified remote working directory or variable."""
    with pytest.raises(ConfigurationError, match="absolute POSIX path|nonempty path"):
        render_remote_shell_path(value, field="core_dir")


@pytest.mark.parametrize(
    "renderer",
    [
        render_remote_shell_value,
        render_remote_shell_path,
        render_systemd_remote_value,
        render_systemd_remote_path,
    ],
)
def test_remote_value_contract_rejects_controls(renderer: _RemoteRenderer) -> None:
    """No remote rendering surface accepts control characters."""
    with pytest.raises(ConfigurationError, match="control characters"):
        renderer("$HOME/path\nforged", field="path")


def test_remote_environment_surfaces_share_the_value_contract() -> None:
    """CLI, acceptance, and transport probes render identical configured meanings."""
    definition = _configured_definition()
    rendered_core_dir = render_remote_shell_path(definition.core_dir, field="core_dir")
    rendered_spool_dir = render_remote_shell_path(definition.spool_dir, field="spool_dir")
    rendered_jarvis_bin = render_remote_shell_value(
        definition.jarvis_bin or "",
        field="jarvis_bin",
    )
    rendered_frpc_bin = render_remote_shell_value(
        definition.frpc_bin or "",
        field="frpc_bin",
    )
    rendered_agent_bin = render_remote_shell_value(
        definition.agent_bin or "",
        field="agent_bin",
    )
    expected = [
        f"CLIO_RELAY_CORE_DIR={rendered_core_dir}",
        f"CLIO_RELAY_SPOOL_DIR={rendered_spool_dir}",
        f"CLIO_RELAY_JARVIS_BIN={rendered_jarvis_bin}",
        f"CLIO_RELAY_FRPC_BIN={rendered_frpc_bin}",
        f"CLIO_RELAY_AGENT_BIN={rendered_agent_bin}",
    ]

    cli_environment = remote_env(definition)
    acceptance_env_renderer = cast(
        Callable[..., str],
        vars(live_acceptance)["_remote_env"],
    )
    probe_script_renderer = cast(
        Callable[..., str],
        vars(transport_probe)["_remote_probe_script"],
    )
    acceptance_environment = acceptance_env_renderer(definition)
    probe_script = probe_script_renderer(
        cluster=definition.name,
        definition=definition,
        probe_id="probe-contract",
        api_token=None,
        api_port=8765,
        frpc_config="",
    )

    for assignment in expected:
        assert assignment in cli_environment
        assert assignment in acceptance_environment
        assert assignment in probe_script


def test_doctor_and_connector_runtime_share_safe_bin_rendering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Diagnostic and connector scripts cannot reevaluate configured executable values."""
    definition = _configured_definition()
    doctor_scripts: list[str] = []

    def fake_run(
        command: list[str],
        *,
        input: bytes,
        capture_output: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        assert capture_output is True
        assert check is False
        doctor_scripts.append(input.decode("utf-8"))
        return subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr("clio_relay.doctor.subprocess.run", fake_run)
    run_cluster_doctor(definition)

    expected_frpc = render_remote_shell_value(definition.frpc_bin or "", field="frpc_bin")
    expected_jarvis = render_remote_shell_value(definition.jarvis_bin or "", field="jarvis_bin")
    expected_agent = render_remote_shell_value(definition.agent_bin or "", field="agent_bin")
    assert f"FRPC_BIN={expected_frpc}" in doctor_scripts[0]
    assert f"JARVIS_BIN={expected_jarvis}" in doctor_scripts[0]
    assert f"AGENT_BIN={expected_agent}" in doctor_scripts[0]

    connector_script_renderer = cast(
        Callable[..., str],
        vars(service_runtime)["_remote_frpc_start_script"],
    )
    connector_script = connector_script_renderer(
        definition=definition,
        session_id="session-contract",
        config_text="serverAddr = 'relay.example.test'\n",
        owner_token="owner-token",
        connector_generation_id="generation-id",
    )
    assert f"frpc_bin={expected_frpc}" in connector_script


def test_endpoint_service_expands_only_leading_home_specifiers() -> None:
    """Systemd receives one leading home specifier while literal HOME text stays literal."""
    definition = ClusterDefinition(
        name="custom",
        ssh_host="cluster.example.test",
        core_dir="$HOME/core $CORE_SUFFIX%h",
        spool_dir="/srv/$HOME/spool $SPOOL_SUFFIX%h",
        jarvis_bin="$HOME/bin/jarvis$(literal)%h",
        frpc_bin="/opt/$HOME/frpc$(literal)%h",
        agent_bin="$HOME/bin/agent`literal`$AGENT_SUFFIX%h",
        spack_executable="$HOME/bin/spack $SPACK_SUFFIX%h",
    )

    service = render_endpoint_user_service(cluster="custom", definition=definition)

    assert 'Environment="CLIO_RELAY_CORE_DIR=%h/core $CORE_SUFFIX%%h"' in service
    assert 'Environment="CLIO_RELAY_SPOOL_DIR=/srv/$HOME/spool $SPOOL_SUFFIX%%h"' in service
    assert 'Environment="CLIO_RELAY_JARVIS_BIN=%h/bin/jarvis$(literal)%%h"' in service
    assert 'Environment="CLIO_RELAY_FRPC_BIN=/opt/$HOME/frpc$(literal)%%h"' in service
    assert 'Environment="CLIO_RELAY_AGENT_BIN=%h/bin/agent`literal`$AGENT_SUFFIX%%h"' in service
    assert 'Environment="JARVIS_MCP_SPACK_COMMAND=%h/bin/spack $SPACK_SUFFIX%%h"' in service
    assert "/srv/%h/spool" not in service


def test_endpoint_service_preserves_literal_leading_systemd_specifier() -> None:
    """Only configured HOME syntax can authorize a systemd home specifier."""
    definition = ClusterDefinition(
        name="custom",
        ssh_host="cluster.example.test",
        jarvis_bin="%h/bin/jarvis",
        frpc_bin="%h/bin/frpc",
        agent_bin="%h/bin/agent",
        spack_executable="/%h/bin/spack",
    )

    service = render_endpoint_user_service(cluster="custom", definition=definition)

    assert 'Environment="CLIO_RELAY_JARVIS_BIN=%%h/bin/jarvis"' in service
    assert 'Environment="CLIO_RELAY_FRPC_BIN=%%h/bin/frpc"' in service
    assert 'Environment="CLIO_RELAY_AGENT_BIN=%%h/bin/agent"' in service
    assert 'Environment="JARVIS_MCP_SPACK_COMMAND=/%%h/bin/spack"' in service


def test_endpoint_service_rejects_control_characters_in_remote_values() -> None:
    """Systemd rendering fails instead of encoding a control-bearing executable value."""
    definition = ClusterDefinition(
        name="custom",
        ssh_host="cluster.example.test",
        agent_bin="/opt/agent\nEnvironment=FORGED=1",
    )

    with pytest.raises(ConfigurationError, match="agent_bin.*control characters"):
        render_endpoint_user_service(cluster="custom", definition=definition)
