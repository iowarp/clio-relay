from __future__ import annotations

from typing import Protocol

import pytest

from clio_relay.adapters.worker.deployment import render_endpoint_user_service
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError
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
