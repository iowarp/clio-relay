from __future__ import annotations

from pathlib import Path

import pytest

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.deployment import render_endpoint_user_service
from clio_relay.doctor import run_doctor
from clio_relay.errors import ConfigurationError
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpsConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
    render_frps_config,
)


def test_render_frps_config_has_no_application_state() -> None:
    rendered = render_frps_config(
        FrpsConfig(
            bind_port=7001,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
        )
    )

    assert "bindPort = 7001" in rendered
    assert 'auth.token = "secret"' in rendered
    assert "job" not in rendered.lower()
    assert "queue" not in rendered.lower()


def test_render_frpc_config_uses_configured_websocket_transport() -> None:
    rendered = render_frpc_config(
        FrpcConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            local_port=8848,
            secret_key="stcp-secret",
        )
    )

    assert 'serverAddr = "relay.example.test"' in rendered
    assert "serverPort = 443" in rendered
    assert 'transport.protocol = "wss"' in rendered
    assert 'type = "stcp"' in rendered


def test_render_frpc_visitor_config_uses_stcp_visitor() -> None:
    rendered = render_frpc_visitor_config(
        FrpcVisitorConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            server_name="cluster-relay",
            visitor_name="desktop-relay",
            bind_port=8765,
            secret_key="stcp-secret",
        )
    )

    assert 'serverAddr = "relay.example.test"' in rendered
    assert "serverPort = 443" in rendered
    assert 'transport.protocol = "wss"' in rendered
    assert "[[visitors]]" in rendered
    assert 'type = "stcp"' in rendered
    assert 'serverName = "cluster-relay"' in rendered
    assert 'bindAddr = "127.0.0.1"' in rendered
    assert "bindPort = 8765" in rendered


def test_render_frpc_config_supports_xtcp_proxy_and_visitor() -> None:
    proxy = render_frpc_config(
        FrpcConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            proxy_name="cluster-direct",
            proxy_type="xtcp",
            local_port=8848,
            secret_key="xtcp-secret",
        )
    )
    visitor = render_frpc_visitor_config(
        FrpcVisitorConfig(
            server_addr="relay.example.test",
            server_port=443,
            token="secret",
            transport_protocol=FrpTransportProtocol.WSS,
            visitor_name="desktop-direct",
            visitor_type="xtcp",
            server_name="cluster-direct",
            bind_port=8765,
            secret_key="xtcp-secret",
            keep_tunnel_open=True,
        )
    )

    assert 'type = "xtcp"' in proxy
    assert 'name = "cluster-direct"' in proxy
    assert 'type = "xtcp"' in visitor
    assert 'serverName = "cluster-direct"' in visitor
    assert "keepTunnelOpen = true" in visitor


def test_live_doctor_requires_frps_address(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")

    with pytest.raises(ConfigurationError, match="CLIO_RELAY_FRPS_ADDR"):
        run_doctor(settings, live=True)


def test_live_doctor_accepts_cluster_frps_address(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frp_token="secret",
        frpc_bin="python",
    )

    lines = run_doctor(settings, live=True, frps_addr="frps.example.test")

    assert "frps_addr: frps.example.test" in lines
    assert "frp_token: configured" in lines
    assert any(line.startswith("frpc:") for line in lines)


def test_live_doctor_reports_missing_frp_token(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frpc_bin="python",
    )

    lines = run_doctor(settings, live=True, frps_addr="frps.example.test")

    assert "frp_token: missing" in lines


def test_live_doctor_does_not_require_cluster_tools_locally(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        frps_addr="frps.example.test",
        frp_token="secret",
        frpc_bin="python",
        jarvis_bin="definitely-not-local-jarvis",
        agent_bin="definitely-not-local-agent",
    )

    lines = run_doctor(settings, live=True)

    assert "frps_addr: frps.example.test" in lines
    assert "frp_token: configured" in lines
    assert any(line.startswith("frpc:") for line in lines)


def test_endpoint_user_service_is_sudo_less_and_configured() -> None:
    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(
            name="test-cluster",
            ssh_host="test-host",
            agent_adapter="exec",
            agent_npm_bin="current-agent",
            agent_args=["--prompt", "{prompt_path}"],
        ),
    )

    assert (
        "ExecStart=%h/.local/bin/clio-relay endpoint start --role worker --cluster test-cluster"
    ) in rendered
    assert 'Environment="CLIO_RELAY_CORE_DIR=%h/.local/share/clio-relay/core"' in rendered
    assert 'Environment="CLIO_RELAY_AGENT_BIN=%h/.local/bin/current-agent"' in rendered
    assert 'Environment="CLIO_RELAY_AGENT_ADAPTER=exec"' in rendered
    assert "sudo" not in rendered


def test_endpoint_user_service_uses_cluster_executable_overrides() -> None:
    rendered = render_endpoint_user_service(
        cluster="test-cluster",
        definition=ClusterDefinition(
            name="test-cluster",
            ssh_host="test-host",
            jarvis_bin="/opt/jarvis/current",
            frpc_bin="/opt/frp/frpc",
            agent_bin="/opt/agents/clio",
        ),
    )

    assert 'Environment="CLIO_RELAY_JARVIS_BIN=/opt/jarvis/current"' in rendered
    assert 'Environment="CLIO_RELAY_FRPC_BIN=/opt/frp/frpc"' in rendered
    assert 'Environment="CLIO_RELAY_AGENT_BIN=/opt/agents/clio"' in rendered
