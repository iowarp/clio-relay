from __future__ import annotations

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
