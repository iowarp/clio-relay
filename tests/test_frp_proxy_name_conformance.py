"""Lock the desktop transport and the cluster-side proxy renderer to ONE proxy name.

clio-relay#279 design point 2: ``frp_transport.py``'s desktop-side
``brokered_tcp``/``udp_rendezvous`` transports and ``frpc_unit.py``'s
cluster-side proxy TOML/unit renderer must resolve the EXACT same stcp/xtcp
proxy name for the same cluster, or the two ends of one pairing silently
diverge. Both now call the single owner, ``frp_link.canonical_proxy_name``;
this module proves that composition, not just that the shared function
exists.

The one thing this file deliberately reproduces is the ORIGINAL trap
(``cli_relay_host.py``'s ``render-frpc-config --proxy-name`` used to default
to the unrelated literal ``"relay-stcp"``): a dedicated test proves that
literal no longer equals the canonical name for an ordinary cluster, so a
future regression that reintroduces a hardcoded default would be caught
here even without touching the CLI layer.
"""

from __future__ import annotations

import os

import pytest

from clio_relay.cluster_config import ClusterDefinition, FrpTransportConfig
from clio_relay.frp_proxy_naming import canonical_proxy_name
from clio_relay.frp_transport import BrokeredTcpTransport, UdpRendezvousTransport
from clio_relay.frpc_unit import render_frpc_proxy_toml


def _frp_definition(*, cluster: str = "ares", proxy_name: str | None = None) -> ClusterDefinition:
    return ClusterDefinition(
        name=cluster,
        ssh_host=f"{cluster}-login",
        frp_transport=FrpTransportConfig(
            server_addr="relay.example.org",
            identity_anchor="preshared_link_secret",
            proxy_name=proxy_name,
        ),
    )


@pytest.fixture(autouse=True)
def _frp_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]
    monkeypatch.setenv("CLIO_RELAY_FRP_TOKEN", "frp-token-from-env")
    monkeypatch.setenv("CLIO_RELAY_STCP_SECRET", "stcp-secret-from-env")


def _transport_proxy_name(
    transport_cls: type, definition: ClusterDefinition, *, cluster: str
) -> str:
    """Build a transport (never established) and read its resolved proxy name.

    Mirrors ``_FrpChannelTransport.__init__``'s own resolution exactly --
    the constructor never dials anything, so this is composition, not a live
    probe.
    """
    transport = transport_cls(
        definition=definition,
        cluster=cluster,
        session_id="sess",
        session_generation_id="gen",
        remote_api_port=8765,
        api_token="owner-token",
        identity_anchor="preshared_link_secret",
        frpc_bin="frpc",
    )
    return transport._proxy_name  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


@pytest.mark.parametrize("transport_cls", [BrokeredTcpTransport, UdpRendezvousTransport])
def test_desktop_transport_and_cluster_side_toml_agree_on_default_proxy_name(
    transport_cls: type,
) -> None:
    definition = _frp_definition(cluster="ares")

    desktop_name = _transport_proxy_name(transport_cls, definition, cluster="ares")
    cluster_side_name = canonical_proxy_name(definition, cluster="ares")

    assert desktop_name == cluster_side_name == "ares-owned-session"
    toml_text = render_frpc_proxy_toml(definition, cluster="ares", local_port=8765)
    assert f'name = "{desktop_name}"' in toml_text


@pytest.mark.parametrize("transport_cls", [BrokeredTcpTransport, UdpRendezvousTransport])
def test_desktop_transport_and_cluster_side_toml_agree_on_explicit_proxy_name(
    transport_cls: type,
) -> None:
    definition = _frp_definition(cluster="ares", proxy_name="ares-custom-proxy")

    desktop_name = _transport_proxy_name(transport_cls, definition, cluster="ares")
    cluster_side_name = canonical_proxy_name(definition, cluster="ares")

    assert desktop_name == cluster_side_name == "ares-custom-proxy"
    toml_text = render_frpc_proxy_toml(definition, cluster="ares", local_port=8765)
    assert 'name = "ares-custom-proxy"' in toml_text


def test_canonical_name_differs_from_the_old_relay_stcp_default_trap() -> None:
    """The regression this whole slice exists to close.

    Before clio-relay#279, ``cli_relay_host.py``'s ``render-frpc-config``
    command defaulted ``--proxy-name`` to the literal ``"relay-stcp"`` --
    unrelated to what the desktop transport (or a proxy unit installed via
    ``relay-host install-proxy``) actually resolves. Locking this
    inequality means a future change that reintroduces a hardcoded literal
    default is caught here even if nobody remembers this history.
    """
    definition = _frp_definition(cluster="ares")

    canonical = canonical_proxy_name(definition, cluster="ares")

    assert canonical != "relay-stcp"
    assert canonical == "ares-owned-session"


def test_canonical_proxy_name_is_deterministic_across_repeated_calls() -> None:
    definition = _frp_definition(cluster="carbonate")

    first = canonical_proxy_name(definition, cluster="carbonate")
    second = canonical_proxy_name(definition, cluster="carbonate")

    assert first == second == "carbonate-owned-session"


def test_canonical_proxy_name_never_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pure composition: the canonical name never depends on secrets or env state."""
    monkeypatch.delenv("CLIO_RELAY_FRP_TOKEN", raising=False)
    monkeypatch.delenv("CLIO_RELAY_STCP_SECRET", raising=False)
    definition = _frp_definition(cluster="ares")

    assert canonical_proxy_name(definition, cluster="ares") == "ares-owned-session"
    assert "CLIO_RELAY_FRP_TOKEN" not in os.environ
