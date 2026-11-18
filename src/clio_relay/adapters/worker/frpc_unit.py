"""Render the site's frpc proxy config with its secrets left to the environment.

The TOML carries frp's ``{{ .Envs.NAME }}`` placeholders instead of the
rendezvous token and the stcp secret. frpc fills them from its own
environment, which ``site-serve`` loads from ``run/frp.env``, so the config
file never holds a secret.
"""

from __future__ import annotations

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.control_channel import TransportIdentityAnchorRequired
from clio_relay.errors import RelayError
from clio_relay.frp_link import (
    FrpLinkConfig,
    FrpVisitorType,
    render_proxy_config,
    require_frp_server_addr,
)
from clio_relay.frp_proxy_naming import canonical_proxy_name
from clio_relay.relay_host import FrpTransportProtocol


def render_frpc_proxy_toml(
    definition: ClusterDefinition,
    *,
    cluster: str,
    local_port: int,
    local_ip: str = "127.0.0.1",
    proxy_type: FrpVisitorType = "stcp",
) -> str:
    """Render the site's frpc proxy TOML for the worker API on ``local_port``."""
    transport = definition.frp_transport
    if transport.identity_anchor != "preshared_link_secret":
        raise TransportIdentityAnchorRequired(
            f"cluster {cluster!r} has not opted into an identity anchor; set "
            'frp_transport.identity_anchor to "preshared_link_secret"'
        )
    config = FrpLinkConfig(
        server_addr=require_frp_server_addr(transport.server_addr, cluster),
        server_port=transport.server_port,
        protocol=FrpTransportProtocol(transport.protocol),
        token=_env_template(transport.token_env),
        secret_key=_env_template(transport.stcp_secret_env),
        proxy_name=canonical_proxy_name(definition, cluster=cluster),
    )
    return render_proxy_config(
        config, proxy_type=proxy_type, local_ip=local_ip, local_port=local_port
    )


def _env_template(name: str) -> str:
    if (
        not name
        or name[0].isdigit()
        or any(not (c.isupper() or c.isdigit() or c == "_") for c in name)
    ):
        raise RelayError(f"unsafe frp environment binding name: {name!r}")
    return "{{ .Envs." + name + " }}"
