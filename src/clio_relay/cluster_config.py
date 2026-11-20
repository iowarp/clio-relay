"""The frp link settings for one site.

The worker adapter builds a one-site :class:`ClusterRegistry`, places it on the
site as JSON, and the site server reads it back. The frp transports read the
same :class:`ClusterDefinition`. Nothing here touches the filesystem.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.errors import ConfigurationError

# The bring-up identity anchor for ``brokered_tcp`` and ``udp_rendezvous``. Those
# modes have no ssh-authenticated act to carry the identity document, so a site
# must opt into the preshared link secret explicitly. ``None`` means not opted in.
IdentityAnchor = Literal["preshared_link_secret"]


class FrpTransportConfig(BaseModel):
    """How frpc reaches the frps rendezvous for one site."""

    model_config = ConfigDict(extra="forbid")

    protocol: str = "wss"
    server_addr: str = ""
    server_port: int = 443
    token_env: str = "CLIO_RELAY_FRP_TOKEN"
    stcp_secret_env: str = "CLIO_RELAY_STCP_SECRET"
    # ``None`` resolves to a per-site default (``frp_proxy_naming``).
    proxy_name: str | None = None
    identity_anchor: IdentityAnchor | None = None
    # The STUN server an xtcp punch asks for its public address; None is frpc's own default.
    stun_server: str | None = None


class ClusterDefinition(BaseModel):
    """One site's name, ssh destination and frp settings."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=256)
    ssh_host: str = Field(min_length=1, max_length=1_024)
    frp_transport: FrpTransportConfig = Field(default_factory=FrpTransportConfig)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _label(value, field="cluster name")

    @field_validator("ssh_host")
    @classmethod
    def _validate_ssh_host(cls, value: str) -> str:
        if value.startswith("-") or any(c.isspace() or not c.isprintable() for c in value):
            raise ValueError(
                "ssh_host must be one non-option SSH destination without whitespace or controls"
            )
        return value


class ClusterRegistry(BaseModel):
    """Site definitions keyed by name."""

    model_config = ConfigDict(extra="forbid")

    clusters: dict[str, ClusterDefinition] = Field(default_factory=dict, max_length=512)

    @model_validator(mode="after")
    def _keys_match_names(self) -> ClusterRegistry:
        mismatches = sorted(key for key, item in self.clusters.items() if item.name != key)
        if mismatches:
            raise ValueError(
                "cluster registry keys must match ClusterDefinition.name: " + ", ".join(mismatches)
            )
        return self

    def require(self, name: str) -> ClusterDefinition:
        """Return a configured site or raise a configuration error."""
        try:
            return self.clusters[name]
        except KeyError as exc:
            raise ConfigurationError(f"cluster is not configured: {name}") from exc


def _label(value: str, *, field: str) -> str:
    if value != value.strip() or any(not c.isprintable() for c in value):
        raise ValueError(f"{field} must not contain surrounding whitespace or controls")
    return value
