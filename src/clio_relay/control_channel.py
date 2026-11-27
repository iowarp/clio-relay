"""The held relay-to-relay link's shared types.

The ``worker`` adapter holds one link to each placed worker: an frp ``stcp``
visitor through the rendezvous (:mod:`clio_relay.frp_transport`). The link is
established once and every call rides it as plain HTTP against the mapped
loopback port. These are the types both ends of that code share.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.cluster_config import IdentityAnchor
from clio_relay.errors import RelayError

CHANNEL_BOOTSTRAP_SCHEMA: Final = "clio-relay.channel-bootstrap.v1"
MAX_CHANNEL_BOOTSTRAP_BYTES: Final = 256 * 1024
DEFAULT_CHANNEL_READY_TIMEOUT_SECONDS: Final = 30.0


class TransportIdentityAnchorRequired(RelayError):
    """An frp link needs the cluster to accept the ``preshared_link_secret`` anchor."""


class ChannelBootstrapError(RelayError):
    """Bring-up could not obtain the exact bootstrap document."""


class ChannelDropped(RelayError):
    """The held link is gone; a new link has to replace it."""


class StreamChannelsUnavailable(RelayError):
    """This transport cannot carry extra stream channels."""


@dataclass(frozen=True)
class ChannelEndpoint:
    """The local address at which the held link reaches the remote relay."""

    host: str
    port: int

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("channel endpoint host must not be empty")
        if not 1 <= self.port <= 65_535:
            raise ValueError("channel endpoint port must be a valid TCP port")

    @property
    def base_url(self) -> str:
        """Return the loopback base URL for HTTP requests over this link."""
        return f"http://{self.host}:{self.port}"


class OwnedSessionChannelBootstrap(BaseModel):
    """The status and identity documents fetched once while the link comes up."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.channel-bootstrap.v1"] = CHANNEL_BOOTSTRAP_SCHEMA
    status: dict[str, object] = Field(default_factory=dict[str, object])
    identity: dict[str, object] = Field(default_factory=dict[str, object])


@dataclass(frozen=True)
class ChannelLink:
    """One established link: where it answers, and what bring-up proved."""

    control_endpoint: ChannelEndpoint
    bootstrap: OwnedSessionChannelBootstrap
    stream_channels: bool = False
    identity_anchor: IdentityAnchor | None = None
