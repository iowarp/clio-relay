"""How one remote connection reaches its remote relay."""

from __future__ import annotations

from typing import Literal

TransportMode = Literal["brokered_tcp", "udp_rendezvous", "ssh_forward"]
"""How one remote connection reaches its remote relay.

The mode is a deployment-time choice, made per connection. Nothing probes one
mode and switches to another: a link failure re-establishes the same mode or
reports a typed failure. ``brokered_tcp`` is TCP joined by a reachable server,
``udp_rendezvous`` is the same rendezvous with NAT traversal, and
``ssh_forward`` is for infrastructure that permits nothing else.
"""
