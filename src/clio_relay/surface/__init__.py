"""Relay's MCP surface: the server, the operations service and the default hooks.

``RelayService`` and ``RelayPaths`` are loaded on first use, so importing a light
submodule such as :mod:`clio_relay.surface.settings` does not load the MCP stack.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService
    from clio_relay.surface.settings import RelayPaths

__all__ = ["RelayPaths", "RelayService"]


def __getattr__(name: str) -> Any:
    if name == "RelayService":
        from clio_relay.surface.service import RelayService

        return RelayService
    if name == "RelayPaths":
        from clio_relay.surface.settings import RelayPaths

        return RelayPaths
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
