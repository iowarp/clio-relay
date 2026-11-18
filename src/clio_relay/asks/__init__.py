"""Asks: relay's questions for the person, and the page that answers them (RL25a, RL27 to RL30).

``create_provider`` is the ``asks`` hook factory, registered under the
``clio_relay.hooks`` entry point group. The package imports nothing heavy, so
the ``clio-relay-askpass`` helper ssh runs per prompt starts fast.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clio_relay.adapters.hooks import HookContext
    from clio_relay.asks.provider import RelayAskProvider

__all__ = ["create_provider"]


def create_provider(context: HookContext) -> RelayAskProvider:
    """Build relay's ask provider. Starts nothing until it is used."""
    from clio_relay.asks.provider import RelayAskProvider

    return RelayAskProvider(context)
