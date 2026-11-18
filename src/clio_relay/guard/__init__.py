"""Relay's guard (RL24 to RL26). See :mod:`clio_relay.guard.policy`."""

from __future__ import annotations

from clio_relay.adapters.hooks import HookContext
from clio_relay.guard.policy import Guard
from clio_relay.guard.warm import WARM_EXECUTABLE

__all__ = ["WARM_EXECUTABLE", "Guard", "create_guard"]


def create_guard(context: HookContext) -> Guard:
    """The ``clio_relay.hooks`` entry point ``guard``."""
    return Guard(context)
