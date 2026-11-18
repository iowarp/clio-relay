"""Adapters: the interface, the registry, the hooks and the ``local`` adapter."""

from clio_relay.adapters.base import (
    VERBS,
    Adapter,
    AdapterContext,
    BaseAdapter,
    EmptyConfig,
    SignInNeed,
    SiteInfo,
    Verb,
    not_implemented,
)

__all__ = [
    "VERBS",
    "Adapter",
    "AdapterContext",
    "BaseAdapter",
    "EmptyConfig",
    "SignInNeed",
    "SiteInfo",
    "Verb",
    "not_implemented",
]
