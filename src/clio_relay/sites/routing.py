"""Verb routing (RL5).

The adapter in use serves a verb when it implements it. Otherwise the first
alternative that implements it serves it. ``describe`` shows every route, so
nothing is routed silently.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from clio_relay.adapters import registry
from clio_relay.adapters.base import VERBS, Verb
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry


class Route(BaseModel):
    """Which adapter serves one verb on one site."""

    verb: Verb
    adapter: str | None
    via: str | None = None
    unavailable: dict[str, str] = {}


def _candidates(entry: SiteEntry) -> list[tuple[str, dict[str, Any]]]:
    return [(entry.adapter, entry.config), *entry.alternatives.items()]


def route(entry: SiteEntry, verb: Verb) -> tuple[str, dict[str, Any]]:
    """Return the adapter name and config that serve ``verb`` on ``entry``."""
    unavailable: dict[str, str] = {}
    for name, config in _candidates(entry):
        try:
            cls = registry.load(name)
        except Refusal as refusal:
            unavailable[name] = refusal.reason.value
            continue
        if verb in cls.implements:
            return name, config
    if unavailable and entry.adapter in unavailable:
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"no adapter on {entry.id!r} can serve {verb}",
            site=entry.id,
            verb=verb,
            unavailable=unavailable,
        )
    raise Refusal(
        RefusalReason.VERB_NOT_IMPLEMENTED,
        f"no adapter on {entry.id!r} implements {verb}",
        site=entry.id,
        verb=verb,
        unavailable=unavailable,
    )


def routes(entry: SiteEntry) -> list[Route]:
    """Return the route for every verb, for ``describe``."""
    found: list[Route] = []
    for verb in VERBS:
        unavailable: dict[str, str] = {}
        chosen: str | None = None
        for name, _config in _candidates(entry):
            try:
                cls = registry.load(name)
            except Refusal as refusal:
                unavailable[name] = refusal.reason.value
                continue
            if verb in cls.implements:
                chosen = name
                break
        via = "alternative" if chosen is not None and chosen != entry.adapter else None
        found.append(Route(verb=verb, adapter=chosen, via=via, unavailable=unavailable))
    return found
