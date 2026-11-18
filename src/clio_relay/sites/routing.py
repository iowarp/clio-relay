"""Verb routing (RL5).

The adapter in use serves a verb when it implements it. Otherwise the first
alternative that implements it serves it. An adapter whose settings fail to
build (``invalid_config``) is skipped, and the skip is part of the route, so
the person sees why it was not used. ``describe`` shows every route, so
nothing is routed silently.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, NamedTuple

from pydantic import BaseModel

from clio_relay.adapters import registry
from clio_relay.adapters.base import VERBS, Verb
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import SiteEntry

Check = Callable[[str, dict[str, Any]], object]
"""Build or validate one candidate; raise a ``Refusal`` when it cannot serve."""


class Skipped(BaseModel):
    """An adapter routing passed over, and why."""

    site: str
    adapter: str
    reason: str
    message: str


class Route(BaseModel):
    """Which adapter serves one verb on one site."""

    verb: Verb
    adapter: str | None
    via: str | None = None
    unavailable: dict[str, str] = {}
    skipped: list[Skipped] = []


class Routed(NamedTuple):
    """The adapter that serves a verb, its settings, and what was skipped first."""

    adapter: str
    config: dict[str, Any]
    skipped: list[Skipped]


def _candidates(entry: SiteEntry) -> list[tuple[str, dict[str, Any]]]:
    return [(entry.adapter, entry.config), *entry.alternatives.items()]


def _validate(name: str, config: dict[str, Any]) -> object:
    return registry.validate_config(name, config)


def _walk(
    entry: SiteEntry, verb: Verb, check: Check, *, strict: bool = True
) -> tuple[tuple[str, dict[str, Any]] | None, dict[str, str], list[Skipped]]:
    unavailable: dict[str, str] = {}
    skipped: list[Skipped] = []
    for name, config in _candidates(entry):
        try:
            cls = registry.load(name)
        except Refusal as refusal:
            unavailable[name] = refusal.reason.value
            continue
        if verb not in cls.implements:
            continue
        try:
            check(name, config)
        except Refusal as refusal:
            if refusal.reason != RefusalReason.INVALID_CONFIG:
                if strict:
                    raise
                # describe still names it; the call itself returns the refusal.
                return (name, config), unavailable, skipped
            skipped.append(
                Skipped(
                    site=entry.id,
                    adapter=name,
                    reason=refusal.reason.value,
                    message=refusal.message,
                )
            )
            continue
        return (name, config), unavailable, skipped
    return None, unavailable, skipped


def route(entry: SiteEntry, verb: Verb, check: Check = _validate) -> Routed:
    """Return the adapter that serves ``verb`` on ``entry``.

    ``check`` builds or validates each candidate; the default validates its
    settings. When every candidate is skipped, the refusal carries each reason.
    """
    chosen, unavailable, skipped = _walk(entry, verb, check)
    if chosen is not None:
        return Routed(chosen[0], chosen[1], skipped)
    details: dict[str, Any] = {
        "site": entry.id,
        "verb": verb,
        "unavailable": unavailable,
        "skipped": [item.model_dump() for item in skipped],
    }
    if unavailable and entry.adapter in unavailable:
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"no adapter on {entry.id!r} can serve {verb}",
            **details,
        )
    if skipped:
        reasons = "; ".join(f"{item.adapter}: {item.message}" for item in skipped)
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            f"every adapter on {entry.id!r} that serves {verb} has unusable settings: {reasons}",
            **details,
        )
    raise Refusal(
        RefusalReason.VERB_NOT_IMPLEMENTED,
        f"no adapter on {entry.id!r} implements {verb}",
        **details,
    )


def routes(entry: SiteEntry, check: Check = _validate) -> list[Route]:
    """Return the route for every verb, for ``describe``."""
    found: list[Route] = []
    for verb in VERBS:
        chosen, unavailable, skipped = _walk(entry, verb, check, strict=False)
        name = chosen[0] if chosen is not None else None
        via = "alternative" if name is not None and name != entry.adapter else None
        found.append(
            Route(verb=verb, adapter=name, via=via, unavailable=unavailable, skipped=skipped)
        )
    return found
