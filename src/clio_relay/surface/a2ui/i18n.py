"""The Compute page's words in the person's language.

A host that knows the person's language sends it with each request, in the
request's ``_meta`` under :data:`LOCALE_META` (a BCP 47 tag such as ``es-ES``).
Relay keeps it for the request in a context variable, and :func:`_` returns a
string in that language when relay has it, else the English one. English
strings are the keys, so a missing translation reads as English, never as a key.
Translations live in ``locales/<language>.json``.
"""

from __future__ import annotations

import json
from contextvars import ContextVar, Token
from functools import cache
from importlib.resources import files
from typing import Any, cast

LOCALE_META = "io.iowarp/locale"
"""The request ``_meta`` key a host puts the person's language in."""

LANGUAGES = ("en", "es", "ja", "el")
"""The languages the page speaks."""

_locale: ContextVar[str] = ContextVar("clio_relay_locale", default="en")


def language(tag: Any) -> str:
    """Return the page language for a BCP 47 tag: ``es-ES`` is ``es``; others are English."""
    if not isinstance(tag, str):
        return "en"
    primary = tag.strip().replace("_", "-").split("-", 1)[0].lower()
    return primary if primary in LANGUAGES else "en"


def use(tag: Any) -> Token[str]:
    """Speak the language ``tag`` names until :func:`reset` gets the returned token."""
    return _locale.set(language(tag))


def reset(token: Token[str]) -> None:
    """Go back to the language before :func:`use`."""
    _locale.reset(token)


def current() -> str:
    """Return the language in use."""
    return _locale.get()


@cache
def _table(lang: str) -> dict[str, str]:
    if lang == "en":
        return {}
    try:
        text = (
            files("clio_relay.surface.a2ui")
            .joinpath("locales", f"{lang}.json")
            .read_text(encoding="utf-8")
        )
        data: Any = json.loads(text)
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in cast(dict[Any, Any], data).items()}


def _(text: str, /, **values: Any) -> str:
    """Return ``text`` in the language in use, with ``{name}`` filled from ``values``."""
    shown = _table(_locale.get()).get(text, text)
    if not values:
        return shown
    try:
        return shown.format(**values)
    except (KeyError, IndexError, ValueError):
        return text.format(**values)
