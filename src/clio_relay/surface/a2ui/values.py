"""Reading what a Compute page form sent: JSON Pointers, form fields and adapter settings."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from clio_relay.adapters import registry
from clio_relay.jobs.refusals import Refusal
from clio_relay.sites.models import SpendAsk
from clio_relay.surface.a2ui.forms import FORM_ERROR, Field, FormError, form, parse, parse_value


def pointer(data: dict[str, Any], path: str) -> Any:
    """Return the value at a JSON Pointer in ``data``, or None."""
    current: Any = data
    for part in path.strip("/").split("/"):
        if not isinstance(current, dict):
            return None
        current = cast(dict[str, Any], current).get(part.replace("~1", "/").replace("~0", "~"))
    return current


def rendered_keys(fields: Sequence[Field], prefix: str) -> set[str]:
    return {prefix + item.key for item in fields}


def located(errors: object, prefix: str, known: set[str]) -> dict[str, str]:
    """Return validation errors by the form field they belong to."""
    found: dict[str, str] = {}
    for raw in cast(list[Any], errors) if isinstance(errors, list) else []:
        if not isinstance(raw, dict):
            continue
        item = cast(dict[str, Any], raw)
        loc = [str(part) for part in cast(list[Any], item.get("loc") or [])]
        message = str(item.get("msg") or "Not valid")
        key = f"{prefix}{loc[0]}" if loc else FORM_ERROR
        if key not in known:
            key, message = FORM_ERROR, f"{' '.join(loc)} {message}".strip()
        found.setdefault(key, message)
    return found


SPEND_LIMIT = "spend_limit"
"""The form field that holds the node-hours above which relay asks."""
SPEND_ABOVE = "above"
"""The spend form's choice that asks above a limit."""


def spend_ask(raw: Any) -> SpendAsk:
    """Return the spend gate's setting from a picker or a typed value.

    ``always`` asks before every job, ``never`` never asks, and a number asks
    above that many node-hours.
    """
    text = str(raw or "").strip().lower() or "always"
    if text == "always":
        return "always"
    if text == "never":
        return "never"
    try:
        value = float(text)
    except ValueError:
        raise FormError("spend", "Choose when relay asks before it spends") from None
    if value <= 0:
        raise FormError("spend", "Enter a number of node-hours above 0")
    return value


def spend_value(ask: str | float) -> str:
    """Return a spend setting as the picker's value: ``always``, ``never`` or the node-hours."""
    return ask if isinstance(ask, str) else f"{ask:g}"


def spend_choice(choice: Any, limit: Any) -> str | float | None:
    """Return the spend setting a form's choice and limit describe, or None when unset."""
    picked = parse_value(Field("spend", "", "choice"), choice)
    if picked is None:
        return None
    if picked != SPEND_ABOVE:
        return spend_ask(picked)
    text = "" if limit is None else str(limit).strip()
    try:
        value = float(text)
    except ValueError:
        raise FormError(SPEND_LIMIT, "Enter the node-hours above which relay asks") from None
    if value <= 0:
        raise FormError(SPEND_LIMIT, "Enter a number of node-hours above 0")
    return value


def site_values(
    fields: Sequence[Field], values: dict[str, Any], errors: dict[str, str]
) -> dict[str, Any]:
    """Parse the site fields of a form. Errors go to ``errors`` under ``site.<key>``.

    The spend choice and its limit parse together, as ``spend``.
    """
    parsed: dict[str, Any] = {}
    for item in fields:
        if item.key == SPEND_LIMIT:
            continue
        try:
            if item.key == "spend":
                parsed[item.key] = spend_choice(values.get("spend"), values.get(SPEND_LIMIT))
            else:
                parsed[item.key] = parse_value(item, values.get(item.key))
        except FormError as exc:
            errors[f"site.{exc.key}"] = str(exc)
    return parsed


def config_values(
    adapter: str, values: dict[str, Any], errors: dict[str, str], prefix: str
) -> dict[str, Any] | None:
    """Parse and validate one adapter's settings, or return None with ``errors`` filled."""
    try:
        fields = form(adapter, registry.config_schema(adapter)).fields
    except Refusal as refusal:
        errors[FORM_ERROR] = refusal.message
        return None
    try:
        config = parse(fields, values)
    except FormError as exc:
        errors[prefix + exc.key] = str(exc)
        return None
    try:
        registry.validate_config(adapter, config)
    except Refusal as refusal:
        errors.update(located(refusal.data.get("errors"), prefix, rendered_keys(fields, prefix)))
        if not errors:
            errors[FORM_ERROR] = refusal.message
        return None
    return config


def first(value: Any) -> str:
    if isinstance(value, list):
        items = cast(list[Any], value)
        return str(items[0]) if items else ""
    return "" if value is None else str(value)


def as_dict(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def sentence(text: str) -> str:
    """Uppercase the first letter only; ``str.capitalize`` lowercases names like TACC."""
    return text[:1].upper() + text[1:]
