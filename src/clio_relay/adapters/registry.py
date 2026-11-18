"""The adapter registry and factory (RL6, RL8).

Adapters register through the entry point group ``clio_relay.adapters``. A
new adapter is a new distribution with an entry point; relay's core does not
change. Tests and embedders may also :func:`register` a class in process.
"""

from __future__ import annotations

from importlib.metadata import EntryPoint, entry_points
from typing import Any

from pydantic import BaseModel, ValidationError

from clio_relay.adapters.base import AdapterContext, BaseAdapter
from clio_relay.jobs.refusals import Refusal, RefusalReason

ENTRY_POINT_GROUP = "clio_relay.adapters"

type AdapterClass = type[BaseAdapter[Any]]

_registered: dict[str, AdapterClass] = {}


def register(cls: AdapterClass, name: str | None = None) -> None:
    """Register an adapter class in this process, ahead of entry points."""
    _registered[name or cls.name] = cls


def unregister(name: str) -> None:
    """Remove an in-process registration."""
    _registered.pop(name, None)


def _entry_points() -> dict[str, EntryPoint]:
    return {point.name: point for point in entry_points(group=ENTRY_POINT_GROUP)}


def names() -> list[str]:
    """Return every adapter name relay knows, loadable or not."""
    return sorted(set(_entry_points()) | set(_registered))


def load(name: str) -> AdapterClass:
    """Return the adapter class registered as ``name``.

    Refuses with ``unknown_adapter`` when nothing is registered under the name,
    and ``adapter_unavailable`` when the entry point exists but does not load,
    usually because its extra is not installed.
    """
    if name in _registered:
        return _registered[name]
    point = _entry_points().get(name)
    if point is None:
        raise Refusal(
            RefusalReason.UNKNOWN_ADAPTER,
            f"no adapter is registered as {name!r}",
            adapter=name,
            known=names(),
        )
    try:
        loaded: object = point.load()
    except (ImportError, AttributeError) as exc:
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"the {name} adapter is not installed: {exc}",
            adapter=name,
            extra=name,
        ) from exc
    if not (isinstance(loaded, type) and issubclass(loaded, BaseAdapter)):
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"entry point {point.value!r} is not an adapter class",
            adapter=name,
        )
    return loaded  # pyright: ignore[reportUnknownVariableType]


def config_schema(name: str) -> dict[str, Any]:
    """Return the JSON Schema of an adapter's ``Config``. Drives the tab's form."""
    return load(name).Config.model_json_schema()


def validate_config(name: str, config: dict[str, Any]) -> BaseModel:
    """Validate a site entry's config for adapter ``name``."""
    cls = load(name)
    try:
        return cls.Config.model_validate(config)
    except ValidationError as exc:
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            f"the {name} adapter's config is not valid: {exc.error_count()} error(s)",
            adapter=name,
            errors=exc.errors(include_url=False, include_context=False),
        ) from exc


def build(name: str, config: dict[str, Any], context: AdapterContext) -> BaseAdapter[Any]:
    """Build an adapter from a site entry's adapter name and config."""
    cls = load(name)
    return cls(validate_config(name, config), context)
