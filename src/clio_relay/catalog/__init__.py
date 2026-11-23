"""The catalog of known sites and site skills (RL4, RL34).

Site files live in ``catalog/sites/<id>.yaml``, one :class:`CatalogEntry` per
file, each written by the workstream that owns that site. Skills live in
``catalog/skills/<name>.md``. Adding a catalog entry copies it into the
person's ``sites.yaml``.
"""

from __future__ import annotations

import json
from importlib.resources import files
from importlib.resources.abc import Traversable
from typing import Any

import yaml
from pydantic import ValidationError

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.sites.models import CatalogEntry

_ROOT = files(__package__ or "clio_relay.catalog")

RETIRED_SKILLS = frozenset({"ares", "alcf-polaris"})
"""Skills the catalog once shipped and no longer does.

A site's facts are JARVIS's now (``jarvis rg skill set``, ``describe``'s
``facts.sched``), so these left the catalog. A site added while the catalog
still linked one keeps the name in its ``skill`` field; relay treats that link
as no link unless the person has their own skill of that name."""


def _dir(name: str) -> Traversable | None:
    path = _ROOT / name
    return path if path.is_dir() else None


def schema() -> dict[str, Any]:
    """Return the JSON Schema a catalog site file follows."""
    return CatalogEntry.model_json_schema()


def schema_json() -> str:
    """Return :func:`schema` as text."""
    return json.dumps(schema(), indent=2, sort_keys=True)


def sites() -> list[CatalogEntry]:
    """Return every catalog site, sorted by id. A malformed file is a refusal."""
    folder = _dir("sites")
    if folder is None:
        return []
    found: list[CatalogEntry] = []
    for item in sorted(folder.iterdir(), key=lambda entry: entry.name):
        if not item.name.endswith((".yaml", ".yml")):
            continue
        try:
            found.append(CatalogEntry.model_validate(yaml.safe_load(item.read_text("utf-8"))))
        except ValidationError as exc:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                f"catalog file {item.name} is not a valid site",
                file=item.name,
                errors=exc.errors(include_url=False, include_context=False),
            ) from exc
    return sorted(found, key=lambda entry: entry.id)


def site(site_id: str) -> CatalogEntry:
    """Return one catalog site. Refuses with ``site_not_found``."""
    for entry in sites():
        if entry.id == site_id:
            return entry
    raise Refusal(
        RefusalReason.SITE_NOT_FOUND,
        f"the catalog has no site named {site_id!r}",
        site=site_id,
        known=[entry.id for entry in sites()],
    )


def skills() -> dict[str, str]:
    """Return every catalog skill: name to Markdown text."""
    folder = _dir("skills")
    if folder is None:
        return {}
    return {
        item.name.removesuffix(".md"): item.read_text("utf-8")
        for item in sorted(folder.iterdir(), key=lambda entry: entry.name)
        if item.name.endswith(".md")
    }
