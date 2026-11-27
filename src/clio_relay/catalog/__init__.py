"""The catalog of known sites and site skills (RL4, RL34).

Site files live in ``catalog/sites/<id>.yaml``, one :class:`CatalogEntry` per
file, each written by the workstream that owns that site. Skills live in
``catalog/skills/<name>.md``. Adding a catalog entry copies it into the
person's ``sites.yaml``.
"""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True)
class Way:
    """One protocol a catalog site offers: the entry it comes from and what it means there."""

    adapter: str
    entry: str
    note: str | None = None


@dataclass(frozen=True)
class CatalogSite:
    """One machine the catalog knows, with every protocol its entries offer.

    The first way is the site's suggested one: its main entry's protocol in use.
    """

    id: str
    name: str
    summary: str | None
    address: str | None
    description: str | None
    ways: tuple[Way, ...]
    featured: int | None = None

    def way(self, adapter: str | None) -> Way:
        """Return the way for ``adapter``, or the suggested one when it is None or not offered."""
        for item in self.ways:
            if item.adapter == adapter:
                return item
        return self.ways[0]


def grouped(entries: list[CatalogEntry] | None = None) -> list[CatalogSite]:
    """Return the catalog as sites: entries that share a ``site`` become one.

    A site's main entry is the one whose id is the site's id, else its first. Its
    protocols are the main entry's, then each other entry's in use, then the
    alternatives, each once. Featured sites come first, in their order, then the
    rest by name, with the local computer last.
    """
    found = sites() if entries is None else entries
    groups: dict[str, list[CatalogEntry]] = {}
    for entry in found:
        groups.setdefault(entry.group, []).append(entry)
    result: list[CatalogSite] = []
    for group_id, members in groups.items():
        main = next((entry for entry in members if entry.id == group_id), members[0])
        ordered = [main, *(entry for entry in members if entry is not main)]
        notes: dict[str, str] = {}
        for entry in ordered:
            for name, note in entry.protocols.items():
                notes.setdefault(name, note)
        ways: list[Way] = []
        seen: set[str] = set()
        candidates = [(entry.adapter, entry) for entry in ordered] + [
            (name, entry) for entry in ordered for name in entry.alternatives
        ]
        for adapter, entry in candidates:
            if adapter in seen:
                continue
            seen.add(adapter)
            ways.append(Way(adapter=adapter, entry=entry.id, note=notes.get(adapter)))
        result.append(
            CatalogSite(
                id=group_id,
                name=main.site_name or main.title,
                summary=main.summary,
                address=next((entry.address for entry in ordered if entry.address), None),
                description=main.description,
                ways=tuple(ways),
                featured=main.featured,
            )
        )
    return sorted(
        result,
        key=lambda item: (
            item.featured is None,
            item.featured or 0,
            item.ways[0].adapter == "local",
            item.name.lower(),
        ),
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
