"""Site skills, served as MCP resources (RL34).

A skill is Markdown that tells an agent how to use a site well. Relay ships one
for each catalog site in ``catalog/skills/<name>.md``. A person adds a skill, or
replaces a shipped one, by putting ``<name>.md`` in ``<config_dir>/skills/``.
A site links its skill with its ``skill`` field: a skill name, or the path of a
Markdown file (a relative path is read from the config directory).

``relay://skills/{name}`` serves each skill and ``relay://sites/{site}/skill``
serves the skill a site links. Skills are optional: a site without one has no
skill resource, and the agent works from what ``describe`` reports.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import mcp_types
from fastmcp.resources import Resource
from fastmcp.resources.types import TextResource
from fastmcp.server.providers import Provider

from clio_relay import catalog
from clio_relay.jobs.refusals import Refusal
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.changes import ServerChanges

logger = logging.getLogger(__name__)

MIME_TYPE = "text/markdown"
WATCH_INTERVAL_S = 5.0


def skill_uri(name: str) -> str:
    """Return the resource URI of one skill."""
    return f"relay://skills/{name}"


def site_skill_uri(site_id: str) -> str:
    """Return the resource URI of the skill a site links."""
    return f"relay://sites/{site_id}/skill"


@dataclass(frozen=True)
class Skill:
    """One skill's text and where relay read it from."""

    name: str
    text: str
    source: str

    @property
    def title(self) -> str:
        """Return the skill's first Markdown heading, or its name."""
        for line in self.text.splitlines():
            if line.startswith("# "):
                return line[2:].strip()
        return self.name


def skills(config_dir: Path) -> dict[str, Skill]:
    """Return every skill by name. The person's own replace shipped ones."""
    found = {name: Skill(name, text, "catalog") for name, text in catalog.skills().items()}
    folder = config_dir / "skills"
    if folder.is_dir():
        for path in sorted(folder.glob("*.md")):
            try:
                found[path.stem] = Skill(path.stem, path.read_text("utf-8"), "person")
            except (OSError, UnicodeDecodeError):
                logger.warning("relay cannot read the skill file %s", path)
    return found


def _is_path(link: str) -> bool:
    return link.endswith(".md") or "/" in link or "\\" in link or link.startswith("~")


def site_skill(
    site: SiteEntry, config_dir: Path, known: dict[str, Skill] | None = None
) -> Skill | None:
    """Return the skill ``site`` links, or None when it links none or it can't be read."""
    link = site.skill
    if not link:
        return None
    if not _is_path(link):
        return (skills(config_dir) if known is None else known).get(link)
    path = Path(link).expanduser()
    if not path.is_absolute():
        path = config_dir / path
    try:
        return Skill(path.stem, path.read_text("utf-8"), str(path))
    except (OSError, UnicodeDecodeError):
        return None


def skill_link(
    site: SiteEntry,
    config_dir: Path,
    known: dict[str, Skill] | None = None,
    *,
    with_text: bool = False,
) -> dict[str, Any] | None:
    """Say what ``describe`` reports about a site's skill: where to read it, or why there
    is none. ``with_text`` adds the guide itself, so a client that never reads MCP
    resources still gets it from the ``describe`` tool."""
    if not site.skill:
        return None
    skill = site_skill(site, config_dir, known)
    if skill is None and site.skill in catalog.RETIRED_SKILLS:
        return None
    if skill is None:
        return {
            "linked": site.skill,
            "uri": None,
            "detail": "relay cannot find or read this skill; fix the site's skill field",
        }
    link: dict[str, Any] = {
        "linked": site.skill,
        "uri": site_skill_uri(site.id),
        "title": skill.title,
    }
    if with_text:
        link["text"] = skill.text
    else:
        link["read_with"] = f"describe(site={site.id!r})"
    return link


def _resource(uri: str, skill: Skill, description: str) -> TextResource:
    return TextResource(
        uri=uri,  # pyright: ignore[reportArgumentType]
        name=uri.removeprefix("relay://"),
        title=skill.title,
        description=description,
        mime_type=MIME_TYPE,
        text=skill.text,
        annotations=mcp_types.Annotations(audience=["assistant"]),
    )


class SkillResources(Provider):
    """Lists the skills and the site skills, read fresh from disk on every request."""

    def __init__(self, config_dir: Path, sites: SiteStore) -> None:
        super().__init__()
        self._config_dir = config_dir
        self._sites = sites

    def resources(self) -> list[TextResource]:
        """Return one resource per skill and one per site that links a readable skill."""
        known = skills(self._config_dir)
        found = [
            _resource(skill_uri(name), skill, f"The {name} skill ({skill.source})")
            for name, skill in known.items()
        ]
        try:
            entries = self._sites.list()
        except Refusal as refusal:  # a broken sites file still serves the skills
            logger.warning("site skills are unavailable: %s", refusal.message)
            entries = []
        for entry in entries:
            skill = site_skill(entry, self._config_dir, known)
            if skill is not None:
                found.append(
                    _resource(site_skill_uri(entry.id), skill, f"How to use the site {entry.title}")
                )
        return found

    async def _list_resources(self) -> Sequence[Resource]:
        return await asyncio.to_thread(self.resources)

    def _snapshot(self) -> dict[str, str]:
        return {
            str(item.uri): hashlib.sha256(item.text.encode()).hexdigest()
            for item in self.resources()
        }

    async def watch(self, changes: ServerChanges, interval_s: float = WATCH_INTERVAL_S) -> None:
        """Announce added, removed and edited skills until cancelled.

        The CLI edits the sites file and the skills folder from another process,
        so relay looks at them every ``interval_s`` seconds.
        """
        before = await asyncio.to_thread(self._snapshot)
        while True:
            await asyncio.sleep(interval_s)
            try:
                after = await asyncio.to_thread(self._snapshot)
            except Exception:  # a half-written file is read again next time
                logger.debug("relay could not read the skills this time", exc_info=True)
                continue
            if after.keys() != before.keys():
                await changes.resources_list_changed()
            for uri in after.keys() & before.keys():
                if after[uri] != before[uri]:
                    await changes.resource_updated(uri)
            before = after
