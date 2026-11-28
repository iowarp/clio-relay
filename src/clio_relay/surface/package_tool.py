"""The ``packages`` tools: what a site's JARVIS root holds, and adding a repository to it.

``run`` takes a package as ``<repository>.<package>`` and pins the review digest
the site's root holds for it. The listing runs on the site, through the adapter
(:meth:`~clio_relay.packages.PackageCatalog.refresh`). A repository the person
wrote reaches the site with ``files``: the agent copies its directory, laid out
``<name>/<name>/<package>/pkg.py``, into the site's user space, then ``add``
registers it with ``jarvis repo add <path>`` (``force`` replaces one of the same
name). Adding runs nothing and asks nobody: on a guarded site the person reviews
each package, by its digest, before its first run there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from clio_relay import jarvis_submit
from clio_relay.jarvis_site import Unset
from clio_relay.jarvis_submit import not_run_here

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

SCHEMA = "relay.packages.v2"
_FIELDS = {"name", "version", "digest", "kind", "title", "summary", "source", "repo_dir"}


async def packages(
    service: RelayService,
    site_id: str,
    *,
    match: str | None = None,
    add: str = "",
    force: bool = False,
) -> dict[str, Any]:
    """List the site's packages, after registering the repository at ``add`` when given."""
    site = service.site(site_id)
    adapter = service.adapter_for(site, "run")
    if not adapter.runs_packages:
        raise not_run_here(adapter.name)
    entries = await service.packages.refresh(site.id, adapter, add=add, force=force)
    if isinstance(entries, Unset):
        raise jarvis_submit.not_at_pin(site.title, entries)
    words = (match or "").lower().split()
    listed = [
        entry.model_dump(mode="json", include=_FIELDS)
        for entry in entries
        if all(
            word in f"{entry.name} {entry.title or ''} {entry.summary or ''}".lower()
            for word in words
        )
    ]
    result: dict[str, Any] = {"schema": SCHEMA, "site": site.id, "packages": listed}
    if add:
        result["added"] = add
    return result
