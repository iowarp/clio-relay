"""The ``packages`` tool: list, stage and remove the packages relay can run.

``run`` takes a package by name and pins the one archive staged under it, so a
package must be staged before it runs. Staging builds the package's archive on
the relay host, from relay's package sources when the call names only the package
(directories a host installed, such as a plugin's copy of clio-jarvis) or from a
directory there, and records it in relay's catalog. It runs no git and fetches
nothing. Nothing runs and no site is touched, so staging asks nobody:
on a guarded site the person reviews the archive, by its SHA-256, before its
first run there. Removing takes entries out of the catalog, and their archives
out of the content store unless an unfinished operation still needs one.

What relay's package sources give for a name is what a bare name runs. When
they change (a plugin update), the next stage or run of that name stages the
new archive and marks the old one ``superseded_by`` it; a bare name never
resolves to a superseded archive, so its review is asked again. An archive
staged from a directory (``dir``) is the person's own and is never superseded.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.packages import (
    PackageEntry,
    refresh_from_sources,
    source_packages,
    stage_dir,
    stage_source,
)

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

SCHEMA = "relay.packages.v1"
_FIELDS = {
    "name",
    "version",
    "sha256",
    "kind",
    "summary",
    "source",
    "origin",
    "superseded_by",
    "staged_at",
}


class PackagesArgs(BaseModel):
    """The arguments of the ``packages`` tool."""

    op: Literal["list", "stage", "remove"]
    package: str | None = None
    dir: str | None = None
    version: str | None = None
    sha256: str | None = None
    match: str | None = None


def _view(entry: PackageEntry) -> dict[str, Any]:
    return entry.model_dump(mode="json", include=_FIELDS)


def _in_use(service: RelayService, digest: str) -> bool:
    """Return whether an operation that is not final names the archive ``digest``."""
    for record in service.operations.all():
        if not record.state.final and digest in json.dumps(record.spec):
            return True
    return False


def _stage(service: RelayService, args: PackagesArgs, package: str) -> dict[str, Any]:
    state = service.paths.state_dir
    if args.dir is not None:
        entry, size = stage_dir(state, Path(args.dir), package, version=args.version)
    else:
        sources = service.paths.package_sources
        entry, size = stage_source(state, sources, package, version=args.version)
    return {"staged": {**_view(entry), "size": size}}


def _remove(service: RelayService, args: PackagesArgs, package: str) -> dict[str, Any]:
    removed = service.packages.remove(package, version=args.version, sha256=args.sha256)
    if not removed:
        known = sorted({entry.name for entry in service.packages.all()})
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"relay has no staged {package} matching that. It has: {', '.join(known) or 'none'}",
            package=package,
            known=known,
        )
    views: list[dict[str, Any]] = []
    for entry in removed:
        kept = _in_use(service, entry.sha256)
        if not kept:
            service.content.drop(entry.sha256)
        views.append({**_view(entry), "archive_kept": kept})
    return {"removed": views}


def _apply(service: RelayService, args: PackagesArgs) -> dict[str, Any]:
    result: dict[str, Any] = {"schema": SCHEMA, "op": args.op}
    if args.op != "list":
        if not args.package:
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                f"{args.op} needs package: <repository>.<package>",
            )
        change = _stage if args.op == "stage" else _remove
        result.update(change(service, args, args.package))
    else:
        refresh_from_sources(service.paths.state_dir, service.paths.package_sources)
    result["packages"] = [_view(entry) for entry in service.packages.all()]
    result["sources"] = [str(source) for source in service.paths.package_sources]
    if args.op == "list":
        staged = {entry.name for entry in service.packages.all()}
        result["available"] = [
            {**item, "staged": item["name"] in staged}
            for item in source_packages(service.paths.package_sources, args.match)
        ]
    return result


async def packages(service: RelayService, args: PackagesArgs) -> dict[str, Any]:
    """Run one ``packages`` call; building an archive runs off the event loop."""
    return await asyncio.to_thread(_apply, service, args)
