"""The ``packages`` tool: list, stage and remove the packages relay can run.

``run`` takes a package by name and pins the one archive staged under it, so a
package must be staged before it runs. Staging builds the package's archive on
the relay host, from a git repository at one commit (relay's pinned clio-jarvis
when the call names only the package) or from a directory there, and records it
in relay's catalog. Nothing runs and no site is touched, so staging asks nobody:
on a guarded site the person reviews the archive, by its SHA-256, before its
first run there. Removing takes entries out of the catalog, and their archives
out of the content store unless an unfinished operation still needs one.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel

from clio_relay.identity.grants import GrantFiles
from clio_relay.jarvis_site import CLIO_JARVIS_COMMIT, CLIO_JARVIS_URL
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.packages import PackageEntry, stage_dir, stage_git

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

SCHEMA = "relay.packages.v1"
_FIELDS = {"name", "version", "sha256", "kind", "summary", "source", "staged_at"}


class PackagesArgs(BaseModel):
    """The arguments of the ``packages`` tool."""

    op: Literal["list", "stage", "remove"]
    package: str | None = None
    git: str | None = None
    commit: str | None = None
    path: str | None = None
    dir: str | None = None
    version: str | None = None
    sha256: str | None = None


def _view(entry: PackageEntry) -> dict[str, Any]:
    return entry.model_dump(mode="json", include=_FIELDS)


def _in_use(service: RelayService, digest: str) -> bool:
    """Return whether an operation that is not final names the archive ``digest``."""
    for record in service.operations.all():
        if not record.state.final and digest in json.dumps(record.spec):
            return True
    return False


def _stage(service: RelayService, args: PackagesArgs, package: str) -> dict[str, Any]:
    if args.dir is not None and (args.git or args.commit or args.path):
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            "give one source: dir, or git and commit (path optional)",
        )
    state = service.paths.state_dir
    if args.dir is not None:
        entry, size = stage_dir(state, Path(args.dir), package, version=args.version)
    else:
        entry, size = stage_git(
            state,
            package,
            url=args.git,
            commit=args.commit,
            path=args.path,
            version=args.version,
            grants=GrantFiles.from_settings({"grant_files": dict(service.paths.grant_files)}),
        )
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
    result["packages"] = [_view(entry) for entry in service.packages.all()]
    result["default_source"] = {"git": CLIO_JARVIS_URL, "commit": CLIO_JARVIS_COMMIT}
    return result


async def packages(service: RelayService, args: PackagesArgs) -> dict[str, Any]:
    """Run one ``packages`` call; the git fetch runs off the event loop."""
    return await asyncio.to_thread(_apply, service, args)
