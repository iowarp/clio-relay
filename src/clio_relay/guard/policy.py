"""Relay's guard: code policy per site, package reviews, pricing and the warm allocation.

It implements the guard hook (RL24 to RL26):

- ``check_run``: an open site runs anything. A guarded site runs only a JARVIS
  package, named with its version and SHA-256. A package nobody has reviewed
  on that site comes back allowed with a review question in ``data["asks"]``,
  which relay asks before it submits. A free-form command is refused with
  ``policy_refused`` and the way to proceed.
- ``check_exec``: a guarded site's ``exec`` runs only the read-only list.
- ``price``: the most a job may use, in node-hours, or the adapter's own price
  when it has one (sky, in US dollars). No walltime means an unknown charge,
  which always asks unless the site never asks.
- ``prepare``: what the adapter submits. A package becomes the package runner,
  unless the adapter runs packages itself, and a warm allocation request
  becomes its holder. A package that is still not reviewed is refused here too.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from clio_relay.adapters import registry
from clio_relay.adapters.base import AdapterContext, BaseAdapter
from clio_relay.adapters.hooks import Charge, GuardDecision, HookContext
from clio_relay.guard import execlist
from clio_relay.guard.config import SiteGuard, load_config
from clio_relay.guard.reviews import REVIEW_KIND, Review, ReviewStore
from clio_relay.guard.warm import WARM_EXECUTABLE, holder_job, is_warm
from clio_relay.jarvis_site import ArchiveInfo, package_job, read_archive
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.records import OperationRecord, RecordStore
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec, PackageRef
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.routing import route

_MAX_LISTED_FILES = 40
_PRICE: str = "price"  # an optional adapter method (sky); not on BaseAdapter

PROCEED = (
    "Pick a JARVIS package for the work: describe(site=...) lists the packages relay has, "
    "with their parameters. Call run with `package: {name, parameters}` instead of an "
    "executable; relay pins the newest version and its SHA-256. For a package relay does "
    "not have yet (a new one written with the JARVIS package skill), stage it first with "
    "`python -m clio_relay.packages stage <repository dir> <repo.package>`. The person "
    "reviews each package once, by its hash, before it runs."
)


def _source_line(source: dict[str, Any] | None) -> str:
    if not source:
        return "Source: not recorded; the archive was not built from a git checkout"
    where = str(source.get("repository", "?"))
    parts = [f"Source: {where}"]
    if source.get("tag"):
        parts.append(f"tag {source['tag']}")
    if source.get("commit"):
        parts.append(f"commit {source['commit']}")
    if source.get("path"):
        parts.append(f"path {source['path']}")
    line = ", ".join(parts)
    if source.get("modified"):
        line += " (with uncommitted changes)"
    return line


def review_message(site: SiteEntry, package: PackageRef, info: ArchiveInfo, archive: Path) -> str:
    """Return the review question's text: what the package is, where it came from, its hash."""
    lines = [
        f"Review the JARVIS package {package.name} {package.version} before it runs on "
        f"{site.title}, a guarded site.",
        f"SHA-256 of the package archive: {package.sha256}",
        _source_line(info.source),
        f"Archive on this relay host: {archive}",
        f"Files ({len(info.files)}, {info.size} bytes packed):",
    ]
    for item in info.files[:_MAX_LISTED_FILES]:
        mode = " x" if item.executable else ""
        lines.append(f"  {item.sha256[:12]}  {item.size:>8}  {item.path}{mode}")
    if len(info.files) > _MAX_LISTED_FILES:
        lines.append(f"  ... and {len(info.files) - _MAX_LISTED_FILES} more")
    if package.parameters:
        shown = ", ".join(f"{key}={value}" for key, value in sorted(package.parameters.items()))
        lines.append(f"Parameters for this run: {shown}")
    lines.append(
        "A yes records this hash as reviewed on this site. A changed package has a "
        "different hash and needs a new review."
    )
    return "\n".join(lines)


def _review_data(review: Review) -> dict[str, Any]:
    return {
        "package": review.package,
        "version": review.version,
        "sha256": review.sha256,
        "reviewed_at": review.reviewed_at.isoformat(),
        "ask_id": review.ask_id,
    }


class Guard:
    """The guard hook relay loads through the ``clio_relay.hooks`` entry point ``guard``."""

    def __init__(self, context: HookContext) -> None:
        self.context = context
        config_dir = context.settings.get("config_dir")
        self.config_dir = Path(config_dir) if config_dir else None
        self.content = ContentStore(context.state_dir / "content")
        self.reviews = ReviewStore(context.state_dir / "guard" / "reviews")
        self.operations: RecordStore[OperationRecord] = RecordStore(
            context.state_dir / "operations", OperationRecord
        )

    def settings(self, site: SiteEntry) -> SiteGuard:
        """Return the site's guard settings from ``guard.yaml``."""
        return load_config(self.config_dir).site(site.id)

    def reviewed(self, site_id: str, package: PackageRef) -> Review | None:
        """Return the review of ``package`` on ``site_id``, picking up new answers first."""
        found = self.reviews.get(site_id, package)
        if found is None and self.reviews.harvest(self.context.asks, self._open):
            found = self.reviews.get(site_id, package)
        return found

    def _open(self, operation_id: str) -> bool:
        record = self.operations.get(operation_id)
        return record is not None and not record.state.final

    # check_run

    async def check_run(self, site: SiteEntry, spec: JobSpec) -> GuardDecision:
        if is_warm(spec):
            if spec.arguments:
                return GuardDecision(
                    allowed=False,
                    reason=RefusalReason.INVALID_SPEC,
                    message=f"{WARM_EXECUTABLE} takes no arguments; set nodes and the queue",
                    data={"site": site.id},
                )
            return GuardDecision(allowed=True, data={"warm": True})
        if site.policy == "open":
            return GuardDecision(allowed=True)
        package = spec.package
        if package is None:
            return GuardDecision(
                allowed=False,
                reason=RefusalReason.POLICY_REFUSED,
                message=(
                    f"{site.title} is a guarded site, so it runs only reviewed JARVIS packages "
                    f"and not a free-form command like {spec.executable!r}. {PROCEED}"
                ),
                data={"site": site.id, "next_step": "package_review", "how": PROCEED},
            )
        if spec.environment or spec.launcher:
            return GuardDecision(
                allowed=False,
                reason=RefusalReason.POLICY_REFUSED,
                message=(
                    f"{site.title} is guarded: a package job takes its settings as package "
                    "parameters, not as environment variables or a launcher"
                ),
                data={"site": site.id, "next_step": "package_parameters"},
            )
        review = await asyncio.to_thread(self.reviewed, site.id, package)
        if review is not None:
            return GuardDecision(allowed=True, data={"review": _review_data(review)})
        version, sha256 = package.pinned()
        info = await asyncio.to_thread(read_archive, self.content, package.name, sha256)
        archive = self.content.path_for(sha256)
        binding: dict[str, Any] = {
            "kind": REVIEW_KIND,
            "package": package.name,
            "version": version,
            "package_hash": sha256,
            "source": info.source,
            "files": len(info.files),
        }
        return GuardDecision(
            allowed=True,
            data={
                "review": None,
                "asks": [
                    {"message": review_message(site, package, info, archive), "binding": binding}
                ],
            },
        )

    # check_exec

    def warm_jobs(self, site_id: str) -> set[str]:
        """Return the native ids of the warm allocations relay holds on ``site_id``."""
        held: set[str] = set()
        for record in self.operations.all():
            if record.site != site_id or record.kind != "run" or record.state.final:
                continue
            handle = record.handle
            if record.spec.get("executable") != WARM_EXECUTABLE or handle is None:
                continue
            if handle.native_id:
                held.add(handle.native_id)
        return held

    async def check_exec(self, site: SiteEntry, command: list[str]) -> GuardDecision:
        if site.policy == "open":
            return GuardDecision(allowed=True)
        settings = self.settings(site)
        warm = await asyncio.to_thread(self.warm_jobs, site.id) if command[:1] == ["srun"] else ()
        reason = execlist.refusal(
            command, extra=settings.read_only, jarvis=settings.jarvis, warm_jobs=warm
        )
        if reason is None:
            return GuardDecision(allowed=True)
        return GuardDecision(
            allowed=False,
            reason=RefusalReason.POLICY_REFUSED,
            message=(
                f"{site.title} is a guarded site, so exec runs only read-only commands, given "
                f"as an argument list: {reason}. Work that changes anything runs as a reviewed "
                "JARVIS package through run."
            ),
            data={
                "site": site.id,
                "program": command[0] if command else None,
                "read_only": execlist.listed(settings.read_only),
                "next_step": "package_review",
            },
        )

    # price

    async def price(self, site: SiteEntry, spec: JobSpec) -> Charge:
        duration = spec.attributes.duration
        node_hours = spec.node_hours()
        charge = Charge(
            site=site.id,
            queue=spec.attributes.queue_name,
            account=spec.attributes.account,
            nodes=spec.resources.nodes(),
            walltime_s=None if duration is None else duration.total_seconds(),
            node_hours=None if node_hours is None else round(node_hours, 4),
        )
        priced = await self._adapter_price(site, spec)
        if priced is None:
            return charge
        return priced.model_copy(
            update={
                "queue": priced.queue or charge.queue,
                "account": priced.account or charge.account,
            }
        )

    async def _adapter_price(self, site: SiteEntry, spec: JobSpec) -> Charge | None:
        """Ask the adapter that would run ``spec`` for its price, when it has one (sky)."""
        try:
            name, config, _skipped = route(site, "run")
            cls = registry.load(name)
        except Refusal:
            return None
        if not callable(getattr(cls, _PRICE, None)):
            return None
        context = AdapterContext(
            site_id=site.id,
            state_dir=self.context.state_dir / "sites" / site.id / name,
            content=self.content,
        )
        adapter = registry.build(name, config, context)
        price = cast(Callable[[JobSpec], object], getattr(adapter, _PRICE))
        result: object = price(spec)
        if inspect.isawaitable(result):
            result = await result
        return result if isinstance(result, Charge) else None

    # prepare

    async def prepare(
        self,
        site: SiteEntry,
        spec: JobSpec,
        *,
        adapter: BaseAdapter[Any],
        service: bool = False,
    ) -> JobSpec:
        settings = self.settings(site)
        if is_warm(spec):
            return holder_job(spec, idle_s=settings.warm_idle_s)
        package = spec.package
        if package is None:
            if site.policy == "guarded":
                raise Refusal(
                    RefusalReason.POLICY_REFUSED,
                    f"{site.title} is guarded and runs only reviewed JARVIS packages",
                    site=site.id,
                    next_step="package_review",
                )
            return spec
        if site.policy == "guarded":
            review = await asyncio.to_thread(self.reviewed, site.id, package)
            if review is None:
                raise Refusal(
                    RefusalReason.POLICY_REFUSED,
                    f"{package.name} {package.version} ({package.sha256}) has not been reviewed "
                    f"on {site.title}",
                    site=site.id,
                    next_step="package_review",
                )
        if adapter.runs_packages:
            if service and not package.service:
                package = package.model_copy(update={"service": True})
                return spec.model_copy(update={"package": package})
            return spec
        return package_job(spec, jarvis=settings.jarvis, service=service)
