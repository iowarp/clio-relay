"""Install a package's software on a site ahead of any run.

An ``install`` operation is a run's install task on its own. JARVIS owns it:

1. relay asks JARVIS's light ``ppl install +check`` on the site, for the same
   pipeline a run of the package would submit. It runs no Spack on the login
   host: JARVIS answers from the record of the last check a job made;
2. when that record says the software is all there, the operation completes;
3. when it is not, or there is no record yet (``install_unchecked``), JARVIS
   exits 3 with its install report, and the operation takes the run's install
   path (:mod:`clio_relay.surface.installing`): a check job when there is no
   record, the same approval and choices, then ``jarvis ppl install +submit`` as
   a job on the site's compute nodes, then the light check again;
4. when the site's JARVIS root is not set up, the install is that setup alone
   (:func:`set_up`): relay asks, sets the root up, and the operation ends.

No app is launched, and no service starts.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from clio_relay import jarvis_report, jarvis_submit
from clio_relay.adapters.base import BaseAdapter
from clio_relay.jobs.records import OperationRecord, OperationState, StateChange, new_id
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import installing

if TYPE_CHECKING:
    from clio_relay.surface.service import RelayService

logger = logging.getLogger(__name__)


def _label(spec: JobSpec) -> str:
    return " then ".join(step.name for step in spec.steps())


def can_install(adapter: BaseAdapter[Any]) -> bool:
    """Say whether the adapter checks and installs a package's software ahead of a run."""
    return all(
        getattr(type(adapter), hook, None) is not getattr(BaseAdapter, hook)
        for hook in ("install_check", "install")
    )


async def start(
    service: RelayService, site_id: str, spec: JobSpec, *, task: bool = False
) -> OperationRecord:
    """Record an install operation, ask what must be asked, and check if nothing must."""
    site = service.site(site_id)
    if not spec.steps():
        raise Refusal(
            RefusalReason.INVALID_SPEC,
            "install takes a package job: name the package (or pipeline) to install",
            site=site.id,
        )
    adapter_name, _, _ = service.route(site, "run")
    if not can_install(service.adapter_named(site, adapter_name)):
        raise Refusal(
            RefusalReason.VERB_NOT_IMPLEMENTED,
            f"the {adapter_name} adapter cannot install a package's software ahead of a run",
            verb="install",
            site=site.id,
            adapter=adapter_name,
        )
    try:
        spec = await service.prepared_run(site, spec)
    except Refusal as refusal:
        if installing.install_kind(refusal) != "jarvis":
            raise
        return await set_up(service, site, adapter_name, spec, refusal, task=task)
    return await service.open_operation(
        site,
        "install",
        spec,
        spec.model_dump(mode="json"),
        spec.digest(),
        f"Install the software of {_label(spec)}",
        task=task,
    )


async def set_up(
    service: RelayService,
    site: SiteEntry,
    adapter_name: str,
    spec: JobSpec,
    refusal: Refusal,
    *,
    task: bool,
) -> OperationRecord:
    """Record an install that sets up the site's JARVIS root, and ask the person to.

    Nothing else is checked: the packages are listed and reviewed as the root
    holds them, so the job is installed or run again once the root is set up.
    """
    record = OperationRecord(
        operation_id=new_id("op"),
        kind="install",
        site=site.id,
        adapter=adapter_name,
        spec=spec.model_dump(mode="json"),
        spec_digest=spec.digest(),
        state=OperationState.AWAITING_INPUT,
        history=[StateChange(state=OperationState.AWAITING_INPUT)],
        task=task,
        result={installing.SETUP_KEY: True},
        owner=service.lease.owner(),
    )
    service.operations.put(record.operation_id, record)
    return await installing.ask_install(service, record.operation_id, site, refusal)


async def check(
    service: RelayService, operation_id: str, site: SiteEntry, adapter: BaseAdapter[Any]
) -> OperationRecord:
    """Ask JARVIS whether the software is installed. The caller holds ``submitting``."""
    record = service.operation(operation_id)
    try:
        spec = await service.guard.prepare(site, installing.job_of(record), adapter=adapter)
        report = await adapter.install_check(spec, operation_id=operation_id)
    except Refusal as refusal:
        return await installing.ask_install(service, operation_id, site, refusal)
    except Exception as exc:
        logger.exception("checking the install of %s failed", operation_id)
        return service.finish(
            operation_id,
            OperationState.FAILED,
            Refusal(RefusalReason.SUBMIT_FAILED, f"checking the install failed: {exc}"),
        )
    names = jarvis_submit.package_names(spec.steps())
    message = f"the software of {_label(spec)} is installed on {site.title}; nothing ran"
    with service.operations.locked(operation_id):
        record = service.operation(operation_id)
        if not record.state.final:
            record.result = {
                **(record.result or {}),
                "installed": jarvis_report.install_summary(report, names),
                "message": message,
            }
            service.operations.put(operation_id, record)
    return service.finish(operation_id, OperationState.COMPLETED, None)
