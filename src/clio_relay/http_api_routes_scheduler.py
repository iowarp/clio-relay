"""Scheduler status/cancel routes (iowarp/clio-relay#179 dial burn-down).

``cli_owned_scheduler_cancel.py`` previously reached the cluster's
scheduler exclusively through a per-operation ``ssh ... clio-relay
scheduler status|status-batch|cancel`` dial -- the SAME shape the local
(on-cluster) CLI subcommands already run when NOT dialing remotely
(``cli_scheduler.py``'s ``scheduler_status_command``/``_status_batch_
command``/``_cancel_command``, each delegating to
``scheduler_providers.provider_for_scheduler(...).poll``/``.cancel`` when
``should_execute_on_cluster`` is false). These routes expose that exact
same local capability over the held owned-session channel instead, so
``session teardown``'s scheduler-sentinel preflight/cancellation rides the
channel when one is already live.

Review S1: proven unauthenticated (200 cancelling an unrelated scheduler
job) when this module was reachable on the GLOBAL app with no owned
session and no required token. Fixed at both layers named in review:

* ``http_api.py`` registers this module ONLY when ``resolved.owner_
  session_id is not None`` -- never on the global app (S1(a)).
* Every handler here ALSO re-proves ownership server-side before touching
  a scheduler job: :func:`_owned_scheduler_job_ids` mirrors ``cli_owned_
  relay_jobs._owned_relay_job``'s ownership proof chain (the exact same
  check ``cli_owned_scheduler_cancel`` already builds client-side) against
  the connected owner session's OWN job records, so a caller supplying an
  arbitrary ``scheduler_job_id`` it does not own gets a typed 403
  (``scheduler_job_ownership_refused``) instead of a live scheduler
  action (S1(b)). ``status-batch`` filters the batch to owned ids and
  reports the rest as ``refused_scheduler_job_ids`` rather than refusing
  the whole request.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from fastapi import FastAPI
from fastapi.params import Depends

from clio_relay import door_errors, scheduler_providers
from clio_relay.errors import ConfigurationError
from clio_relay.http_api_context import RelayApiContext
from clio_relay.http_api_models import SchedulerCancelRequest, SchedulerStatusBatchRequest
from clio_relay.http_api_redaction import (
    _public_payload,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _public_record,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.models_scheduling import SchedulerStatus
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS

SCHEDULER_STATUS_BATCH_SCHEMA = "clio-relay.scheduler-status-batch.v1"
MAX_SCHEDULER_STATUS_BATCH_REQUEST = 256
#: Review LOW: bound the scheduler binary's own stdout/stderr in the cancel
#: response -- a live HTTP response body, not a durable T3 record, so a
#: simple byte cap (mirroring the codebase's other response-scoped bounds,
#: e.g. session_lifecycle.py's remote-session stdout/stderr caps) is
#: proportionate rather than the full bounded_payload T3 elision record.
_MAX_SCHEDULER_OUTPUT_BYTES = 64 * 1024


def _owned_scheduler_job_ids(ctx: RelayApiContext) -> set[str]:
    """Scheduler job ids with a proven ownership record under ctx's connected session.

    Review S1(b): mirrors ``cli_owned_relay_jobs._owned_relay_job``'s
    client-side ownership proof chain, run SERVER-side instead of trusted
    from a caller-supplied id. Reached by module-attribute (function-scope
    import, the established cross-module pattern) rather than a module-
    scope import, since nothing in ``cli_owned_relay_jobs.py`` needs to
    reach back into this route module.
    """
    import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs

    if (
        ctx.resolved.owner_session_id is None
        or ctx.resolved.owner_session_generation_id is None
        or ctx.owner_session_cluster_definition is None
    ):
        return set()
    owned_ids: set[str] = set()
    cursor: str | None = None
    while True:
        jobs, next_cursor, _total, _window = ctx.queue.list_owner_session_jobs_page(
            ctx.resolved.owner_session_id,
            session_generation_id=ctx.resolved.owner_session_generation_id,
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
            include_terminal=True,
        )
        for job in jobs:
            tasks, truncated = ctx.queue.scan_job_tasks(job.job_id, limit=1_000)
            if truncated:
                # Can't prove the full ownership record for this job; never
                # credit it with owning anything on an unproven scan.
                continue
            candidate = cli_owned_relay_jobs._owned_relay_job(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                job.model_dump(mode="json"),
                [task.model_dump(mode="json") for task in tasks],
                scheduler_provider=ctx.owner_session_cluster_definition.scheduler_provider,
            )
            owned_ids.update(candidate.scheduler_job_ids)
        if next_cursor is None:
            break
        cursor = next_cursor
    return owned_ids


def _require_owned_scheduler_job(ctx: RelayApiContext, scheduler_job_id: str) -> None:
    if scheduler_job_id not in _owned_scheduler_job_ids(ctx):
        raise door_errors.http_problem(
            "scheduler_job_ownership_refused",
            f"scheduler job {scheduler_job_id} is not owned by this relay session",
        )


def _bounded_output(value: str) -> str:
    """Cap one scheduler-provider output stream to a live-response-sized bound."""
    encoded = value.encode("utf-8")
    if len(encoded) <= _MAX_SCHEDULER_OUTPUT_BYTES:
        return value
    truncated = encoded[:_MAX_SCHEDULER_OUTPUT_BYTES].decode("utf-8", errors="replace")
    return truncated + "...[truncated]"


def register_scheduler_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: Depends,
) -> None:
    """Register the scheduler status/status-batch/cancel routes.

    Reachable only on an owned-session app (``http_api.py`` conditionally
    registers this module, review S1(a)).
    """

    @app.get(
        "/scheduler/jobs/{scheduler_job_id}/status",
        response_model=SchedulerStatus,
        dependencies=[auth_dependency],
    )
    def scheduler_status(scheduler_job_id: str, provider: str) -> SchedulerStatus:
        _require_owned_scheduler_job(ctx, scheduler_job_id)
        try:
            return _public_record(
                scheduler_providers.provider_for_scheduler(provider).poll(scheduler_job_id)
            )
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "configuration_error", exc=door_errors.public_message_error(exc)
            ) from exc

    @app.post("/scheduler/status-batch", dependencies=[auth_dependency])
    def scheduler_status_batch(request: SchedulerStatusBatchRequest) -> dict[str, object]:
        if len(request.scheduler_job_ids) > MAX_SCHEDULER_STATUS_BATCH_REQUEST:
            raise door_errors.http_problem(
                "queue_query_refused",
                f"provide at most {MAX_SCHEDULER_STATUS_BATCH_REQUEST} scheduler job ids",
            )
        if len(set(request.scheduler_job_ids)) != len(request.scheduler_job_ids):
            raise door_errors.http_problem(
                "queue_query_refused", "scheduler job ids cannot contain duplicates"
            )
        owned_ids = _owned_scheduler_job_ids(ctx)
        refused_ids = sorted(sid for sid in request.scheduler_job_ids if sid not in owned_ids)
        owned_requested = [sid for sid in request.scheduler_job_ids if sid in owned_ids]
        try:
            scheduler = scheduler_providers.provider_for_scheduler(request.provider)
            statuses = [
                scheduler.poll(scheduler_job_id).model_dump(mode="json")
                for scheduler_job_id in owned_requested
            ]
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "configuration_error", exc=door_errors.public_message_error(exc)
            ) from exc
        return _public_payload(
            {
                "schema_version": SCHEDULER_STATUS_BATCH_SCHEMA,
                "scheduler": request.provider,
                "statuses": statuses,
                "refused_scheduler_job_ids": refused_ids,
            }
        )

    @app.post("/scheduler/jobs/{scheduler_job_id}/cancel", dependencies=[auth_dependency])
    def scheduler_cancel(
        scheduler_job_id: str, request: SchedulerCancelRequest
    ) -> dict[str, object]:
        _require_owned_scheduler_job(ctx, scheduler_job_id)
        try:
            result = scheduler_providers.provider_for_scheduler(request.provider).cancel(
                scheduler_job_id
            )
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "configuration_error", exc=door_errors.public_message_error(exc)
            ) from exc
        return _public_payload(
            {
                "scheduler": request.provider,
                "scheduler_job_id": scheduler_job_id,
                "cancel_requested": True,
                "accepted": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": _bounded_output(result.stdout.strip()),
                "stderr": _bounded_output(result.stderr.strip()),
            }
        )


__all__ = [
    "MAX_SCHEDULER_STATUS_BATCH_REQUEST",
    "SCHEDULER_STATUS_BATCH_SCHEMA",
    "register_scheduler_routes",
]
