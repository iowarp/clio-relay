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

Scheduler job ids are not relay-job-scoped data -- ``session teardown``
legitimately polls status for scheduler sentinels it does NOT own, purely
to prove they are unrelated and still active before proceeding (see
``cli_owned_scheduler_cancel._preflight_scheduler_sentinels``). These
routes are therefore reachable from either an owned or a global
authenticated channel, like ``GET /jobs/{job_id}/status`` and unlike the
explicitly global-only routes in ``http_api_routes_queue.py`` -- an
unrecognized ``provider`` (or an invalid ``scheduler_job_id``) raises
``ConfigurationError``, caught here and rendered as the existing
``configuration_error`` reason, the same explicit catch-and-render every
other route in this codebase uses (the global ``Exception`` handler is a
last-resort safety net for a truly novel failure, not the intended path
for an already-typed exception -- see ``door_errors.py``'s own dispatch-
order docstring).
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from fastapi import FastAPI
from fastapi.params import Depends

from clio_relay import door_errors, scheduler_providers
from clio_relay.errors import ConfigurationError
from clio_relay.http_api_context import RelayApiContext
from clio_relay.http_api_models import SchedulerCancelRequest, SchedulerStatusBatchRequest
from clio_relay.models_scheduling import SchedulerStatus

SCHEDULER_STATUS_BATCH_SCHEMA = "clio-relay.scheduler-status-batch.v1"
MAX_SCHEDULER_STATUS_BATCH_REQUEST = 256


def register_scheduler_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: Depends,
) -> None:
    """Register the scheduler status/status-batch/cancel routes.

    ``ctx`` is accepted (unused) only to match every other
    ``register_*_routes(app, ctx, *, auth_dependency)`` call shape
    ``http_api.py`` invokes uniformly -- these three routes carry no
    owner-session-scoped state of their own (see module docstring).
    """

    @app.get(
        "/scheduler/jobs/{scheduler_job_id}/status",
        response_model=SchedulerStatus,
        dependencies=[auth_dependency],
    )
    def scheduler_status(scheduler_job_id: str, provider: str) -> SchedulerStatus:
        try:
            return scheduler_providers.provider_for_scheduler(provider).poll(scheduler_job_id)
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
        try:
            scheduler = scheduler_providers.provider_for_scheduler(request.provider)
            statuses = [
                scheduler.poll(scheduler_job_id).model_dump(mode="json")
                for scheduler_job_id in request.scheduler_job_ids
            ]
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "configuration_error", exc=door_errors.public_message_error(exc)
            ) from exc
        return {
            "schema_version": SCHEDULER_STATUS_BATCH_SCHEMA,
            "scheduler": request.provider,
            "statuses": statuses,
        }

    @app.post("/scheduler/jobs/{scheduler_job_id}/cancel", dependencies=[auth_dependency])
    def scheduler_cancel(
        scheduler_job_id: str, request: SchedulerCancelRequest
    ) -> dict[str, object]:
        try:
            result = scheduler_providers.provider_for_scheduler(request.provider).cancel(
                scheduler_job_id
            )
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "configuration_error", exc=door_errors.public_message_error(exc)
            ) from exc
        return {
            "scheduler": request.provider,
            "scheduler_job_id": scheduler_job_id,
            "cancel_requested": True,
            "accepted": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }


__all__ = [
    "MAX_SCHEDULER_STATUS_BATCH_REQUEST",
    "SCHEDULER_STATUS_BATCH_SCHEMA",
    "register_scheduler_routes",
]
