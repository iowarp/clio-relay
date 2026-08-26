"""Cancel/queue-listing/retention/stale-scan/worker/monitor-rule routes.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``create_app()`` in ``http_api.py``. Every reference to a
``create_app()``-local closure (``resolved``, ``queue``, ``require_owned_job``,
``owns_job``) is rewritten to the equivalent ``ctx.<name>`` attribute/method
on the shared ``RelayApiContext`` (see ``http_api_context.py``'s own
docstring) -- the same mechanical bare-name -> qualified-name rewrite this
codebase's other AST-driven extractions already use; no other line changes.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import FastAPI, Query
from fastapi.params import Depends

from clio_relay import door_errors
from clio_relay.errors import ConfigurationError, NotFoundError, QueueConflictError
from clio_relay.http_api_context import RelayApiContext
from clio_relay.http_api_models import QueueCancelRequest, RetentionCollectRequest
from clio_relay.http_api_queue_paging import _list_owned_session_queue
from clio_relay.http_api_redaction import _public_payload, _public_record
from clio_relay.identifiers import DurableRecordId
from clio_relay.models import JobKind, JobState, MonitorRule, RelayJob
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.queue_management import (
    DEFAULT_STALE_SCAN_LIMIT,
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    diagnose_queue,
    discover_stale_jobs,
    list_queue_jobs,
    worker_status,
)
from clio_relay.relay_ops import cancel_job as request_cancel_job
from clio_relay.relay_ops import evaluate_monitor_rules
from clio_relay.retention import TerminalRetentionCoordinator


def register_queue_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: Depends,
) -> None:
    """Register the cancel/queue/retention/stale/worker/monitor-rule routes."""

    @app.post("/jobs/{job_id}/cancel", response_model=RelayJob, dependencies=[auth_dependency])
    def cancel_job(
        job_id: DurableRecordId,
        request: QueueCancelRequest | None = None,
    ) -> RelayJob:
        job = ctx.require_owned_job(job_id)
        if request is not None and request.cluster is not None and request.cluster != job.cluster:
            raise door_errors.http_problem(
                "job_cluster_mismatch",
                (
                    f"job {job_id} belongs to cluster {job.cluster}, "
                    f"not requested cluster {request.cluster}"
                ),
            )
        cancel_scheduler = False if request is None else request.cancel_scheduler_job
        return _public_record(
            request_cancel_job(ctx.queue, job_id, cancel_scheduler=cancel_scheduler)
        )

    @app.post("/queue/jobs/{job_id}/cancel", dependencies=[auth_dependency])
    def cancel_queue_job_route(
        job_id: DurableRecordId,
        request: QueueCancelRequest | None = None,
    ) -> dict[str, object]:
        cancel_scheduler = False if request is None else request.cancel_scheduler_job
        try:
            ctx.require_owned_job(job_id)
            return _public_payload(
                cancel_queue_job(
                    ctx.queue,
                    job_id,
                    cluster=None if request is None else request.cluster,
                    scheduler_policy="request-scheduler" if cancel_scheduler else "relay-only",
                )
            )
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "queue_operation_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc

    @app.get("/queue", dependencies=[auth_dependency])
    def list_queue(
        cluster: str | None = None,
        state: str | None = None,
        kind: JobKind | None = None,
        include_terminal: bool = False,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
    ) -> dict[str, object]:
        job_state = None
        if state is not None:
            try:
                job_state = JobState(state)
            except ValueError as exc:
                raise door_errors.http_problem(
                    "queue_query_refused",
                    exc=exc,
                    message=f"unknown job state: {state}",
                ) from exc
        if scan_limit < limit:
            raise door_errors.http_problem(
                "queue_query_refused", "scan_limit must be greater than or equal to limit"
            )
        if ctx.resolved.owner_session_id is not None:
            generation_id = ctx.resolved.owner_session_generation_id
            if generation_id is None:
                raise door_errors.http_problem(
                    "session_generation_identity_unavailable",
                    "owned session generation is missing",
                )
            try:
                return _public_payload(
                    _list_owned_session_queue(
                        ctx.queue,
                        owner_session_id=ctx.resolved.owner_session_id,
                        session_generation_id=generation_id,
                        cluster=cluster,
                        state=job_state,
                        kind=kind,
                        include_terminal=include_terminal,
                        cursor=cursor,
                        limit=limit,
                        scan_limit=scan_limit,
                    )
                )
            except QueueConflictError as exc:
                raise door_errors.http_problem(
                    "queue_operation_conflict", exc=door_errors.public_message_error(exc)
                ) from exc
            except ValueError as exc:
                raise door_errors.http_problem(
                    "queue_query_refused", exc=door_errors.public_message_error(exc)
                ) from exc
        try:
            payload = list_queue_jobs(
                ctx.queue,
                cluster=cluster,
                state=job_state,
                kind=kind,
                include_terminal=include_terminal,
                cursor=cursor,
                limit=limit,
                scan_limit=scan_limit,
            )
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "queue_operation_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        except ValueError as exc:
            raise door_errors.http_problem(
                "queue_query_refused", exc=door_errors.public_message_error(exc)
            ) from exc
        return _public_payload(payload)

    @app.get("/queue/jobs/{job_id}/diagnose", dependencies=[auth_dependency])
    def diagnose_queue_job_route(
        job_id: DurableRecordId,
        cluster: str | None = None,
        older_than_seconds: Annotated[int, Query(ge=1)] = 7_200,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
    ) -> dict[str, object]:
        try:
            ctx.require_owned_job(job_id)
            return _public_payload(
                diagnose_job(
                    ctx.queue,
                    job_id,
                    cluster=cluster,
                    stale_after_seconds=older_than_seconds,
                    scan_limit=scan_limit,
                )
            )
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "queue_operation_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc

    @app.get("/retention/jobs/{job_id}/plan", dependencies=[auth_dependency])
    def retention_plan(
        job_id: DurableRecordId,
        expected_updated_at: datetime | None = None,
    ) -> dict[str, object]:
        """Build a read-only terminal-retention plan."""
        if ctx.resolved.owner_session_id is not None:
            raise door_errors.http_problem(
                "session_scope_refused",
                "session-scoped APIs cannot inspect global retention state",
            )
        try:
            plan = TerminalRetentionCoordinator(ctx.queue, ctx.resolved.spool_dir).plan(
                job_id,
                expected_updated_at=expected_updated_at,
            )
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        except QueueConflictError as exc:
            raise door_errors.http_problem(
                "retention_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        return _public_payload(
            {
                "plan": plan.model_dump(mode="json"),
                "scheduler_cancel_requested": False,
            }
        )

    @app.get("/retention/jobs/{job_id}/status", dependencies=[auth_dependency])
    def retention_status(job_id: DurableRecordId) -> dict[str, object]:
        """Read the current crash-resumable retention phase without mutation."""
        if ctx.resolved.owner_session_id is not None:
            raise door_errors.http_problem(
                "session_scope_refused",
                "session-scoped APIs cannot inspect global retention state",
            )
        try:
            plan = TerminalRetentionCoordinator(ctx.queue, ctx.resolved.spool_dir).plan(job_id)
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        except QueueConflictError as exc:
            raise door_errors.http_problem(
                "retention_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        return {
            "job_id": job_id,
            "receipt_id": plan.receipt_id,
            "phase": None if plan.receipt_phase is None else plan.receipt_phase.value,
            "complete": plan.receipt_phase is not None and plan.receipt_phase.value == "complete",
            "eligible": plan.eligible,
            "protections": plan.protections,
            "scheduler_cancel_requested": False,
        }

    @app.post("/retention/jobs/{job_id}/collect", dependencies=[auth_dependency])
    def retention_collect(
        job_id: DurableRecordId,
        request: RetentionCollectRequest | None = None,
    ) -> dict[str, object]:
        """Dry-run by default or advance bounded retention without scheduler cancellation."""
        if ctx.resolved.owner_session_id is not None:
            raise door_errors.http_problem(
                "session_scope_refused", "session-scoped APIs cannot mutate global retention state"
            )
        options = request or RetentionCollectRequest()
        try:
            result = TerminalRetentionCoordinator(ctx.queue, ctx.resolved.spool_dir).collect(
                job_id,
                execute=options.execute,
                batch_size=options.batch_size,
                expected_updated_at=options.expected_updated_at,
            )
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        except QueueConflictError as exc:
            raise door_errors.http_problem(
                "retention_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        return _public_payload(result.model_dump(mode="json"))

    @app.get("/queue/stale", dependencies=[auth_dependency])
    def discover_stale_queue_route(
        cluster: str,
        older_than_seconds: Annotated[int, Query(ge=1)] = 7_200,
        job_id: DurableRecordId | None = None,
        kind: JobKind | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = DEFAULT_STALE_SCAN_LIMIT,
    ) -> dict[str, object]:
        if ctx.resolved.owner_session_id is not None:
            raise door_errors.http_problem(
                "session_scope_refused",
                "session-scoped APIs cannot inspect global stale-job state",
            )
        try:
            return _public_payload(
                discover_stale_jobs(
                    ctx.queue,
                    cluster=cluster,
                    older_than_seconds=older_than_seconds,
                    job_id=job_id,
                    kind=kind,
                    limit=limit,
                    scan_limit=scan_limit,
                )
            )
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "queue_operation_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        except ValueError as exc:
            raise door_errors.http_problem(
                "queue_query_refused", exc=door_errors.public_message_error(exc)
            ) from exc

    @app.get("/queue/diagnostics", dependencies=[auth_dependency])
    def diagnose_queue_route(cluster: str | None = None) -> dict[str, object]:
        if ctx.resolved.owner_session_id is not None:
            raise door_errors.http_problem(
                "session_scope_refused",
                "session-scoped APIs cannot inspect global queue diagnostics",
            )
        return _public_payload(diagnose_queue(ctx.queue, cluster=cluster))

    @app.post("/queue/cleanup-stale", dependencies=[auth_dependency])
    def cleanup_stale_queue_route(
        cluster: str,
        older_than_seconds: Annotated[int, Query(ge=1)] = 7_200,
        job_id: DurableRecordId | None = None,
        kind: JobKind | None = None,
        max_attempts: Annotated[int, Query(ge=1)] = 3,
        dry_run: bool = True,
        cancel_queued: bool = False,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = DEFAULT_STALE_SCAN_LIMIT,
    ) -> dict[str, object]:
        if ctx.resolved.owner_session_id is not None:
            raise door_errors.http_problem(
                "session_scope_refused",
                "session-scoped APIs cannot mutate global stale-job state",
            )
        try:
            return _public_payload(
                cleanup_stale_jobs(
                    ctx.queue,
                    cluster=cluster,
                    older_than_seconds=older_than_seconds,
                    job_id=job_id,
                    kind=kind,
                    max_attempts=max_attempts,
                    dry_run=dry_run,
                    cancel_queued=cancel_queued,
                    limit=limit,
                    scan_limit=scan_limit,
                )
            )
        except ConfigurationError as exc:
            raise door_errors.http_problem(
                "queue_operation_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        except ValueError as exc:
            raise door_errors.http_problem(
                "queue_query_refused", exc=door_errors.public_message_error(exc)
            ) from exc

    @app.get("/workers", dependencies=[auth_dependency])
    def worker_status_route(cluster: str | None = None) -> dict[str, object]:
        if ctx.resolved.owner_session_id is not None:
            raise door_errors.http_problem(
                "session_scope_refused", "session-scoped APIs cannot inspect global worker state"
            )
        return _public_payload(worker_status(ctx.queue, cluster=cluster))

    @app.post("/monitor/rules", response_model=MonitorRule, dependencies=[auth_dependency])
    def create_monitor_rule(rule: MonitorRule) -> MonitorRule:
        try:
            ctx.ensure_intake_open()
            ctx.require_owned_job(rule.job_id)
            return _public_record(ctx.queue.append_monitor_rule(rule))
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc

    @app.get("/monitor/rules", dependencies=[auth_dependency])
    def list_monitor_rules(
        job_id: DurableRecordId | None = None,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        if job_id is not None:
            ctx.require_owned_job(job_id)
        rules, next_cursor, total = ctx.queue.list_monitor_rules_page(
            cursor=cursor,
            limit=limit,
            job_id=job_id,
        )
        if ctx.resolved.owner_session_id is not None:
            rules = [rule for rule in rules if ctx.owns_job(ctx.queue.get_job(rule.job_id))]
        return _public_payload(
            {
                "rules": [rule.model_dump(mode="json") for rule in rules],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_monitor_rule_sequence_high_water",
                "filters_apply_within_source_window": True,
                "visibility_filter": (
                    "owner_session_within_source_window"
                    if ctx.resolved.owner_session_id is not None
                    else None
                ),
            }
        )

    @app.post("/monitor/run-once", dependencies=[auth_dependency])
    def run_monitor_once(
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> list[dict[str, object]]:
        if ctx.resolved.owner_session_id is not None:
            raise door_errors.http_problem(
                "session_scope_refused", "session-scoped APIs cannot evaluate global monitor rules"
            )
        return [_public_payload(item) for item in evaluate_monitor_rules(ctx.queue, limit=limit)]
