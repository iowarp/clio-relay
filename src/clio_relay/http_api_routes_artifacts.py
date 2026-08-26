"""Log/artifact/progress read routes plus progress-write and content-read.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``create_app()`` in ``http_api.py``. Every reference to a
``create_app()``-local closure (``resolved``, ``queue``, ``require_owned_job``,
``require_owned_artifact``) is rewritten to the equivalent ``ctx.<name>``
attribute/method on the shared ``RelayApiContext`` (see
``http_api_context.py``'s own docstring) -- the same mechanical bare-name ->
qualified-name rewrite this codebase's other AST-driven extractions already
use; no other line changes. The two gateway-session routes that originally
sat between ``get_progress`` and ``record_progress`` move to
``http_api_routes_gateway.py`` instead -- their paths (``/gateway-sessions*``)
never overlap this module's (``/jobs/*``, ``/artifacts/*``), so calling
``register_gateway_routes`` before or after this one is observationally
identical for FastAPI/Starlette route matching.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, StreamingResponse

from clio_relay import door_error_adapters, door_errors
from clio_relay.bounded_payload import describe_delivery_refusal, is_delivery_refusal
from clio_relay.errors import NotFoundError, RelayError
from clio_relay.http_api_context import RelayApiContext
from clio_relay.http_api_models import ProgressUpdateRequest
from clio_relay.http_api_redaction import _public_model_page, _public_payload, _public_record
from clio_relay.http_api_streaming import LOG_SSE_FOLLOW_POLL_SECONDS, _log_tail_sse_events
from clio_relay.identifiers import DurableRecordId
from clio_relay.models import ProgressRecord
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.relay_ops import read_artifact_bytes, read_job_log
from clio_relay.spool import LOG_STREAM_NAMES


def register_artifact_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: object,
) -> None:
    """Register the log/artifact/progress read, progress-write, and content routes."""

    @app.get("/jobs/{job_id}/logs/{stream_name}", dependencies=[auth_dependency])
    def get_log(
        job_id: DurableRecordId,
        stream_name: str,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=1_048_576)] = 65_536,
    ) -> dict[str, object]:
        try:
            if stream_name not in LOG_STREAM_NAMES:
                raise door_errors.http_problem(
                    "log_stream_invalid",
                    message=f"stream must be one of: {', '.join(LOG_STREAM_NAMES)}",
                )
            return _public_payload(
                read_job_log(
                    ctx.resolved,
                    ctx.require_owned_job(job_id),
                    # pyright narrows str -> LogStreamName from the `not in
                    # LOG_STREAM_NAMES` guard above (a tuple of literals) --
                    # no cast needed.
                    stream_name=stream_name,
                    offset=offset,
                    limit=limit,
                )
            )
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc

    @app.get("/jobs/{job_id}/logs/{stream_name}/sse", dependencies=[auth_dependency])
    def get_log_sse(
        job_id: DurableRecordId,
        stream_name: str,
        offset: Annotated[int, Query(ge=0)] = 0,
        poll_seconds: float = LOG_SSE_FOLLOW_POLL_SECONDS,
    ) -> StreamingResponse:
        """Stream a job log as Server-Sent Events from a byte offset.

        clio-relay#221/#259 live-console lane: the push-side sibling of
        ``GET /jobs/{job_id}/logs/{stream_name}`` immediately above -- same
        auth (``dependencies=[auth_dependency]``), same owned-job admission
        (``ctx.require_owned_job``), and the exact same typed refusal
        vocabulary (``log_stream_invalid`` for an unknown stream,
        ``job_not_found`` for a job this session cannot see or that does not
        exist) -- so a client that already handles the byte-range route's
        errors needs nothing new to handle this one's. Rides the caller's
        already-open connection to the door; it opens no ssh dial or
        transport of its own.
        """
        if poll_seconds <= 0:
            raise door_errors.http_problem("poll_interval_invalid", "poll_seconds must be positive")
        if stream_name not in LOG_STREAM_NAMES:
            raise door_errors.http_problem(
                "log_stream_invalid",
                message=f"stream must be one of: {', '.join(LOG_STREAM_NAMES)}",
            )
        try:
            ctx.require_owned_job(job_id)
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        return StreamingResponse(
            _log_tail_sse_events(
                ctx.resolved,
                ctx.queue,
                job_id,
                # pyright narrows str -> LogStreamName from the `not in
                # LOG_STREAM_NAMES` guard above, matching `get_log`'s own
                # narrowing comment -- no cast needed.
                stream_name=stream_name,
                offset=offset,
                poll_seconds=poll_seconds,
            ),
            media_type="text/event-stream",
        )

    @app.get(
        "/jobs/{job_id}/artifacts",
        dependencies=[auth_dependency],
    )
    def get_artifacts(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        ctx.require_owned_job(job_id)
        artifacts, next_cursor, total = ctx.queue.list_artifacts_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        return _public_payload(
            _public_model_page(
                "artifacts",
                artifacts,
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            )
        )

    @app.get(
        "/jobs/{job_id}/used-artifacts",
        dependencies=[auth_dependency],
    )
    def get_used_artifacts(
        job_id: DurableRecordId,
        cursor: DurableRecordId | None = None,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        """Return one page of immutable artifact dependencies for a job."""
        ctx.require_owned_job(job_id)
        records, next_cursor, total = ctx.queue.list_used_artifacts_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        for record in records:
            ctx.require_owned_artifact(record.artifact_id)
        return _public_payload(
            {
                "used_artifacts": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            }
        )

    @app.get(
        "/artifacts/{artifact_id}/used-by",
        dependencies=[auth_dependency],
    )
    def get_artifact_users(
        artifact_id: DurableRecordId,
        cursor: DurableRecordId | None = None,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        """Return one page of jobs that consumed a content-pinned artifact."""
        ctx.require_owned_artifact(artifact_id)
        records, next_cursor, total = ctx.queue.list_artifact_users_page(
            artifact_id,
            cursor=cursor,
            limit=limit,
        )
        for record in records:
            ctx.require_owned_job(record.consumer_job_id)
        return _public_payload(
            {
                "used_by": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            }
        )

    @app.get(
        "/jobs/{job_id}/progress",
        dependencies=[auth_dependency],
    )
    def get_progress(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        ctx.require_owned_job(job_id)
        progress, next_cursor, total = ctx.queue.list_progress_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        return _public_payload(
            _public_model_page(
                "progress",
                progress,
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            )
        )

    @app.post(
        "/jobs/{job_id}/progress",
        response_model=ProgressRecord,
        dependencies=[auth_dependency],
    )
    def record_progress(
        job_id: DurableRecordId,
        request: ProgressUpdateRequest,
    ) -> ProgressRecord:
        try:
            ctx.require_owned_job(job_id)
            metadata = external_progress_metadata("external_http", dict(request.metadata))
            return _public_record(
                ctx.queue.append_progress(
                    ProgressRecord(
                        job_id=job_id,
                        label=request.label,
                        current=request.current,
                        total=request.total,
                        unit=request.unit,
                        message=request.message,
                        source_event_seq=request.source_event_seq,
                        metadata=metadata,
                    )
                )
            )
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc

    @app.get(
        "/artifacts/{artifact_id}/content",
        dependencies=[auth_dependency],
        response_model=None,
    )
    def get_artifact_content(artifact_id: DurableRecordId) -> dict[str, object] | JSONResponse:
        try:
            ctx.require_owned_artifact(artifact_id)
            document = read_artifact_bytes(ctx.queue, artifact_id)
        except NotFoundError as exc:
            raise door_errors.http_problem("artifact_not_found", exc=exc) from exc
        if is_delivery_refusal(document):
            # An over-budget read is a typed refusal, never a 200-shaped failure.
            message = describe_delivery_refusal(document)
            fault = door_errors.classify(
                RelayError(message),
                reason="payload_too_large",
                data=document,
            )
            problem = door_error_adapters.as_http_problem(fault)
            return JSONResponse(
                problem, status_code=fault.http_status, media_type="application/problem+json"
            )
        return _public_payload(document)
