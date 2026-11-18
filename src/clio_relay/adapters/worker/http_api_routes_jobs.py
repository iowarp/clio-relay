"""Job submission and job/transform/status read routes.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``create_app()`` in ``http_api.py``. Every reference to a
``create_app()``-local closure (``resolved``, ``queue``,
``owner_session_cluster_definition``, ``submit_owned``, ``require_owned_job``)
is rewritten to the equivalent ``ctx.<name>`` attribute/method on the shared
``RelayApiContext`` (see ``http_api_context.py``'s own docstring) -- the same
mechanical bare-name -> qualified-name rewrite this codebase's other AST-
driven extractions already use; no other line changes.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from fastapi import FastAPI
from fastapi.params import Depends

from clio_relay.adapters.worker import door_errors
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.adapters.worker.http_api_context import RelayApiContext
from clio_relay.adapters.worker.http_api_models import (
    JarvisPipelineSubmitRequest,
    JarvisSubmitRequest,
    RemoteAgentSubmitRequest,
)
from clio_relay.adapters.worker.http_api_redaction import _public_payload, _public_record
from clio_relay.identifiers import DurableRecordId
from clio_relay.job_identity import JobOwnerSessionIdentity
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    RelayJob,
    RemoteAgentTaskSpec,
    TransformRef,
)
from clio_relay.relay_ops import job_status as get_job_status_operation


def register_job_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: Depends,
    job_identity_parameter: JobOwnerSessionIdentity | None,
) -> None:
    """Register the job-submission and job/transform/status read routes."""

    @app.post(
        "/jobs",
        response_model=RelayJob,
        dependencies=[auth_dependency],
    )
    def submit_job(
        job: RelayJob,
        owner_session_identity: JobOwnerSessionIdentity | None = job_identity_parameter,
    ) -> RelayJob:
        if job.kind is JobKind.INPUT_INGEST:
            raise door_errors.http_problem(
                "job_route_refused",
                "this job kind must use its dedicated authenticated internal route",
            )
        return ctx.submit_owned(job, owner_session_identity=owner_session_identity)

    @app.post(
        "/jobs/jarvis",
        response_model=RelayJob,
        dependencies=[auth_dependency],
    )
    def submit_jarvis(
        request: JarvisSubmitRequest,
        owner_session_identity: JobOwnerSessionIdentity | None = job_identity_parameter,
    ) -> RelayJob:
        return ctx.submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(pipeline_yaml=request.pipeline_yaml),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
            ),
            owner_session_identity=owner_session_identity,
        )

    @app.post(
        "/jobs/jarvis-pipeline",
        response_model=RelayJob,
        dependencies=[auth_dependency],
    )
    def submit_jarvis_pipeline(
        request: JarvisPipelineSubmitRequest,
        owner_session_identity: JobOwnerSessionIdentity | None = job_identity_parameter,
    ) -> RelayJob:
        return ctx.submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(pipeline_name=request.pipeline_name),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
            ),
            owner_session_identity=owner_session_identity,
        )

    @app.post(
        "/jobs/remote-agent",
        response_model=RelayJob,
        dependencies=[auth_dependency],
    )
    def submit_remote_agent(
        request: RemoteAgentSubmitRequest,
        owner_session_identity: JobOwnerSessionIdentity | None = job_identity_parameter,
    ) -> RelayJob:
        return ctx.submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.REMOTE_AGENT,
                spec=RemoteAgentTaskSpec(
                    prompt_path=request.prompt_path,
                    mcp_config_path=request.mcp_config_path,
                    model=request.model,
                    workdir=request.workdir,
                    timeout_seconds=request.timeout_seconds,
                ),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
            ),
            owner_session_identity=owner_session_identity,
        )

    @app.get("/jobs/{job_id}", response_model=RelayJob, dependencies=[auth_dependency])
    def get_job(job_id: DurableRecordId) -> RelayJob:
        try:
            return _public_record(ctx.require_owned_job(job_id))
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc

    @app.post(
        "/jobs/{job_id}/transform",
        response_model=TransformRef,
        dependencies=[auth_dependency],
    )
    def record_job_transform(job_id: DurableRecordId, transform: TransformRef) -> TransformRef:
        """Record one immutable, execution-owned transform for an exact owned job."""
        try:
            ctx.require_owned_job(job_id)
            if transform.job_id != job_id:
                raise door_errors.http_problem(
                    "transform_refused",
                    message="transform job_id does not match path",
                )
            return _public_record(ctx.queue.record_transform_ref(transform))
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        except QueueConflictError as exc:
            raise door_errors.http_problem(
                "transform_conflict", exc=door_errors.public_message_error(exc)
            ) from exc

    @app.get(
        "/jobs/{job_id}/transform",
        response_model=TransformRef | None,
        dependencies=[auth_dependency],
    )
    def get_job_transform(job_id: DurableRecordId) -> TransformRef | None:
        """Return the nullable immutable transform for one exact owned job."""
        try:
            ctx.require_owned_job(job_id)
            transform = ctx.queue.get_transform_ref(job_id)
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        return None if transform is None else _public_record(transform)

    @app.get("/jobs/{job_id}/status", dependencies=[auth_dependency])
    def get_job_status(job_id: DurableRecordId) -> dict[str, object]:
        try:
            ctx.require_owned_job(job_id)
            return _public_payload(get_job_status_operation(ctx.queue, job_id))
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
