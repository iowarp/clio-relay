"""Health/session-identity/storage-status/JARVIS-authority/ingest routes.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``create_app()`` in ``http_api.py``. Every reference to a
``create_app()``-local closure (``resolved``, ``queue``,
``owner_session_cluster``, ``owner_session_cluster_definition``,
``require_owned_job``, ``require_owned_artifact``, ``submit_owned``) is
rewritten to the equivalent ``ctx.<name>`` attribute/method on the shared
``RelayApiContext`` (see ``http_api_context.py``'s own docstring) -- the same
mechanical bare-name -> qualified-name rewrite this codebase's other AST-
driven extractions already use; no other line changes.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, Query
from fastapi.params import Depends

from clio_relay import door_errors
from clio_relay.errors import ConfigurationError, NotFoundError, QueueConflictError, RelayError
from clio_relay.http_api_context import RelayApiContext
from clio_relay.http_api_models import (
    InputArtifactIngestRequest,
    JarvisRuntimeAuthorityRequest,
    _decode_input_artifact_payload,
)
from clio_relay.http_api_redaction import _public_payload, _public_record
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.jarvis_service_runtime import (
    OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
    private_jarvis_service_runtime_authority_document,
    resolve_local_verified_jarvis_service_runtime_authority,
    reverify_jarvis_service_runtime,
)
from clio_relay.models import (
    InputArtifactIngestPolicy,
    InputArtifactSpec,
    JobKind,
    JobState,
    RelayJob,
    deterministic_input_artifact_id,
    new_id,
)
from clio_relay.session_api import session_identity_document
from clio_relay.spool import JobSpool
from clio_relay.storage_runtime import StorageAdmissionError

OWNED_SESSION_STATUS_SCHEMA = "clio-relay.owned-session-status.v1"


def register_session_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: Depends,
    session_submission_dependency: Depends,
) -> None:
    """Register the healthz/session-identity/storage/JARVIS-authority/ingest routes."""

    @app.get("/healthz")
    def healthz() -> dict[str, object]:
        # clio-relay#221/#259: negotiation, never timing (house rule) -- a
        # client decides whether to open the SSE log-tail route or fall back
        # to polling the byte-range route by reading this flag, not by
        # probing/timing the SSE route itself. Always True: it names a
        # code-level capability this build ships, not an environment-
        # dependent condition, so it never varies by request.
        return {
            "ok": True,
            "auth": ctx.resolved.api_token is not None,
            "console_sse": True,
        }

    @app.get("/session-identity")
    def session_identity(nonce: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")]) -> dict[str, str]:
        """Prove the exact owned session identity without accepting credentials."""
        if (
            ctx.resolved.owner_session_id is None
            or ctx.resolved.owner_session_generation_id is None
            or ctx.owner_session_cluster is None
            or ctx.resolved.session_owner_token is None
        ):
            raise door_errors.http_problem(
                "session_identity_unavailable", "owned session identity is unavailable"
            )
        return session_identity_document(
            owner_token=ctx.resolved.session_owner_token,
            cluster=ctx.owner_session_cluster,
            session_id=ctx.resolved.owner_session_id,
            generation_id=ctx.resolved.owner_session_generation_id,
            nonce=nonce,
        )

    @app.get("/session-status", dependencies=[auth_dependency])
    def session_status() -> dict[str, object]:
        """Report this live owned session over the connection's held channel.

        This is the HTTP replacement for the per-operation ``ssh ... bash -s``
        status probe.  It is the running API process describing itself, so its
        ``evidence`` is named exactly that: it proves liveness and exact
        identity, not the cluster-local filesystem and process ownership audit.
        That stronger audit stays where it can actually be produced -- the
        cluster-local recovery-status executor, carried out of band once by the
        transport that establishes the channel.
        """
        if (
            ctx.resolved.owner_session_id is None
            or ctx.resolved.owner_session_generation_id is None
            or ctx.owner_session_cluster is None
        ):
            raise door_errors.http_problem(
                "session_status_unavailable", "owned session status is unavailable"
            )
        return {
            "schema_version": OWNED_SESSION_STATUS_SCHEMA,
            "owner": "clio-relay",
            "cluster": ctx.owner_session_cluster,
            "session_id": ctx.resolved.owner_session_id,
            "session_generation_id": ctx.resolved.owner_session_generation_id,
            "remote_api_port": ctx.resolved.owner_session_api_port,
            "running": True,
            "evidence": "live_api_self_report",
        }

    @app.get("/storage/status", dependencies=[auth_dependency])
    def storage_status() -> dict[str, object]:
        """Return the machine-readable queue admission and storage decision."""
        return _public_payload(ctx.queue.storage_runtime.status())

    @app.post(
        OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
        dependencies=[auth_dependency],
        include_in_schema=False,
    )
    def resolve_owned_jarvis_runtime_authority(
        request: JarvisRuntimeAuthorityRequest,
    ) -> dict[str, object]:
        """Resolve one private capability on its exact receipt-owning cluster host."""
        if (
            ctx.resolved.owner_session_id is None
            or ctx.owner_session_cluster_definition is None
            or not ctx.resolved.api_token
        ):
            raise door_errors.http_problem(
                "jarvis_runtime_authority_unavailable",
                "owned JARVIS runtime authority resolver is unavailable",
            )
        binding = request.binding
        try:
            ctx.require_owned_job(validate_durable_record_id(binding.source_relay_job_id))
            ctx.require_owned_artifact(validate_durable_record_id(binding.source_relay_artifact_id))
            verified = reverify_jarvis_service_runtime(
                queue=ctx.queue,
                definition=ctx.owner_session_cluster_definition,
                settings=None,
                binding_document=binding.model_dump(mode="json"),
            )
            authority = resolve_local_verified_jarvis_service_runtime_authority(
                jarvis_bin=ctx.resolved.jarvis_bin,
                verified=verified,
            )
            if authority is None:
                raise ConfigurationError("legacy JARVIS service runtimes have no private authority")
            # This one response is intentionally not passed through the public
            # payload redactor. It travels only on the authenticated,
            # identity-bound owned-session connection and is never persisted.
            return private_jarvis_service_runtime_authority_document(authority)
        except NotFoundError as exc:
            raise door_errors.http_problem("jarvis_runtime_authority_unavailable", exc=exc) from exc
        except (ConfigurationError, RelayError, ValueError) as exc:
            raise door_errors.http_problem(
                "jarvis_runtime_authority_conflict",
                exc=door_errors.public_message_error(exc),
            ) from exc

    @app.post(
        "/input-artifacts/ingest",
        dependencies=[auth_dependency, session_submission_dependency],
        include_in_schema=False,
    )
    def ingest_input_artifact(
        request: InputArtifactIngestRequest,
    ) -> dict[str, object]:
        """Persist one authenticated owner-session input without exposing upload tooling."""
        if (
            ctx.resolved.owner_session_id is None
            or ctx.resolved.owner_session_generation_id is None
            or ctx.owner_session_cluster is None
        ):
            raise door_errors.http_problem(
                "input_ingest_unavailable", "owned-session input artifact ingest is unavailable"
            )
        try:
            payload = _decode_input_artifact_payload(
                request,
                max_bytes=ctx.resolved.input_file_max_bytes,
            )
            spec = InputArtifactSpec(
                logical_name=request.logical_name,
                size_bytes=request.size_bytes,
                sha256=request.sha256,
            )
        except ValueError as exc:
            raise door_errors.http_problem(
                "input_ingest_refused", exc=door_errors.public_message_error(exc)
            ) from exc

        input_ingest_policy = InputArtifactIngestPolicy(
            max_file_count=ctx.resolved.input_file_max_count,
            max_total_bytes=ctx.resolved.input_total_max_bytes,
        )
        job = ctx.submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.INPUT_INGEST,
                spec=spec,
                idempotency_key=request.idempotency_key,
            ),
            input_ingest_policy=input_ingest_policy,
        )
        attempt_id = new_id("ingest_attempt")
        claimed = False
        try:
            ctx.queue.recover_abandoned_input_ingests(cluster=request.cluster)
            current, claimed = ctx.queue.begin_input_ingest(
                job.job_id,
                attempt_id=attempt_id,
                policy=input_ingest_policy,
            )
            if current.state is JobState.SUCCEEDED:
                artifact = ctx.queue.get_artifact(deterministic_input_artifact_id(current.job_id))
                return {
                    "job": _public_record(current).model_dump(mode="json"),
                    "artifact": _public_record(artifact).model_dump(mode="json"),
                }
            spool = JobSpool(
                ctx.resolved.spool_dir,
                current,
                max_log_bytes_per_stream=ctx.resolved.spool_max_log_bytes_per_stream,
                max_log_bytes_per_job=ctx.resolved.spool_max_log_bytes_per_job,
            )
            path = spool.write_input_artifact(
                spec.logical_name,
                payload,
                size_bytes=spec.size_bytes,
                sha256=spec.sha256,
            )
            candidate = spool.artifact_for(path, kind="input")
            candidate = candidate.model_copy(
                update={
                    "artifact_id": deterministic_input_artifact_id(current.job_id),
                    "created_at": current.created_at,
                    "metadata": {
                        **candidate.metadata,
                        "schema_version": spec.schema_version,
                        "logical_name": spec.logical_name,
                    },
                }
            )
            artifact = ctx.queue.reconcile_input_artifact(candidate, attempt_id=attempt_id)
            current, _changed = ctx.queue.complete_input_ingest(
                current.job_id,
                attempt_id=attempt_id,
            )
        except StorageAdmissionError as exc:
            raise door_errors.http_problem("storage_admission_refused", exc=exc) from exc
        except ValueError as exc:
            if claimed:
                try:
                    ctx.queue.fail_input_ingest(
                        job.job_id,
                        attempt_id=attempt_id,
                        error=str(exc),
                    )
                except (QueueConflictError, StorageAdmissionError) as cleanup_exc:
                    raise door_errors.http_problem(
                        "input_ingest_terminalization_failed",
                        exc=cleanup_exc,
                        message=(
                            "input artifact ingest failed and its attempt could not be terminalized"
                        ),
                    ) from cleanup_exc
            raise door_errors.http_problem(
                "input_ingest_refused", exc=door_errors.public_message_error(exc)
            ) from exc
        except (OSError, RuntimeError, QueueConflictError) as exc:
            if claimed:
                try:
                    ctx.queue.fail_input_ingest(
                        job.job_id,
                        attempt_id=attempt_id,
                        error=str(exc),
                    )
                except (QueueConflictError, StorageAdmissionError) as cleanup_exc:
                    raise door_errors.http_problem(
                        "input_ingest_terminalization_failed",
                        exc=cleanup_exc,
                        message=(
                            "input artifact ingest failed and its attempt could not be terminalized"
                        ),
                    ) from cleanup_exc
            raise door_errors.http_problem(
                "input_ingest_conflict",
                exc=(
                    door_errors.public_message_error(exc)
                    if isinstance(exc, QueueConflictError)
                    else exc
                ),
            ) from exc
        return {
            "job": _public_record(current).model_dump(mode="json"),
            "artifact": _public_record(artifact).model_dump(mode="json"),
        }
