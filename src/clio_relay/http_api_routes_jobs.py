"""Job submission and job/transform/status read routes.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``create_app()`` in ``http_api.py``. Every reference to a
``create_app()``-local closure (``resolved``, ``queue``,
``owner_session_cluster_definition``, ``submit_owned``, ``require_owned_job``)
is rewritten to the equivalent ``ctx.<name>`` attribute/method on the shared
``RelayApiContext`` (see ``http_api_context.py``'s own docstring) -- the same
mechanical bare-name -> qualified-name rewrite this codebase's other AST-
driven extractions already use; no other line changes.

``jarvis_mcp_server``/``jarvis_mcp_server_args``/``jarvis_mcp_artifact_binding``
are imported here (not re-exported from ``http_api.py``) because
``submit_jarvis_mcp_call`` is the only caller that moved with them;
``tests/test_http_api.py``'s/``tests/test_jarvis_handle_first_admission.py``'s
``monkeypatch.setattr("clio_relay.http_api.jarvis_mcp_server", ...)`` sites
re-point to ``clio_relay.http_api_routes_jobs.jarvis_mcp_server`` (etc.) to
follow the move, the same "test re-points to the new owner module" pattern
this repo's own decomposition history already establishes repeatedly.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import os
import secrets
from pathlib import Path

from fastapi import FastAPI

from clio_relay import door_errors
from clio_relay.cluster_config import ClusterRegistry, default_registry_path
from clio_relay.dev_mode import VerificationFindings
from clio_relay.errors import ConfigurationError, NotFoundError, QueueConflictError
from clio_relay.http_api_context import RelayApiContext
from clio_relay.http_api_models import (
    JarvisMcpCallSubmitRequest,
    JarvisPipelineSubmitRequest,
    JarvisSubmitRequest,
    McpCallSubmitRequest,
    RemoteAgentSubmitRequest,
)
from clio_relay.http_api_redaction import _public_payload, _public_record
from clio_relay.identifiers import DurableRecordId
from clio_relay.jarvis_mcp import (
    JARVIS_MCP_AMBIENT_LAUNCHER_UNVERIFIED_REASON,
    JARVIS_MCP_LAUNCHER_DOWNGRADE_METADATA_KEY,
    JARVIS_MCP_PINNED_LAUNCHER_UNVERIFIED_REASON,
    is_virtual_jarvis_control_query,
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_artifact_binding,
    jarvis_mcp_env_from,
    jarvis_mcp_server,
    jarvis_mcp_server_args,
)
from clio_relay.job_identity import JobOwnerSessionIdentity
from clio_relay.models import (
    JarvisRunSpec,
    JobKind,
    McpAdmissionClass,
    McpCallSpec,
    McpOperation,
    RelayJob,
    RemoteAgentTaskSpec,
    TransformRef,
)
from clio_relay.relay_ops import job_status as get_job_status_operation
from clio_relay.remote_mcp import (
    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
    resolve_pinned_mcp_admission,
    resolve_registered_remote_mcp_admission,
)
from clio_relay.remote_values import expand_remote_value_on_host


def register_job_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: object,
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
        if job.kind in {JobKind.MCP_CALL, JobKind.INPUT_INGEST}:
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

    @app.post(
        "/jobs/mcp-call",
        response_model=RelayJob,
        dependencies=[auth_dependency],
    )
    def submit_mcp_call(
        request: McpCallSubmitRequest,
        owner_session_identity: JobOwnerSessionIdentity | None = job_identity_parameter,
    ) -> RelayJob:
        registry_path = default_registry_path()
        try:
            definition = (
                ctx.owner_session_cluster_definition
                if ctx.owner_session_cluster_definition is not None
                else (
                    ClusterRegistry.load(registry_path).clusters.get(request.cluster)
                    if registry_path.exists()
                    else None
                )
            )
            admission_class, admission_authority = resolve_registered_remote_mcp_admission(
                queue=ctx.queue,
                definition=definition,
                cluster=request.cluster,
                server=request.server,
                server_args=request.server_args,
                env_from=request.env_from,
                operation=request.operation,
                tool=request.tool,
                expected_server_artifact_digest=request.expected_server_artifact_digest,
                evidence=request.control_query_evidence,
                expected_registered_contract=request.expected_registered_contract,
                timeout_seconds=request.timeout_seconds,
            )
        except (ConfigurationError, ValueError) as exc:
            # clio-relay#242 actionability audit (R9 doctrine): every failure
            # here is a stale/mismatched cached admission fact (route,
            # registration, contract, or discovery evidence changed since
            # the caller last refreshed it) -- name the fix explicitly
            # rather than leaving the agent only the raw mismatch detail.
            # Live case: the ares agent's spack_find hit this reason with no
            # way to tell retry from refresh-and-resubmit apart.
            raise door_errors.http_problem(
                "mcp_submission_conflict",
                exc=exc,
                message=(
                    f"{exc}; not retryable with this cached admission/discovery "
                    "evidence -- call tools/list for this server again to refresh "
                    "it (or resubmit without control-query evidence, as a "
                    "workload call), then retry"
                ),
            ) from exc
        return ctx.submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=request.server,
                    server_args=request.server_args,
                    env_from=request.env_from,
                    expected_server_artifact_digest=(request.expected_server_artifact_digest),
                    expected_registered_contract=request.expected_registered_contract,
                    admission_class=admission_class,
                    operation=request.operation,
                    tool=request.tool,
                    arguments=request.arguments,
                    jarvis_input_manifest=request.jarvis_input_manifest,
                    timeout_seconds=request.timeout_seconds,
                ),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
            ),
            owner_session_identity=owner_session_identity,
            mcp_admission_authority=admission_authority,
        )

    @app.post(
        "/jobs/jarvis-mcp-call",
        response_model=RelayJob,
        dependencies=[auth_dependency],
    )
    def submit_jarvis_mcp_call(
        request: JarvisMcpCallSubmitRequest,
        owner_session_identity: JobOwnerSessionIdentity | None = job_identity_parameter,
    ) -> RelayJob:
        expected_digest = request.expected_server_artifact_digest
        try:
            admission_class, admission_authority = resolve_pinned_mcp_admission(
                operation=request.operation,
                tool=request.tool,
                expected_server_artifact_digest=expected_digest,
                pinned_control_query=(
                    request.tool is not None and is_virtual_jarvis_control_query(request.tool)
                ),
                timeout_seconds=request.timeout_seconds,
            )
        except ValueError as exc:
            raise door_errors.http_problem(
                "jarvis_submission_refused", exc=door_errors.public_message_error(exc)
            ) from exc
        timeout_seconds = request.timeout_seconds
        if admission_class is McpAdmissionClass.CONTROL_QUERY and timeout_seconds is None:
            timeout_seconds = MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS
        if (
            ctx.resolved.owner_session_id is not None
            and request.operation is McpOperation.TOOLS_CALL
            and expected_digest is None
        ):
            raise door_errors.http_problem(
                "jarvis_submission_refused",
                "owned JARVIS MCP submission requires expected_server_artifact_digest",
            )
        # An owned API preserves its desktop-supplied digest because discovery
        # caches are process-local; the runner verifies it immediately before launch.
        if (
            request.operation is McpOperation.TOOLS_CALL
            and expected_digest is not None
            and ctx.resolved.owner_session_id is None
        ):
            try:
                observed_digest = jarvis_mcp_artifact_binding(request.cluster)
            except ValueError as exc:
                raise door_errors.http_problem(
                    "jarvis_artifact_conflict", exc=door_errors.public_message_error(exc)
                ) from exc
            if not secrets.compare_digest(expected_digest, observed_digest):
                raise door_errors.http_problem(
                    "jarvis_artifact_conflict",
                    message=(
                        "JARVIS MCP artifact identity changed; refresh discovery before submission"
                    ),
                )
        # Resolve through the cluster pin when one exists (#228); otherwise use
        # the ambient identity. ``None`` preserves host dev-mode OR semantics (#211).
        pinned_dev_mode = (
            True
            if ctx.owner_session_cluster_definition is not None
            and ctx.owner_session_cluster_definition.dev_mode
            else None
        )
        launcher_findings = VerificationFindings()
        try:
            # relay_install_receipt may be recorded ``$HOME/``-anchored (the
            # same convention jarvis_run_environment.registered_site_spack_command
            # expands for spack_executable). ``Path.expanduser()`` only
            # expands a leading ``~`` and leaves a literal ``$HOME/`` prefix
            # untouched, so a ``$HOME/``-anchored pin previously resolved to
            # a nonexistent path and, because jarvis_mcp_command() silently
            # fell back to the box default launcher when its receipt_path
            # override was absent on disk, every such pin silently ran the
            # wrong (default, unpinned) launcher instead of refusing -- the
            # exact wrong-tenant hazard #228 exists to kill (clio-relay#228
            # rework). Expand it against this process's own home before
            # resolving. This call raises ConfigurationError on a malformed
            # value; it lives inside this try (not before it) so that
            # failure surfaces as the same typed 409 below rather than an
            # unhandled 500 (clio-relay#228 rework round 2).
            pinned_receipt_path = (
                Path(
                    expand_remote_value_on_host(
                        ctx.owner_session_cluster_definition.relay_install_receipt,
                        field="relay_install_receipt",
                        home=os.path.expanduser("~"),
                    )
                ).expanduser()
                if ctx.owner_session_cluster_definition is not None
                and ctx.owner_session_cluster_definition.relay_install_receipt is not None
                else None
            )
            registered_jarvis = (
                ctx.owner_session_cluster_definition.remote_mcp_servers.get("jarvis")
                if ctx.owner_session_cluster_definition is not None
                and ctx.owner_session_cluster_definition.remote_mcp_servers
                else None
            )
            registered_jarvis_command = (
                [registered_jarvis.command, *registered_jarvis.args]
                if registered_jarvis is not None
                else None
            )
            server = jarvis_mcp_server(
                receipt_path=pinned_receipt_path,
                cluster=request.cluster,
                dev_mode=pinned_dev_mode,
                registered_command=registered_jarvis_command,
                # clio-relay#228 rework round 2: this route's own findings
                # instance, not an internally-constructed-and-discarded one
                # -- otherwise a dev-mode verification downgrade is recorded
                # nowhere this caller can read it. Threaded through only the
                # server call (not server_args below): both calls resolve
                # the identical receipt/identity, so recording once is
                # sufficient and avoids double-recording the same warning.
                findings=launcher_findings,
            )
            server_args = jarvis_mcp_server_args(
                receipt_path=pinned_receipt_path,
                cluster=request.cluster,
                dev_mode=pinned_dev_mode,
                registered_command=registered_jarvis_command,
            )
            env_from = jarvis_mcp_env_from()
        except (ValueError, ConfigurationError) as exc:
            # A failed explicit launcher identity never falls back (#228).
            raise door_errors.http_problem(
                "launcher_resolution_failed", exc=door_errors.public_message_error(exc)
            ) from exc
        # clio-relay#228 rework round 2 (design ruling): dev mode relaxes
        # VERIFICATION of a receipt, it must never SILENTLY substitute a
        # different binary with no queryable trace. When resolution above
        # downgraded a failed identity check to a warning (pinned route: the
        # pinned receipt's own unverified launcher was used; ambient route:
        # DEFAULT_JARVIS_MCP_COMMAND was used, its historical behavior),
        # attach that typed reason to the durable job record -- the same
        # "never a silent downgrade" contract VerificationFindings/
        # DEV_MODE_BANNER already establishes for every other dev-mode-gated
        # check in this codebase.
        downgrade_payload = launcher_findings.payload()
        job_metadata: dict[str, object] = (
            {
                JARVIS_MCP_LAUNCHER_DOWNGRADE_METADATA_KEY: {
                    "reason": (
                        JARVIS_MCP_PINNED_LAUNCHER_UNVERIFIED_REASON
                        if pinned_receipt_path is not None
                        else JARVIS_MCP_AMBIENT_LAUNCHER_UNVERIFIED_REASON
                    ),
                    "cluster": request.cluster,
                    **downgrade_payload,
                }
            }
            if downgrade_payload is not None
            else {}
        )
        return ctx.submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=server,
                    server_args=server_args,
                    env_from=env_from,
                    expected_server_artifact_digest=expected_digest,
                    expected_jarvis_cd_lock_binding=jarvis_cd_lock_binding_expectation(),
                    admission_class=admission_class,
                    operation=request.operation,
                    tool=request.tool,
                    arguments=request.arguments,
                    timeout_seconds=timeout_seconds,
                ),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
                metadata=job_metadata,
            ),
            owner_session_identity=owner_session_identity,
            mcp_admission_authority=admission_authority,
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
