"""Shared owned-resource/admission context for the ``http_api`` routes.

split/http-api-w3 (iowarp/clio-relay#231): ``create_app()`` used to define
``ensure_intake_open``/``owns_job``/``require_owned_job``/
``require_owned_task``/``require_owned_artifact``/``submit_owned``/
``require_owned_gateway`` as nested functions closing over its own locals
(``queue``, ``resolved``, ``owner_session_cluster``). Splitting the ~1900
line route body across owner modules (``http_api_routes_*.py``) means those
seven can no longer be closures -- there is no single enclosing function
left for them to close over. They move here as methods on
``RelayApiContext`` instead: each ``create_app()`` call builds exactly one
``RelayApiContext`` (mirroring the one closure-capture set it used to build)
and passes it to every ``register_*_routes`` call, so ``ctx.<name>(...)``
resolves to the identical logic the old bare-name closure call did. This is
the same "closures -> composed object, same call graph" shape
``endpoint.py``'s own slice-10 mixin-class split already established for
exactly this problem (core-queue-split-2026-08.md).

``_bound_owner_session_cluster_definition`` and its two env-var constants
move here too: their only caller is ``create_app()``'s
``owner_session_cluster_definition`` construction, and the resulting value
is itself a ``RelayApiContext`` field every route this split needed already
declares dependencies through this module.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from clio_relay import door_errors
from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
    read_bounded_configuration_bytes,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import (
    INPUT_INGEST_ATTEMPT_METADATA_KEY,
    INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY,
    ClioCoreQueue,
)
from clio_relay.errors import ConfigurationError, QueueConflictError
from clio_relay.http_api_redaction import _public_record
from clio_relay.identifiers import DurableRecordId
from clio_relay.job_identity import (
    JobOwnerSessionIdentity,
    OwnerSessionIdentityError,
    job_owned_by_session,
    require_job_owner_session_identity,
)
from clio_relay.models import (
    INPUT_INGEST_POLICY_METADATA_KEY,
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    ArtifactRef,
    GatewaySession,
    InputArtifactIngestPolicy,
    JobKind,
    McpAdmissionAuthority,
    McpAdmissionClass,
    McpCallSpec,
    RelayJob,
    RelayTask,
    new_id,
)
from clio_relay.storage_runtime import StorageAdmissionError

_SESSION_REGISTRY_SHA256_ENV = "CLIO_RELAY_SESSION_REGISTRY_SHA256"
_SESSION_ROUTE_REVISION_ENV = "CLIO_RELAY_SESSION_ROUTE_REVISION"


def _bound_owner_session_cluster_definition(
    *, owner_session_id: str | None, owner_session_cluster: str | None
) -> ClusterDefinition | None:
    """Load one immutable process-bound cluster authority for an owned API."""
    raw_bindings = {
        CLUSTER_REGISTRY_ENV: os.getenv(CLUSTER_REGISTRY_ENV),
        _SESSION_REGISTRY_SHA256_ENV: os.getenv(_SESSION_REGISTRY_SHA256_ENV),
        _SESSION_ROUTE_REVISION_ENV: os.getenv(_SESSION_ROUTE_REVISION_ENV),
    }
    session_bindings = {
        _SESSION_REGISTRY_SHA256_ENV,
        _SESSION_ROUTE_REVISION_ENV,
    }
    configured_session_bindings = {
        name for name in session_bindings if raw_bindings[name] is not None
    }
    if not configured_session_bindings:
        if owner_session_id is not None:
            raise ConfigurationError(
                "owned relay session API requires process-bound cluster authority"
            )
        return None
    configured = {name for name, value in raw_bindings.items() if value is not None}
    if configured_session_bindings != session_bindings or configured != set(raw_bindings):
        raise ConfigurationError(
            "owned session cluster authority path, digest, and route revision must be configured "
            "together"
        )
    if owner_session_id is None or owner_session_cluster is None:
        raise ConfigurationError("session cluster authority requires an owned relay session")
    registry_path_raw = raw_bindings[CLUSTER_REGISTRY_ENV]
    if not registry_path_raw:
        raise ConfigurationError("owned session cluster registry path must not be blank")
    registry_sha256 = raw_bindings[_SESSION_REGISTRY_SHA256_ENV]
    route_revision = raw_bindings[_SESSION_ROUTE_REVISION_ENV]
    if (
        not registry_sha256
        or len(registry_sha256) != 64
        or any(character not in "0123456789abcdef" for character in registry_sha256)
    ):
        raise ConfigurationError("owned session cluster registry SHA-256 is invalid")
    if (
        not route_revision
        or len(route_revision) != 64
        or any(character not in "0123456789abcdef" for character in route_revision)
    ):
        raise ConfigurationError("owned session cluster route revision is invalid")
    registry_path = Path(registry_path_raw).expanduser()
    if not registry_path.is_absolute():
        raise ConfigurationError("owned session cluster registry path must be absolute")
    try:
        payload = read_bounded_configuration_bytes(
            registry_path,
            max_bytes=MAX_CLUSTER_REGISTRY_BYTES,
        )
    except (ConfigurationError, OSError) as exc:
        raise ConfigurationError("owned session cluster registry is unavailable") from exc
    if not secrets.compare_digest(hashlib.sha256(payload).hexdigest(), registry_sha256):
        raise ConfigurationError("owned session cluster registry digest does not match")
    try:
        registry = ClusterRegistry.model_validate_json(payload)
    except ValidationError as exc:
        raise ConfigurationError("owned session cluster registry is invalid") from exc
    if set(registry.clusters) != {owner_session_cluster}:
        raise ConfigurationError(
            "owned session cluster registry must contain exactly the owner session cluster"
        )
    definition = registry.require(owner_session_cluster)
    if not secrets.compare_digest(cluster_route_revision(definition), route_revision):
        raise ConfigurationError("owned session cluster route revision does not match")
    return definition


@dataclass
class RelayApiContext:
    """The owned-resource/admission surface every ``http_api`` route shares."""

    queue: ClioCoreQueue
    resolved: RelaySettings
    owner_session_cluster: str | None
    owner_session_cluster_definition: ClusterDefinition | None

    def ensure_intake_open(self) -> None:
        if self.resolved.owner_session_id is None:
            return
        generation_id = self.resolved.owner_session_generation_id
        if generation_id is None:
            raise door_errors.http_problem(
                "session_generation_identity_unavailable",
                "relay session has no exact generation identity",
            )
        admission = self.queue.owner_session_generation_status(
            self.resolved.owner_session_id,
            session_generation_id=generation_id,
        )
        if admission.get("open") is not True:
            raise door_errors.http_problem(
                "session_intake_closed", "relay session generation is not open for new work"
            )

    def owns_job(self, job: RelayJob) -> bool:
        return job_owned_by_session(
            job,
            owner_session_id=self.resolved.owner_session_id,
            owner_session_generation_id=self.resolved.owner_session_generation_id,
        )

    def require_owned_job(self, job_id: DurableRecordId) -> RelayJob:
        job = self.queue.get_job(job_id)
        if not self.owns_job(job):
            raise door_errors.http_problem(
                "resource_ownership_refused", "job is not owned by this relay session"
            )
        return job

    def require_owned_task(self, task_id: DurableRecordId) -> RelayTask:
        task = self.queue.get_task(task_id)
        self.require_owned_job(task.job_id)
        return task

    def require_owned_artifact(self, artifact_id: DurableRecordId) -> ArtifactRef:
        artifact = self.queue.get_artifact(artifact_id)
        self.require_owned_job(artifact.job_id)
        return artifact

    def submit_owned(
        self,
        job: RelayJob,
        *,
        owner_session_identity: JobOwnerSessionIdentity | None = None,
        mcp_admission_authority: McpAdmissionAuthority | None = None,
        input_ingest_policy: InputArtifactIngestPolicy | None = None,
    ) -> RelayJob:
        self.ensure_intake_open()
        if self.owner_session_cluster is not None and job.cluster != self.owner_session_cluster:
            raise door_errors.http_problem(
                "job_cluster_mismatch", "job cluster does not match this owned relay session"
            )
        metadata = dict(job.metadata)
        protected = sorted(
            {
                "owner",
                "owner_session_id",
                "owner_session_generation_id",
                "owner_session_admission_id",
                MCP_ADMISSION_AUTHORITY_METADATA_KEY,
                INPUT_INGEST_ATTEMPT_METADATA_KEY,
                INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY,
                INPUT_INGEST_POLICY_METADATA_KEY,
            }.intersection(metadata)
        )
        if protected:
            raise door_errors.http_problem(
                "job_submission_refused",
                message=(
                    "job ownership metadata is server-managed and cannot be supplied: "
                    + ", ".join(protected)
                ),
            )
        if job.owner_session_id is not None or job.owner_session_generation_id is not None:
            raise door_errors.http_problem(
                "job_submission_refused",
                message=(
                    "job owner-session identity is server-managed and must be "
                    "supplied through headers"
                ),
            )
        try:
            owner_session_identity = require_job_owner_session_identity(
                job.kind,
                owner_session_identity,
            )
        except OwnerSessionIdentityError as exc:
            raise door_errors.http_problem("owner_session_identity_refused", exc=exc) from exc
        if job.kind is JobKind.MCP_CALL:
            if not isinstance(job.spec, McpCallSpec):
                raise door_errors.http_problem(
                    "mcp_admission_refused",
                    message="MCP job has an invalid specification",
                )
            if job.spec.admission_class is McpAdmissionClass.CONTROL_QUERY:
                if mcp_admission_authority is None:
                    raise door_errors.http_problem(
                        "mcp_admission_refused",
                        message="control-query MCP admission requires server authority",
                    )
                metadata[MCP_ADMISSION_AUTHORITY_METADATA_KEY] = mcp_admission_authority.model_dump(
                    mode="json"
                )
            elif mcp_admission_authority is not None:
                raise door_errors.http_problem(
                    "mcp_admission_refused",
                    message="workload MCP admission must not carry control-query authority",
                )
        elif mcp_admission_authority is not None:
            raise door_errors.http_problem(
                "mcp_admission_refused",
                message="MCP admission authority cannot be attached to another job kind",
            )
        if job.kind is JobKind.INPUT_INGEST:
            if input_ingest_policy is None:
                raise door_errors.http_problem(
                    "input_ingest_refused",
                    message="input ingest requires server-owned generation quota policy",
                )
            metadata[INPUT_INGEST_POLICY_METADATA_KEY] = input_ingest_policy.model_dump(mode="json")
        elif input_ingest_policy is not None:
            raise door_errors.http_problem(
                "input_ingest_refused",
                message="input ingest policy cannot be attached to another job kind",
            )
        if self.resolved.owner_session_id is not None:
            for use in job.used_artifact_refs:
                self.require_owned_artifact(use.artifact_id)
            metadata.update(
                {
                    "owner": "clio-relay",
                    "owner_session_id": self.resolved.owner_session_id,
                }
            )
            if self.resolved.owner_session_generation_id is not None:
                metadata["owner_session_generation_id"] = self.resolved.owner_session_generation_id
        # Job ids crossing HTTP are caller-controlled, including on the raw
        # /jobs route. Generate new-admission entropy inside the server; an
        # idempotent retry is still canonicalized by the durable key record.
        job_updates: dict[str, object] = {"job_id": new_id("job")}
        if owner_session_identity is not None:
            job_updates.update(
                {
                    "owner_session_id": owner_session_identity.owner_session_id,
                    "owner_session_generation_id": (
                        owner_session_identity.owner_session_generation_id
                    ),
                }
            )
        job = job.model_copy(update=job_updates)
        try:
            return _public_record(
                self.queue.submit_job(job.model_copy(update={"metadata": metadata}))
            )
        except ValueError as exc:
            raise door_errors.http_problem(
                "job_submission_refused", exc=door_errors.public_message_error(exc)
            ) from exc
        except QueueConflictError as exc:
            # clio-relay#242 actionability audit (R9 doctrine): name the
            # exact idempotency_key at fault and the two ways to move
            # forward, instead of leaving the agent only the raw storage
            # invariant text.
            raise door_errors.http_problem(
                "job_submission_conflict",
                exc=exc,
                message=(
                    f"{exc}; not retryable with idempotency_key="
                    f"{job.idempotency_key!r} -- submit again with a new "
                    "idempotency_key for a genuinely new request, or if this was "
                    "meant to replay the same request, its payload changed since "
                    "the key was first used"
                ),
            ) from exc
        except StorageAdmissionError as exc:
            raise door_errors.http_problem("storage_admission_refused", exc=exc) from exc

    def require_owned_gateway(self, session_id: DurableRecordId) -> GatewaySession:
        session = self.queue.get_gateway_session(session_id)
        if self.resolved.owner_session_id is None:
            return session
        if (
            session.metadata.get("owner") != "clio-relay"
            or session.metadata.get("owner_session_id") != self.resolved.owner_session_id
            or session.metadata.get("owner_session_generation_id")
            != self.resolved.owner_session_generation_id
        ):
            raise door_errors.http_problem(
                "resource_ownership_refused", "gateway session is not owned by this relay session"
            )
        return session
