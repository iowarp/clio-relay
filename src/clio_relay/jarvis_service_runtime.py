"""Verified binding from durable JARVIS MCP results to service runtimes.

This module is the facade over a per-file decomposition (clio-relay file-size
ratchet, scripts/check_file_size.py): the wire/data models moved to
``jarvis_service_runtime_models.py`` and the source-job/MCP-result validation
plus ready-service selection moved to ``jarvis_service_runtime_validation.py``.
Every name is re-exported here under its original path so no other module's
import changes.

What stays resident here is the cluster that reads (or re-verifies) the
durable relay queue and resolves a private JARVIS authority bearer -- the
half of the flow that calls the six collaborators
(``read_artifact_bytes``, ``should_execute_on_cluster``, ``run_remote_clio``,
``run_remote_jarvis_runtime_authority``, ``OwnedSessionApiClient``,
``JarvisCdProvider``) this module's own test suite patches by module
attribute (``monkeypatch.setattr(jarvis_service_runtime, "read_artifact_bytes", ...)``
and friends): a bare-name call inside a function only observes a patch on
the module whose global namespace that call resolves through, so every
function reachable from one of those six calls -- directly or through
``_resolve_jarvis_service_runtime`` -- has to keep living in the same module
as the import, not move to an owner module the patch could no longer reach.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Literal, cast

from clio_relay.bounded_payload import describe_delivery_refusal, is_delivery_refusal
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_DATASET_DESCRIPTOR_SCHEMA as JARVIS_DATASET_DESCRIPTOR_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_AUTHORITY_SCHEMA as JARVIS_SERVICE_RUNTIME_AUTHORITY_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_SCHEMA as JARVIS_SERVICE_RUNTIME_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V1,
    JARVIS_SERVICE_RUNTIME_SCHEMA_V2,
    JSON,
    RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V1,
    RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V2,
    JarvisServiceRuntimeAuthority,
    JarvisServiceRuntimeBinding,
    VerifiedJarvisServiceRuntime,
    _canonical_json_bytes,
    _canonical_json_sha256,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_SNAPSHOT_SCHEMA as JARVIS_SERVICE_RUNTIME_SNAPSHOT_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    RELAY_JARVIS_RUNTIME_BINDING_SCHEMA as RELAY_JARVIS_RUNTIME_BINDING_SCHEMA,
)
from clio_relay.jarvis_service_runtime_models import (
    ClioKitJarvisExecutionQuery as ClioKitJarvisExecutionQuery,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisArtifactIdentity as JarvisArtifactIdentity,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisDatasetArray as JarvisDatasetArray,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisDatasetDescriptor as JarvisDatasetDescriptor,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisDatasetFingerprint as JarvisDatasetFingerprint,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisDatasetMember as JarvisDatasetMember,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisExecutionServiceRuntimes as JarvisExecutionServiceRuntimes,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisPrivateServiceAuthorization as JarvisPrivateServiceAuthorization,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisServiceAuthorization as JarvisServiceAuthorization,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisServiceRuntime as JarvisServiceRuntime,
)
from clio_relay.jarvis_service_runtime_models import (
    JarvisServiceRuntimeHandoff as JarvisServiceRuntimeHandoff,
)
from clio_relay.jarvis_service_runtime_validation import (
    _json_object,
    _select_ready_runtime,
    _validate_mcp_result,
    _validate_runtime_package,
    _validate_snapshot_execution,
    _validate_source_job,
)
from clio_relay.jarvis_service_runtime_validation import (
    derive_jarvis_service_runtime_handoffs as derive_jarvis_service_runtime_handoffs,
)
from clio_relay.models import ArtifactRef, RelayJob
from clio_relay.relay_ops import read_artifact_bytes
from clio_relay.remote_cli import (
    run_remote_clio,
    run_remote_jarvis_runtime_authority,
    should_execute_on_cluster,
)
from clio_relay.runtime_metadata import native_execution_documents
from clio_relay.session_api import OwnedSessionApiClient

OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH = "/internal/jarvis-runtime-authority"
_MAX_AUTHORITY_OUTPUT_BYTES = 32 * 1024
_AUTHORITY_QUERY_TIMEOUT_SECONDS = 30


def resolve_jarvis_service_runtime(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    settings: RelaySettings | None = None,
    source_job_id: str,
    source_artifact_id: str,
    package_id: str,
    package_name: str,
    service_instance_id: str | None = None,
) -> VerifiedJarvisServiceRuntime:
    """Resolve one ready service solely from a verified durable JARVIS MCP result."""
    return _resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        settings=settings,
        source_job_id=source_job_id,
        source_artifact_id=source_artifact_id,
        package_id=package_id,
        package_name=package_name,
        service_instance_id=service_instance_id,
        allow_legacy_v1=False,
    )


def _resolve_jarvis_service_runtime(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    settings: RelaySettings | None,
    source_job_id: str,
    source_artifact_id: str,
    package_id: str,
    package_name: str,
    service_instance_id: str | None,
    allow_legacy_v1: bool,
) -> VerifiedJarvisServiceRuntime:
    """Resolve an exact runtime, optionally for re-verifying a released v1 binding."""
    job, artifact, document = _load_source(
        queue=queue,
        definition=definition,
        settings=settings,
        source_job_id=source_job_id,
        source_artifact_id=source_artifact_id,
    )
    spec = _validate_source_job(job, cluster=definition.name)
    query = _validate_mcp_result(document, job=job, spec=spec)
    native = native_execution_documents(query.model_dump(mode="json"))
    if native is None:
        raise ValueError("JARVIS service runtime result omitted native execution documents")
    snapshot = query.service_runtimes
    _validate_snapshot_execution(snapshot, native=native)
    runtime = _select_ready_runtime(
        snapshot,
        package_id=package_id,
        package_name=package_name,
        service_instance_id=service_instance_id,
    )
    if runtime.schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1 and not allow_legacy_v1:
        raise ValueError(
            "legacy unauthenticated JARVIS service runtimes cannot create new relay bindings"
        )
    _validate_runtime_package(native, runtime=runtime)
    scheduler_provider = native.execution_handle.scheduler_provider
    scheduler_native_id = native.execution_handle.scheduler_native_id
    if native.execution_handle.mode == "scheduler":
        if scheduler_native_id is None:
            raise ValueError("ready scheduler service has no scheduler-native identity")
        if scheduler_provider != definition.scheduler_provider:
            raise ValueError(
                "JARVIS scheduler provider does not match the configured cluster provider"
            )
    descriptor_payload = runtime.dataset_descriptor.model_dump(mode="json")
    runtime_payload = runtime.model_dump(mode="json")
    authorization_sha256 = (
        runtime.authorization.token_sha256 if runtime.authorization is not None else None
    )
    binding = JarvisServiceRuntimeBinding(
        schema_version=(
            RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V2
            if runtime.schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V2
            else RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V1
        ),
        source_relay_job_id=job.job_id,
        source_relay_artifact_id=artifact.artifact_id,
        source_relay_artifact_sha256=cast(str, artifact.sha256),
        source_tool=cast(Literal["jarvis_get_execution"], spec.tool),
        jarvis_execution_id=native.execution_handle.execution_id,
        scheduler_provider=scheduler_provider,
        scheduler_native_id=scheduler_native_id,
        package_id=runtime.package_id,
        package_name=runtime.package_name,
        service_instance_id=runtime.service_instance_id,
        service_revision=runtime.revision,
        service_report_sha256=_canonical_json_sha256(runtime_payload),
        service_runtime_schema_version=(
            JARVIS_SERVICE_RUNTIME_SCHEMA_V2
            if runtime.schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V2
            else None
        ),
        authorization_sha256=authorization_sha256,
        dataset_descriptor_sha256=_canonical_json_sha256(descriptor_payload),
        dataset_descriptor=runtime.dataset_descriptor,
    )
    return VerifiedJarvisServiceRuntime(binding=binding, runtime=runtime, native_execution=native)


def reverify_jarvis_service_runtime(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    settings: RelaySettings | None = None,
    binding_document: object,
) -> VerifiedJarvisServiceRuntime:
    """Re-read an exact source artifact and require its persisted binding to remain unchanged."""
    expected = JarvisServiceRuntimeBinding.model_validate(binding_document)
    observed = _resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        settings=settings,
        source_job_id=expected.source_relay_job_id,
        source_artifact_id=expected.source_relay_artifact_id,
        package_id=expected.package_id,
        package_name=expected.package_name,
        service_instance_id=expected.service_instance_id,
        allow_legacy_v1=(expected.schema_version == RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V1),
    )
    if not hmac.compare_digest(
        _canonical_json_bytes(observed.binding.model_dump(mode="json")),
        _canonical_json_bytes(expected.model_dump(mode="json")),
    ):
        raise ValueError("bound JARVIS service runtime no longer matches its durable source")
    return observed


def resolve_jarvis_service_runtime_authorization(
    *,
    definition: ClusterDefinition,
    settings: RelaySettings | None,
    verified: VerifiedJarvisServiceRuntime,
) -> str | None:
    """Resolve a private bearer for one exact verified v2 runtime without persisting it."""
    runtime = verified.runtime
    binding = verified.binding
    public_authorization = runtime.authorization
    expected_digest = binding.authorization_sha256
    if runtime.schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
        if (
            binding.schema_version != RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V1
            or public_authorization is not None
            or expected_digest is not None
        ):
            raise RelayError("legacy JARVIS runtime authorization provenance is inconsistent")
        return None
    if public_authorization is None or expected_digest is None:
        raise RelayError("authenticated JARVIS runtime omitted its public authority digest")
    if not hmac.compare_digest(public_authorization.token_sha256, expected_digest):
        raise RelayError("JARVIS runtime authority digest disagrees with its durable binding")

    pipeline_id = verified.native_execution.execution_handle.pipeline_id
    arguments = _authority_cli_arguments(
        execution_id=binding.jarvis_execution_id,
        pipeline_id=pipeline_id,
        package_id=binding.package_id,
        service_instance_id=binding.service_instance_id,
        revision=binding.service_revision,
        token_sha256=expected_digest,
    )
    if settings is not None and settings.owner_session_id is not None:
        # Browser attachment is deliberately desktop-local, but the private
        # capability belongs to JARVIS on the cluster that owns the immutable
        # execution receipt. Resolve it through the already identity-proven,
        # exact-generation session API instead of consulting desktop PATH.
        with OwnedSessionApiClient(definition=definition, settings=settings) as client:
            document = _json_object(
                client.request_json(
                    method="POST",
                    path=OWNED_SESSION_JARVIS_RUNTIME_AUTHORITY_PATH,
                    body={"binding": binding.model_dump(mode="json")},
                ),
                "owned JARVIS service runtime authority resolver",
            )
        authority = JarvisServiceRuntimeAuthority.model_validate(document)
    elif should_execute_on_cluster(definition):
        payload = run_remote_jarvis_runtime_authority(
            definition,
            arguments,
            timeout_seconds=_AUTHORITY_QUERY_TIMEOUT_SECONDS,
            maximum_stdout_bytes=_MAX_AUTHORITY_OUTPUT_BYTES,
        )
        document = _decode_unique_json_object(
            payload,
            label="JARVIS service runtime authority resolver",
        )
        authority = JarvisServiceRuntimeAuthority.model_validate(document)
    else:
        resolved_settings = settings or RelaySettings.from_env()
        authority = resolve_local_verified_jarvis_service_runtime_authority(
            jarvis_bin=resolved_settings.jarvis_bin,
            verified=verified,
        )
        if authority is None:
            raise RelayError("authenticated JARVIS runtime authority unexpectedly resolved empty")
    _validate_resolved_authority(verified=verified, authority=authority)
    return f"Bearer {authority.authorization.token.get_secret_value()}"


def resolve_local_verified_jarvis_service_runtime_authority(
    *,
    jarvis_bin: str,
    verified: VerifiedJarvisServiceRuntime,
) -> JarvisServiceRuntimeAuthority | None:
    """Resolve and revalidate one exact verified runtime on its cluster host."""
    runtime = verified.runtime
    binding = verified.binding
    if runtime.schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
        return None
    expected_digest = binding.authorization_sha256
    if expected_digest is None:
        raise RelayError("authenticated JARVIS runtime omitted its authority digest")
    authority = resolve_local_jarvis_service_runtime_authority(
        jarvis_bin=jarvis_bin,
        execution_id=binding.jarvis_execution_id,
        pipeline_id=verified.native_execution.execution_handle.pipeline_id,
        package_id=binding.package_id,
        service_instance_id=binding.service_instance_id,
        revision=binding.service_revision,
        token_sha256=expected_digest,
    )
    _validate_resolved_authority(verified=verified, authority=authority)
    return authority


def resolve_local_jarvis_service_runtime_authority(
    *,
    jarvis_bin: str,
    execution_id: str,
    pipeline_id: str,
    package_id: str,
    service_instance_id: str,
    revision: int,
    token_sha256: str,
) -> JarvisServiceRuntimeAuthority:
    """Invoke JARVIS's bounded trusted resolver on the current cluster host."""
    if not jarvis_bin:
        raise ConfigurationError("JARVIS service runtime authority resolver is not configured")
    provider = JarvisCdProvider(jarvis_bin=jarvis_bin)
    provider.require_available()
    arguments = _authority_cli_arguments(
        execution_id=execution_id,
        pipeline_id=pipeline_id,
        package_id=package_id,
        service_instance_id=service_instance_id,
        revision=revision,
        token_sha256=token_sha256,
    )
    result = provider.run_command_streaming(
        [jarvis_bin, "execution", "resolve-service-runtime-authority", *arguments, "+json"],
        timeout_seconds=_AUTHORITY_QUERY_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        raise RelayError(
            f"JARVIS service runtime authority resolution failed with exit code {result.returncode}"
        )
    if len(result.stdout.encode("utf-8")) > _MAX_AUTHORITY_OUTPUT_BYTES:
        raise RelayError("JARVIS service runtime authority response exceeded its byte limit")
    document = _decode_unique_json_object(
        result.stdout,
        label="JARVIS service runtime authority resolver",
    )
    return JarvisServiceRuntimeAuthority.model_validate(document)


def private_jarvis_service_runtime_authority_document(
    authority: JarvisServiceRuntimeAuthority,
) -> JSON:
    """Render the resolver's raw private wire document for relay-internal transport only."""
    document = authority.model_dump(mode="json")
    authorization = cast(dict[str, object], document["authorization"])
    authorization["token"] = authority.authorization.token.get_secret_value()
    return document


def _authority_cli_arguments(
    *,
    execution_id: str,
    pipeline_id: str,
    package_id: str,
    service_instance_id: str,
    revision: int,
    token_sha256: str,
) -> list[str]:
    """Build the exact identity-complete argument vector for JARVIS's private resolver."""
    return [
        execution_id,
        "--pipeline-id",
        pipeline_id,
        "--package-id",
        package_id,
        "--service-instance-id",
        service_instance_id,
        "--revision",
        str(revision),
        "--token-sha256",
        token_sha256,
    ]


def _validate_resolved_authority(
    *,
    verified: VerifiedJarvisServiceRuntime,
    authority: JarvisServiceRuntimeAuthority,
) -> None:
    """Require the resolver response to match every durable public identity."""
    binding = verified.binding
    pipeline_id = verified.native_execution.execution_handle.pipeline_id
    if (
        authority.execution_id != binding.jarvis_execution_id
        or authority.pipeline_id != pipeline_id
        or authority.package_id != binding.package_id
        or authority.service_instance_id != binding.service_instance_id
        or authority.revision != binding.service_revision
    ):
        raise RelayError("JARVIS service runtime authority returned a different runtime identity")
    expected_digest = binding.authorization_sha256
    if expected_digest is None or not hmac.compare_digest(
        authority.token_sha256,
        expected_digest,
    ):
        raise RelayError("JARVIS service runtime authority returned a different token digest")


def _decode_unique_json_object(value: str, *, label: str) -> JSON:
    """Decode one bounded JSON object while rejecting duplicate keys and constants."""

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, nested in pairs:
            if key in result:
                raise ValueError(f"{label} returned duplicate JSON key: {key}")
            result[key] = nested
        return result

    def reject_constant(constant: str) -> object:
        raise ValueError(f"{label} returned non-finite JSON constant: {constant}")

    try:
        document: object = json.loads(
            value,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, UnicodeError, RecursionError):
        raise RelayError(f"{label} returned invalid JSON") from None
    if not isinstance(document, dict):
        raise RelayError(f"{label} did not return a JSON object")
    return cast(JSON, document)


def _load_source(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    settings: RelaySettings | None,
    source_job_id: str,
    source_artifact_id: str,
) -> tuple[RelayJob, ArtifactRef, JSON]:
    # The source receipt belongs to its exact owner-session generation, regardless of
    # where the current operation executes.  In particular, browser attachment is a
    # desktop-local operation, but it must re-verify the remote JARVIS receipt through
    # the authenticated owner-session API rather than looking for that job in the
    # desktop queue.  CLI locality controls command placement, not provenance storage.
    if settings is not None and settings.owner_session_id is not None:
        with OwnedSessionApiClient(definition=definition, settings=settings) as client:
            status = _json_object(
                client.request_json(
                    method="GET",
                    path=f"/jobs/{source_job_id}/status",
                ),
                "JARVIS service source job",
            )
            envelope = _json_object(
                client.request_json(
                    method="GET",
                    path=f"/artifacts/{source_artifact_id}/content",
                ),
                "JARVIS service source artifact",
            )
        raw_job = status.get("job")
    elif should_execute_on_cluster(definition):
        status = _remote_json(
            definition,
            ["job", "status", source_job_id],
            "JARVIS service source job",
        )
        raw_job = status.get("job")
        envelope = _remote_json(
            definition,
            ["job", "read-artifact", source_artifact_id],
            "JARVIS service source artifact",
        )
    else:
        raw_job = queue.get_job(source_job_id).model_dump(mode="json")
        envelope = cast(JSON, read_artifact_bytes(queue, source_artifact_id))
    job = RelayJob.model_validate(raw_job)
    if job.job_id != source_job_id:
        raise ValueError("JARVIS service source returned a different relay job")
    if is_delivery_refusal(envelope):
        # F5 (#231 R6 review): a T2 refusal (doc §6.4) is not a malformed
        # envelope -- report the refusal's own message/code instead of the
        # generic "is not a base64 envelope" the checks below would raise,
        # which misdescribes WHY the artifact is unavailable.
        # A2 (#231 R6 review): the message extraction itself now delegates
        # to bounded_payload.describe_delivery_refusal, the single owner.
        code = cast(dict[str, object], envelope.get("delivery", {})).get("code")
        raise ValueError(
            f"JARVIS service source artifact delivery refused ({code}): "
            f"{describe_delivery_refusal(envelope)}"
        )
    raw_artifact = envelope.get("artifact")
    artifact = ArtifactRef.model_validate(raw_artifact)
    if (
        artifact.artifact_id != source_artifact_id
        or artifact.job_id != source_job_id
        or artifact.kind != "mcp_result"
    ):
        raise ValueError("JARVIS service source artifact identity did not match the request")
    if artifact.sha256 is None:
        raise ValueError("JARVIS service source artifact has no durable SHA-256")
    encoded = envelope.get("data")
    if envelope.get("encoding") != "base64" or not isinstance(encoded, str):
        raise ValueError("JARVIS service source artifact is not a base64 envelope")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError("JARVIS service source artifact contains invalid base64") from exc
    digest = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(digest, artifact.sha256):
        raise ValueError("JARVIS service source artifact digest did not match durable metadata")
    try:
        document = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("JARVIS service source artifact must contain UTF-8 JSON") from exc
    if not isinstance(document, dict):
        raise ValueError("JARVIS service source artifact must contain a JSON object")
    return job, artifact, cast(JSON, document)


def _remote_json(
    definition: ClusterDefinition,
    arguments: list[str],
    label: str,
) -> JSON:
    output = run_remote_clio(definition, arguments)
    try:
        value = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} did not return a JSON object")
    return cast(JSON, value)
