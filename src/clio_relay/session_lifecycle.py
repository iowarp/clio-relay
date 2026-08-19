"""Owned remote relay session lifecycle helpers."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import os
import secrets
import stat
import subprocess
import sys
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast
from uuid import uuid4

import clio_relay.session_api_readiness as session_api_readiness
import clio_relay.session_cleanup_targets as session_cleanup_targets
import clio_relay.session_lifecycle_report as session_lifecycle_report
import clio_relay.session_process_scope as session_process_scope
import clio_relay.session_recovery_attempt_status as session_recovery_attempt_status
import clio_relay.session_recovery_cleaned_receipt as session_recovery_cleaned_receipt
import clio_relay.session_recovery_cleanup_receipt as session_recovery_cleanup_receipt
import clio_relay.session_remote_command as session_remote_command
import clio_relay.session_remote_scripts as session_remote_scripts
import clio_relay.session_start_attempt_validation as session_start_attempt_validation
import clio_relay.session_startup_receipt as session_startup_receipt
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.config import ALLOW_UNAUTHENTICATED_OWNED_SESSION_ENV
from clio_relay.errors import (
    RelayError,
    RemoteExecutableMissingError,
)
from clio_relay.identifiers import DurableRecordId, validate_durable_record_id
from clio_relay.session_lifecycle_report import (
    OwnedSessionCleanupFinalizeRequest,
    OwnedSessionCleanupReportReadRequest,
    SessionLifecycleReport,
    session_lifecycle_report_bytes,
    session_lifecycle_report_sha256,
)
from clio_relay.session_transaction import (
    _MAX_OWNED_SESSION_DOCUMENT_BYTES,
    open_owned_session_transaction,
)
from clio_relay.session_validation import _validate_durable_session_identity, _validate_session

# Wire models moved to session_wire_models.py (#231 R8(iii), design doc §4.4).
# Re-exported here under their original names so every existing caller,
# test, and `session_lifecycle.<Symbol>` monkeypatch seam keeps resolving
# unchanged -- this is a pure move, not a behavior change.
from clio_relay.session_wire_models import (
    MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
    MAX_SESSION_START_ERROR_CHARS,
    CleanupResource,
    OwnedSessionCleanupReportReference,
    OwnedSessionIdentityChallengeRequest,
    OwnedSessionInputPolicy,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartPlan,
    OwnedSessionStartReceipt,
    OwnedSessionStartRequest,
    OwnedSessionStartResult,
    OwnedSessionStartRetrySelector,
    OwnedSessionStartStatusSelector,
    OwnedSessionTeardownRequest,
    RemoteSessionStateEvidence,
    SessionApiReleaseIdentity,
)

if TYPE_CHECKING:
    from clio_relay.session_process_scope import _OwnedGenerationProcess
    from clio_relay.session_recovery_attempt_status import _FailedStartCleanupQueue
    from clio_relay.session_transaction import _OwnedSessionTransaction

logger = logging.getLogger(__name__)

_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS = 120.0
_REMOTE_SESSION_START_RECOVERY_TIMEOUT_SECONDS = 15.0
# One start watch is a bounded server-side wait, never a client redial loop.
# The cap matches the ordinary remote-command budget above, so the CLI's default
# 120-second watch costs exactly one connection; a longer watch costs one more
# per cap rather than one per polling interval.
MAX_REMOTE_SESSION_START_WAIT_SECONDS = _REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS
_REMOTE_SESSION_START_WAIT_TRANSPORT_MARGIN_SECONDS = 15.0
_REMOTE_API_READINESS_TIMEOUT_SECONDS = 60.0
# MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES and MAX_SESSION_START_ERROR_CHARS
# moved to session_wire_models.py (#231 R8(iii)) -- they bound wire-model
# Field() constraints there; imported below for the business logic here that
# must agree with the same bound.
MAX_OWNED_SESSION_CLEANUP_FINALIZE_BYTES = MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES + 256 * 1024
_MAX_PROC_RECORD_BYTES = 1024 * 1024
_MAX_REMOTE_SESSION_SCRIPT_BYTES = MAX_CLUSTER_REGISTRY_BYTES + 128 * 1024
_MAX_REMOTE_SESSION_STDOUT_BYTES = 1024 * 1024
_MAX_REMOTE_SESSION_STDERR_BYTES = 1024 * 1024
_MAX_API_HEALTH_RESPONSE_BYTES = 64 * 1024
_MAX_API_STARTUP_RECEIPT_BYTES = 64 * 1024
_API_STARTUP_RECEIPT_ENV = "CLIO_RELAY_SESSION_STARTUP_RECEIPT"
_SYSTEMD_UNIT_ENV = "CLIO_RELAY_SESSION_SYSTEMD_UNIT"
_SYSTEMD_CGROUP_ENV = "CLIO_RELAY_SESSION_SYSTEMD_CGROUP"
_SYSTEMD_INVOCATION_ENV = "CLIO_RELAY_SESSION_SYSTEMD_INVOCATION_ID"
_SYSTEMD_DESCRIPTION_ENV = "CLIO_RELAY_SESSION_SYSTEMD_DESCRIPTION"


# TODO(#231 rework): _OwnedSessionQueue travels with its sole consumer,
# _promote_resumable_contained_start, in a later split/session-lifecycle
# slice. Kept resident here for now since that function has not moved out of
# this module yet. _FailedStartCleanupQueue already moved to
# session_recovery_attempt_status.py (its primary consumer); this module
# imports it (TYPE_CHECKING) for _execute_owned_failed_start_teardown's own
# still-resident use, which has no import-cycle risk since that Protocol has
# no dependency back on this module.
class _OwnedSessionQueue(Protocol):
    """Typed core-queue surface required by crash-surviving start promotion."""

    root: Path

    def clear_owner_session_closing(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> None:
        """Clear a matching closing marker after exact API recovery."""


def publish_owned_session_api_startup_receipt() -> bool:
    """Publish the signed API identity after gated environment and cgroup entry."""
    receipt_path_raw = os.environ.get(_API_STARTUP_RECEIPT_ENV)
    if receipt_path_raw is None:
        return False
    required_names = (
        "CLIO_RELAY_SESSION_OWNER_TOKEN",
        "CLIO_RELAY_SESSION_GENERATION_ID",
        "CLIO_RELAY_OWNER_SESSION_ID",
        "CLIO_RELAY_OWNER_SESSION_CLUSTER",
        "CLIO_RELAY_API_RELEASE_IDENTITY_SHA256",
        "CLIO_RELAY_CLUSTER_REGISTRY",
        "CLIO_RELAY_SESSION_REGISTRY_SHA256",
        "CLIO_RELAY_SESSION_ROUTE_REVISION",
        _SYSTEMD_UNIT_ENV,
        _SYSTEMD_CGROUP_ENV,
        _SYSTEMD_INVOCATION_ENV,
        _SYSTEMD_DESCRIPTION_ENV,
    )
    values = {name: os.environ.get(name) for name in required_names}
    if any(not value for value in values.values()):
        raise RelayError("owned API startup receipt environment is incomplete")
    owner_token = cast(str, values["CLIO_RELAY_SESSION_OWNER_TOKEN"])
    generation_id = validate_durable_record_id(
        cast(str, values["CLIO_RELAY_SESSION_GENERATION_ID"])
    )
    receipt_path = Path(receipt_path_raw)
    registry_path = Path(cast(str, values["CLIO_RELAY_CLUSTER_REGISTRY"]))
    expected_receipt = registry_path.parent / f"api-startup-{generation_id}.json"
    if receipt_path != expected_receipt:
        raise RelayError("owned API startup receipt path is not generation-scoped")
    invocation_id = cast(str, values[_SYSTEMD_INVOCATION_ENV])
    if os.environ.get("INVOCATION_ID") != invocation_id:
        raise RelayError("owned API process systemd invocation identity mismatched")
    pid = os.getpid()
    process_identity = session_process_scope._read_proc_identity(proc_root=Path("/proc"), pid=pid)
    observed_cgroup = session_process_scope._current_linux_cgroup_path(pid=pid)
    expected_cgroup = Path(cast(str, values[_SYSTEMD_CGROUP_ENV])).resolve(strict=True)
    if observed_cgroup != expected_cgroup:
        raise RelayError("owned API process is outside its persisted cgroup")
    document: dict[str, object] = {
        "schema_version": "clio-relay.owner-session-api-startup.v1",
        "cluster": values["CLIO_RELAY_OWNER_SESSION_CLUSTER"],
        "session_id": values["CLIO_RELAY_OWNER_SESSION_ID"],
        "session_generation_id": generation_id,
        "api_pid": pid,
        "api_pgid": process_identity.process_group_id,
        "process_start_ticks": process_identity.start_ticks,
        "api_release_identity_sha256": values["CLIO_RELAY_API_RELEASE_IDENTITY_SHA256"],
        "cluster_registry_path": str(registry_path),
        "cluster_registry_sha256": values["CLIO_RELAY_SESSION_REGISTRY_SHA256"],
        "cluster_route_revision": values["CLIO_RELAY_SESSION_ROUTE_REVISION"],
        "systemd_unit": values[_SYSTEMD_UNIT_ENV],
        "systemd_cgroup_path": str(expected_cgroup),
        "systemd_invocation_id": invocation_id,
        "systemd_description": values[_SYSTEMD_DESCRIPTION_ENV],
        "observed_at": datetime.now(UTC).isoformat(),
    }
    document["hmac_sha256"] = session_startup_receipt._startup_receipt_signature(
        document, owner_token=owner_token
    )
    session_startup_receipt._atomic_write_startup_receipt(
        receipt_path,
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )
    os.environ.pop("CLIO_RELAY_SESSION_OWNER_TOKEN", None)
    return True


def inspect_owned_session_recovery_status(
    *,
    cluster: str,
    session_id: str,
    core_dir: Path,
    home: Path | None = None,
    proc_root: Path = Path("/proc"),
    effective_uid: int | None = None,
    transaction: _OwnedSessionTransaction | None = None,
    expected_start_operation_id: str | None = None,
    expected_cluster_route_revision: str | None = None,
) -> OwnedSessionRecoveryStatus:
    """Inspect durable metadata, process identity, and core admission for recovery.

    This function is deliberately read-only.  A dead process is recoverable only
    when its protected metadata, exact cluster registry, and authoritative core
    generation all agree.  A live or reused PID must additionally pass the full
    process identity check before any teardown coordinator may mutate state.
    """
    from clio_relay.core_queue import ClioCoreQueue

    _validate_session(session_id=session_id, remote_api_port=1)
    if not cluster:
        raise RelayError("cluster must not be empty")
    # Keep path identity aligned with ``open_owned_session_transaction``, which
    # pins the canonical home directory before walking its private components.
    # Cluster homes are commonly exposed through an alias (for example,
    # ``/home/user`` -> ``/mnt/common/user``); comparing an unresolved alias to
    # the pinned transaction path otherwise rejects our own committed metadata.
    selected_home = (home or Path.home()).resolve(strict=True)
    session_dir = selected_home / ".local" / "share" / "clio-relay" / "sessions" / session_id
    metadata_path = session_dir / "metadata.json"
    errors: list[str] = []
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    uid = (
        get_effective_uid()
        if effective_uid is None and get_effective_uid is not None
        else effective_uid
    )
    try:
        if transaction is None:
            document, _ = _read_owned_session_document(
                metadata_path,
                label="owned session metadata",
                effective_uid=uid,
            )
        else:
            if transaction.session_id != session_id or transaction.path != session_dir:
                raise RelayError("owned session transaction identity does not match recovery")
            transaction_document = transaction.read_json("metadata.json")
            if transaction_document is None:  # pragma: no cover - required read
                raise RelayError("owned session metadata is unavailable")
            document = transaction_document
    except RelayError as exc:
        if transaction is not None:
            attempt_status = (
                session_recovery_attempt_status._inspect_owned_session_start_attempt_status(
                    cluster=cluster,
                    session_id=session_id,
                    core_dir=core_dir,
                    proc_root=proc_root,
                    transaction=transaction,
                    metadata_error=str(exc),
                    expected_start_operation_id=expected_start_operation_id,
                    expected_cluster_route_revision=expected_cluster_route_revision,
                )
            )
            if attempt_status is not None:
                return attempt_status
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            errors=[str(exc)],
        )

    if document.get("schema_version") == ("clio-relay.owner-session-failed-cleaned-receipt.v1"):
        if transaction is None:
            try:
                with open_owned_session_transaction(
                    session_id=session_id,
                    create=False,
                    timeout_seconds=10.0,
                    home=selected_home,
                ) as pinned_transaction:
                    pinned_document = pinned_transaction.read_json("metadata.json")
                    if pinned_document is None:  # pragma: no cover - required read
                        raise RelayError("failed-start cleanup receipt is unavailable")
                    inspect_failed_cleaned = (  # noqa: E501 -- no shorter qualified name exists
                        session_recovery_cleaned_receipt._inspect_owned_session_failed_cleaned_receipt
                    )
                    return inspect_failed_cleaned(
                        cluster=cluster,
                        session_id=session_id,
                        document=pinned_document,
                        core_dir=core_dir,
                        transaction=pinned_transaction,
                        proc_root=proc_root,
                    )
            except RelayError as exc:
                return OwnedSessionRecoveryStatus(
                    cluster=cluster,
                    session_id=session_id,
                    errors=[str(exc)],
                )
        return session_recovery_cleaned_receipt._inspect_owned_session_failed_cleaned_receipt(
            cluster=cluster,
            session_id=session_id,
            document=document,
            core_dir=core_dir,
            transaction=transaction,
            proc_root=proc_root,
        )

    if document.get("schema_version") == "clio-relay.owner-session-cleanup-receipt.v1":
        if transaction is None and document.get("coordinator_report_ref") is not None:
            try:
                with open_owned_session_transaction(
                    session_id=session_id,
                    create=False,
                    timeout_seconds=10.0,
                    home=selected_home,
                ) as pinned_transaction:
                    return inspect_owned_session_recovery_status(
                        cluster=cluster,
                        session_id=session_id,
                        core_dir=core_dir,
                        home=selected_home,
                        proc_root=proc_root,
                        effective_uid=uid,
                        transaction=pinned_transaction,
                    )
            except RelayError as exc:
                return OwnedSessionRecoveryStatus(
                    cluster=cluster,
                    session_id=session_id,
                    errors=[str(exc)],
                )
        return session_recovery_cleanup_receipt._inspect_owned_session_cleanup_receipt(
            cluster=cluster,
            session_id=session_id,
            document=document,
            core_dir=core_dir,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )

    owner = document.get("owner")
    generation = document.get("session_generation_id")
    recorded_cluster = document.get("cluster")
    owner_token = document.get("owner_token")
    api_pid = document.get("api_pid")
    api_pgid = document.get("api_pgid")
    process_start = document.get("process_start_ticks")
    registry_path_raw = document.get("cluster_registry_path")
    registry_sha256 = document.get("cluster_registry_sha256")
    route_revision = document.get("cluster_route_revision")
    release_identity = document.get("api_release_identity")
    release_sha256 = document.get("api_release_identity_sha256")
    started_at = document.get("started_at")
    remote_api_port = document.get("remote_api_port")
    containment_mode = document.get("containment_mode")
    systemd_unit = document.get("systemd_unit")
    systemd_cgroup_path = document.get("systemd_cgroup_path")
    systemd_invocation_id = document.get("systemd_invocation_id")
    systemd_description = document.get("systemd_description")
    containment_broker_pid = document.get("containment_broker_pid")
    containment_broker_start = document.get("containment_broker_start_identity")
    startup_receipt_path_raw = document.get("api_startup_receipt_path")
    startup_receipt_sha256 = document.get("api_startup_receipt_sha256")
    raw_input_policy = document.get("input_policy")

    try:
        validated_release = SessionApiReleaseIdentity.model_validate(release_identity)
    except ValueError:
        validated_release = None
    try:
        validated_input_policy = (
            OwnedSessionInputPolicy.model_validate(raw_input_policy)
            if "input_policy" in document
            else None
        )
    except ValueError:
        validated_input_policy = None
    try:
        parsed_started_at = (
            datetime.fromisoformat(started_at) if isinstance(started_at, str) else None
        )
    except ValueError:
        parsed_started_at = None

    try:
        validated_generation = (
            validate_durable_record_id(generation) if isinstance(generation, str) else None
        )
    except ValueError:
        validated_generation = None
    expected_metadata_keys = {
        "cluster",
        "session_id",
        "remote_api_port",
        "api_pid",
        "api_pgid",
        "owner_token",
        "session_generation_id",
        "api_release_identity",
        "api_release_identity_sha256",
        "cluster_registry_path",
        "cluster_registry_sha256",
        "cluster_route_revision",
        "cluster_authority_verified",
        "process_start_ticks",
        "containment_mode",
        "systemd_unit",
        "systemd_cgroup_path",
        "systemd_invocation_id",
        "systemd_description",
        "containment_broker_pid",
        "containment_broker_start_identity",
        "api_startup_receipt_path",
        "api_startup_receipt_sha256",
        "started_at",
        "owner",
    }
    current_metadata_keys = expected_metadata_keys | {"input_policy"}
    metadata_verified = bool(
        frozenset(document) in {frozenset(expected_metadata_keys), frozenset(current_metadata_keys)}
        and owner == "clio-relay"
        and document.get("session_id") == session_id
        and recorded_cluster == cluster
        and isinstance(remote_api_port, int)
        and not isinstance(remote_api_port, bool)
        and remote_api_port > 0
        and validated_generation is not None
        and isinstance(owner_token, str)
        and len(owner_token) == 64
        and all(character in "0123456789abcdef" for character in owner_token)
        and isinstance(api_pid, int)
        and not isinstance(api_pid, bool)
        and api_pid > 1
        and isinstance(api_pgid, int)
        and not isinstance(api_pgid, bool)
        and api_pgid > 0
        and isinstance(process_start, str)
        and process_start.isdigit()
        and isinstance(registry_path_raw, str)
        and isinstance(registry_sha256, str)
        and len(registry_sha256) == 64
        and all(character in "0123456789abcdef" for character in registry_sha256)
        and isinstance(route_revision, str)
        and bool(route_revision)
        and document.get("cluster_authority_verified") is True
        and validated_release is not None
        and isinstance(release_sha256, str)
        and len(release_sha256) == 64
        and all(character in "0123456789abcdef" for character in release_sha256)
        and validated_release.sha256() == release_sha256
        and ("input_policy" not in document or validated_input_policy is not None)
        and containment_mode == "linux_systemd_scope"
        and systemd_unit == f"clio-relay-session-{validated_generation}.scope"
        and isinstance(systemd_cgroup_path, str)
        and bool(systemd_cgroup_path)
        and isinstance(systemd_invocation_id, str)
        and len(systemd_invocation_id) == 32
        and all(character in "0123456789abcdef" for character in systemd_invocation_id)
        and isinstance(systemd_description, str)
        and systemd_description.startswith(
            f"clio-relay-owned-session:{session_id}:{validated_generation}:"
        )
        and isinstance(containment_broker_pid, int)
        and not isinstance(containment_broker_pid, bool)
        and containment_broker_pid > 1
        and isinstance(containment_broker_start, str)
        and bool(containment_broker_start)
        and startup_receipt_path_raw
        == str(session_dir / f"api-startup-{validated_generation}.json")
        and isinstance(startup_receipt_sha256, str)
        and len(startup_receipt_sha256) == 64
        and all(character in "0123456789abcdef" for character in startup_receipt_sha256)
        and parsed_started_at is not None
        and parsed_started_at.tzinfo is not None
    )
    if not metadata_verified:
        errors.append("owned session metadata identity is incomplete or mismatched")

    startup_receipt_verified = False
    if (
        metadata_verified
        and isinstance(startup_receipt_path_raw, str)
        and isinstance(startup_receipt_sha256, str)
        and isinstance(owner_token, str)
        and isinstance(api_pid, int)
        and isinstance(api_pgid, int)
        and isinstance(process_start, str)
        and isinstance(systemd_unit, str)
        and isinstance(systemd_cgroup_path, str)
        and isinstance(systemd_invocation_id, str)
        and isinstance(systemd_description, str)
        and isinstance(release_sha256, str)
        and isinstance(registry_path_raw, str)
        and isinstance(registry_sha256, str)
        and isinstance(route_revision, str)
    ):
        try:
            receipt_path = Path(startup_receipt_path_raw)
            if transaction is None:
                receipt_document, receipt_payload = _read_owned_session_document(
                    receipt_path,
                    label="owned API startup receipt",
                    effective_uid=uid,
                )
            else:
                receipt_payload = transaction.read_bytes(
                    receipt_path.name,
                    maximum_bytes=_MAX_API_STARTUP_RECEIPT_BYTES,
                )
                if receipt_payload is None:  # pragma: no cover - required read
                    raise RelayError("owned API startup receipt is unavailable")
                raw_receipt = cast(object, json.loads(receipt_payload))
                if not isinstance(raw_receipt, dict):
                    raise RelayError("owned API startup receipt is not a JSON object")
                receipt_document = {
                    str(key): value
                    for key, value in cast(dict[object, object], raw_receipt).items()
                }
            expected_receipt = {
                "cluster": cluster,
                "session_id": session_id,
                "session_generation_id": validated_generation,
                "api_pid": api_pid,
                "api_pgid": api_pgid,
                "process_start_ticks": process_start,
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": registry_path_raw,
                "cluster_registry_sha256": registry_sha256,
                "cluster_route_revision": route_revision,
                "systemd_unit": systemd_unit,
                "systemd_cgroup_path": systemd_cgroup_path,
                "systemd_invocation_id": systemd_invocation_id,
                "systemd_description": systemd_description,
            }
            signature = receipt_document.get("hmac_sha256")
            startup_receipt_verified = bool(
                hashlib.sha256(receipt_payload).hexdigest() == startup_receipt_sha256
                and receipt_document.get("schema_version")
                == "clio-relay.owner-session-api-startup.v1"
                and all(
                    receipt_document.get(key) == value for key, value in expected_receipt.items()
                )
                and isinstance(signature, str)
                and hmac.compare_digest(
                    signature,
                    session_startup_receipt._startup_receipt_signature(
                        receipt_document, owner_token=owner_token
                    ),
                )
            )
        except (OSError, RelayError, ValueError) as exc:
            errors.append(str(exc))
        if not startup_receipt_verified:
            errors.append("owned API startup receipt identity is invalid")

    cluster_registry_verified = False
    registry_path: Path | None = None
    if (
        metadata_verified
        and validated_generation is not None
        and isinstance(registry_path_raw, str)
    ):
        registry_path = Path(registry_path_raw)
        expected_registry_path = session_dir / f"cluster-registry-{validated_generation}.json"
        if registry_path != expected_registry_path:
            errors.append("owned session cluster registry path is not generation-scoped")
        else:
            try:
                if transaction is None:
                    registry_document, registry_bytes = _read_owned_session_document(
                        registry_path,
                        label="owned session cluster registry",
                        effective_uid=uid,
                    )
                else:
                    registry_bytes = transaction.read_bytes(
                        registry_path.name,
                        maximum_bytes=_MAX_OWNED_SESSION_DOCUMENT_BYTES,
                    )
                    if registry_bytes is None:  # pragma: no cover - required read
                        raise RelayError("owned session cluster registry is unavailable")
                    raw_registry = cast(object, json.loads(registry_bytes))
                    if not isinstance(raw_registry, dict):
                        raise RelayError("owned session cluster registry is not a JSON object")
                    registry_document = {
                        str(key): value
                        for key, value in cast(dict[object, object], raw_registry).items()
                    }
                registry = ClusterRegistry.model_validate(registry_document)
                # clio-relay#217: the frozen snapshot's sha256 already proves these
                # exact cluster-definition bytes are untampered (tamper-clean); do
                # NOT also require a fresh cluster_route_revision() recomputation to
                # match the value recorded at mint time. cluster_route_revision()'s
                # canonicalization can change between relay releases, and
                # recomputing it here with a different algorithm generation than
                # the one that minted this session strands every session across an
                # upgrade with a false "digest or identity mismatched" refusal --
                # a version-skew artifact, never evidence of tampering. Trust the
                # tamper-clean recorded route revision instead.
                cluster_registry_verified = bool(
                    hashlib.sha256(registry_bytes).hexdigest() == registry_sha256
                    and set(registry.clusters) == {cluster}
                    and registry.clusters[cluster].name == cluster
                )
                if cluster_registry_verified:
                    recomputed_route_revision = cluster_route_revision(registry.clusters[cluster])
                    if recomputed_route_revision != route_revision:
                        logger.warning(
                            "cluster_route_revision_algorithm_skew: session %r cluster %r "
                            "recorded route revision %r but the installed package "
                            "recomputes %r from the identical tamper-clean snapshot; "
                            "trusting the recorded value (clio-relay#217)",
                            session_id,
                            cluster,
                            route_revision,
                            recomputed_route_revision,
                        )
            except (RelayError, ValueError) as exc:
                errors.append(str(exc))
            if not cluster_registry_verified and not any(
                "cluster registry" in error for error in errors
            ):
                errors.append("owned session cluster registry digest or identity mismatched")

    process_state: Literal[
        "absent",
        "owned_running",
        "owned_terminal",
        "reused",
        "foreign",
        "cleanup_pending",
        "already_closed",
        "unverified",
    ] = "unverified"
    leader_process_state: Literal[
        "absent",
        "owned_running",
        "owned_terminal",
        "reused",
        "foreign",
        "unverified",
    ] = "unverified"
    running = False
    process_absence_verified = False
    generation_processes: list[_OwnedGenerationProcess] = []
    generation_process_scan_verified = False
    if (
        metadata_verified
        and startup_receipt_verified
        and isinstance(api_pid, int)
        and isinstance(api_pgid, int)
        and isinstance(process_start, str)
        and validated_generation is not None
        and isinstance(systemd_unit, str)
        and isinstance(systemd_cgroup_path, str)
        and isinstance(systemd_invocation_id, str)
        and isinstance(systemd_description, str)
    ):
        try:
            generation_processes = session_process_scope._recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            generation_process_scan_verified = True
        except RelayError as exc:
            errors.append(str(exc))
        proc = proc_root / str(api_pid)
        try:
            stat_text = session_process_scope._read_bounded_proc_bytes(
                proc / "stat",
                maximum_bytes=_MAX_PROC_RECORD_BYTES,
            ).decode("utf-8")
        except FileNotFoundError:
            leader_process_state = "absent"
        except (OSError, UnicodeDecodeError, RelayError) as exc:
            errors.append(f"could not inspect recorded API pid {api_pid}: {exc}")
        else:
            try:
                fields = stat_text.rsplit(")", 1)[1].split()
                observed_state = fields[0]
                observed_pgid = int(fields[2])
                observed_start = fields[19]
            except (IndexError, ValueError) as exc:
                errors.append(f"recorded API pid {api_pid} has invalid proc stat: {exc}")
            else:
                if observed_start != process_start:
                    leader_process_state = "reused"
                    errors.append(f"recorded API pid {api_pid} was reused")
                elif observed_pgid != api_pgid:
                    leader_process_state = "foreign"
                    errors.append(f"recorded API pid {api_pid} changed process group")
                elif observed_state == "Z":
                    leader_process_state = "owned_terminal"
                elif any(
                    process.pid == api_pid
                    and process.process_group_id == api_pgid
                    and process.start_ticks == process_start
                    for process in generation_processes
                ) and session_process_scope._is_clio_relay_api_leader(
                    proc_root=proc_root, pid=api_pid
                ):
                    leader_process_state = "owned_running"
                else:
                    leader_process_state = "foreign"
                    errors.append(f"recorded API pid {api_pid} failed process identity")

        if leader_process_state in {"reused", "foreign"}:
            process_state = leader_process_state
        elif generation_processes:
            process_state = "owned_running"
            running = True
        elif generation_process_scan_verified:
            process_state = (
                "owned_terminal" if leader_process_state == "owned_terminal" else "absent"
            )
            process_absence_verified = True

    admission_status: dict[str, object] | None = None
    durable_generation_verified = False
    if validated_generation is not None:
        try:
            admission_status = ClioCoreQueue(core_dir).owner_session_generation_status(
                session_id,
                session_generation_id=validated_generation,
            )
            active_generation = admission_status.get("active_generation_id")
            closing_generation = admission_status.get("closing_generation_id")
            durable_generation_verified = bool(
                admission_status.get("owner_session_id") == session_id
                and admission_status.get("session_generation_id") == validated_generation
                and active_generation in {None, validated_generation}
                and closing_generation in {None, validated_generation}
                and (
                    (
                        admission_status.get("open") is True
                        and active_generation == validated_generation
                        and closing_generation is None
                    )
                    or (
                        admission_status.get("closing") is True
                        and closing_generation == validated_generation
                    )
                )
            )
        except (OSError, RelayError, ValueError) as exc:
            errors.append(f"could not verify durable owner-session generation: {exc}")
        if not durable_generation_verified:
            errors.append("durable owner-session generation is not active or closing")

    start_operation_id: str | None = None
    start_phase: Literal["pending", "admitted", "scope_bound", "contained"] | None = None
    start_attempt_verified = False
    start_replace: bool | None = None
    start_require_token: bool | None = None
    start_input_policy: OwnedSessionInputPolicy | None = None
    start_expected_release_sha256: str | None = None
    start_attempt_release_sha256: str | None = None
    start_error: str | None = None
    if transaction is not None:
        attempt_status = (
            session_recovery_attempt_status._inspect_owned_session_start_attempt_status(
                cluster=cluster,
                session_id=session_id,
                core_dir=core_dir,
                proc_root=proc_root,
                transaction=transaction,
                metadata_error="owned session metadata exists without its start journal",
                expected_start_operation_id=expected_start_operation_id,
                expected_cluster_route_revision=expected_cluster_route_revision,
            )
        )
        if attempt_status is not None and attempt_status.start_state == "not_current":
            return attempt_status
        if (
            attempt_status is not None
            and attempt_status.start_attempt_verified
            and attempt_status.session_generation_id == validated_generation
            and attempt_status.remote_api_port == remote_api_port
            and attempt_status.cluster_route_revision == route_revision
        ):
            start_operation_id = attempt_status.start_operation_id
            start_phase = attempt_status.start_phase
            start_attempt_verified = True
            start_replace = attempt_status.start_replace
            start_require_token = attempt_status.start_require_token
            start_input_policy = attempt_status.start_input_policy
            start_expected_release_sha256 = (
                attempt_status.start_expected_api_release_identity_sha256
            )
            bound_attempt = session_start_attempt_validation._validated_start_attempt(
                transaction,
                cluster=cluster,
                session_id=session_id,
                start_operation_id=start_operation_id,
                cluster_route_revision_value=cast(str, route_revision),
            )
            if bound_attempt is None:  # pragma: no cover - status validated the same journal
                raise RelayError("owned-session start journal disappeared during inspection")
            start_attempt_release_sha256 = cast(
                str,
                bound_attempt["api_release_identity_sha256"],
            )
            start_error = attempt_status.start_error
        elif expected_start_operation_id is not None:
            errors.extend(
                attempt_status.errors
                if attempt_status is not None and attempt_status.errors
                else ["owned-session start selector has no exact durable journal"]
            )

    start_release_committed = bool(
        start_attempt_verified
        and isinstance(release_sha256, str)
        and start_attempt_release_sha256 == release_sha256
    )
    replacement_in_progress = bool(
        start_attempt_verified and start_replace is True and not start_release_committed
    )
    if start_attempt_verified and not (start_release_committed or replacement_in_progress):
        errors.append("owned-session start journal release does not match committed metadata")

    acceptable_process_state = process_state in {
        "absent",
        "owned_running",
        "owned_terminal",
    }
    recovery_verified = bool(
        metadata_verified
        and cluster_registry_verified
        and durable_generation_verified
        and acceptable_process_state
        and not errors
    )
    return OwnedSessionRecoveryStatus(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=validated_generation,
        start_operation_id=start_operation_id,
        cluster_route_revision=route_revision if isinstance(route_revision, str) else None,
        owner=owner if isinstance(owner, str) else None,
        api_pid=api_pid if isinstance(api_pid, int) and not isinstance(api_pid, bool) else None,
        remote_api_port=(
            remote_api_port
            if isinstance(remote_api_port, int) and not isinstance(remote_api_port, bool)
            else None
        ),
        process_start_marker=process_start if isinstance(process_start, str) else None,
        leader_process_state=leader_process_state,
        process_state=process_state,
        running=running,
        process_absence_verified=process_absence_verified,
        generation_process_pids=[process.pid for process in generation_processes],
        generation_process_absence_verified=(
            generation_process_scan_verified and not generation_processes
        ),
        metadata_verified=metadata_verified,
        cluster_registry_verified=cluster_registry_verified,
        durable_generation_verified=durable_generation_verified,
        ownership_verified=recovery_verified,
        recovery_verified=recovery_verified,
        api_release_identity=validated_release,
        api_release_identity_verified=bool(validated_release is not None and running),
        ownership_token_present=isinstance(owner_token, str) and bool(owner_token),
        admission_status=admission_status,
        start_state=(
            "ready"
            if recovery_verified and start_release_committed
            else "starting"
            if recovery_verified and replacement_in_progress
            else "unknown"
        ),
        start_phase=start_phase,
        start_attempt_verified=start_attempt_verified,
        start_retryable=bool(recovery_verified and replacement_in_progress),
        start_replace=start_replace,
        start_require_token=start_require_token,
        start_input_policy=start_input_policy,
        start_expected_api_release_identity_sha256=start_expected_release_sha256,
        start_error=start_error,
        errors=errors,
    )


def _read_owned_session_document(
    path: Path,
    *,
    label: str,
    effective_uid: int | None,
) -> tuple[dict[str, object], bytes]:
    """Read one bounded, regular, owner-scoped JSON document without following links."""
    descriptor: int | None = None
    parent_descriptor: int | None = None
    session_descriptor: int | None = None
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    binary = getattr(os, "O_BINARY", 0)
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    try:
        if os.name == "posix" and no_follow and directory_flag:
            parent_descriptor = os.open(
                path.parent.parent,
                os.O_RDONLY | directory_flag | no_follow,
            )
            parent_status = os.fstat(parent_descriptor)
            if not stat.S_ISDIR(parent_status.st_mode):
                raise RelayError(f"{label} parent is not a directory")
            if effective_uid is not None and parent_status.st_uid != effective_uid:
                raise RelayError(f"{label} parent is not owned by the current user")
            session_descriptor = os.open(
                path.parent.name,
                os.O_RDONLY | directory_flag | no_follow,
                dir_fd=parent_descriptor,
            )
            session_status = os.fstat(session_descriptor)
            if not stat.S_ISDIR(session_status.st_mode):
                raise RelayError(f"{label} session path is not a directory")
            if effective_uid is not None and session_status.st_uid != effective_uid:
                raise RelayError(f"{label} session path is not owned by the current user")
            descriptor = os.open(
                path.name,
                os.O_RDONLY | binary | no_follow,
                dir_fd=session_descriptor,
            )
        else:
            for directory, directory_label in (
                (path.parent.parent, "parent"),
                (path.parent, "session path"),
            ):
                directory_status = directory.lstat()
                if not stat.S_ISDIR(directory_status.st_mode):
                    raise RelayError(f"{label} {directory_label} is not a directory")
                if effective_uid is not None and directory_status.st_uid != effective_uid:
                    raise RelayError(f"{label} {directory_label} is not owned by the current user")
            path_status = path.lstat()
            if not stat.S_ISREG(path_status.st_mode):
                raise RelayError(f"{label} is not a regular file")
            if effective_uid is not None and path_status.st_uid != effective_uid:
                raise RelayError(f"{label} is not owned by the current user")
            descriptor = os.open(path, os.O_RDONLY | binary | no_follow)
        file_status = os.fstat(descriptor)
        if not stat.S_ISREG(file_status.st_mode):
            raise RelayError(f"{label} is not a regular file")
        if effective_uid is not None and file_status.st_uid != effective_uid:
            raise RelayError(f"{label} is not owned by the current user")
        if not 0 < file_status.st_size <= _MAX_OWNED_SESSION_DOCUMENT_BYTES:
            raise RelayError(f"{label} has an invalid size")
        payload = os.read(descriptor, _MAX_OWNED_SESSION_DOCUMENT_BYTES + 1)
        if len(payload) != file_status.st_size:
            raise RelayError(f"{label} changed while it was read")
    except FileNotFoundError as exc:
        raise RelayError(f"{label} is unavailable") from exc
    except RelayError:
        raise
    except OSError as exc:
        raise RelayError(f"{label} cannot be opened safely: {exc}") from exc
    finally:
        for open_descriptor in (descriptor, session_descriptor, parent_descriptor):
            if open_descriptor is not None:
                os.close(open_descriptor)
    try:
        raw = cast(object, json.loads(payload))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelayError(f"{label} is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise RelayError(f"{label} is not a JSON object")
    return {str(key): value for key, value in cast(dict[object, object], raw).items()}, payload


class _RecoveredStartProbe:
    """Minimal process observation used while adopting an exact persistent scope."""

    def poll(self) -> None:
        """The receipt and scope checks, not a stale parent handle, prove liveness."""
        return None


def _promote_resumable_contained_start(
    *,
    transaction: _OwnedSessionTransaction,
    attempt: dict[str, object],
    request: OwnedSessionStartRequest,
    release_identity: SessionApiReleaseIdentity,
    queue: _OwnedSessionQueue,
    proc_root: Path,
    home: Path | None,
) -> OwnedSessionStartReceipt | None:
    """Commit ready metadata when an exact crash-surviving API already exists."""
    if attempt.get("start_phase") != "contained" or attempt.get("error") is not None:
        return None
    generation_id = cast(str, attempt["session_generation_id"])
    owner_token = cast(str, attempt["owner_token"])
    receipt_name = f"api-startup-{generation_id}.json"
    receipt_path = transaction.path / receipt_name
    expected_receipt = {
        "cluster": request.cluster,
        "session_id": request.session_id,
        "session_generation_id": generation_id,
        "api_release_identity_sha256": release_identity.sha256(),
        "cluster_registry_path": attempt["cluster_registry_path"],
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "systemd_unit": attempt["systemd_unit"],
        "systemd_cgroup_path": attempt["systemd_cgroup_path"],
        "systemd_invocation_id": attempt["systemd_invocation_id"],
        "systemd_description": attempt["systemd_description"],
    }
    probe = cast(subprocess.Popen[Any], _RecoveredStartProbe())
    try:
        process_identity = session_api_readiness._wait_for_api_startup_receipt(
            transaction=transaction,
            process=probe,
            receipt_name=receipt_name,
            owner_token=owner_token,
            expected=expected_receipt,
            proc_root=proc_root,
        )
        ready_seconds = session_api_readiness._wait_for_api_ready(
            process=cast(subprocess.Popen[bytes], probe),
            port=request.remote_api_port,
            require_token=request.require_token,
        )
        final_process_identity = session_api_readiness._wait_for_api_startup_receipt(
            transaction=transaction,
            process=probe,
            receipt_name=receipt_name,
            owner_token=owner_token,
            expected=expected_receipt,
            proc_root=proc_root,
        )
        if final_process_identity != process_identity:
            raise RelayError("recovered owned API identity changed after health verification")
    except RelayError:
        return None
    receipt_payload = transaction.read_bytes(
        receipt_name,
        maximum_bytes=_MAX_API_STARTUP_RECEIPT_BYTES,
    )
    if receipt_payload is None:  # pragma: no cover - required read
        return None
    metadata = {
        "cluster": request.cluster,
        "session_id": request.session_id,
        "remote_api_port": request.remote_api_port,
        "api_pid": process_identity.pid,
        "api_pgid": process_identity.process_group_id,
        "owner_token": owner_token,
        "session_generation_id": generation_id,
        "api_release_identity": release_identity.model_dump(mode="json"),
        "api_release_identity_sha256": release_identity.sha256(),
        "cluster_registry_path": attempt["cluster_registry_path"],
        "cluster_registry_sha256": request.cluster_registry_sha256,
        "cluster_route_revision": request.cluster_route_revision,
        "cluster_authority_verified": True,
        "input_policy": request.input_policy.model_dump(mode="json"),
        "process_start_ticks": process_identity.start_ticks,
        "containment_mode": "linux_systemd_scope",
        "systemd_unit": attempt["systemd_unit"],
        "systemd_cgroup_path": attempt["systemd_cgroup_path"],
        "systemd_invocation_id": attempt["systemd_invocation_id"],
        "systemd_description": attempt["systemd_description"],
        "containment_broker_pid": attempt["containment_broker_pid"],
        "containment_broker_start_identity": attempt["containment_broker_start_identity"],
        "api_startup_receipt_path": str(receipt_path),
        "api_startup_receipt_sha256": hashlib.sha256(receipt_payload).hexdigest(),
        "started_at": datetime.now(UTC).isoformat(),
        "owner": "clio-relay",
    }
    transaction.atomic_write("api.pid", f"{process_identity.pid}\n".encode("ascii"))
    transaction.atomic_write("metadata.json", json.dumps(metadata, indent=2).encode("utf-8"))
    queue.clear_owner_session_closing(request.session_id, session_generation_id=generation_id)
    promoted_status = inspect_owned_session_recovery_status(
        cluster=request.cluster,
        session_id=request.session_id,
        core_dir=queue.root,
        home=home,
        proc_root=proc_root,
        transaction=transaction,
        expected_start_operation_id=request.start_operation_id,
        expected_cluster_route_revision=request.cluster_route_revision,
    )
    if not (
        promoted_status.recovery_verified
        and promoted_status.leader_process_state == "owned_running"
        and promoted_status.api_pid == process_identity.pid
        and promoted_status.ownership_verified
        and promoted_status.session_generation_id == generation_id
        and promoted_status.start_attempt_verified
    ):
        raise RelayError("recovered owned API did not pass post-commit identity verification")
    return OwnedSessionStartReceipt(
        cluster=request.cluster,
        session_id=request.session_id,
        start_operation_id=request.start_operation_id,
        cluster_route_revision=request.cluster_route_revision,
        session_generation_id=generation_id,
        remote_api_port=request.remote_api_port,
        api_pid=process_identity.pid,
        outcome="recovered",
        ready_seconds=ready_seconds,
    )


def execute_owned_session_identity_challenge(
    request: OwnedSessionIdentityChallengeRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> dict[str, object]:
    """Sign one nonce only after pinned metadata and live leader verification."""
    from clio_relay.config import RelaySettings

    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned session challenge cannot verify the effective user")
    uid = get_effective_uid()
    with open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session metadata is unavailable")
        status = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if not (
            status.recovery_verified
            and status.session_generation_id == request.session_generation_id
            and status.running
            and status.leader_process_state == "owned_running"
            and status.api_pid is not None
            and status.api_pid in status.generation_process_pids
        ):
            detail = "; ".join(status.errors) or "live API leader proof was incomplete"
            raise RelayError(f"owned session identity challenge was refused: {detail}")
        owner_token = document.get("owner_token")
        if not isinstance(owner_token, str) or len(owner_token) != 64:
            raise RelayError("owned session identity challenge token is invalid")
        message = "\n".join(
            (
                "clio-relay.session-identity.v1",
                request.cluster,
                request.session_id,
                request.session_generation_id,
                request.nonce,
            )
        ).encode("utf-8")
        signature = hmac.new(
            owner_token.encode("utf-8"),
            message,
            hashlib.sha256,
        ).hexdigest()
        return {
            "schema_version": "clio-relay.session-identity.v1",
            "cluster": request.cluster,
            "session_id": request.session_id,
            "session_generation_id": request.session_generation_id,
            "nonce": request.nonce,
            "hmac_sha256": signature,
        }


def execute_owned_session_start(
    request: OwnedSessionStartRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> OwnedSessionStartReceipt:
    """Execute one exact cluster-local start under the pinned session transaction."""
    from clio_relay.config import RelaySettings
    from clio_relay.core_queue import ClioCoreQueue

    _validate_session(
        session_id=request.session_id,
        remote_api_port=request.remote_api_port,
    )
    _, registry_payload = session_api_readiness._validated_start_registry(request)
    release_identity = session_api_readiness._current_session_api_release_identity()
    if (
        request.expected_api_release_identity is not None
        and release_identity != request.expected_api_release_identity
    ):
        from clio_relay.session_install_identity import release_identity_is_accepted

        if not release_identity_is_accepted(
            release_identity,
            request.expected_api_release_identity,
        ):
            raise RelayError("session API installation changed after compatibility verification")
    api_token = session_api_readiness._owned_session_api_token(require_token=request.require_token)
    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    queue = ClioCoreQueue(settings_core_dir)
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned session start cannot verify the effective user")
    uid = get_effective_uid()

    with open_owned_session_transaction(
        session_id=request.session_id,
        create=True,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        existing = transaction.read_json("metadata.json", required=False)
        raw_attempt = transaction.read_json("start-attempt.json", required=False)
        legacy_migrated = bool(
            raw_attempt is not None
            and raw_attempt.get("schema_version")
            in {
                "clio-relay.owner-session-attempt.v1",
                "clio-relay.owner-session-attempt.v2",
            }
        )
        if legacy_migrated:
            legacy_attempt = session_start_attempt_validation._validated_start_attempt(
                transaction,
                cluster=request.cluster,
                session_id=request.session_id,
                cluster_registry_sha256=request.cluster_registry_sha256,
                cluster_route_revision_value=request.cluster_route_revision,
                remote_api_port=request.remote_api_port,
                allow_legacy=True,
            )
            if legacy_attempt is None:  # pragma: no cover - raw attempt exists
                raise RelayError("legacy owned-session start attempt disappeared")
            replacement_identity_verified = False
            if existing is not None:
                if not request.replace:
                    raise RelayError(
                        "a pre-policy owned session requires --replace before input policy "
                        "can be proven"
                    )
                legacy_status = inspect_owned_session_recovery_status(
                    cluster=request.cluster,
                    session_id=request.session_id,
                    core_dir=settings_core_dir,
                    home=home,
                    proc_root=proc_root,
                    effective_uid=uid,
                )
                if not (
                    legacy_status.recovery_verified
                    and legacy_status.ownership_verified
                    and legacy_status.session_generation_id
                    == legacy_attempt.get("session_generation_id")
                    and legacy_status.cluster_route_revision
                    == legacy_attempt.get("cluster_route_revision")
                    and legacy_status.remote_api_port == legacy_attempt.get("remote_api_port")
                    and legacy_status.api_release_identity is not None
                    and legacy_status.api_release_identity.sha256()
                    == legacy_attempt.get("api_release_identity_sha256")
                    and session_start_attempt_validation._legacy_start_attempt_matches_metadata(
                        attempt=legacy_attempt,
                        metadata=existing,
                    )
                ):
                    raise RelayError(
                        "legacy start journal does not match exact verified session metadata"
                    )
                replacement_identity_verified = True
                if not request.replace:
                    if not (
                        legacy_status.running
                        and legacy_status.leader_process_state == "owned_running"
                        and legacy_status.api_pid is not None
                    ):
                        raise RelayError(
                            "legacy owned session cannot be adopted without exact live proof; "
                            "use --replace"
                        )
                    if (
                        session_start_attempt_validation._owned_api_requires_token(
                            proc_root=proc_root,
                            pid=legacy_status.api_pid,
                        )
                        is not request.require_token
                    ):
                        raise RelayError("legacy owned session auth policy differs; use --replace")
            elif (
                request.replace
                and legacy_attempt.get("api_release_identity_sha256") != release_identity.sha256()
            ):
                legacy_generation = cast(str, legacy_attempt["session_generation_id"])
                legacy_phase = cast(str, legacy_attempt["start_phase"])
                admission = queue.owner_session_generation_status(
                    request.session_id,
                    session_generation_id=legacy_generation,
                )
                active_generation = admission.get("active_generation_id")
                admission_verified = bool(
                    admission.get("owner_session_id") == request.session_id
                    and admission.get("session_generation_id") == legacy_generation
                    and admission.get("closing_generation_id") is None
                    and (
                        (
                            legacy_phase == "pending"
                            and active_generation in {None, legacy_generation}
                        )
                        or (
                            legacy_phase != "pending"
                            and active_generation == legacy_generation
                            and admission.get("open") is True
                        )
                    )
                )
                if not admission_verified:
                    raise RelayError(
                        "legacy owned-session generation conflicts with durable core admission"
                    )
                if legacy_phase in {"scope_bound", "contained"}:
                    session_process_scope._recorded_scope_processes(
                        proc_root=proc_root,
                        systemd_unit=cast(str, legacy_attempt["systemd_unit"]),
                        systemd_cgroup_path=cast(
                            str,
                            legacy_attempt["systemd_cgroup_path"],
                        ),
                        systemd_invocation_id=cast(
                            str,
                            legacy_attempt["systemd_invocation_id"],
                        ),
                        systemd_description=cast(
                            str,
                            legacy_attempt["systemd_description"],
                        ),
                    )
                replacement_identity_verified = True
            elif (
                existing is None
                and legacy_attempt.get("start_phase") == "contained"
                and not request.replace
            ):
                raise RelayError(
                    "legacy contained start requires --replace because v1 did not bind auth policy"
                )
            prior_attempt = session_start_attempt_validation._migrate_legacy_start_attempt(
                transaction,
                request=request,
                release_identity_sha256=release_identity.sha256(),
                replacement_identity_verified=replacement_identity_verified,
            )
        else:
            prior_attempt = session_start_attempt_validation._validated_start_attempt(
                transaction,
                cluster=request.cluster,
                session_id=request.session_id,
            )
        exact_prior_attempt: dict[str, object] | None = None
        if (
            prior_attempt is not None
            and prior_attempt.get("start_operation_id") == request.start_operation_id
        ):
            exact_prior_attempt = (
                session_start_attempt_validation._validated_resumable_start_attempt(
                    transaction,
                    request=request,
                    release_identity_sha256=release_identity.sha256(),
                )
            )
        if (
            existing is None
            and prior_attempt is not None
            and prior_attempt.get("error") is not None
        ):
            if prior_attempt.get("start_operation_id") == request.start_operation_id:
                raise RelayError("owned-session start operation already failed terminally")
            if not request.replace:
                raise RelayError("a new start operation requires --replace after terminal failure")
            if not (
                prior_attempt.get("api_release_identity_sha256") == release_identity.sha256()
                and prior_attempt.get("cluster_registry_sha256") == request.cluster_registry_sha256
                and prior_attempt.get("cluster_route_revision") == request.cluster_route_revision
                and prior_attempt.get("remote_api_port") == request.remote_api_port
            ):
                raise RelayError(
                    "failed owned-session generation identity changed before replacement"
                )
            replacement_attempt = {
                key: value
                for key, value in prior_attempt.items()
                if key not in {"schema_version", "operation", "observed_at", "error"}
            }
            replacement_attempt.update(
                {
                    "start_operation_id": request.start_operation_id,
                    "replace": request.replace,
                    "require_token": request.require_token,
                    "input_policy": request.input_policy.model_dump(mode="json"),
                    "expected_api_release_identity_sha256": (
                        request.expected_api_release_identity.sha256()
                        if request.expected_api_release_identity is not None
                        else None
                    ),
                }
            )
            session_start_attempt_validation._write_session_attempt(
                transaction,
                operation="start",
                identity=replacement_attempt,
            )
        resumable_attempt = (
            exact_prior_attempt
            or session_start_attempt_validation._validated_resumable_start_attempt(
                transaction,
                request=request,
                release_identity_sha256=release_identity.sha256(),
            )
            if existing is None
            else None
        )
        recorded_generation: str | None = None
        existing_status: OwnedSessionRecoveryStatus | None = None
        if existing is not None:
            existing_status = inspect_owned_session_recovery_status(
                cluster=request.cluster,
                session_id=request.session_id,
                core_dir=settings_core_dir,
                home=home,
                proc_root=proc_root,
                effective_uid=uid,
                transaction=transaction,
            )
            if not existing_status.recovery_verified:
                detail = "; ".join(existing_status.errors) or "recovery proof was incomplete"
                raise RelayError(f"existing owned session recovery was refused: {detail}")
            recorded_generation = existing_status.session_generation_id
            if recorded_generation is None:
                raise RelayError("existing owned session has no durable generation")
            if existing_status.cleanup_receipt:
                if existing.get("cleanup_paths_pending") is True:
                    raise RelayError(
                        "owned session cleanup receipt still has pending file deletion"
                    )
                if not (
                    isinstance(existing_status.admission_status, dict)
                    and existing_status.admission_status.get("closed") is True
                ):
                    raise RelayError(
                        "owned session cleanup is complete but its authoritative generation "
                        "is still closing; retry after the teardown coordinator marks it closed"
                    )
            else:
                same_completed_operation = bool(
                    not legacy_migrated
                    and prior_attempt is not None
                    and prior_attempt.get("start_operation_id") == request.start_operation_id
                    and existing_status.start_attempt_verified
                    and existing_status.start_state == "ready"
                )
                if same_completed_operation and (request.replace or not existing_status.running):
                    raise RelayError(
                        "owned-session start operation already completed; use a fresh operation id"
                    )
                existing_release = existing_status.api_release_identity
                registry_matches = bool(
                    existing.get("cluster_registry_sha256") == request.cluster_registry_sha256
                    and existing.get("cluster_route_revision") == request.cluster_route_revision
                )
                release_matches = existing_release == release_identity
                port_matches = existing_status.remote_api_port == request.remote_api_port
                input_policy_matches = existing.get("input_policy") == (
                    request.input_policy.model_dump(mode="json")
                )
                if existing_status.running and existing_status.leader_process_state != (
                    "owned_running"
                ):
                    if not request.replace:
                        raise RelayError(
                            "owned session API leader is absent while generation children remain; "
                            "use --replace"
                        )
                elif existing_status.running and not request.replace:
                    if not (
                        registry_matches
                        and release_matches
                        and port_matches
                        and input_policy_matches
                    ):
                        raise RelayError("existing owned session identity differs; use --replace")
                    if (
                        prior_attempt is None
                        or prior_attempt.get("require_token") is not request.require_token
                    ):
                        raise RelayError(
                            "existing owned session token policy is not proven; use --replace"
                        )
                    existing_owner_token = cast(str, existing["owner_token"])
                    session_start_attempt_validation._write_session_attempt(
                        transaction,
                        operation="start",
                        identity={
                            "cluster": request.cluster,
                            "session_id": request.session_id,
                            "start_operation_id": request.start_operation_id,
                            "session_generation_id": recorded_generation,
                            "owner_token": existing_owner_token,
                            "owner_token_sha256": hashlib.sha256(
                                existing_owner_token.encode("utf-8")
                            ).hexdigest(),
                            "api_release_identity_sha256": release_identity.sha256(),
                            "expected_api_release_identity_sha256": (
                                request.expected_api_release_identity.sha256()
                                if request.expected_api_release_identity is not None
                                else None
                            ),
                            "cluster_registry_path": existing["cluster_registry_path"],
                            "cluster_registry_sha256": request.cluster_registry_sha256,
                            "cluster_route_revision": request.cluster_route_revision,
                            "remote_api_port": request.remote_api_port,
                            "replace": request.replace,
                            "require_token": request.require_token,
                            "input_policy": request.input_policy.model_dump(mode="json"),
                            "start_phase": "contained",
                            "systemd_unit": existing["systemd_unit"],
                            "systemd_description": existing["systemd_description"],
                            "systemd_cgroup_path": existing["systemd_cgroup_path"],
                            "systemd_invocation_id": existing["systemd_invocation_id"],
                            "containment_broker_pid": existing["containment_broker_pid"],
                            "containment_broker_start_identity": existing[
                                "containment_broker_start_identity"
                            ],
                        },
                    )
                    queue.clear_owner_session_closing(
                        request.session_id,
                        session_generation_id=recorded_generation,
                    )
                    if existing_status.api_pid is None:
                        raise RelayError("verified existing owned session omitted its API pid")
                    return OwnedSessionStartReceipt(
                        cluster=request.cluster,
                        session_id=request.session_id,
                        start_operation_id=request.start_operation_id,
                        cluster_route_revision=request.cluster_route_revision,
                        session_generation_id=recorded_generation,
                        remote_api_port=request.remote_api_port,
                        api_pid=existing_status.api_pid,
                        outcome="already_running",
                    )
                if not registry_matches:
                    raise RelayError(
                        "an owned generation cannot change cluster authority during restart"
                    )
                if existing_status.generation_process_pids:
                    if not request.replace:
                        raise RelayError("owned generation processes remain; use --replace")
                    existing_unit = existing.get("systemd_unit")
                    existing_cgroup = existing.get("systemd_cgroup_path")
                    existing_invocation = existing.get("systemd_invocation_id")
                    existing_description = existing.get("systemd_description")
                    if not all(
                        isinstance(value, str)
                        for value in (
                            existing_unit,
                            existing_cgroup,
                            existing_invocation,
                            existing_description,
                        )
                    ):
                        raise RelayError("owned generation process identity is incomplete")
                    session_process_scope._terminate_recorded_session_scope(
                        systemd_unit=cast(str, existing_unit),
                        systemd_cgroup_path=cast(str, existing_cgroup),
                        systemd_invocation_id=cast(str, existing_invocation),
                        systemd_description=cast(str, existing_description),
                    )
        elif resumable_attempt is not None:
            recorded_generation = cast(str, resumable_attempt["session_generation_id"])
            attempt_phase = cast(str, resumable_attempt["start_phase"])
            if attempt_phase in {"pending", "admitted"}:
                from clio_relay.process_containment import adopt_linux_systemd_scope_identity

                try:
                    adopted_scope = adopt_linux_systemd_scope_identity(
                        unit=cast(str, resumable_attempt["systemd_unit"]),
                        description=cast(str, resumable_attempt["systemd_description"]),
                    )
                except RuntimeError as exc:
                    raise RelayError(f"prior owned-session scope recovery failed: {exc}") from exc
                if adopted_scope is not None:
                    resumable_attempt.update(
                        {
                            "start_phase": "scope_bound",
                            "systemd_cgroup_path": adopted_scope["cgroup_path"],
                            "systemd_invocation_id": adopted_scope["systemd_invocation_id"],
                        }
                    )
                    session_start_attempt_validation._write_session_attempt(
                        transaction,
                        operation="start",
                        identity={
                            key: value
                            for key, value in resumable_attempt.items()
                            if key not in {"schema_version", "operation", "observed_at", "error"}
                        },
                    )
                    attempt_phase = "scope_bound"
            if attempt_phase in {"scope_bound", "contained"}:
                if attempt_phase == "contained":
                    promoted = _promote_resumable_contained_start(
                        transaction=transaction,
                        attempt=resumable_attempt,
                        request=request,
                        release_identity=release_identity,
                        queue=queue,
                        proc_root=proc_root,
                        home=home,
                    )
                    if promoted is not None:
                        return promoted
                    if legacy_migrated and not request.replace:
                        raise RelayError(
                            "legacy contained start could not be adopted exactly; use --replace"
                        )
                session_process_scope._recorded_scope_processes(
                    proc_root=proc_root,
                    systemd_unit=cast(str, resumable_attempt["systemd_unit"]),
                    systemd_cgroup_path=cast(str, resumable_attempt["systemd_cgroup_path"]),
                    systemd_invocation_id=cast(
                        str,
                        resumable_attempt["systemd_invocation_id"],
                    ),
                    systemd_description=cast(str, resumable_attempt["systemd_description"]),
                )
                session_process_scope._terminate_recorded_session_scope(
                    systemd_unit=cast(str, resumable_attempt["systemd_unit"]),
                    systemd_cgroup_path=cast(str, resumable_attempt["systemd_cgroup_path"]),
                    systemd_invocation_id=cast(
                        str,
                        resumable_attempt["systemd_invocation_id"],
                    ),
                    systemd_description=cast(str, resumable_attempt["systemd_description"]),
                )
                resumable_attempt.update(
                    {
                        "start_phase": "admitted",
                        "systemd_cgroup_path": None,
                        "systemd_invocation_id": None,
                        "containment_broker_pid": None,
                        "containment_broker_start_identity": None,
                    }
                )
                session_start_attempt_validation._write_session_attempt(
                    transaction,
                    operation="start",
                    identity={
                        key: value
                        for key, value in resumable_attempt.items()
                        if key not in {"schema_version", "operation", "observed_at", "error"}
                    },
                )

        session_api_readiness._assert_remote_port_available(request.remote_api_port)
        release_sha256 = release_identity.sha256()
        expected_release_sha256 = (
            request.expected_api_release_identity.sha256()
            if request.expected_api_release_identity is not None
            else None
        )
        if existing is None:
            if resumable_attempt is None:
                candidate_generation = uuid4().hex
                owner_token = secrets.token_hex(32)
                owner_token_sha256 = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
                registry_name = f"cluster-registry-{candidate_generation}.json"
                registry_path = transaction.path / registry_name
                systemd_unit = f"clio-relay-session-{candidate_generation}.scope"
                systemd_description = (
                    f"clio-relay-owned-session:{request.session_id}:{candidate_generation}:"
                    f"{secrets.token_hex(16)}"
                )
                attempt_identity: dict[str, object] = {
                    "cluster": request.cluster,
                    "session_id": request.session_id,
                    "start_operation_id": request.start_operation_id,
                    "session_generation_id": candidate_generation,
                    "owner_token": owner_token,
                    "owner_token_sha256": owner_token_sha256,
                    "api_release_identity_sha256": release_sha256,
                    "expected_api_release_identity_sha256": expected_release_sha256,
                    "cluster_registry_path": str(registry_path),
                    "cluster_registry_sha256": request.cluster_registry_sha256,
                    "cluster_route_revision": request.cluster_route_revision,
                    "remote_api_port": request.remote_api_port,
                    "replace": request.replace,
                    "require_token": request.require_token,
                    "input_policy": request.input_policy.model_dump(mode="json"),
                    "start_phase": "pending",
                    "systemd_unit": systemd_unit,
                    "systemd_description": systemd_description,
                    "systemd_cgroup_path": None,
                    "systemd_invocation_id": None,
                    "containment_broker_pid": None,
                    "containment_broker_start_identity": None,
                }
                session_start_attempt_validation._write_session_attempt(
                    transaction,
                    operation="start",
                    identity=attempt_identity,
                )
            else:
                attempt_identity = {
                    key: value
                    for key, value in resumable_attempt.items()
                    if key not in {"schema_version", "operation", "observed_at", "error"}
                }
                candidate_generation = cast(str, attempt_identity["session_generation_id"])
                owner_token = cast(str, attempt_identity["owner_token"])
                owner_token_sha256 = cast(str, attempt_identity["owner_token_sha256"])
                registry_name = f"cluster-registry-{candidate_generation}.json"
                registry_path = transaction.path / registry_name
                systemd_unit = cast(str, attempt_identity["systemd_unit"])
                systemd_description = cast(str, attempt_identity["systemd_description"])
            admission = queue.owner_session_generation_status(
                request.session_id,
                session_generation_id=candidate_generation,
            )
            active_generation = admission.get("active_generation_id")
            if active_generation is None:
                selected_generation = queue.prepare_owner_session_start(
                    request.session_id,
                    recorded_generation_id=None,
                    candidate_generation_id=candidate_generation,
                )
            elif active_generation == candidate_generation and admission.get("closing") is False:
                selected_generation = candidate_generation
            else:
                raise RelayError("core selected a different unrecorded owned-session generation")
            if selected_generation != candidate_generation:
                raise RelayError("core selected an unrecorded owned-session generation")
            if attempt_identity["start_phase"] != "contained":
                attempt_identity["start_phase"] = "admitted"
                session_start_attempt_validation._write_session_attempt(
                    transaction,
                    operation="start",
                    identity=attempt_identity,
                )
        else:
            candidate_generation = uuid4().hex
            selected_generation = queue.prepare_owner_session_start(
                request.session_id,
                recorded_generation_id=recorded_generation,
                candidate_generation_id=candidate_generation,
            )
            registry_name = f"cluster-registry-{selected_generation}.json"
            registry_path = transaction.path / registry_name
            owner_token = secrets.token_hex(32)
            owner_token_sha256 = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
            systemd_unit = f"clio-relay-session-{selected_generation}.scope"
            systemd_description = (
                f"clio-relay-owned-session:{request.session_id}:{selected_generation}:"
                f"{secrets.token_hex(16)}"
            )
            attempt_identity = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "start_operation_id": request.start_operation_id,
                "session_generation_id": selected_generation,
                "owner_token": owner_token,
                "owner_token_sha256": owner_token_sha256,
                "api_release_identity_sha256": release_sha256,
                "expected_api_release_identity_sha256": expected_release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "remote_api_port": request.remote_api_port,
                "replace": request.replace,
                "require_token": request.require_token,
                "input_policy": request.input_policy.model_dump(mode="json"),
                "start_phase": "admitted",
                "systemd_unit": systemd_unit,
                "systemd_description": systemd_description,
                "systemd_cgroup_path": None,
                "systemd_invocation_id": None,
                "containment_broker_pid": None,
                "containment_broker_start_identity": None,
            }
            session_start_attempt_validation._write_session_attempt(
                transaction,
                operation="start",
                identity=attempt_identity,
            )
        registry_name = f"cluster-registry-{selected_generation}.json"
        registry_path = transaction.path / registry_name
        existing_registry = transaction.read_bytes(
            registry_name,
            maximum_bytes=MAX_CLUSTER_REGISTRY_BYTES,
            required=False,
        )
        if existing_registry is None:
            transaction.atomic_write(registry_name, registry_payload)
        elif hashlib.sha256(existing_registry).hexdigest() != request.cluster_registry_sha256:
            raise RelayError("owned generation cluster registry changed before restart")

        log_descriptor = transaction.open_output("api.log")
        process: subprocess.Popen[Any] | None = None
        metadata_committed = False
        child_environment: dict[str, str] = {}
        startup_secret_values: set[str] = set()
        try:
            from clio_relay.process_containment import (
                broker_child_environment_payload,
                process_start_identity,
                spawn_owned_process,
            )

            child_environment = {
                "CLIO_RELAY_SESSION_OWNER_TOKEN": owner_token,
                "CLIO_RELAY_SESSION_GENERATION_ID": selected_generation,
                "CLIO_RELAY_API_RELEASE_IDENTITY_SHA256": release_sha256,
                "CLIO_RELAY_CLUSTER_REGISTRY": str(registry_path),
                "CLIO_RELAY_SESSION_REGISTRY_SHA256": request.cluster_registry_sha256,
                "CLIO_RELAY_SESSION_ROUTE_REVISION": request.cluster_route_revision,
                "CLIO_RELAY_OWNER_SESSION_ID": request.session_id,
                "CLIO_RELAY_OWNER_SESSION_CLUSTER": request.cluster,
                "CLIO_RELAY_OWNER_SESSION_API_PORT": str(request.remote_api_port),
                "CLIO_RELAY_REMOTE_CLUSTER": request.cluster,
                **request.input_policy.environment(),
            }
            if not request.require_token:
                child_environment[ALLOW_UNAUTHENTICATED_OWNED_SESSION_ENV] = "1"
            if api_token is not None:
                child_environment["CLIO_RELAY_API_TOKEN"] = api_token
            environment = dict(os.environ)
            startup_secret_values.update(
                value
                for name, value in {**environment, **child_environment}.items()
                if value
                and any(
                    marker in name.upper()
                    for marker in (
                        "TOKEN",
                        "SECRET",
                        "PASSWORD",
                        "CREDENTIAL",
                        "API_KEY",
                        "AUTHORIZATION",
                    )
                )
            )
            environment.pop(ALLOW_UNAUTHENTICATED_OWNED_SESSION_ENV, None)
            for name in child_environment:
                environment.pop(name, None)
            provider_interpreter = Path(sys.executable).absolute()
            interpreter_identity = provider_interpreter.stat()
            command = [
                str(provider_interpreter),
                "-I",
                "-c",
                "from clio_relay.cli import app; app()",
                "api",
                "start",
                "--host",
                "127.0.0.1",
                "--port",
                str(request.remote_api_port),
            ]
            if request.require_token:
                command.append("--require-token")
            receipt_name = f"api-startup-{selected_generation}.json"
            receipt_path = transaction.path / receipt_name
            containment_identity: dict[str, object] = {}

            def persist_containment(
                broker_pid: int,
                containment: dict[str, object],
            ) -> None:
                if not (
                    containment.get("mode") == "linux_systemd_scope"
                    and containment.get("enforceable") is True
                    and containment.get("systemd_unit") == systemd_unit
                    and containment.get("systemd_description") == systemd_description
                    and isinstance(containment.get("cgroup_path"), str)
                    and isinstance(containment.get("systemd_invocation_id"), str)
                ):
                    raise RelayError("owned API containment identity is incomplete")
                broker_start = process_start_identity(broker_pid)
                if broker_start is None:
                    raise RelayError("owned API containment broker identity is unavailable")
                containment_identity.update(containment)
                attempt_identity.update(
                    {
                        "start_phase": "contained",
                        "systemd_cgroup_path": containment["cgroup_path"],
                        "systemd_invocation_id": containment["systemd_invocation_id"],
                        "containment_broker_pid": broker_pid,
                        "containment_broker_start_identity": broker_start,
                    }
                )
                session_start_attempt_validation._write_session_attempt(
                    transaction,
                    operation="start",
                    identity=attempt_identity,
                )

            def release_child_environment(
                _broker_pid: int,
                containment: dict[str, object],
            ) -> str:
                gated_environment = dict(child_environment)
                gated_environment.update(
                    {
                        _API_STARTUP_RECEIPT_ENV: str(receipt_path),
                        _SYSTEMD_UNIT_ENV: cast(str, containment["systemd_unit"]),
                        _SYSTEMD_CGROUP_ENV: cast(str, containment["cgroup_path"]),
                        _SYSTEMD_INVOCATION_ENV: cast(
                            str,
                            containment["systemd_invocation_id"],
                        ),
                        _SYSTEMD_DESCRIPTION_ENV: cast(
                            str,
                            containment["systemd_description"],
                        ),
                    }
                )
                return broker_child_environment_payload(gated_environment)

            process = cast(
                subprocess.Popen[Any],
                spawn_owned_process(
                    command,
                    stdout=log_descriptor,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    close_fds=True,
                    require_enforceable=True,
                    linux_systemd_unit_base=systemd_unit.removesuffix(".scope"),
                    linux_systemd_description=systemd_description,
                    on_ready=persist_containment,
                    credential_payload_factory=release_child_environment,
                ),
            )
            final_interpreter_identity = provider_interpreter.stat()
            if (final_interpreter_identity.st_dev, final_interpreter_identity.st_ino) != (
                interpreter_identity.st_dev,
                interpreter_identity.st_ino,
            ):
                raise RelayError("verified provider interpreter changed during API spawn")
            expected_receipt = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "session_generation_id": selected_generation,
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "systemd_unit": containment_identity["systemd_unit"],
                "systemd_cgroup_path": containment_identity["cgroup_path"],
                "systemd_invocation_id": containment_identity["systemd_invocation_id"],
                "systemd_description": containment_identity["systemd_description"],
            }
            process_identity = session_api_readiness._wait_for_api_startup_receipt(
                transaction=transaction,
                process=process,
                receipt_name=receipt_name,
                owner_token=owner_token,
                expected=expected_receipt,
                proc_root=proc_root,
            )
            ready_seconds = session_api_readiness._wait_for_api_ready(
                process=cast(subprocess.Popen[bytes], process),
                port=request.remote_api_port,
                require_token=request.require_token,
            )
            receipt_payload = transaction.read_bytes(
                receipt_name,
                maximum_bytes=_MAX_API_STARTUP_RECEIPT_BYTES,
            )
            if receipt_payload is None:  # pragma: no cover - required read
                raise RelayError("owned API startup receipt disappeared before metadata commit")
            metadata = {
                "cluster": request.cluster,
                "session_id": request.session_id,
                "remote_api_port": request.remote_api_port,
                "api_pid": process_identity.pid,
                "api_pgid": process_identity.process_group_id,
                "owner_token": owner_token,
                "session_generation_id": selected_generation,
                "api_release_identity": release_identity.model_dump(mode="json"),
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": str(registry_path),
                "cluster_registry_sha256": request.cluster_registry_sha256,
                "cluster_route_revision": request.cluster_route_revision,
                "cluster_authority_verified": True,
                "input_policy": request.input_policy.model_dump(mode="json"),
                "process_start_ticks": process_identity.start_ticks,
                "containment_mode": "linux_systemd_scope",
                "systemd_unit": containment_identity["systemd_unit"],
                "systemd_cgroup_path": containment_identity["cgroup_path"],
                "systemd_invocation_id": containment_identity["systemd_invocation_id"],
                "systemd_description": containment_identity["systemd_description"],
                "containment_broker_pid": process.pid,
                "containment_broker_start_identity": attempt_identity[
                    "containment_broker_start_identity"
                ],
                "api_startup_receipt_path": str(receipt_path),
                "api_startup_receipt_sha256": hashlib.sha256(receipt_payload).hexdigest(),
                "started_at": datetime.now(UTC).isoformat(),
                "owner": "clio-relay",
            }
            transaction.atomic_write("api.pid", f"{process_identity.pid}\n".encode("ascii"))
            transaction.atomic_write(
                "metadata.json",
                json.dumps(metadata, indent=2).encode("utf-8"),
            )
            metadata_committed = True
            queue.clear_owner_session_closing(
                request.session_id,
                session_generation_id=selected_generation,
            )
            return OwnedSessionStartReceipt(
                cluster=request.cluster,
                session_id=request.session_id,
                start_operation_id=request.start_operation_id,
                cluster_route_revision=request.cluster_route_revision,
                session_generation_id=selected_generation,
                remote_api_port=request.remote_api_port,
                api_pid=process_identity.pid,
                outcome="started",
                ready_seconds=ready_seconds,
            )
        except BaseException as exc:
            if not metadata_committed:
                startup_detail = session_api_readiness._owned_api_startup_log_detail(
                    transaction,
                    secret_values=startup_secret_values,
                )
                try:
                    if attempt_identity.get("start_phase") == "contained":
                        session_process_scope._terminate_recorded_session_scope(
                            systemd_unit=cast(str, attempt_identity["systemd_unit"]),
                            systemd_cgroup_path=cast(
                                str,
                                attempt_identity["systemd_cgroup_path"],
                            ),
                            systemd_invocation_id=cast(
                                str,
                                attempt_identity["systemd_invocation_id"],
                            ),
                            systemd_description=cast(str, attempt_identity["systemd_description"]),
                        )
                        attempt_identity.update(
                            {
                                "start_phase": "admitted",
                                "systemd_cgroup_path": None,
                                "systemd_invocation_id": None,
                                "containment_broker_pid": None,
                                "containment_broker_start_identity": None,
                            }
                        )
                except (RelayError, RuntimeError) as cleanup_error:
                    cleanup_detail = f"{exc}; cleanup failed: {cleanup_error}"
                else:
                    cleanup_detail = str(exc)
                if startup_detail:
                    cleanup_detail = f"{cleanup_detail}; api_log={startup_detail}"
                with suppress(RelayError):
                    session_start_attempt_validation._write_session_attempt(
                        transaction,
                        operation="start",
                        identity=attempt_identity,
                        error=cleanup_detail,
                    )
                if startup_detail and isinstance(exc, RelayError):
                    raise RelayError(cleanup_detail) from exc
            raise
        finally:
            os.close(log_descriptor)


def _stop_owned_worker_service(*, cluster: str) -> CleanupResource:
    """Stop only a user service whose unit metadata proves relay ownership."""
    service = f"clio-relay-worker-{cluster}.service"
    try:
        ownership = session_remote_command._run_bounded_command(
            [
                "systemctl",
                "--user",
                "show",
                service,
                "--property=LoadState",
                "--property=FragmentPath",
                "--property=ExecStart",
            ],
            timeout_seconds=20.0,
            stdout_limit=64 * 1024,
            stderr_limit=64 * 1024,
        )
        ownership_text = ownership.stdout.decode("utf-8", errors="replace")
        missing = "LoadState=not-found" in ownership_text
        owned = bool(
            ownership.returncode == 0
            and not missing
            and "clio-relay" in ownership_text
            and "endpoint start" in ownership_text
        )
        stopped: session_remote_command._BoundedCommandResult | None = None
        if owned:
            stopped = session_remote_command._run_bounded_command(
                ["systemctl", "--user", "stop", service],
                timeout_seconds=20.0,
                stdout_limit=64 * 1024,
                stderr_limit=64 * 1024,
            )
        active = session_remote_command._run_bounded_command(
            ["systemctl", "--user", "is-active", service],
            timeout_seconds=20.0,
            stdout_limit=64 * 1024,
            stderr_limit=64 * 1024,
        )
    except (OSError, RelayError) as exc:
        return CleanupResource(
            kind="worker_service",
            resource_id=service,
            location=cluster,
            action="stop",
            ownership_verified=False,
            outcome="failed",
            residual=True,
            detail=str(exc),
        )
    state = active.stdout.decode("utf-8", errors="replace").strip().lower() or "unknown"
    observed_state = "not-found" if missing else state
    verified = bool(
        missing
        or (owned and stopped is not None and stopped.returncode == 0 and state == "inactive")
    )
    if missing:
        outcome: Literal["stopped", "missing", "refused", "failed"] = "missing"
        detail = "worker service is not installed"
    elif not owned:
        outcome = "refused"
        detail = "worker service ownership proof failed; service was not stopped"
    elif verified:
        outcome = "stopped"
        detail = None
    else:
        outcome = "failed"
        detail = (
            stopped.stderr.decode("utf-8", errors="replace").strip()
            if stopped is not None
            else "worker service stop was not attempted"
        )
    return CleanupResource(
        kind="worker_service",
        resource_id=service,
        location=cluster,
        action="stop",
        ownership_verified=owned or missing,
        outcome=outcome,
        verified_after_operation=verified,
        observed_state=observed_state,
        residual=not verified,
        detail=detail,
    )


def _complete_cleanup_receipt_retry(
    *,
    transaction: _OwnedSessionTransaction,
    document: dict[str, object],
    request: OwnedSessionTeardownRequest,
) -> SessionLifecycleReport:
    """Complete only deletions authorized by an exact sanitized receipt."""
    if document.get("cleanup_operation_id") != request.expected_cleanup_operation_id:
        raise RelayError("cleanup receipt operation does not match the teardown request")
    expected_policy = {
        "stop_worker": request.stop_worker,
        "cancel_jobs": request.cancel_jobs,
        "cancel_scheduler_jobs": request.cancel_scheduler_jobs,
    }
    if document.get("cleanup_policy") != expected_policy:
        raise RelayError("cleanup receipt policy does not match the teardown request")
    targets = session_cleanup_targets._validate_cleanup_targets(
        document.get("cleanup_targets"),
        generation_id=request.expected_session_generation_id,
    )
    report = SessionLifecycleReport.model_validate(document.get("report"))
    if document.get("cleanup_paths_pending") is True:
        session_cleanup_targets._delete_cleanup_targets(transaction, targets)
        for target in targets:
            if transaction.stat_regular(target.name, required=False) is not None:
                raise RelayError(f"owned session cleanup target remained: {target.name}")
        completed = dict(document)
        completed["cleanup_paths_pending"] = False
        completed["cluster_registry_removed"] = True
        transaction.atomic_write(
            "metadata.json",
            json.dumps(completed, indent=2).encode("utf-8"),
        )
    return report


def _terminate_failed_start_scope(
    *,
    attempt: dict[str, object],
    proc_root: Path,
) -> list[int]:
    """Terminate and prove absence of the exact scope named by a start journal."""
    from clio_relay.process_containment import adopt_linux_systemd_scope_identity

    phase = attempt.get("start_phase")
    systemd_unit = cast(str, attempt["systemd_unit"])
    systemd_description = cast(str, attempt["systemd_description"])
    if phase in {"scope_bound", "contained"}:
        cgroup_path = cast(str, attempt["systemd_cgroup_path"])
        invocation_id = cast(str, attempt["systemd_invocation_id"])
    else:
        try:
            adopted = adopt_linux_systemd_scope_identity(
                unit=systemd_unit,
                description=systemd_description,
            )
        except RuntimeError as exc:
            raise RelayError(f"failed-start scope recovery failed: {exc}") from exc
        if adopted is None:
            return []
        cgroup_path = adopted["cgroup_path"]
        invocation_id = adopted["systemd_invocation_id"]
    processes = session_process_scope._recorded_scope_processes(
        proc_root=proc_root,
        systemd_unit=systemd_unit,
        systemd_cgroup_path=cgroup_path,
        systemd_invocation_id=invocation_id,
        systemd_description=systemd_description,
    )
    targeted = [process.pid for process in processes]
    session_process_scope._terminate_recorded_session_scope(
        systemd_unit=systemd_unit,
        systemd_cgroup_path=cgroup_path,
        systemd_invocation_id=invocation_id,
        systemd_description=systemd_description,
    )
    residual = session_process_scope._recorded_scope_processes(
        proc_root=proc_root,
        systemd_unit=systemd_unit,
        systemd_cgroup_path=cgroup_path,
        systemd_invocation_id=invocation_id,
        systemd_description=systemd_description,
    )
    if residual:
        raise RelayError("failed-start owned scope retained processes after termination")
    return targeted


def _execute_owned_failed_start_teardown(
    *,
    transaction: _OwnedSessionTransaction,
    request: OwnedSessionTeardownRequest,
    queue: _FailedStartCleanupQueue,
    proc_root: Path,
) -> SessionLifecycleReport:
    """Close an admitted start that failed before API metadata was committed."""
    attempt = session_start_attempt_validation._validated_start_attempt(
        transaction,
        cluster=request.cluster,
        session_id=request.session_id,
    )
    if attempt is None:
        raise RelayError("owned session has neither metadata nor a start attempt")
    generation_id = cast(str, attempt["session_generation_id"])
    if generation_id != request.expected_session_generation_id:
        raise RelayError("failed-start generation does not match the teardown request")
    registry_name = f"cluster-registry-{generation_id}.json"
    registry_payload = transaction.read_bytes(
        registry_name,
        maximum_bytes=MAX_CLUSTER_REGISTRY_BYTES,
    )
    if registry_payload is None:  # pragma: no cover - required read
        raise RelayError("failed-start cluster registry is unavailable")
    registry_sha256 = hashlib.sha256(registry_payload).hexdigest()
    if registry_sha256 != attempt.get("cluster_registry_sha256"):
        raise RelayError("failed-start cluster registry digest changed")
    try:
        registry = ClusterRegistry.model_validate_json(registry_payload)
    except ValueError as exc:
        raise RelayError(f"failed-start cluster registry is invalid: {exc}") from exc
    definition = registry.clusters.get(request.cluster)
    if definition is None or cluster_route_revision(definition) != attempt.get(
        "cluster_route_revision"
    ):
        raise RelayError("failed-start cluster route identity changed")

    admission = queue.owner_session_generation_status(
        request.session_id,
        session_generation_id=generation_id,
    )
    existing_intent = admission.get("cleanup_intent")
    expected_policy = {
        "stop_worker": request.stop_worker,
        "cancel_jobs": request.cancel_jobs,
        "cancel_scheduler_jobs": request.cancel_scheduler_jobs,
    }
    exact_open = bool(
        admission.get("active_generation_id") == generation_id
        and admission.get("closing_generation_id") is None
        and admission.get("open") is True
    )
    exact_closing = bool(
        admission.get("active_generation_id") == generation_id
        and admission.get("closing_generation_id") == generation_id
        and admission.get("closing") is True
        and isinstance(existing_intent, dict)
        and cast(dict[str, object], existing_intent).get("operation_id")
        == request.expected_cleanup_operation_id
        and {
            key: cast(dict[str, object], existing_intent).get(key)
            for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
        }
        == expected_policy
    )
    if not (exact_open or exact_closing):
        raise RelayError("failed-start generation is not the exact open or closing admission")
    intent = queue.set_owner_session_closing(
        request.session_id,
        session_generation_id=generation_id,
        operation_id=request.expected_cleanup_operation_id,
        stop_worker=request.stop_worker,
        cancel_jobs=request.cancel_jobs,
        cancel_scheduler_jobs=request.cancel_scheduler_jobs,
    )
    if not session_cleanup_targets._cleanup_intent_matches_request(intent, request):
        raise RelayError("failed-start cleanup intent changed during teardown")

    jobs_before = session_recovery_attempt_status._owned_generation_job_ids(
        queue,
        session_id=request.session_id,
        session_generation_id=generation_id,
    )
    targeted_pids = _terminate_failed_start_scope(attempt=attempt, proc_root=proc_root)
    jobs_after = session_recovery_attempt_status._owned_generation_job_ids(
        queue,
        session_id=request.session_id,
        session_generation_id=generation_id,
    )
    if jobs_after != jobs_before:
        raise RelayError("failed-start job membership changed after intake was quiesced")

    resources = [
        CleanupResource(
            kind="remote_relay_api",
            resource_id="failed-start",
            location=request.cluster,
            action="stop",
            ownership_verified=True,
            outcome="stopped" if targeted_pids else "missing",
            verified_after_operation=True,
            observed_state="absent",
            residual=False,
            detail="the exact pre-metadata owned scope is absent",
            metadata={
                "failed_start": True,
                "start_operation_id": attempt["start_operation_id"],
                "targeted_process_pids": targeted_pids,
            },
        )
    ]
    if request.stop_worker:
        worker_resource = _stop_owned_worker_service(cluster=request.cluster)
        resources.append(worker_resource)
        if worker_resource.residual:
            raise RelayError(worker_resource.detail or "owned worker service cleanup failed")

    target_names = sorted(
        (
            "api.log",
            "api.pid",
            f"api-startup-{generation_id}.json",
            registry_name,
        )
    )
    targets = [
        session_cleanup_targets._capture_cleanup_target(
            transaction,
            name=name,
            maximum_bytes=(
                None
                if name == "api.log"
                else _MAX_API_STARTUP_RECEIPT_BYTES
                if name.startswith("api-startup-")
                else MAX_CLUSTER_REGISTRY_BYTES
                if name.startswith("cluster-registry-")
                else _MAX_OWNED_SESSION_DOCUMENT_BYTES
            ),
        )
        for name in target_names
    ]
    registry_target = next(target for target in targets if target.name == registry_name)
    if not registry_target.present or registry_target.sha256 != registry_sha256:
        raise RelayError("failed-start registry cleanup identity changed")
    resources.append(
        CleanupResource(
            kind="remote_session_files",
            resource_id=f"{request.session_id}:{generation_id}",
            location=request.cluster,
            action="close",
            ownership_verified=True,
            outcome="closed",
            verified_after_operation=True,
            observed_state="sanitized",
            residual=False,
            metadata={
                "cleanup_paths": target_names,
                "metadata_sanitized": True,
                "transition_lock_retained": True,
                "failed_start": True,
                "target_identities": [target.model_dump(mode="json") for target in targets],
            },
        )
    )
    now = datetime.now(UTC)
    failure = cast(str | None, attempt.get("error")) or (
        "owned-session start ended before API metadata commit"
    )
    report = SessionLifecycleReport(
        cluster=request.cluster,
        session_id=request.session_id,
        session_generation_id=generation_id,
        mode="teardown",
        cleanup_operation_id=request.expected_cleanup_operation_id,
        cleanup_policy=expected_policy,
        relay_cancel_requested=request.cancel_jobs,
        scheduler_cancel_requested=request.cancel_scheduler_jobs,
        prior_session_status=RemoteSessionStateEvidence(
            api_pid=None,
            session_generation_id=generation_id,
            running=bool(targeted_pids),
            ownership_verified=True,
            observed_at=now,
        ),
        post_session_status=RemoteSessionStateEvidence(
            api_pid=None,
            session_generation_id=generation_id,
            running=False,
            ownership_verified=True,
            observed_at=datetime.now(UTC),
        ),
        resources=resources,
    )
    receipt = {
        "schema_version": "clio-relay.owner-session-failed-cleaned-receipt.v1",
        "owner": "clio-relay",
        "cluster": request.cluster,
        "session_id": request.session_id,
        "session_generation_id": generation_id,
        "start_operation_id": attempt["start_operation_id"],
        "start_phase": attempt["start_phase"],
        "failure": failure[:MAX_SESSION_START_ERROR_CHARS],
        "remote_api_port": attempt["remote_api_port"],
        "owner_token_sha256": attempt["owner_token_sha256"],
        "api_release_identity_sha256": attempt["api_release_identity_sha256"],
        "cluster_registry_path": attempt["cluster_registry_path"],
        "cluster_registry_sha256": attempt["cluster_registry_sha256"],
        "cluster_route_revision": attempt["cluster_route_revision"],
        "systemd_unit": attempt["systemd_unit"],
        "systemd_description": attempt["systemd_description"],
        "systemd_cgroup_path": attempt["systemd_cgroup_path"],
        "systemd_invocation_id": attempt["systemd_invocation_id"],
        "process_absence_verified": True,
        "owned_relay_job_ids": jobs_after,
        "cleanup_operation_id": request.expected_cleanup_operation_id,
        "cleanup_policy": expected_policy,
        "cleanup_paths": target_names,
        "cleanup_targets": [target.model_dump(mode="json") for target in targets],
        "cleanup_paths_pending": True,
        "cluster_registry_verified": True,
        "cluster_registry_removed": False,
        "completed_at": datetime.now(UTC).isoformat(),
        "report": report.model_dump(mode="json"),
        "coordinator_report_ref": None,
    }
    transaction.atomic_write(
        "metadata.json",
        json.dumps(receipt, indent=2).encode("utf-8"),
    )
    session_cleanup_targets._delete_cleanup_targets(transaction, targets)
    for target in targets:
        if transaction.stat_regular(target.name, required=False) is not None:
            raise RelayError(f"failed-start cleanup target remained: {target.name}")
    receipt["cleanup_paths_pending"] = False
    receipt["cluster_registry_removed"] = True
    transaction.atomic_write(
        "metadata.json",
        json.dumps(receipt, indent=2).encode("utf-8"),
    )
    return report


def execute_owned_session_teardown(
    request: OwnedSessionTeardownRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> SessionLifecycleReport:
    """Execute exact cluster-local teardown with fail-closed durable evidence."""
    from clio_relay.config import RelaySettings
    from clio_relay.core_queue import ClioCoreQueue

    _validate_session(session_id=request.session_id, remote_api_port=1)
    if request.cancel_scheduler_jobs and not request.cancel_jobs:
        raise RelayError("cancel_scheduler_jobs requires cancel_jobs")
    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    queue = ClioCoreQueue(settings_core_dir)
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned session teardown cannot verify the effective user")
    uid = get_effective_uid()
    expected_policy = {
        "stop_worker": request.stop_worker,
        "cancel_jobs": request.cancel_jobs,
        "cancel_scheduler_jobs": request.cancel_scheduler_jobs,
    }

    with open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json", required=False)
        if document is None:
            return _execute_owned_failed_start_teardown(
                transaction=transaction,
                request=request,
                queue=queue,
                proc_root=proc_root,
            )
        original_metadata = transaction.read_bytes(
            "metadata.json",
            maximum_bytes=_MAX_OWNED_SESSION_DOCUMENT_BYTES,
        )
        if original_metadata is None:  # pragma: no cover - required read
            raise RelayError("owned session metadata is unavailable")
        status = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if (
            not status.recovery_verified
            or status.session_generation_id != request.expected_session_generation_id
        ):
            detail = "; ".join(status.errors) or "generation identity did not match"
            raise RelayError(f"owned session teardown recovery was refused: {detail}")

        intent = queue.set_owner_session_closing(
            request.session_id,
            session_generation_id=request.expected_session_generation_id,
            operation_id=request.expected_cleanup_operation_id,
            stop_worker=request.stop_worker,
            cancel_jobs=request.cancel_jobs,
            cancel_scheduler_jobs=request.cancel_scheduler_jobs,
        )
        if not session_cleanup_targets._cleanup_intent_matches_request(intent, request):
            raise RelayError("durable cleanup intent does not match the teardown request")
        if status.cleanup_receipt:
            return _complete_cleanup_receipt_retry(
                transaction=transaction,
                document=document,
                request=request,
            )

        owner_token = document.get("owner_token")
        api_pid = document.get("api_pid")
        api_pgid = document.get("api_pgid")
        remote_api_port = document.get("remote_api_port")
        process_start = document.get("process_start_ticks")
        release_sha256 = document.get("api_release_identity_sha256")
        registry_path = document.get("cluster_registry_path")
        registry_sha256 = document.get("cluster_registry_sha256")
        route_revision = document.get("cluster_route_revision")
        systemd_unit = document.get("systemd_unit")
        systemd_cgroup_path = document.get("systemd_cgroup_path")
        systemd_invocation_id = document.get("systemd_invocation_id")
        systemd_description = document.get("systemd_description")
        containment_broker_pid = document.get("containment_broker_pid")
        containment_broker_start = document.get("containment_broker_start_identity")
        startup_receipt_path = document.get("api_startup_receipt_path")
        started_at_raw = document.get("started_at")
        if not (
            isinstance(owner_token, str)
            and isinstance(api_pid, int)
            and not isinstance(api_pid, bool)
            and isinstance(api_pgid, int)
            and not isinstance(api_pgid, bool)
            and isinstance(remote_api_port, int)
            and not isinstance(remote_api_port, bool)
            and isinstance(process_start, str)
            and isinstance(release_sha256, str)
            and isinstance(registry_path, str)
            and isinstance(registry_sha256, str)
            and isinstance(route_revision, str)
            and isinstance(systemd_unit, str)
            and isinstance(systemd_cgroup_path, str)
            and isinstance(systemd_invocation_id, str)
            and isinstance(systemd_description, str)
            and isinstance(containment_broker_pid, int)
            and not isinstance(containment_broker_pid, bool)
            and isinstance(containment_broker_start, str)
            and isinstance(startup_receipt_path, str)
            and isinstance(started_at_raw, str)
        ):
            raise RelayError("owned session metadata became incomplete before teardown")
        try:
            started_at = datetime.fromisoformat(started_at_raw)
        except ValueError as exc:  # pragma: no cover - recovery validated
            raise RelayError("owned session start timestamp is invalid") from exc
        owner_token_sha256 = hashlib.sha256(owner_token.encode("utf-8")).hexdigest()
        attempt_identity: dict[str, object] = {
            "cluster": request.cluster,
            "session_id": request.session_id,
            "session_generation_id": request.expected_session_generation_id,
            "cleanup_operation_id": request.expected_cleanup_operation_id,
            "cleanup_policy": expected_policy,
            "owner_token_sha256": owner_token_sha256,
            "api_release_identity_sha256": release_sha256,
            "cluster_registry_path": registry_path,
            "cluster_registry_sha256": registry_sha256,
            "cluster_route_revision": route_revision,
            "systemd_unit": systemd_unit,
            "systemd_cgroup_path": systemd_cgroup_path,
            "systemd_invocation_id": systemd_invocation_id,
            "systemd_description": systemd_description,
        }
        receipt_committed = False
        try:
            processes = session_process_scope._recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            prior_running = bool(processes)
            prior_observed_at = datetime.now(UTC)
            targeted_pids = [process.pid for process in processes]
            session_process_scope._terminate_recorded_session_scope(
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            final_processes = session_process_scope._recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=systemd_unit,
                systemd_cgroup_path=systemd_cgroup_path,
                systemd_invocation_id=systemd_invocation_id,
                systemd_description=systemd_description,
            )
            if final_processes:
                raise RelayError("owned generation process absence was not verified")
            api_resource = CleanupResource(
                kind="remote_relay_api",
                resource_id=str(api_pid),
                location=request.cluster,
                action="stop",
                ownership_verified=True,
                outcome="stopped" if targeted_pids else "missing",
                verified_after_operation=True,
                observed_state="absent",
                residual=False,
                detail=(
                    "the exact owned-generation systemd cgroup was stopped"
                    if targeted_pids
                    else "no exact owned-generation process remained"
                ),
                metadata={"targeted_process_pids": targeted_pids},
            )
            resources = [api_resource]
            if request.stop_worker:
                worker_resource = _stop_owned_worker_service(cluster=request.cluster)
                resources.append(worker_resource)
                if worker_resource.residual:
                    raise RelayError(
                        worker_resource.detail or "owned worker service cleanup failed"
                    )

            generation_id = request.expected_session_generation_id
            target_names = sorted(
                (
                    "api.log",
                    "api.pid",
                    Path(startup_receipt_path).name,
                    f"cluster-registry-{generation_id}.json",
                )
            )
            targets = [
                session_cleanup_targets._capture_cleanup_target(
                    transaction,
                    name=name,
                    maximum_bytes=(
                        None
                        if name == "api.log"
                        else _MAX_API_STARTUP_RECEIPT_BYTES
                        if name.startswith("api-startup-")
                        else MAX_CLUSTER_REGISTRY_BYTES
                        if name.startswith("cluster-registry-")
                        else _MAX_OWNED_SESSION_DOCUMENT_BYTES
                    ),
                )
                for name in target_names
            ]
            registry_target = next(
                target for target in targets if target.name.startswith("cluster-registry-")
            )
            if not registry_target.present or registry_target.sha256 != registry_sha256:
                raise RelayError("owned session registry cleanup identity changed")
            pid_target = next(target for target in targets if target.name == "api.pid")
            if pid_target.present:
                pid_payload = transaction.read_bytes(
                    "api.pid",
                    maximum_bytes=_MAX_OWNED_SESSION_DOCUMENT_BYTES,
                )
                if pid_payload is None or pid_payload.strip() != str(api_pid).encode("ascii"):
                    raise RelayError("owned session PID file content is not authoritative")

            resources.append(
                CleanupResource(
                    kind="remote_session_files",
                    resource_id=f"{request.session_id}:{generation_id}",
                    location=request.cluster,
                    action="close",
                    ownership_verified=True,
                    outcome="closed",
                    verified_after_operation=True,
                    residual=False,
                    metadata={
                        "cleanup_paths": target_names,
                        "metadata_sanitized": True,
                        "transition_lock_retained": True,
                        "target_identities": [target.model_dump(mode="json") for target in targets],
                    },
                )
            )
            report = SessionLifecycleReport(
                cluster=request.cluster,
                session_id=request.session_id,
                session_generation_id=generation_id,
                mode="teardown",
                cleanup_operation_id=request.expected_cleanup_operation_id,
                cleanup_policy=expected_policy,
                relay_cancel_requested=request.cancel_jobs,
                scheduler_cancel_requested=request.cancel_scheduler_jobs,
                prior_session_status=RemoteSessionStateEvidence(
                    api_pid=api_pid,
                    session_generation_id=generation_id,
                    process_start_marker=process_start,
                    running=prior_running,
                    ownership_verified=True,
                    observed_at=prior_observed_at,
                    started_at=started_at,
                ),
                post_session_status=RemoteSessionStateEvidence(
                    api_pid=api_pid,
                    session_generation_id=generation_id,
                    process_start_marker=process_start,
                    running=False,
                    ownership_verified=True,
                    observed_at=datetime.now(UTC),
                    started_at=started_at,
                ),
                resources=resources,
            )
            receipt = {
                "schema_version": "clio-relay.owner-session-cleanup-receipt.v1",
                "owner": "clio-relay",
                "cluster": request.cluster,
                "session_id": request.session_id,
                "session_generation_id": generation_id,
                "api_pid": api_pid,
                "api_pgid": api_pgid,
                "remote_api_port": remote_api_port,
                "process_start_ticks": process_start,
                "owner_token_sha256": owner_token_sha256,
                "api_release_identity_sha256": release_sha256,
                "cluster_registry_path": registry_path,
                "cluster_registry_sha256": registry_sha256,
                "cluster_route_revision": route_revision,
                "containment_mode": "linux_systemd_scope",
                "systemd_unit": systemd_unit,
                "systemd_cgroup_path": systemd_cgroup_path,
                "systemd_invocation_id": systemd_invocation_id,
                "systemd_description": systemd_description,
                "containment_broker_pid": containment_broker_pid,
                "containment_broker_start_identity": containment_broker_start,
                "metadata_sha256": hashlib.sha256(original_metadata).hexdigest(),
                "cleanup_operation_id": request.expected_cleanup_operation_id,
                "cleanup_policy": expected_policy,
                "cleanup_paths": target_names,
                "cleanup_targets": [target.model_dump(mode="json") for target in targets],
                "cleanup_paths_pending": True,
                "cluster_registry_verified": True,
                "cluster_registry_removed": False,
                "completed_at": datetime.now(UTC).isoformat(),
                "report": report.model_dump(mode="json"),
                "coordinator_report_ref": None,
            }
            transaction.atomic_write(
                "metadata.json",
                json.dumps(receipt, indent=2).encode("utf-8"),
            )
            receipt_committed = True
            session_cleanup_targets._delete_cleanup_targets(transaction, targets)
            for target in targets:
                if transaction.stat_regular(target.name, required=False) is not None:
                    raise RelayError(f"owned session cleanup target remained: {target.name}")
            receipt["cleanup_paths_pending"] = False
            receipt["cluster_registry_removed"] = True
            transaction.atomic_write(
                "metadata.json",
                json.dumps(receipt, indent=2).encode("utf-8"),
            )
            return report
        except BaseException as exc:
            if not receipt_committed:
                with suppress(RelayError):
                    session_start_attempt_validation._write_session_attempt(
                        transaction,
                        operation="teardown",
                        identity=attempt_identity,
                        error=str(exc),
                    )
            raise


def execute_owned_session_cleanup_finalize(
    request: OwnedSessionCleanupFinalizeRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> OwnedSessionRecoveryStatus:
    """Immutably bind a coordinator-verified report to a completed receipt."""
    from clio_relay.config import RelaySettings

    _validate_session(session_id=request.session_id, remote_api_port=1)
    expected_policy_keys = {"stop_worker", "cancel_jobs", "cancel_scheduler_jobs"}
    if set(request.expected_cleanup_policy) != expected_policy_keys:
        raise RelayError("coordinator cleanup policy has unexpected fields")
    if (
        request.expected_cleanup_policy["cancel_scheduler_jobs"]
        and not request.expected_cleanup_policy["cancel_jobs"]
    ):
        raise RelayError("cancel_scheduler_jobs requires cancel_jobs")
    report = request.coordinator_report
    report_reference, report_payload = session_lifecycle_report._coordinator_report_reference(
        report
    )
    if report_reference.sha256 != request.coordinator_report_sha256:
        raise RelayError("coordinator cleanup report digest does not match its request")
    if not (
        report.cluster == request.cluster
        and report.session_id == request.session_id
        and report.session_generation_id == request.expected_session_generation_id
        and report.mode == "teardown"
        and report.cleanup_operation_id == request.expected_cleanup_operation_id
        and report.cleanup_policy == request.expected_cleanup_policy
        and report.relay_cancel_requested is request.expected_cleanup_policy["cancel_jobs"]
        and report.scheduler_cancel_requested
        is request.expected_cleanup_policy["cancel_scheduler_jobs"]
    ):
        raise RelayError("coordinator cleanup report identity or policy does not match")

    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned cleanup finalization cannot verify the effective user")
    uid = get_effective_uid()
    with open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session cleanup receipt is unavailable")
        status = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if not (
            status.recovery_verified
            and status.cleanup_receipt
            and status.cleanup_paths_pending is False
            and status.session_generation_id == request.expected_session_generation_id
        ):
            detail = "; ".join(status.errors) or "completed receipt was not exact"
            raise RelayError(f"coordinator cleanup finalization was refused: {detail}")
        if document.get("cleanup_operation_id") != request.expected_cleanup_operation_id:
            raise RelayError("cleanup receipt operation does not match coordinator report")
        if document.get("cleanup_policy") != request.expected_cleanup_policy:
            raise RelayError("cleanup receipt policy does not match coordinator report")

        remote_report = SessionLifecycleReport.model_validate(document.get("report"))
        if not session_lifecycle_report._coordinator_report_extends_remote_report(
            report, remote_report
        ):
            raise RelayError("coordinator cleanup report does not extend the exact remote report")

        existing_reference_raw = document.get("coordinator_report_ref")
        existing_report = document.get("coordinator_report")
        existing_sha256 = document.get("coordinator_report_sha256")
        if existing_reference_raw is not None:
            try:
                existing_reference = OwnedSessionCleanupReportReference.model_validate(
                    existing_reference_raw
                )
            except ValueError as exc:
                raise RelayError(
                    "existing coordinator cleanup report reference is invalid"
                ) from exc
            if not (
                existing_reference == report_reference
                and status.coordinator_report_bound
                and status.coordinator_report_ref == report_reference
                and status.coordinator_report_sha256 == report_reference.sha256
                and status.coordinator_report is None
            ):
                raise RelayError(
                    "coordinator cleanup report is immutable and cannot be replaced or downgraded"
                )
            return status

        legacy_bound = existing_report is not None or existing_sha256 is not None
        if legacy_bound and not (
            existing_sha256 == request.coordinator_report_sha256
            and existing_report == report.model_dump(mode="json")
            and status.coordinator_report_bound
            and status.coordinator_report_ref is None
        ):
            raise RelayError(
                "coordinator cleanup report is immutable and cannot be replaced or downgraded"
            )

        session_lifecycle_report._prune_unreferenced_cleanup_report_sidecars(
            transaction,
            preserve_names={
                report_reference.name,
                f".{report_reference.name}.pending",
            },
        )
        transaction.atomic_write_immutable(
            report_reference.name,
            report_payload,
            maximum_bytes=MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
        )
        finalized = dict(document)
        finalized.pop("coordinator_report", None)
        finalized.pop("coordinator_report_sha256", None)
        finalized["coordinator_report_ref"] = report_reference.model_dump(mode="json")
        transaction.atomic_write(
            "metadata.json",
            json.dumps(finalized, indent=2).encode("utf-8"),
        )
        reread = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if not (
            reread.recovery_verified
            and reread.coordinator_report_bound
            and reread.coordinator_report_ref == report_reference
            and reread.coordinator_report_sha256 == report_reference.sha256
            and reread.coordinator_report is None
        ):
            raise RelayError("coordinator cleanup report was not durably re-read after commit")
        return reread


def execute_owned_session_cleanup_report_read(
    request: OwnedSessionCleanupReportReadRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> SessionLifecycleReport:
    """Read one exact finalized report only through its pinned receipt reference."""
    from clio_relay.config import RelaySettings

    _validate_session(session_id=request.session_id, remote_api_port=1)
    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned cleanup report read cannot verify the effective user")
    uid = get_effective_uid()
    with open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session cleanup receipt is unavailable")
        status = inspect_owned_session_recovery_status(
            cluster=request.cluster,
            session_id=request.session_id,
            core_dir=settings_core_dir,
            home=home,
            proc_root=proc_root,
            effective_uid=uid,
            transaction=transaction,
        )
        if not (
            status.recovery_verified
            and status.cleanup_receipt
            and status.cleanup_paths_pending is False
            and status.session_generation_id == request.expected_session_generation_id
            and status.coordinator_report_bound
            and status.coordinator_report is None
            and status.coordinator_report_ref == request.coordinator_report_ref
            and status.coordinator_report_sha256 == request.coordinator_report_ref.sha256
        ):
            detail = "; ".join(status.errors) or "finalized report reference was not exact"
            raise RelayError(f"owned cleanup report read was refused: {detail}")
        cleanup_operation_id = document.get("cleanup_operation_id")
        if not isinstance(cleanup_operation_id, str):
            raise RelayError("owned cleanup report receipt omitted its operation id")
        return session_lifecycle_report._read_coordinator_report_sidecar(
            transaction,
            request.coordinator_report_ref,
            expected_session_generation_id=request.expected_session_generation_id,
            expected_cleanup_operation_id=cleanup_operation_id,
        )


def plan_remote_session_start(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    remote_api_port: int,
    replace: bool,
    require_token: bool,
    input_policy: OwnedSessionInputPolicy | None = None,
    start_operation_id: str | None = None,
    expected_cluster_route_revision: str | None = None,
    expected_api_release_identity_sha256: str | None = None,
) -> OwnedSessionStartPlan:
    """Create a read-only exact selector plan before any remote mutation."""
    _validate_session(session_id=session_id, remote_api_port=remote_api_port)
    _, _, route_revision = session_remote_scripts._session_cluster_registry_authority(
        cluster=cluster,
        definition=definition,
    )
    if (
        expected_cluster_route_revision is not None
        and expected_cluster_route_revision != route_revision
    ):
        raise RelayError("owned-session start plan route revision changed")
    operation_id = start_operation_id or f"start_{uuid4().hex}"
    _validate_durable_session_identity(operation_id, field="start_operation_id")
    resolved_input_policy = input_policy or OwnedSessionInputPolicy()
    status_selector = OwnedSessionStartStatusSelector(
        cluster=cluster,
        session_id=session_id,
        start_operation_id=operation_id,
        cluster_route_revision=route_revision,
        remote_api_port=remote_api_port,
        replace=replace,
        require_token=require_token,
        input_policy=resolved_input_policy,
        expected_api_release_identity_sha256=expected_api_release_identity_sha256,
    )
    retry_selector = OwnedSessionStartRetrySelector(
        cluster=cluster,
        session_id=session_id,
        start_operation_id=operation_id,
        cluster_route_revision=route_revision,
        remote_api_port=remote_api_port,
        replace=replace,
        require_token=require_token,
        input_policy=resolved_input_policy,
        expected_api_release_identity_sha256=expected_api_release_identity_sha256,
    )
    return OwnedSessionStartPlan(
        cluster=cluster,
        session_id=session_id,
        start_operation_id=operation_id,
        cluster_route_revision=route_revision,
        remote_api_port=remote_api_port,
        input_policy=resolved_input_policy,
        expected_api_release_identity_sha256=expected_api_release_identity_sha256,
        status_selector=status_selector,
        retry_selector=retry_selector,
    )


def start_remote_session(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    remote_api_port: int,
    api_token: str | None,
    input_policy: OwnedSessionInputPolicy | None = None,
    expected_api_release_identity: SessionApiReleaseIdentity | None = None,
    replace: bool = False,
    start_operation_id: str | None = None,
    expected_cluster_route_revision: str | None = None,
) -> OwnedSessionStartReceipt:
    """Start a cluster-side relay API and validate its typed receipt."""
    plan = plan_remote_session_start(
        cluster=cluster,
        definition=definition,
        session_id=session_id,
        remote_api_port=remote_api_port,
        replace=replace,
        require_token=api_token is not None,
        input_policy=input_policy,
        start_operation_id=start_operation_id,
        expected_cluster_route_revision=expected_cluster_route_revision,
        expected_api_release_identity_sha256=(
            expected_api_release_identity.sha256()
            if expected_api_release_identity is not None
            else None
        ),
    )
    output = session_remote_scripts._ssh_script(
        definition,
        session_remote_scripts._start_script(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            start_operation_id=plan.start_operation_id,
            remote_api_port=remote_api_port,
            api_token=api_token,
            expected_api_release_identity=expected_api_release_identity,
            input_policy=plan.input_policy,
            replace=replace,
            expected_cluster_route_revision=plan.cluster_route_revision,
        ),
    )
    try:
        receipt = OwnedSessionStartReceipt.model_validate_json(output)
    except ValueError as exc:
        raise RelayError(f"owned-session start receipt is invalid: {exc}") from exc
    if not (
        receipt.cluster == plan.cluster
        and receipt.session_id == plan.session_id
        and receipt.start_operation_id == plan.start_operation_id
        and receipt.cluster_route_revision == plan.cluster_route_revision
        and receipt.remote_api_port == plan.remote_api_port
    ):
        raise RelayError("owned-session start receipt changed its exact plan identity")
    return receipt


def status_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
    pre_start_cleanup_probe: bool = False,
) -> dict[str, object]:
    """Return status for a previously started remote relay session.

    The pre-start cleanup probe is an internal, read-only observation that may
    report an uninitialized transition.  It must not be used as authoritative
    absence evidence by teardown or cleanup callers.
    """
    _validate_session(session_id=session_id, remote_api_port=1)
    output = session_remote_scripts._ssh_script(
        definition,
        session_remote_scripts._owned_status_script(
            definition=definition,
            cluster=definition.name,
            session_id=session_id,
            pre_start_cleanup_probe=pre_start_cleanup_probe,
        ),
    )
    return cast(dict[str, object], json.loads(output))


def status_remote_session_start(
    *,
    definition: ClusterDefinition,
    selector: OwnedSessionStartStatusSelector,
    wait_seconds: float = 0.0,
) -> OwnedSessionRecoveryStatus:
    """Return one remote observation for one exact start operation.

    With ``wait_seconds`` the cluster-local command blocks against its durable
    state until the start is terminal, so a watch does not redial per interval.
    """
    if definition.name != selector.cluster:
        raise RelayError("owned-session start status selector changed cluster")
    bounded_wait = max(0.0, min(wait_seconds, MAX_REMOTE_SESSION_START_WAIT_SECONDS))
    transport_timeout = (
        _REMOTE_SESSION_START_RECOVERY_TIMEOUT_SECONDS
        if bounded_wait <= 0
        else bounded_wait + _REMOTE_SESSION_START_WAIT_TRANSPORT_MARGIN_SECONDS
    )
    try:
        output = session_remote_scripts._ssh_script(
            definition,
            session_remote_scripts._owned_start_status_script(
                definition=definition,
                selector=selector,
                wait_seconds=bounded_wait,
            ),
            timeout_seconds=transport_timeout,
        )
    except session_remote_command._RemoteSessionCommandDeadline as exc:
        return OwnedSessionRecoveryStatus(
            cluster=selector.cluster,
            session_id=selector.session_id,
            start_operation_id=selector.start_operation_id,
            cluster_route_revision=selector.cluster_route_revision,
            start_state="starting",
            start_retryable=True,
            errors=[str(exc)],
        )
    try:
        status = OwnedSessionRecoveryStatus.model_validate_json(output)
    except ValueError as exc:
        raise RelayError(f"owned-session start status is invalid: {exc}") from exc
    if not (
        status.cluster == selector.cluster
        and status.session_id == selector.session_id
        and status.start_operation_id == selector.start_operation_id
        and status.cluster_route_revision == selector.cluster_route_revision
    ):
        raise RelayError("owned-session start status changed its exact selector")
    return status


def _owned_session_start_result(
    *,
    plan: OwnedSessionStartPlan,
    state: Literal["ready", "starting", "ambiguous", "failed", "not_current"],
    terminal: bool,
    retryable: bool,
    transition_accepted: bool | None,
    transport_deadline_exceeded: bool,
    session_generation_id: str | None = None,
    running: bool = False,
    ownership_verified: bool = False,
    recovery_verified: bool = False,
    start_phase: Literal["pending", "admitted", "scope_bound", "contained"] | None = None,
    error: str | None = None,
) -> OwnedSessionStartResult:
    """Build one typed result while copying the exact immutable plan identity."""
    return OwnedSessionStartResult(
        cluster=plan.cluster,
        session_id=plan.session_id,
        start_operation_id=plan.start_operation_id,
        cluster_route_revision=plan.cluster_route_revision,
        session_generation_id=session_generation_id,
        remote_api_port=plan.remote_api_port,
        state=state,
        terminal=terminal,
        retryable=retryable,
        usable=state == "ready",
        transition_accepted=transition_accepted,
        transport_deadline_exceeded=transport_deadline_exceeded,
        running=running,
        ownership_verified=ownership_verified,
        recovery_verified=recovery_verified,
        start_phase=start_phase,
        error=error,
        status_selector=plan.status_selector,
        retry_selector=plan.retry_selector,
    )


def _session_start_result_from_status(
    *,
    plan: OwnedSessionStartPlan,
    status: OwnedSessionRecoveryStatus,
    transport_deadline_exceeded: bool,
) -> OwnedSessionStartResult:
    """Project exact remote recovery evidence into the public start contract."""
    generation_id = status.session_generation_id
    if status.start_state == "not_current":
        detail = "; ".join(status.errors) or "owned-session start selector is no longer current"
        return _owned_session_start_result(
            plan=plan,
            state="not_current",
            terminal=True,
            retryable=False,
            transition_accepted=None,
            transport_deadline_exceeded=transport_deadline_exceeded,
            error=detail[:MAX_SESSION_START_ERROR_CHARS],
        )
    if status.start_attempt_verified and not (
        status.start_replace is plan.retry_selector.replace
        and status.start_require_token is plan.retry_selector.require_token
        and status.start_input_policy == plan.input_policy
        and status.start_expected_api_release_identity_sha256
        == plan.expected_api_release_identity_sha256
        and status.remote_api_port == plan.remote_api_port
    ):
        return _owned_session_start_result(
            plan=plan,
            state="failed",
            terminal=True,
            retryable=False,
            transition_accepted=None,
            transport_deadline_exceeded=transport_deadline_exceeded,
            error="remote start journal does not match the persisted retry selector",
        )
    if (
        status.recovery_verified
        and status.ownership_verified
        and generation_id is not None
        and status.start_attempt_verified
        and status.start_state == "ready"
    ):
        return _owned_session_start_result(
            plan=plan,
            session_generation_id=generation_id,
            state="ready",
            terminal=True,
            retryable=False,
            transition_accepted=True,
            transport_deadline_exceeded=transport_deadline_exceeded,
            running=status.leader_process_state == "owned_running",
            ownership_verified=True,
            recovery_verified=True,
            start_phase=status.start_phase,
        )
    if status.start_attempt_verified and generation_id is not None:
        if status.start_state in {"failed", "failed_cleaned"}:
            detail = status.start_error or "owned-session start attempt failed"
            return _owned_session_start_result(
                plan=plan,
                session_generation_id=generation_id,
                state="failed",
                terminal=True,
                retryable=False,
                transition_accepted=True,
                transport_deadline_exceeded=transport_deadline_exceeded,
                start_phase=status.start_phase,
                error=detail,
            )
        return _owned_session_start_result(
            plan=plan,
            session_generation_id=generation_id,
            state="starting",
            terminal=False,
            retryable=True,
            transition_accepted=True,
            transport_deadline_exceeded=transport_deadline_exceeded,
            start_phase=status.start_phase,
        )
    detail = "; ".join(status.errors) or "remote start transition is not yet observable"
    return _owned_session_start_result(
        plan=plan,
        state="ambiguous",
        terminal=False,
        retryable=True,
        transition_accepted=None,
        transport_deadline_exceeded=transport_deadline_exceeded,
        error=detail[:MAX_SESSION_START_ERROR_CHARS],
    )


def query_remote_session_start(
    *,
    definition: ClusterDefinition,
    plan: OwnedSessionStartPlan,
    transport_deadline_exceeded: bool = False,
    wait_seconds: float = 0.0,
) -> OwnedSessionStartResult:
    """Query one exact start once; callers choose any aggregate polling policy."""
    try:
        status = status_remote_session_start(
            definition=definition,
            selector=plan.status_selector,
            wait_seconds=wait_seconds,
        )
    except RemoteExecutableMissingError:
        # A dead pin is a broken DEPLOYMENT, not an in-flight start: the shell
        # executed nothing, and every retry re-executes the same missing
        # binary. Laundering it into starting/retryable below would rebuild the
        # retry-forever loop the typed 127 discrimination exists to remove
        # (clio-relay#158). Genuinely ambiguous transport errors still fall
        # through to the recovery status, which is what that path is for.
        raise
    except RelayError as exc:
        status = OwnedSessionRecoveryStatus(
            cluster=plan.cluster,
            session_id=plan.session_id,
            start_operation_id=plan.start_operation_id,
            cluster_route_revision=plan.cluster_route_revision,
            start_state="starting",
            start_retryable=True,
            errors=[str(exc)[:MAX_SESSION_START_ERROR_CHARS]],
        )
    return _session_start_result_from_status(
        plan=plan,
        status=status,
        transport_deadline_exceeded=transport_deadline_exceeded,
    )


def watch_remote_session_start(
    *,
    definition: ClusterDefinition,
    plan: OwnedSessionStartPlan,
    timeout_seconds: float,
    poll_seconds: float = 0.5,
    query: Callable[[], OwnedSessionStartResult] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> OwnedSessionStartResult:
    """Watch one exact start until ready, terminal failure, or bounded detach.

    The watch is a bounded server-side wait against the remote relay's durable
    start state, not a client redial loop: each observation blocks remotely for
    what remains of the deadline and returns as soon as the start is terminal.

    A watch timeout does not erase or reinterpret the durable operation.  The
    returned nonterminal result remains a handle carrying the exact status and
    retry selectors, is explicitly unusable, and can be watched again later.
    """
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("start watch timeout_seconds must be finite and positive")
    if not math.isfinite(poll_seconds) or poll_seconds <= 0:
        raise ValueError("start watch poll_seconds must be finite and positive")
    deadline = monotonic() + timeout_seconds

    def _durable_wait() -> OwnedSessionStartResult:
        remaining_wait = max(deadline - monotonic(), 0.0)
        return query_remote_session_start(
            definition=definition,
            plan=plan,
            wait_seconds=min(remaining_wait, MAX_REMOTE_SESSION_START_WAIT_SECONDS),
        )

    query_once = query or _durable_wait
    while True:
        result = query_once()
        if result.terminal:
            return result
        remaining = deadline - monotonic()
        if remaining <= 0:
            detail = "start watch detached at its bounded deadline; use status_selector to resume"
            if result.error:
                detail = f"{result.error}; {detail}"
            return result.model_copy(
                update={
                    "watch_deadline_exceeded": True,
                    "error": detail[-MAX_SESSION_START_ERROR_CHARS:],
                }
            )
        sleep(min(poll_seconds, remaining))


def start_remote_session_durable(
    *,
    definition: ClusterDefinition,
    plan: OwnedSessionStartPlan,
    api_token: str | None,
    expected_api_release_identity: SessionApiReleaseIdentity | None = None,
    starter: Callable[..., OwnedSessionStartReceipt] | None = None,
) -> OwnedSessionStartResult:
    """Start or recover one exact remote transition without erasing deadline ambiguity."""
    if (api_token is not None) is not plan.retry_selector.require_token:
        raise RelayError("owned-session start token policy changed after planning")
    observed_release_sha256 = (
        expected_api_release_identity.sha256()
        if expected_api_release_identity is not None
        else None
    )
    if observed_release_sha256 != plan.expected_api_release_identity_sha256:
        raise RelayError("owned-session start release identity changed after planning")
    start_callable = starter or start_remote_session
    try:
        receipt = start_callable(
            cluster=plan.cluster,
            definition=definition,
            session_id=plan.session_id,
            remote_api_port=plan.remote_api_port,
            api_token=api_token,
            input_policy=plan.input_policy,
            expected_api_release_identity=expected_api_release_identity,
            replace=plan.retry_selector.replace,
            start_operation_id=plan.start_operation_id,
            expected_cluster_route_revision=plan.cluster_route_revision,
        )
    except session_remote_command._RemoteSessionCommandDeadline:
        return query_remote_session_start(
            definition=definition,
            plan=plan,
            transport_deadline_exceeded=True,
        )
    except session_remote_command._RemoteSessionCommandAmbiguous:
        # The durable start may exist: resolve it against remote state instead
        # of escaping as a bare RelayError. Not a deadline, so the flag stays
        # false (clio-relay#158).
        return query_remote_session_start(definition=definition, plan=plan)
    except session_remote_command._RemoteSessionCommandRejected as exc:
        rejection = exc.rejection
        if not (
            rejection.cluster == plan.cluster
            and rejection.session_id == plan.session_id
            and rejection.start_operation_id == plan.start_operation_id
            and rejection.cluster_route_revision == plan.cluster_route_revision
        ):
            return query_remote_session_start(definition=definition, plan=plan)
        observed = query_remote_session_start(definition=definition, plan=plan)
        if observed.state != "ambiguous":
            return observed
        return observed.model_copy(update={"error": str(exc)[:MAX_SESSION_START_ERROR_CHARS]})
    if (
        receipt.cluster != plan.cluster
        or receipt.session_id != plan.session_id
        or receipt.start_operation_id != plan.start_operation_id
        or receipt.cluster_route_revision != plan.cluster_route_revision
        or receipt.remote_api_port != plan.remote_api_port
    ):
        raise RelayError("owned-session start receipt changed its exact plan identity")
    return OwnedSessionStartResult(
        cluster=plan.cluster,
        session_id=plan.session_id,
        start_operation_id=plan.start_operation_id,
        cluster_route_revision=plan.cluster_route_revision,
        session_generation_id=receipt.session_generation_id,
        remote_api_port=plan.remote_api_port,
        state="ready",
        terminal=True,
        retryable=False,
        usable=True,
        transition_accepted=True,
        running=receipt.running,
        ownership_verified=receipt.ownership_verified,
        recovery_verified=receipt.recovery_verified,
        start_phase=receipt.start_phase,
        status_selector=plan.status_selector,
        retry_selector=plan.retry_selector,
    )


def challenge_remote_session_identity(
    *,
    definition: ClusterDefinition,
    session_id: str,
    session_generation_id: DurableRecordId,
    nonce: str,
) -> dict[str, object]:
    """Return an SSH-authenticated HMAC challenge for one live session API."""
    _validate_session(session_id=session_id, remote_api_port=1)
    validate_durable_record_id(session_generation_id)
    if len(nonce) != 64 or any(character not in "0123456789abcdef" for character in nonce):
        raise ValueError("session identity nonce must be a lowercase 256-bit hexadecimal value")
    output = session_remote_scripts._ssh_script(
        definition,
        session_remote_scripts._owned_identity_challenge_script(
            definition=definition,
            cluster=definition.name,
            session_id=session_id,
            session_generation_id=session_generation_id,
            nonce=nonce,
        ),
    )
    return cast(dict[str, object], json.loads(output))


def teardown_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
    expected_session_generation_id: str,
    expected_cleanup_operation_id: str | None = None,
    stop_worker: bool = False,
    cancel_jobs: bool = False,
    cancel_scheduler_jobs: bool = False,
    cluster: str | None = None,
) -> SessionLifecycleReport:
    """Stop processes owned by a remote relay session."""
    _validate_session(session_id=session_id, remote_api_port=1)
    _validate_durable_session_identity(
        expected_session_generation_id,
        field="expected_session_generation_id",
    )
    cleanup_operation_id = expected_cleanup_operation_id or f"cleanup_{uuid4().hex}"
    _validate_durable_session_identity(
        cleanup_operation_id,
        field="expected_cleanup_operation_id",
    )
    output = session_remote_scripts._ssh_script(
        definition,
        session_remote_scripts._owned_teardown_script(
            definition=definition,
            session_id=session_id,
            expected_session_generation_id=expected_session_generation_id,
            expected_cleanup_operation_id=cleanup_operation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            cluster=cluster,
        ),
    )
    report = SessionLifecycleReport.model_validate_json(output)
    if report.cleanup_operation_id != cleanup_operation_id:
        raise RelayError(
            "remote teardown cleanup operation does not match the durable owner-session intent"
        )
    expected_policy = {
        "stop_worker": stop_worker,
        "cancel_jobs": cancel_jobs,
        "cancel_scheduler_jobs": cancel_scheduler_jobs,
    }
    if report.cleanup_policy != expected_policy:
        raise RelayError(
            "remote teardown cleanup policy does not match the durable owner-session intent"
        )
    if (
        report.relay_cancel_requested is not cancel_jobs
        or report.scheduler_cancel_requested is not cancel_scheduler_jobs
    ):
        raise RelayError(
            "remote teardown cancellation evidence does not match the durable owner-session intent"
        )
    return report


def finalize_remote_session_cleanup_report(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    cleanup_operation_id: str,
    cleanup_policy: dict[str, bool],
    report: SessionLifecycleReport,
) -> OwnedSessionRecoveryStatus:
    """Persist and re-read one immutable coordinator-verified cleanup report."""
    request = OwnedSessionCleanupFinalizeRequest(
        cluster=cluster,
        session_id=session_id,
        expected_session_generation_id=session_generation_id,
        expected_cleanup_operation_id=cleanup_operation_id,
        expected_cleanup_policy=cleanup_policy,
        coordinator_report=report,
        coordinator_report_sha256=session_lifecycle_report_sha256(report),
    )
    request_payload = request.model_dump_json().encode("utf-8")
    output = session_remote_scripts._ssh_stdin_command(
        definition,
        session_remote_scripts._owned_cleanup_finalize_script(definition=definition),
        input_bytes=request_payload,
        input_limit=MAX_OWNED_SESSION_CLEANUP_FINALIZE_BYTES,
        stdout_limit=_MAX_REMOTE_SESSION_STDOUT_BYTES,
    )
    status = OwnedSessionRecoveryStatus.model_validate_json(output)
    expected_reference, _ = session_lifecycle_report._coordinator_report_reference(report)
    if not (
        status.recovery_verified
        and status.cleanup_receipt
        and status.cleanup_paths_pending is False
        and status.session_generation_id == session_generation_id
        and status.coordinator_report_bound
        and status.coordinator_report is None
        and status.coordinator_report_ref == expected_reference
        and status.coordinator_report_sha256 == expected_reference.sha256
    ):
        raise RelayError("remote coordinator cleanup report finalization was not exact")
    return status


def read_remote_session_cleanup_report(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
    status: OwnedSessionRecoveryStatus,
) -> SessionLifecycleReport:
    """Retrieve one finalized report through its exact bounded sidecar reference."""
    reference = status.coordinator_report_ref
    generation_id = status.session_generation_id
    if not (
        status.cluster == cluster
        and status.session_id == session_id
        and status.recovery_verified
        and status.cleanup_receipt
        and status.cleanup_paths_pending is False
        and generation_id is not None
        and status.coordinator_report_bound
        and status.coordinator_report is None
        and reference is not None
        and status.coordinator_report_sha256 == reference.sha256
    ):
        raise RelayError("remote coordinator cleanup report reference is not exact")
    request = OwnedSessionCleanupReportReadRequest(
        cluster=cluster,
        session_id=session_id,
        expected_session_generation_id=generation_id,
        coordinator_report_ref=reference,
    )
    output = session_remote_scripts._ssh_stdin_command(
        definition,
        session_remote_scripts._owned_cleanup_report_read_script(definition=definition),
        input_bytes=request.model_dump_json().encode("utf-8"),
        input_limit=256 * 1024,
        stdout_limit=MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES + 64 * 1024,
    )
    try:
        report = SessionLifecycleReport.model_validate_json(output)
    except ValueError as exc:
        raise RelayError(f"remote coordinator cleanup report is invalid: {exc}") from exc
    payload = session_lifecycle_report_bytes(report)
    if not (
        len(payload) == reference.size
        and hmac.compare_digest(hashlib.sha256(payload).hexdigest(), reference.sha256)
        and report.cluster == cluster
        and report.session_id == session_id
        and report.session_generation_id == generation_id
    ):
        raise RelayError("remote coordinator cleanup report did not match its exact reference")
    return report


def detach_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
    cluster: str | None = None,
) -> SessionLifecycleReport:
    """Detach the desktop while intentionally retaining the remote session."""
    status = status_remote_session(definition=definition, session_id=session_id)
    pid = status.get("api_pid")
    running = status.get("running") is True
    ownership_verified = status.get("ownership_verified") is True
    identity_verified = status.get("session_id") == session_id
    generation_id = status.get("session_generation_id")
    generation_verified = isinstance(generation_id, str) and bool(generation_id)
    retained = running and ownership_verified and identity_verified and generation_verified
    resource_id = str(pid) if isinstance(pid, int) else session_id
    if retained:
        outcome: Literal["retained", "missing", "refused"] = "retained"
        detail = "remote relay session intentionally retained for reattachment"
    elif not running:
        outcome = "missing"
        detail = "remote relay API was not running after detach"
    else:
        outcome = "refused"
        detail = "remote relay API retention could not be tied to the requested owned generation"
    return SessionLifecycleReport(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=str(generation_id) if generation_verified else None,
        mode="detach",
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id=resource_id,
                location=definition.ssh_host,
                action="retain",
                ownership_verified=ownership_verified and identity_verified,
                outcome=outcome,
                verified_after_operation=retained,
                residual=not retained,
                detail=detail,
            )
        ],
        errors=[] if retained else [detail],
    )
