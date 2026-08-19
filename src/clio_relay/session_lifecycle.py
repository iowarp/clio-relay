"""Owned remote relay session lifecycle helpers."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import stat
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast
from uuid import uuid4

import clio_relay.session_lifecycle_report as session_lifecycle_report
import clio_relay.session_process_scope as session_process_scope
import clio_relay.session_recovery_attempt_status as session_recovery_attempt_status
import clio_relay.session_recovery_cleaned_receipt as session_recovery_cleaned_receipt
import clio_relay.session_recovery_cleanup_receipt as session_recovery_cleanup_receipt
import clio_relay.session_remote_command as session_remote_command
import clio_relay.session_remote_scripts as session_remote_scripts
import clio_relay.session_start_attempt_validation as session_start_attempt_validation
import clio_relay.session_start_query as session_start_query
import clio_relay.session_startup_receipt as session_startup_receipt
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.errors import (
    RelayError,
)
from clio_relay.identifiers import validate_durable_record_id

# cli.py compatibility re-exports (#231 split/session-lifecycle rework).
# cli.py is another agent's active split-in-progress territory (see the
# rework SETUP notes) and carries its own line-count ratchet baseline --
# repointing its bare imports for the names below to their real new homes
# (session_cleanup_execution.py, session_cleanup_reporting.py,
# session_lifecycle_report.py, session_start_execution.py,
# session_start_query.py, session_start_wait.py) is a net LOC increase
# there (each single-purpose `from module import name` statement cannot
# collapse into fewer lines than the names removed from the old
# consolidated block) and would regress that ratchet. cli.py's own imports
# and every call site stay byte-for-byte unchanged; only
# session_lifecycle.py re-exports these under their original names so
# `from clio_relay.session_lifecycle import X` keeps resolving. Every
# OTHER consumer (this module's own internal calls, tests,
# transport_probe.py) is repointed to the real owner module -- this is the
# one deliberate exception, not a default escape hatch, and should be
# deleted the moment cli.py's own split lands and can absorb the
# "one-line import repoint" itself.
from clio_relay.session_cleanup_execution import (
    execute_owned_session_teardown,  # noqa: F401
)
from clio_relay.session_cleanup_reporting import (
    execute_owned_session_cleanup_finalize,  # noqa: F401
    execute_owned_session_cleanup_report_read,  # noqa: F401
)
from clio_relay.session_lifecycle_report import (
    OwnedSessionCleanupFinalizeRequest,
    OwnedSessionCleanupReportReadRequest,
    SessionLifecycleReport,
    cleanup_connectors_cover_gateways,  # noqa: F401
    session_lifecycle_report_bytes,
    session_lifecycle_report_sha256,
)
from clio_relay.session_start_execution import (
    execute_owned_session_identity_challenge,  # noqa: F401
    execute_owned_session_start,  # noqa: F401
)
from clio_relay.session_start_query import (
    plan_remote_session_start,  # noqa: F401
    query_remote_session_start,  # noqa: F401
    watch_remote_session_start,  # noqa: F401
)
from clio_relay.session_start_wait import (
    wait_owned_session_start_status,  # noqa: F401
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
    OwnedSessionIdentityChallengeRequest,  # noqa: F401 -- cli.py bare-imports this; see rationale above
    OwnedSessionInputPolicy,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartPlan,
    OwnedSessionStartReceipt,
    OwnedSessionStartRejection,  # noqa: F401 -- cli.py bare-imports this; see rationale above
    OwnedSessionStartRequest,  # noqa: F401 -- cli.py bare-imports this; see rationale above
    OwnedSessionStartResult,
    OwnedSessionTeardownRequest,  # noqa: F401 -- cli.py bare-imports this; see rationale above
    SessionApiReleaseIdentity,
)

if TYPE_CHECKING:
    from clio_relay.session_process_scope import _OwnedGenerationProcess
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
    plan = session_start_query.plan_remote_session_start(
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
        return session_start_query.query_remote_session_start(
            definition=definition,
            plan=plan,
            transport_deadline_exceeded=True,
        )
    except session_remote_command._RemoteSessionCommandAmbiguous:
        # The durable start may exist: resolve it against remote state instead
        # of escaping as a bare RelayError. Not a deadline, so the flag stays
        # false (clio-relay#158).
        return session_start_query.query_remote_session_start(definition=definition, plan=plan)
    except session_remote_command._RemoteSessionCommandRejected as exc:
        rejection = exc.rejection
        if not (
            rejection.cluster == plan.cluster
            and rejection.session_id == plan.session_id
            and rejection.start_operation_id == plan.start_operation_id
            and rejection.cluster_route_revision == plan.cluster_route_revision
        ):
            return session_start_query.query_remote_session_start(definition=definition, plan=plan)
        observed = session_start_query.query_remote_session_start(definition=definition, plan=plan)
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
