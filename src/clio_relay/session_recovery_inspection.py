"""Owned remote relay session recovery-status inspection.

split/session-lifecycle slice K (#231): the single, dominant read path
(inspect_owned_session_recovery_status and its private metadata/registry
reader _read_owned_session_document) moved out of session_lifecycle.py.
Fully self-contained -- no other resident session_lifecycle.py function is
its consumer, and it needs nothing back from session_lifecycle.py except
the single shared byte-cap constant _MAX_API_STARTUP_RECEIPT_BYTES, which
stays defined there (session_start_execution.py and
session_cleanup_execution.py already qualify it as
session_lifecycle._MAX_API_STARTUP_RECEIPT_BYTES per slice J) -- this
module references it the same deferred-import qualified way, inside the
one function that needs it, to stay import-order-independent.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import stat
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import clio_relay.session_process_scope as session_process_scope
import clio_relay.session_recovery_attempt_status as session_recovery_attempt_status
import clio_relay.session_recovery_cleaned_receipt as session_recovery_cleaned_receipt
import clio_relay.session_recovery_cleanup_receipt as session_recovery_cleanup_receipt
import clio_relay.session_start_attempt_validation as session_start_attempt_validation
import clio_relay.session_startup_receipt as session_startup_receipt
from clio_relay.cluster_config import ClusterRegistry
from clio_relay.errors import RelayError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.session_transaction import (
    _MAX_OWNED_SESSION_DOCUMENT_BYTES,
    open_owned_session_transaction,
)
from clio_relay.session_validation import _validate_session
from clio_relay.session_wire_models import (
    OwnedSessionInputPolicy,
    OwnedSessionRecoveryStatus,
    SessionApiReleaseIdentity,
)

if TYPE_CHECKING:
    from clio_relay.session_process_scope import _OwnedGenerationProcess
    from clio_relay.session_transaction import _OwnedSessionTransaction

logger = logging.getLogger(__name__)

_MAX_PROC_RECORD_BYTES = 1024 * 1024


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
    # Deferred import: breaks the module-load-time cycle with session_lifecycle
    # (which imports this module back for its cli.py-compatibility re-export
    # block) -- see the module docstring.
    import clio_relay.session_lifecycle as session_lifecycle
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
                    maximum_bytes=session_lifecycle._MAX_API_STARTUP_RECEIPT_BYTES,
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
                    recomputed_route_revision = session_lifecycle.cluster_route_revision(
                        registry.clusters[cluster]
                    )
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
