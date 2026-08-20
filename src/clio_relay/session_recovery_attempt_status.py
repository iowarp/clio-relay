"""Pre-metadata start-attempt recovery status inspection (#231 rework).

Extracted from ``session_lifecycle.py``: projecting a durable start-attempt
journal (before ``metadata.json`` exists) into read-only recovery status
evidence, and the bounded exact-membership job page reader used by both this
inspector and the failed-start cleanup executor. Called by
``inspect_owned_session_recovery_status``, which stays resident in
``session_lifecycle.py``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Literal, Protocol, cast

import clio_relay.session_process_scope as session_process_scope
import clio_relay.session_start_attempt_validation as session_start_attempt_validation
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.errors import RelayError
from clio_relay.session_wire_models import OwnedSessionInputPolicy, OwnedSessionRecoveryStatus

if TYPE_CHECKING:
    from pathlib import Path

    from clio_relay.models import RelayJob
    from clio_relay.session_transaction import _OwnedSessionTransaction

logger = logging.getLogger(__name__)


class _FailedStartCleanupQueue(Protocol):
    """Core operations required to close an admitted pre-metadata start."""

    def owner_session_generation_status(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> dict[str, object]:
        """Return exact admission state for one generation."""
        ...

    def set_owner_session_closing(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
        operation_id: str | None = None,
        stop_worker: bool = False,
        cancel_jobs: bool = False,
        cancel_scheduler_jobs: bool = False,
    ) -> dict[str, object]:
        """Persist one immutable cleanup intent."""
        ...

    def list_owner_session_jobs_page(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str | None,
        cursor: str | None = None,
        limit: int = 500,
        cluster: str | None = None,
        include_terminal: bool = False,
    ) -> tuple[list[RelayJob], str | None, int, int]:
        """Return one exact generation-scoped membership page."""
        ...


def _inspect_owned_session_start_attempt_status(
    *,
    cluster: str,
    session_id: str,
    core_dir: Path,
    proc_root: Path,
    transaction: _OwnedSessionTransaction,
    metadata_error: str,
    expected_start_operation_id: str | None = None,
    expected_cluster_route_revision: str | None = None,
) -> OwnedSessionRecoveryStatus | None:
    """Project one exact pre-metadata start journal into read-only status evidence."""
    from clio_relay.core_queue import ClioCoreQueue

    try:
        current_attempt = session_start_attempt_validation._validated_start_attempt(
            transaction,
            cluster=cluster,
            session_id=session_id,
        )
    except RelayError:
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            errors=[metadata_error, "owned-session start attempt identity is invalid"],
        )
    if (
        expected_start_operation_id is not None
        and current_attempt is not None
        and current_attempt.get("start_operation_id") != expected_start_operation_id
    ):
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            start_operation_id=expected_start_operation_id,
            cluster_route_revision=expected_cluster_route_revision,
            start_state="not_current",
            start_retryable=False,
            errors=[
                "another operation owns the current transition; this selector was never "
                "accepted or is no longer current"
            ],
        )
    try:
        attempt = session_start_attempt_validation._validated_start_attempt(
            transaction,
            cluster=cluster,
            session_id=session_id,
            start_operation_id=expected_start_operation_id,
            cluster_route_revision_value=expected_cluster_route_revision,
        )
    except RelayError:
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            errors=[metadata_error, "owned-session start attempt identity is invalid"],
        )
    if attempt is None:
        return None
    generation_id = cast(str, attempt["session_generation_id"])
    validated_start_operation_id = cast(str, attempt["start_operation_id"])
    registry_sha256 = attempt.get("cluster_registry_sha256")
    route_revision = attempt.get("cluster_route_revision")
    remote_api_port = attempt.get("remote_api_port")
    start_phase = attempt.get("start_phase")
    systemd_unit = attempt.get("systemd_unit")
    systemd_description = attempt.get("systemd_description")
    cgroup_path = attempt.get("systemd_cgroup_path")
    invocation_id = attempt.get("systemd_invocation_id")
    phase = cast(Literal["pending", "admitted", "scope_bound", "contained"], start_phase)
    errors: list[str] = []
    admission_status: dict[str, object] | None = None
    durable_generation_verified = False
    try:
        admission_status = ClioCoreQueue(core_dir).owner_session_generation_status(
            session_id,
            session_generation_id=generation_id,
        )
        active_generation = admission_status.get("active_generation_id")
        closing_generation = admission_status.get("closing_generation_id")
        common_admission_identity = bool(
            admission_status.get("owner_session_id") == session_id
            and admission_status.get("session_generation_id") == generation_id
            and closing_generation is None
        )
        if phase == "pending":
            admission_consistent = common_admission_identity and active_generation in {
                None,
                generation_id,
            }
        else:
            admission_consistent = bool(
                common_admission_identity
                and active_generation == generation_id
                and admission_status.get("open") is True
            )
        durable_generation_verified = bool(
            common_admission_identity
            and active_generation == generation_id
            and admission_status.get("open") is True
        )
        if not admission_consistent:
            errors.append("owned-session start attempt conflicts with durable core admission")
    except (OSError, RelayError, ValueError) as exc:
        errors.append(f"could not verify owned-session start admission: {exc}")

    cluster_registry_verified = False
    registry_payload = transaction.read_bytes(
        f"cluster-registry-{generation_id}.json",
        maximum_bytes=MAX_CLUSTER_REGISTRY_BYTES,
        required=False,
    )
    if registry_payload is not None:
        try:
            raw_registry = cast(object, json.loads(registry_payload))
            registry = ClusterRegistry.model_validate(raw_registry)
            # clio-relay#217 rework: the SAME snapshot-trust relaxation
            # inspect_owned_session_recovery_status applies below -- this
            # pre-metadata start-attempt path reads the identical frozen
            # per-generation cluster-registry snapshot and strands the same
            # way across a relay upgrade if it also requires a FRESH
            # cluster_route_revision() recomputation to match the value
            # recorded in start-attempt.json. The sha256 check already
            # proves these exact bytes are tamper-clean; recomputing the
            # route revision with a different algorithm generation than the
            # one that wrote this attempt adds no additional tamper
            # detection, only false positives that block session start
            # --replace with no recovery path.
            cluster_registry_verified = bool(
                hashlib.sha256(registry_payload).hexdigest() == registry_sha256
                and set(registry.clusters) == {cluster}
                and registry.clusters[cluster].name == cluster
            )
            if cluster_registry_verified:
                recomputed_route_revision = cluster_route_revision(registry.clusters[cluster])
                if recomputed_route_revision != route_revision:
                    logger.warning(
                        "cluster_route_revision_algorithm_skew: session %r cluster %r "
                        "start attempt recorded route revision %r but the installed "
                        "package recomputes %r from the identical tamper-clean "
                        "snapshot; trusting the recorded value (clio-relay#217)",
                        session_id,
                        cluster,
                        route_revision,
                        recomputed_route_revision,
                    )
        except (TypeError, ValueError):
            cluster_registry_verified = False
        if not cluster_registry_verified:
            errors.append("owned-session start registry identity is invalid")

    generation_processes: list[session_process_scope._OwnedGenerationProcess] = []
    generation_process_scan_verified = False
    if phase in {"scope_bound", "contained"}:
        try:
            generation_processes = session_process_scope._recorded_scope_processes(
                proc_root=proc_root,
                systemd_unit=cast(str, systemd_unit),
                systemd_cgroup_path=cast(str, cgroup_path),
                systemd_invocation_id=cast(str, invocation_id),
                systemd_description=cast(str, systemd_description),
            )
            generation_process_scan_verified = True
        except RelayError as exc:
            errors.append(str(exc))
    else:
        from clio_relay.process_containment import adopt_linux_systemd_scope_identity

        try:
            adopted_scope = adopt_linux_systemd_scope_identity(
                unit=cast(str, systemd_unit),
                description=cast(str, systemd_description),
            )
            if adopted_scope is None:
                generation_process_scan_verified = True
            else:
                generation_processes = session_process_scope._recorded_scope_processes(
                    proc_root=proc_root,
                    systemd_unit=adopted_scope["systemd_unit"],
                    systemd_cgroup_path=adopted_scope["cgroup_path"],
                    systemd_invocation_id=adopted_scope["systemd_invocation_id"],
                    systemd_description=cast(str, systemd_description),
                )
                generation_process_scan_verified = True
        except (RelayError, RuntimeError) as exc:
            errors.append(f"could not verify predeclared owned-session scope: {exc}")

    attempt_verified = not errors
    start_error = cast(str | None, attempt.get("error"))
    recovery_verified = bool(
        attempt_verified
        and cluster_registry_verified
        and durable_generation_verified
        and generation_process_scan_verified
    )
    return OwnedSessionRecoveryStatus(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=generation_id,
        start_operation_id=validated_start_operation_id,
        cluster_route_revision=cast(str, route_revision),
        owner="clio-relay",
        remote_api_port=cast(int, remote_api_port),
        leader_process_state="absent" if not generation_processes else "unverified",
        process_state=(
            "owned_running"
            if recovery_verified and generation_processes
            else "absent"
            if recovery_verified
            else "unverified"
        ),
        running=bool(recovery_verified and generation_processes),
        process_absence_verified=(
            recovery_verified and generation_process_scan_verified and not generation_processes
        ),
        generation_process_pids=[process.pid for process in generation_processes],
        generation_process_absence_verified=(
            generation_process_scan_verified and not generation_processes
        ),
        metadata_verified=False,
        cluster_registry_verified=cluster_registry_verified,
        durable_generation_verified=durable_generation_verified,
        ownership_verified=recovery_verified,
        recovery_verified=recovery_verified,
        ownership_token_present=True,
        admission_status=admission_status,
        start_state=("failed" if start_error is not None else "starting"),
        start_phase=phase,
        start_attempt_verified=attempt_verified,
        start_retryable=bool(attempt_verified and start_error is None),
        start_replace=cast(bool, attempt["replace"]),
        start_require_token=cast(bool, attempt["require_token"]),
        start_input_policy=(
            OwnedSessionInputPolicy.model_validate(attempt["input_policy"])
            if "input_policy" in attempt
            else None
        ),
        start_expected_api_release_identity_sha256=cast(
            str | None,
            attempt["expected_api_release_identity_sha256"],
        ),
        start_error=start_error,
        errors=errors,
    )


def _owned_generation_job_ids(
    queue: _FailedStartCleanupQueue,
    *,
    session_id: str,
    session_generation_id: str,
) -> list[str]:
    """Return one bounded, exact generation membership snapshot."""
    cursor: str | None = None
    job_ids: list[str] = []
    expected_total: int | None = None
    while True:
        jobs, next_cursor, source_total, _scanned = queue.list_owner_session_jobs_page(
            session_id,
            session_generation_id=session_generation_id,
            cursor=cursor,
            limit=500,
            include_terminal=True,
        )
        if expected_total is None:
            expected_total = source_total
            if expected_total > 1_000:
                raise RelayError("failed-start cleanup job membership exceeds its safe bound")
        elif source_total != expected_total:
            raise RelayError("failed-start cleanup job membership changed while observed")
        job_ids.extend(job.job_id for job in jobs)
        if len(job_ids) > 1_000:
            raise RelayError("failed-start cleanup job membership exceeds its safe bound")
        if next_cursor is None:
            break
        if next_cursor == cursor:
            raise RelayError("failed-start cleanup job paging made no progress")
        cursor = next_cursor
    if len(job_ids) != expected_total:
        raise RelayError("failed-start cleanup job membership was not read exactly")
    if job_ids != sorted(set(job_ids)):
        raise RelayError("failed-start cleanup job membership is not unique and sorted")
    return job_ids
