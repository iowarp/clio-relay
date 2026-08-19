"""Owned remote relay session teardown execution.

split/session-lifecycle slice J (#231): the failed-start and normal teardown
execution cluster (worker-service stop, cleanup-receipt retry, failed-scope
termination, the failed-start teardown path, and execute_owned_session_
teardown itself) moved out of session_lifecycle.py. One-directional
dependent on session_lifecycle.inspect_owned_session_recovery_status (the
read path this cluster verifies against) and the shared byte-cap constants
still defined there. session_lifecycle is imported back INSIDE each
top-level function (not at module scope): session_lifecycle imports this
module for its cli.py-compatibility re-export block, so a module-scope
back-import here creates a load-order-dependent circular import --
deferred to call time, it is import-order-independent, matching the
standard pattern for breaking a two-module cycle.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import clio_relay.session_cleanup_targets as session_cleanup_targets
import clio_relay.session_process_scope as session_process_scope
import clio_relay.session_recovery_attempt_status as session_recovery_attempt_status
import clio_relay.session_remote_command as session_remote_command
import clio_relay.session_start_attempt_validation as session_start_attempt_validation
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterRegistry,
    cluster_route_revision,
)
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle_report import SessionLifecycleReport
from clio_relay.session_transaction import (
    _MAX_OWNED_SESSION_DOCUMENT_BYTES,
)
from clio_relay.session_validation import _validate_session
from clio_relay.session_wire_models import (
    MAX_SESSION_START_ERROR_CHARS,
    CleanupResource,
    OwnedSessionTeardownRequest,
    RemoteSessionStateEvidence,
)

if TYPE_CHECKING:
    from clio_relay.session_recovery_attempt_status import _FailedStartCleanupQueue
    from clio_relay.session_transaction import _OwnedSessionTransaction


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
    # Deferred import: breaks the module-load-time cycle with session_lifecycle
    # (which imports this module back for its cli.py-compatibility re-export
    # block) -- see the module docstring.
    import clio_relay.session_lifecycle as session_lifecycle

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
                else session_lifecycle._MAX_API_STARTUP_RECEIPT_BYTES
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
    # Deferred import: breaks the module-load-time cycle with session_lifecycle
    # (which imports this module back for its cli.py-compatibility re-export
    # block) -- see the module docstring.
    import clio_relay.session_lifecycle as session_lifecycle
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

    with session_lifecycle.open_owned_session_transaction(
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
        status = session_lifecycle.inspect_owned_session_recovery_status(
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
                        else session_lifecycle._MAX_API_STARTUP_RECEIPT_BYTES
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
