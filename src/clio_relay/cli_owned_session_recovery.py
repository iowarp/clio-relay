"""Owner-session generation and recovery-status verification (iowarp/
clio-relay#231 continuation): the helpers that establish, verify, and
close out one owner session's authoritative generation identity across
``session start`` and ``session teardown``."""

from __future__ import annotations

import json
import os
import re
import stat
import time
from pathlib import Path
from typing import cast

from filelock import FileLock
from filelock import Timeout as FileLockTimeout
from pydantic import ValidationError

import clio_relay.cli_cleanup_report as cli_cleanup_report
import clio_relay.core_queue as core_queue
import clio_relay.remote_cli as remote_cli
import clio_relay.session_lifecycle as session_lifecycle
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.errors import RelayError
from clio_relay.models import (
    OwnerSessionClosure,
)
from clio_relay.session_lifecycle import (
    OwnedSessionRecoveryStatus,
    SessionLifecycleReport,
    open_owned_session_transaction,
)
from clio_relay.validation_report import (
    ValidationResource,
)

OWNED_SESSION_RECOVERY_TRANSITION_TIMEOUT_SECONDS = 90.0


SPACK_CONFIGURATION_OBSERVATION_TIMEOUT_SECONDS = 60.0


MAX_SPACK_CONFIGURATION_OBSERVATION_OUTPUT_BYTES = 128 * 1024


MAX_SPACK_CONFIGURATION_TREE_ENTRIES = 1_024


def _inspect_owned_session_recovery_after_transition(
    *,
    cluster: str,
    session_id: str,
    core_dir: Path,
    home: Path | None = None,
    timeout_seconds: float = OWNED_SESSION_RECOVERY_TRANSITION_TIMEOUT_SECONDS,
) -> OwnedSessionRecoveryStatus:
    """Wait for an ambiguous start transition, then inspect its exact durable identity."""
    if re.fullmatch(r"[A-Za-z0-9_-]+", session_id) is None:
        raise RelayError("session_id must contain only letters, numbers, hyphen, or underscore")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    selected_home = home or Path.home()
    session_dir = selected_home / ".local" / "share" / "clio-relay" / "sessions" / session_id
    transition_path = session_dir / "transition.lock"
    deadline = time.monotonic() + timeout_seconds
    transition_status: os.stat_result | None = None
    while transition_status is None:
        try:
            transition_status = transition_path.lstat()
        except FileNotFoundError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RelayError(
                    "owned session transition lock did not materialize during the bounded "
                    "recovery wait; a delayed remote start cannot be ruled out"
                ) from None
            time.sleep(min(0.05, remaining))
    if not stat.S_ISREG(transition_status.st_mode):
        raise RelayError("owned session transition lock is not a regular file")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RelayError(
            "owned session start transition could not be inspected during the bounded recovery wait"
        )
    if os.name == "posix" and getattr(os, "O_NOFOLLOW", 0) and getattr(os, "O_DIRECTORY", 0):
        with open_owned_session_transaction(
            session_id=session_id,
            create=False,
            timeout_seconds=remaining,
            home=selected_home,
        ) as transaction:
            locked_status = os.fstat(transaction.lock_fd)
            if (locked_status.st_dev, locked_status.st_ino) != (
                transition_status.st_dev,
                transition_status.st_ino,
            ):
                raise RelayError("owned session transition lock changed during recovery")
            return session_lifecycle.inspect_owned_session_recovery_status(
                cluster=cluster,
                session_id=session_id,
                core_dir=core_dir,
                home=selected_home,
                transaction=transaction,
            )
    try:
        with FileLock(
            str(transition_path),
            timeout=remaining,
            mode=0o600,
        ):
            locked_status = transition_path.lstat()
            lock_identity_changed = os.name == "posix" and (
                locked_status.st_dev,
                locked_status.st_ino,
            ) != (transition_status.st_dev, transition_status.st_ino)
            if not stat.S_ISREG(locked_status.st_mode) or lock_identity_changed:
                raise RelayError("owned session transition lock changed during recovery")
            return session_lifecycle.inspect_owned_session_recovery_status(
                cluster=cluster,
                session_id=session_id,
                core_dir=core_dir,
                home=selected_home,
            )
    except FileLockTimeout as exc:
        raise RelayError(
            "owned session start is still in progress after the bounded recovery wait"
        ) from exc


def _verified_owner_session_generation(
    status: dict[str, object],
    *,
    session_id: str,
) -> str:
    """Return the exact durable generation for a session teardown attempt."""
    import clio_relay.cli as cli

    if status.get("session_id") != session_id or status.get("owner") != "clio-relay":
        raise RelayError("remote session status did not prove the requested owned session")
    generation_id = status.get("session_generation_id")
    if not isinstance(generation_id, str) or not generation_id:
        raise RelayError("remote session status did not contain an owned generation id")
    cli._require_durable_session_identity(generation_id, field="session_generation_id")
    if status.get("running") is True and status.get("ownership_verified") is not True:
        raise RelayError("running remote session failed process ownership verification")
    return generation_id


def _owned_session_recovery_status(
    *,
    queue: core_queue.ClioCoreQueue,
    definition: ClusterDefinition,
    remote_execution: bool,
    cluster: str,
    session_id: str,
) -> OwnedSessionRecoveryStatus:
    """Read exact dead-session recovery evidence at the authoritative boundary."""
    if remote_execution:
        raw_status = cast(
            object,
            json.loads(
                remote_cli.run_remote_clio(
                    definition,
                    [
                        "session",
                        "recovery-status",
                        "--cluster",
                        cluster,
                        "--session-id",
                        session_id,
                    ],
                )
            ),
        )
        return OwnedSessionRecoveryStatus.model_validate(raw_status)
    return _inspect_owned_session_recovery_after_transition(
        cluster=cluster,
        session_id=session_id,
        core_dir=queue.root,
    )


def _verified_recovered_owner_session_generation(
    status: OwnedSessionRecoveryStatus,
    *,
    cluster: str,
    session_id: str,
) -> str:
    """Return an exact generation only from complete recovery evidence."""
    import clio_relay.cli as cli

    generation_id = status.session_generation_id
    committed_identity = status.metadata_verified
    pre_metadata_identity = bool(
        not status.metadata_verified
        and not status.cleanup_receipt
        and status.start_attempt_verified
        and status.start_state in {"starting", "failed"}
        and status.start_phase is not None
    )
    if not (
        status.cluster == cluster
        and status.session_id == session_id
        and status.owner == "clio-relay"
        and (committed_identity or pre_metadata_identity)
        and status.cluster_registry_verified
        and status.durable_generation_verified
        and status.ownership_verified
        and status.recovery_verified
        and not status.errors
        and generation_id is not None
    ):
        detail = "; ".join(status.errors) or "recovery proof was incomplete"
        raise RelayError(f"owned session recovery was refused: {detail}")
    cli._require_durable_session_identity(generation_id, field="session_generation_id")
    if status.running and status.process_state != "owned_running":
        raise RelayError("owned session recovery did not prove the running process identity")
    if not status.running and status.process_state not in {
        "absent",
        "owned_terminal",
        "cleanup_pending",
        "already_closed",
    }:
        raise RelayError("owned session recovery did not prove the recorded process stopped")
    return generation_id


def _owner_session_recovery_validation_resource(
    status: OwnedSessionRecoveryStatus,
) -> ValidationResource:
    """Project recovery evidence into the canonical machine-readable report."""
    generation_id = status.session_generation_id or "generation-unverified"
    return ValidationResource(
        kind="owner_session_recovery",
        resource_id=f"{status.session_id}:{generation_id}",
        role="cleanup_identity_recovery",
        cluster=status.cluster,
        state="verified" if status.recovery_verified else "refused",
        metadata={
            "session_generation_id": status.session_generation_id,
            "api_pid": status.api_pid,
            "remote_api_port": status.remote_api_port,
            "process_start_marker": status.process_start_marker,
            "leader_process_state": status.leader_process_state,
            "process_state": status.process_state,
            "running": status.running,
            "process_absence_verified": status.process_absence_verified,
            "generation_process_pids": status.generation_process_pids,
            "generation_process_absence_verified": (status.generation_process_absence_verified),
            "metadata_verified": status.metadata_verified,
            "cluster_registry_verified": status.cluster_registry_verified,
            "durable_generation_verified": status.durable_generation_verified,
            "cleanup_receipt": status.cleanup_receipt,
            "cleanup_paths_pending": status.cleanup_paths_pending,
            "api_release_identity_verified": status.api_release_identity_verified,
            "ownership_token_present": status.ownership_token_present,
            "ownership_verified": status.ownership_verified,
            "recovery_verified": status.recovery_verified,
            "errors": status.errors,
            "admission_status": status.admission_status,
        },
    )


def _verified_owner_session_detach(
    report: SessionLifecycleReport,
    *,
    session_id: str,
    expected_session_generation_id: str | None = None,
) -> str:
    """Return the exact generation only when detach retained its owned API."""
    import clio_relay.cli as cli

    if report.mode != "detach" or report.session_id != session_id:
        raise RelayError("session detach report identity did not match the requested session")
    generation_id = report.session_generation_id
    if not isinstance(generation_id, str) or not generation_id:
        raise RelayError("session detach did not prove an owned session generation")
    cli._require_durable_session_identity(generation_id, field="session_generation_id")
    if (
        expected_session_generation_id is not None
        and generation_id != expected_session_generation_id
    ):
        raise RelayError("owned session generation changed during desktop detach")
    if report.errors or report.residual_resources:
        raise RelayError("session detach did not prove remote session retention")
    api_resources = [
        resource for resource in report.resources if resource.kind == "remote_relay_api"
    ]
    if len(api_resources) != 1:
        raise RelayError("session detach must contain exactly one remote relay API result")
    api_resource = api_resources[0]
    if not (
        api_resource.action == "retain"
        and api_resource.outcome == "retained"
        and api_resource.ownership_verified
        and api_resource.verified_after_operation
        and not api_resource.residual
    ):
        raise RelayError("session detach did not verify remote relay API retention")
    return generation_id


def _require_exact_owner_session_admission(
    status: dict[str, object],
    *,
    owner_session_id: str,
    session_generation_id: str,
    cleanup_operation_id: str,
    cleanup_policy: dict[str, bool],
    closed: bool,
    label: str,
) -> dict[str, object] | None:
    """Require one exact closing or closed admission record before closure handling."""
    raw_intent = status.get("cleanup_intent")
    intent = cast(dict[str, object], raw_intent) if isinstance(raw_intent, dict) else None
    expected_policy = {
        key: cleanup_policy[key] for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
    }
    raw_closure = status.get("closure")
    closure: dict[str, object] | None = None
    closure_matches = False
    if closed:
        try:
            closure_model = OwnerSessionClosure.model_validate(raw_closure)
        except ValidationError as exc:
            raise RelayError(f"{label} admission closure evidence was invalid") from exc
        closure_matches = (
            closure_model.schema_version == "clio-relay.owner-session-closure.v1"
            and closure_model.owner_session_id == owner_session_id
            and closure_model.session_generation_id == session_generation_id
            and closure_model.covered_by_session_generation_id is None
            and closure_model.covered_legacy_job_ids == []
            and closure_model.residual_resource_ids == []
        )
        closure = cast(dict[str, object], closure_model.model_dump(mode="json"))
    if not (
        status.get("schema_version") == "clio-relay.owner-session-admission-status.v1"
        and status.get("owner_session_id") == owner_session_id
        and status.get("session_generation_id") == session_generation_id
        and status.get("active_generation_id") == (None if closed else session_generation_id)
        and status.get("closing_generation_id") == session_generation_id
        and status.get("active") is (not closed)
        and status.get("closing") is True
        and status.get("closed") is closed
        and status.get("open") is False
        and intent is not None
        and intent.get("schema_version") == "clio-relay.owner-session-cleanup-intent.v1"
        and intent.get("owner_session_id") == owner_session_id
        and intent.get("session_generation_id") == session_generation_id
        and intent.get("operation_id") == cleanup_operation_id
        and {
            key: intent.get(key) for key in ("stop_worker", "cancel_jobs", "cancel_scheduler_jobs")
        }
        == expected_policy
        and (closure_matches if closed else raw_closure is None)
    ):
        raise RelayError(f"{label} admission evidence was incomplete or inconsistent")
    return closure if closed else None


def _mark_owner_session_closed(
    *,
    queue: core_queue.ClioCoreQueue,
    definition: ClusterDefinition,
    cluster: str,
    remote_execution: bool,
    session_id: str,
    local_admission_session_id: str,
    session_generation_id: str,
    legacy_unversioned_job_ids: list[str],
    finalized_recovery: OwnedSessionRecoveryStatus,
    finalized_report: SessionLifecycleReport,
) -> None:
    """Close the authoritative generation, then its cluster-scoped desktop mirror."""
    if definition.name != cluster:
        raise RelayError("owner-session closure cluster identity changed")
    verified_report = cli_cleanup_report._verified_finalized_cleanup_report(
        finalized_recovery,
        report=finalized_report,
        cluster=cluster,
        session_id=session_id,
        expected_generation_id=session_generation_id,
        expected_cleanup_operation_id=finalized_report.cleanup_operation_id,
        expected_cleanup_policy=finalized_report.cleanup_policy,
    )
    cleanup_operation_id = verified_report.cleanup_operation_id
    if cleanup_operation_id is None:
        raise RelayError("finalized owner-session closure omitted its operation identity")
    raw_authoritative_admission = finalized_recovery.admission_status
    if not isinstance(raw_authoritative_admission, dict):
        raise RelayError("finalized owner-session closure omitted authoritative admission evidence")
    authoritative_admission = raw_authoritative_admission
    authoritative_already_closed = authoritative_admission.get("closed") is True
    authoritative_closure_evidence = _require_exact_owner_session_admission(
        authoritative_admission,
        owner_session_id=session_id,
        session_generation_id=session_generation_id,
        cleanup_operation_id=cleanup_operation_id,
        cleanup_policy=verified_report.cleanup_policy,
        closed=authoritative_already_closed,
        label="authoritative owner-session",
    )
    if authoritative_already_closed:
        if finalized_recovery.process_state != "already_closed":
            raise RelayError("authoritative closure evidence disagreed with recovery process state")
        if remote_execution and legacy_unversioned_job_ids:
            raise RelayError(
                "authoritative closure retry cannot infer legacy job coverage from admission status"
            )

    payload: dict[str, object]
    if remote_execution and not authoritative_already_closed:
        args = [
            "session",
            "mark-closed",
            "--session-id",
            session_id,
            "--session-generation-id",
            session_generation_id,
        ]
        for job_id in legacy_unversioned_job_ids:
            args.extend(["--legacy-unversioned-job-id", job_id])
        raw_payload = cast(
            object,
            json.loads(remote_cli.run_remote_clio(definition, args)),
        )
        if not isinstance(raw_payload, dict):
            raise RelayError("remote owner-session closure did not return a JSON object")
        payload = cast(dict[str, object], raw_payload)
    elif remote_execution:
        if authoritative_closure_evidence is None:  # pragma: no cover - verifier invariant
            raise RelayError("authoritative owner-session closure evidence disappeared")
        payload = authoritative_closure_evidence
    elif authoritative_already_closed:
        closure = queue.get_owner_session_closed(
            session_id,
            session_generation_id=session_generation_id,
        )
        if closure is None:
            raise RelayError("authoritative owner-session closure disappeared after admission read")
        payload = cast(dict[str, object], closure.model_dump(mode="json"))
        if legacy_unversioned_job_ids:
            legacy_closure = queue.get_owner_session_closed(
                session_id,
                session_generation_id=None,
            )
            if legacy_closure is None:
                raise RelayError("legacy owner-session closure disappeared after admission read")
            payload["legacy_closure"] = legacy_closure.model_dump(mode="json")
    else:
        closure = queue.set_owner_session_closed(
            session_id,
            session_generation_id=session_generation_id,
            residual_resource_ids=[],
            legacy_unversioned_job_ids=legacy_unversioned_job_ids,
        )
        payload = cast(dict[str, object], closure.model_dump(mode="json"))
        if legacy_unversioned_job_ids:
            legacy_closure = queue.get_owner_session_closed(
                session_id,
                session_generation_id=None,
            )
            if legacy_closure is None:
                raise RelayError("legacy owner-session closure was not persisted")
            payload["legacy_closure"] = legacy_closure.model_dump(mode="json")
    if (
        payload.get("owner_session_id") != session_id
        or payload.get("session_generation_id") != session_generation_id
        or payload.get("residual_resource_ids") != []
    ):
        raise RelayError("owner-session closure did not match the verified teardown generation")
    if legacy_unversioned_job_ids:
        raw_legacy_closure = payload.get("legacy_closure")
        if not isinstance(raw_legacy_closure, dict):
            raise RelayError("owner-session closure omitted legacy job coverage")
        legacy_closure = cast(dict[str, object], raw_legacy_closure)
        if (
            legacy_closure.get("session_generation_id") is not None
            or legacy_closure.get("covered_by_session_generation_id") != session_generation_id
            or legacy_closure.get("covered_legacy_job_ids")
            != sorted(set(legacy_unversioned_job_ids))
        ):
            raise RelayError("owner-session legacy coverage did not match verified job ids")
    local_status = queue.owner_session_generation_status(
        local_admission_session_id,
        session_generation_id=session_generation_id,
    )
    local_already_closed = local_status.get("closed") is True
    local_closure_evidence = _require_exact_owner_session_admission(
        local_status,
        owner_session_id=local_admission_session_id,
        session_generation_id=session_generation_id,
        cleanup_operation_id=cleanup_operation_id,
        cleanup_policy=verified_report.cleanup_policy,
        closed=local_already_closed,
        label="desktop owner-session mirror",
    )
    if local_already_closed:
        local_closure = queue.get_owner_session_closed(
            local_admission_session_id,
            session_generation_id=session_generation_id,
        )
        if local_closure is None:
            raise RelayError("desktop owner-session closure disappeared after admission read")
        if local_closure.model_dump(mode="json") != local_closure_evidence:
            raise RelayError("desktop owner-session closure changed after admission read")
    else:
        local_closure = queue.set_owner_session_closed(
            local_admission_session_id,
            session_generation_id=session_generation_id,
            residual_resource_ids=[],
        )
    if (
        local_closure.owner_session_id != local_admission_session_id
        or local_closure.session_generation_id != session_generation_id
        or local_closure.residual_resource_ids
    ):
        raise RelayError("desktop owner-session admission mirror did not close exactly")
