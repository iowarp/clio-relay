"""Owned remote relay session cleanup finalize/report-read execution.

split/session-lifecycle slice J (#231): the two remaining owned-session
cleanup entry points (finalize the coordinator report, read back a remote
cleanup report) moved out of session_lifecycle.py -- a small, self-contained
pair with no dependents inside the teardown-execution cluster
(session_cleanup_execution.py, extracted alongside it). session_lifecycle is
imported back INSIDE each top-level function (not at module scope):
session_lifecycle imports this module for its cli.py-compatibility
re-export block, so a module-scope back-import here creates a
load-order-dependent circular import -- deferred to call time, it is
import-order-independent, matching the standard pattern for breaking a
two-module cycle.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import cast

import clio_relay.session_lifecycle_report as session_lifecycle_report
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle_report import (
    OwnedSessionCleanupFinalizeRequest,
    OwnedSessionCleanupReportReadRequest,
    SessionLifecycleReport,
)
from clio_relay.session_validation import _validate_session
from clio_relay.session_wire_models import (
    MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
    OwnedSessionCleanupReportReference,
    OwnedSessionRecoveryStatus,
)


def execute_owned_session_cleanup_finalize(
    request: OwnedSessionCleanupFinalizeRequest,
    *,
    home: Path | None = None,
    core_dir: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> OwnedSessionRecoveryStatus:
    """Immutably bind a coordinator-verified report to a completed receipt."""
    # Deferred import: breaks the module-load-time cycle with session_lifecycle
    # (which imports this module back for its cli.py-compatibility re-export
    # block) -- see the module docstring.
    import clio_relay.session_lifecycle as session_lifecycle
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
    with session_lifecycle.open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session cleanup receipt is unavailable")
        status = session_lifecycle.inspect_owned_session_recovery_status(
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
        reread = session_lifecycle.inspect_owned_session_recovery_status(
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
    # Deferred import: breaks the module-load-time cycle with session_lifecycle
    # (which imports this module back for its cli.py-compatibility re-export
    # block) -- see the module docstring.
    import clio_relay.session_lifecycle as session_lifecycle
    from clio_relay.config import RelaySettings

    _validate_session(session_id=request.session_id, remote_api_port=1)
    settings_core_dir = RelaySettings.from_env().core_dir if core_dir is None else core_dir
    get_effective_uid = cast(Callable[[], int] | None, getattr(os, "geteuid", None))
    if get_effective_uid is None:
        raise RelayError("owned cleanup report read cannot verify the effective user")
    uid = get_effective_uid()
    with session_lifecycle.open_owned_session_transaction(
        session_id=request.session_id,
        create=False,
        timeout_seconds=10.0,
        home=home,
    ) as transaction:
        document = transaction.read_json("metadata.json")
        if document is None:  # pragma: no cover - required read
            raise RelayError("owned session cleanup receipt is unavailable")
        status = session_lifecycle.inspect_owned_session_recovery_status(
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
