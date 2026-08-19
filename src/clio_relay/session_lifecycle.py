"""Owned remote relay session lifecycle helpers."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast
from uuid import uuid4

import clio_relay.session_lifecycle_report as session_lifecycle_report
import clio_relay.session_process_scope as session_process_scope
import clio_relay.session_remote_command as session_remote_command
import clio_relay.session_remote_scripts as session_remote_scripts
import clio_relay.session_start_query as session_start_query
import clio_relay.session_startup_receipt as session_startup_receipt
from clio_relay.cluster_config import (
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterDefinition,
    cluster_route_revision,  # noqa: F401 -- session_recovery_inspection.py qualifies this back-reference
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

# inspect_owned_session_recovery_status re-export (#231 split/session-lifecycle
# slice K): the function itself moved to session_recovery_inspection.py
# (fully self-contained -- no other resident function here is its consumer
# or dependency), but cli.py reaches it through module-qualified attribute
# access (`session_lifecycle.inspect_owned_session_recovery_status(...)`),
# not a bare import, and every owner module extracted in slices G through J
# (session_start_wait, session_start_execution, session_cleanup_execution,
# session_cleanup_reporting) ALSO reaches it the same qualified way as their
# one shared read path. Python attribute lookup on a module resolves a
# re-exported name identically to one defined there, so this re-export
# keeps every one of those qualified call sites resolving unchanged.
from clio_relay.session_recovery_inspection import (
    inspect_owned_session_recovery_status,  # noqa: F401
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
    open_owned_session_transaction,  # noqa: F401 -- cli.py bare-imports this; see rationale above
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
