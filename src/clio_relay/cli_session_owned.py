"""The internal ``session ...-owned``/intake handoff commands (iowarp/clio-relay#231).

Sibling to ``cli_session.py`` (which owns the canonical ``session_app``
Typer instance) in the two-file-one-Typer split of ``session_app`` -- see
that module's docstring for the full design rationale. This module owns the
twelve ``hidden=True`` commands that exist only as the coordinator/worker's
own stdin-JSON internal handoff protocol, never invoked directly by a human:
``quiesce-intake``, ``admission-status``, ``recovery-status``,
``start-status-owned``, ``start-owned``, ``teardown-owned``,
``challenge-owned``, ``finalize-cleanup-owned``, ``read-cleanup-report-owned``,
``prepare-start``, ``resume-intake``, ``mark-closed``. Registered onto
``cli_session.session_app`` via ``@cli_session.session_app.command(...)``,
matching ``cli_queue_maintenance.py``'s registration onto ``cli_queue.
queue_app``.

**``_inspect_owned_session_recovery_before_start`` moves here with
``recovery-status``.** It was interleaved in ``cli.py`` between two other
commands, physically adjacent to its sibling
``_inspect_owned_session_recovery_after_transition`` -- but that sibling has
a second call site outside ``session_app`` entirely (``cli.py``'s
``_owned_session_recovery_status``, part of the shared owner-session helper
zone far below), so it stays in ``cli.py`` as a shared collaborator.
``_inspect_owned_session_recovery_before_start``'s only caller was
``recovery-status``, so it is genuinely exclusive and moves. One adaptation
was required, not a logic change: its ``timeout_seconds`` parameter
defaulted to ``cli.py``'s module-level
``OWNED_SESSION_RECOVERY_TRANSITION_TIMEOUT_SECONDS`` constant, evaluated at
``def``-time -- a cross-module default value cannot resolve through the
function-local ``import clio_relay.cli as cli`` discipline this file uses
everywhere else, since that import only executes when the function body
runs, not when its signature is defined. The default is now ``None``,
resolved to the identical constant on the first line of the function body
instead; every caller (here, and the moved ``recovery-status`` command,
neither of which ever passed an explicit ``timeout_seconds``) observes the
exact same effective default it did before.

**Domain logic stays where it lives.** ``session_lifecycle``'s owned-session
execute/wait functions and wire-request models are imported directly from
their true owner (none of them are audited patch-seam collaborators --
``tests/test_cli_patch_seam.py`` -- so there is no coupling risk in the bare
form, matching ``cli.py``'s own pre-extraction style for these exact names).
``core_queue.ClioCoreQueue`` is imported module-attribute style since it *is*
audited, even though its caller stays ``cli`` (several other ``cli.py`` call
sites survive this move) -- this file is not itself added to
``tests/test_cli_patch_seam.py``'s ``_GUARDED_CALLERS``, but matching the
module-attribute form here costs nothing and avoids ever having to
reconsider it if that changes.

**The import-cycle discipline.** ``cli`` is never bound as a module-level
name here, matching every prior extraction: it is imported function-locally,
inside every command body and inside
``_inspect_owned_session_recovery_before_start`` itself, each time a
cross-cutting ``cli.py`` collaborator (``_run_or_exit``,
``_inspect_owned_session_recovery_after_transition``,
``OWNED_SESSION_RECOVERY_TRANSITION_TIMEOUT_SECONDS``) is needed.
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path
from typing import Annotated

import typer

import clio_relay.cli_session as cli_session
import clio_relay.core_queue as core_queue
from clio_relay.cluster_config import MAX_CLUSTER_REGISTRY_BYTES
from clio_relay.config import RelaySettings
from clio_relay.errors import RelayError
from clio_relay.session_lifecycle import (
    MAX_OWNED_SESSION_CLEANUP_FINALIZE_BYTES,
    OwnedSessionCleanupFinalizeRequest,
    OwnedSessionCleanupReportReadRequest,
    OwnedSessionIdentityChallengeRequest,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartRejection,
    OwnedSessionStartRequest,
    OwnedSessionTeardownRequest,
    execute_owned_session_cleanup_finalize,
    execute_owned_session_cleanup_report_read,
    execute_owned_session_identity_challenge,
    execute_owned_session_start,
    execute_owned_session_teardown,
    wait_owned_session_start_status,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.

logger = logging.getLogger(__name__)


@cli_session.session_app.command("quiesce-intake", hidden=True)
def session_quiesce_intake(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact owned relay session generation id."),
    ],
    cleanup_operation_id: Annotated[
        str | None,
        typer.Option(help="Exact cleanup operation id selected by the desktop coordinator."),
    ] = None,
    cleanup_stop_worker: Annotated[
        bool,
        typer.Option(help="Persist worker-stop scope in the immutable cleanup intent."),
    ] = False,
    cleanup_cancel_jobs: Annotated[
        bool,
        typer.Option(help="Persist relay cancellation scope in the immutable cleanup intent."),
    ] = False,
    cleanup_cancel_scheduler_jobs: Annotated[
        bool,
        typer.Option(help="Persist scheduler cancellation scope in the cleanup intent."),
    ] = False,
) -> None:
    """Durably stop one owned API session from accepting new work."""
    import clio_relay.cli as cli

    def action() -> None:
        queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
        cleanup_intent = queue.set_owner_session_closing(
            session_id,
            session_generation_id=session_generation_id,
            operation_id=cleanup_operation_id,
            stop_worker=cleanup_stop_worker,
            cancel_jobs=cleanup_cancel_jobs,
            cancel_scheduler_jobs=cleanup_cancel_scheduler_jobs,
        )
        typer.echo(
            json.dumps(
                {
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                    "intake": "quiesced",
                    "cleanup_intent": cleanup_intent,
                }
            )
        )

    cli._run_or_exit(action)


@cli_session.session_app.command("admission-status", hidden=True)
def session_admission_status(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact owned relay session generation id."),
    ],
) -> None:
    """Return machine-readable intake state for one exact session generation."""
    import clio_relay.cli as cli

    def action() -> None:
        queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
        typer.echo(
            json.dumps(
                queue.owner_session_generation_status(
                    session_id,
                    session_generation_id=session_generation_id,
                )
            )
        )

    cli._run_or_exit(action)


@cli_session.session_app.command("recovery-status", hidden=True)
def session_recovery_status(
    cluster: Annotated[str, typer.Option(help="Exact cluster recorded by the owned session.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    pre_start_cleanup_probe: Annotated[
        bool,
        typer.Option(
            help=(
                "Return a structured unverified observation when no transition exists yet; "
                "reserved for the read-only pre-start cleanup probe."
            )
        ),
    ] = False,
) -> None:
    """Return fail-closed recovery evidence for an ambiguous or dead session start."""
    import clio_relay.cli as cli
    import clio_relay.cli_owned_session_recovery as cli_owned_session_recovery

    def action() -> None:
        settings_core_dir = RelaySettings.from_env().core_dir
        status = (
            _inspect_owned_session_recovery_before_start(
                cluster=cluster,
                session_id=session_id,
                core_dir=settings_core_dir,
            )
            if pre_start_cleanup_probe
            else cli_owned_session_recovery._inspect_owned_session_recovery_after_transition(
                cluster=cluster,
                session_id=session_id,
                core_dir=settings_core_dir,
            )
        )
        status = _with_owner_session_lease_projection(status, core_dir=settings_core_dir)
        typer.echo(status.model_dump_json(indent=2))

    cli._run_or_exit(action)


def _with_owner_session_lease_projection(
    status: OwnedSessionRecoveryStatus,
    *,
    core_dir: Path,
) -> OwnedSessionRecoveryStatus:
    """Attach the owned-session client-liveness lease as one informational field.

    iowarp/clio-relay#277: a `model_copy` post-processing step onto the
    unchanged inspection result -- see
    ``session_recovery_lease_projection.py``'s module docstring for why this
    lives outside ``session_recovery_inspection.py`` and how it reaches
    ``RemoteConnection``'s bootstrap verification at zero extra dial cost.
    Never raises, never gates ``recovery_verified``: a missing/unreadable
    lease record leaves the status exactly as the inspection returned it.
    """
    import clio_relay.session_recovery_lease_projection as session_recovery_lease_projection

    if status.session_generation_id is None:
        return status
    lease_status = session_recovery_lease_projection.owner_session_lease_status_projection(
        core_dir=core_dir,
        session_id=status.session_id,
        session_generation_id=status.session_generation_id,
    )
    if lease_status is None:
        return status
    return status.model_copy(update={"owner_session_lease_status": lease_status})


@cli_session.session_app.command("start-status-owned", hidden=True)
def session_start_status_owned(
    cluster: Annotated[str, typer.Option(help="Exact cluster selected by the start plan.")],
    session_id: Annotated[str, typer.Option(help="Exact owned relay session id.")],
    start_operation_id: Annotated[str, typer.Option(help="Exact start operation id.")],
    cluster_route_revision: Annotated[
        str,
        typer.Option(help="Exact cluster route revision selected by the start plan."),
    ],
    wait_seconds: Annotated[
        float,
        typer.Option(
            help=(
                "Block here against durable start state until the observation is terminal "
                "or this bound elapses. Zero returns one nonblocking observation."
            )
        ),
    ] = 0.0,
) -> None:
    """Return one cluster-local start observation, optionally waiting for terminal."""
    import clio_relay.cli as cli

    def action() -> None:
        status = wait_owned_session_start_status(
            cluster=cluster,
            session_id=session_id,
            start_operation_id=start_operation_id,
            cluster_route_revision=cluster_route_revision,
            core_dir=RelaySettings.from_env().core_dir,
            wait_seconds=wait_seconds,
        )
        typer.echo(status.model_dump_json(indent=2))

    cli._run_or_exit(action)


@cli_session.session_app.command("start-owned", hidden=True)
def session_start_owned() -> None:
    """Execute a bounded stdin-carried owned-session start on the cluster."""
    import clio_relay.cli as cli

    def action() -> None:
        maximum_bytes = MAX_CLUSTER_REGISTRY_BYTES + 128 * 1024
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session start request exceeds its byte limit")
        try:
            request = OwnedSessionStartRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session start request is invalid: {exc}") from exc
        try:
            typer.echo(execute_owned_session_start(request).model_dump_json())
        except RelayError as exc:
            typer.echo(
                OwnedSessionStartRejection(
                    cluster=request.cluster,
                    session_id=request.session_id,
                    start_operation_id=request.start_operation_id,
                    cluster_route_revision=request.cluster_route_revision,
                    error=str(exc)[:8192] or "owned-session start was rejected",
                ).model_dump_json()
            )
            raise typer.Exit(code=1) from exc

    cli._run_or_exit(action)


@cli_session.session_app.command("teardown-owned", hidden=True)
def session_teardown_owned() -> None:
    """Execute a bounded stdin-carried owned-session teardown on the cluster."""
    import clio_relay.cli as cli

    def action() -> None:
        maximum_bytes = 128 * 1024
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session teardown request exceeds its byte limit")
        try:
            request = OwnedSessionTeardownRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session teardown request is invalid: {exc}") from exc
        report = execute_owned_session_teardown(request)
        _close_owner_session_lease_on_client_teardown(request)
        typer.echo(report.model_dump_json())

    cli._run_or_exit(action)


def _close_owner_session_lease_on_client_teardown(request: OwnedSessionTeardownRequest) -> None:
    """Close the client-liveness lease with the CLIENT_CLOSE typed reason.

    iowarp/clio-relay#277: distinct from the worker sweep's LEASE_EXPIRED
    closure (``endpoint_owner_session_sweep.py``) -- this is what makes a
    clean, explicit ``session teardown`` distinguishable from a TTL reap in
    the durable record. Best-effort and never fatal to the teardown itself:
    a race against a concurrent sweep that already closed the SAME lease
    with a DIFFERENT reason is logged, not raised -- the process is already
    dead and the generation already closed either way, so the only thing at
    stake is which typed label got there first.
    """
    queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
    try:
        queue.close_owner_session_lease(
            request.session_id,
            session_generation_id=request.expected_session_generation_id,
            reason="client_close",
        )
    except RelayError:
        logger.info(
            "owner_session.lease_close_raced",
            extra={
                "owner_session_id": request.session_id,
                "session_generation_id": request.expected_session_generation_id,
            },
            exc_info=True,
        )


@cli_session.session_app.command("challenge-owned", hidden=True)
def session_challenge_owned() -> None:
    """Answer a bounded stdin-carried owned-session identity challenge."""
    import clio_relay.cli as cli

    def action() -> None:
        maximum_bytes = 64 * 1024
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session challenge request exceeds its byte limit")
        try:
            request = OwnedSessionIdentityChallengeRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session challenge request is invalid: {exc}") from exc
        typer.echo(json.dumps(execute_owned_session_identity_challenge(request)))

    cli._run_or_exit(action)


@cli_session.session_app.command("finalize-cleanup-owned", hidden=True)
def session_finalize_cleanup_owned() -> None:
    """Bind a bounded coordinator-verified report to one cleanup receipt."""
    import clio_relay.cli as cli

    def action() -> None:
        maximum_bytes = MAX_OWNED_SESSION_CLEANUP_FINALIZE_BYTES
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session cleanup finalization exceeds its byte limit")
        try:
            request = OwnedSessionCleanupFinalizeRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session cleanup finalization is invalid: {exc}") from exc
        typer.echo(execute_owned_session_cleanup_finalize(request).model_dump_json())

    cli._run_or_exit(action)


@cli_session.session_app.command("read-cleanup-report-owned", hidden=True)
def session_read_cleanup_report_owned() -> None:
    """Read one finalized cleanup report through its exact sidecar reference."""
    import clio_relay.cli as cli

    def action() -> None:
        maximum_bytes = 256 * 1024
        payload = sys.stdin.buffer.read(maximum_bytes + 1)
        if len(payload) > maximum_bytes:
            raise RelayError("owned session cleanup report read exceeds its byte limit")
        try:
            request = OwnedSessionCleanupReportReadRequest.model_validate_json(payload)
        except ValueError as exc:
            raise RelayError(f"owned session cleanup report read is invalid: {exc}") from exc
        typer.echo(execute_owned_session_cleanup_report_read(request).model_dump_json())

    cli._run_or_exit(action)


def _inspect_owned_session_recovery_before_start(
    *,
    cluster: str,
    session_id: str,
    core_dir: Path,
    home: Path | None = None,
    timeout_seconds: float | None = None,
) -> OwnedSessionRecoveryStatus:
    """Observe cleanup state without requiring a fresh session transition to exist."""
    import clio_relay.cli_owned_session_recovery as cli_owned_session_recovery

    resolved_timeout_seconds = (
        cli_owned_session_recovery.OWNED_SESSION_RECOVERY_TRANSITION_TIMEOUT_SECONDS
        if timeout_seconds is None
        else timeout_seconds
    )
    if re.fullmatch(r"[A-Za-z0-9_-]+", session_id) is None:
        raise RelayError("session_id must contain only letters, numbers, hyphen, or underscore")
    if not cluster:
        raise RelayError("cluster must not be empty")
    if resolved_timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    selected_home = home or Path.home()
    transition_path = (
        selected_home
        / ".local"
        / "share"
        / "clio-relay"
        / "sessions"
        / session_id
        / "transition.lock"
    )
    try:
        transition_path.lstat()
    except FileNotFoundError:
        return OwnedSessionRecoveryStatus(
            cluster=cluster,
            session_id=session_id,
            cleanup_receipt=False,
            ownership_verified=False,
            recovery_verified=False,
            errors=[
                "owned session transition is not currently observable; "
                "start-owned remains the mutation authority"
            ],
        )
    return cli_owned_session_recovery._inspect_owned_session_recovery_after_transition(
        cluster=cluster,
        session_id=session_id,
        core_dir=core_dir,
        home=selected_home,
        timeout_seconds=resolved_timeout_seconds,
    )


@cli_session.session_app.command("prepare-start", hidden=True)
def session_prepare_start(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    candidate_generation_id: Annotated[
        str,
        typer.Option(help="Fresh candidate generation for an initial start or verified reopen."),
    ],
    recorded_generation_id: Annotated[
        str | None,
        typer.Option(help="Generation from verified durable API-session metadata, if present."),
    ] = None,
) -> None:
    """Atomically select the authoritative generation for an owned API start."""
    import clio_relay.cli as cli

    def action() -> None:
        queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
        generation_id = queue.prepare_owner_session_start(
            session_id,
            recorded_generation_id=recorded_generation_id,
            candidate_generation_id=candidate_generation_id,
        )
        typer.echo(
            json.dumps(
                {
                    "session_id": session_id,
                    "session_generation_id": generation_id,
                }
            )
        )

    cli._run_or_exit(action)


@cli_session.session_app.command("resume-intake", hidden=True)
def session_resume_intake(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact new or reopened relay session generation id."),
    ],
) -> None:
    """Clear durable intake quiescence for a new owned API generation."""
    import clio_relay.cli as cli

    def action() -> None:
        queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
        queue.clear_owner_session_closing(
            session_id,
            session_generation_id=session_generation_id,
        )
        typer.echo(
            json.dumps(
                {
                    "session_id": session_id,
                    "session_generation_id": session_generation_id,
                    "intake": "open",
                }
            )
        )

    cli._run_or_exit(action)


@cli_session.session_app.command("mark-closed", hidden=True)
def session_mark_closed(
    session_id: Annotated[str, typer.Option(help="Owned relay session id.")],
    session_generation_id: Annotated[
        str,
        typer.Option(help="Exact verified relay session generation id."),
    ],
    legacy_unversioned_job_id: Annotated[
        list[str] | None,
        typer.Option(help="Exact verified legacy job id covered by this first upgraded teardown."),
    ] = None,
) -> None:
    """Durably close one verified, already-quiesced owner session generation."""
    import clio_relay.cli as cli

    def action() -> None:
        queue = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir)
        closure = queue.set_owner_session_closed(
            session_id,
            session_generation_id=session_generation_id,
            residual_resource_ids=[],
            legacy_unversioned_job_ids=legacy_unversioned_job_id or [],
        )
        payload = closure.model_dump(mode="json")
        if legacy_unversioned_job_id:
            legacy_closure = queue.get_owner_session_closed(
                session_id,
                session_generation_id=None,
            )
            if legacy_closure is None:
                raise RelayError("legacy owner-session closure was not persisted")
            payload["legacy_closure"] = legacy_closure.model_dump(mode="json")
        typer.echo(json.dumps(payload))

    cli._run_or_exit(action)
