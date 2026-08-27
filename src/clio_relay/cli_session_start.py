"""The ``session start`` command (iowarp/clio-relay#231 continuation):
extracted verbatim off ``cli.py`` alongside the private helpers only it
calls. Registers directly onto ``cli_session.session_app`` the same way
``cli_session_owned.py`` registers the owned-session command group, so
``cli.py`` only needs a side-effect import."""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, cast

import typer
from pydantic import ValidationError

import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence
import clio_relay.cli_cleanup_report as cli_cleanup_report
import clio_relay.cli_owned_session_recovery as cli_owned_session_recovery
import clio_relay.cli_remote_worker_probe as cli_remote_worker_probe
import clio_relay.cli_session as cli_session
import clio_relay.installation as installation
import clio_relay.remote_cli as remote_cli
import clio_relay.session_lifecycle as session_lifecycle
from clio_relay.cluster_config import (
    ClusterDefinition,
)
from clio_relay.config import RelaySettings
from clio_relay.dev_mode import VerificationFindings, dev_mode_enabled
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.installation import (
    InstallReceipt,
    verify_remote_worker_info,
)
from clio_relay.owned_session_record import save_owned_session_record
from clio_relay.owner_session_admission import (
    desktop_owner_session_admission_id as _desktop_owner_session_admission_id,
)
from clio_relay.session_lifecycle import (
    OwnedSessionRecoveryStatus,
    SessionApiReleaseIdentity,
    plan_remote_session_start,
)
from clio_relay.validation_report import (
    SoftwareIdentity,
)


@cli_session.session_app.command("start")
def session_start(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    session_id: Annotated[str, typer.Option(help="Owned remote relay session id.")],
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    replace: Annotated[
        bool,
        typer.Option("--replace/--no-replace", help="Replace an existing session API process."),
    ] = False,
    require_token: Annotated[
        bool,
        typer.Option(help="Require CLIO_RELAY_API_TOKEN on the remote API."),
    ] = True,
    start_operation_id: Annotated[
        str | None,
        typer.Option(help="Exact id from session plan-start; omitted mints a fresh operation."),
    ] = None,
    expected_cluster_route_revision: Annotated[
        str | None,
        typer.Option(help="Fail before mutation if the planned cluster route changed."),
    ] = None,
    expected_api_release_identity_sha256: Annotated[
        str | None,
        typer.Option(help="Exact release digest from session plan-start."),
    ] = None,
) -> None:
    """Start an owned API; exit 0 means ready and exit 2 means handle-only."""
    import clio_relay.cli as cli

    settings = RelaySettings.from_env()
    if require_token and settings.api_token is None:
        raise typer.BadParameter(
            "CLIO_RELAY_API_TOKEN is required unless --no-require-token is explicit"
        )
    definition = cli._require_cluster(cluster)

    def action() -> None:
        preliminary_plan = plan_remote_session_start(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            replace=replace,
            require_token=require_token,
            input_policy=cli_cleanup_evidence._owned_session_input_policy(settings),
            start_operation_id=start_operation_id,
            expected_cluster_route_revision=expected_cluster_route_revision,
            expected_api_release_identity_sha256=expected_api_release_identity_sha256,
        )
        with cli._session_transition_lock(cluster=cluster, session_id=session_id):
            dev_mode_findings = VerificationFindings()
            api_release_identity = _verify_session_start_worker_release_identity(
                definition,
                dev_mode=dev_mode_enabled(cluster_dev_mode=definition.dev_mode),
                findings=dev_mode_findings,
            )
            _echo_dev_mode_findings(dev_mode_findings)
            if (
                expected_api_release_identity_sha256 is not None
                and api_release_identity.sha256() != expected_api_release_identity_sha256
            ):
                raise RelayError("session API release identity changed after planning")
            plan = plan_remote_session_start(
                cluster=cluster,
                definition=definition,
                session_id=session_id,
                remote_api_port=remote_api_port,
                replace=replace,
                require_token=require_token,
                input_policy=preliminary_plan.input_policy,
                start_operation_id=preliminary_plan.start_operation_id,
                expected_cluster_route_revision=preliminary_plan.cluster_route_revision,
                expected_api_release_identity_sha256=api_release_identity.sha256(),
            )
            _finalize_completed_cleanup_receipt_before_start(
                definition=definition,
                cluster=cluster,
                session_id=session_id,
            )
            result = session_lifecycle.start_remote_session_durable(
                definition=definition,
                plan=plan,
                api_token=settings.api_token if require_token else None,
                expected_api_release_identity=api_release_identity,
                starter=session_lifecycle.start_remote_session,
            )
            typer.echo(result.model_dump_json(indent=2))
            if result.state in {"failed", "not_current"}:
                raise typer.Exit(code=1)
            if not result.usable:
                # A durable operation handle is useful for status/retry/cleanup,
                # but must never look like a successfully attached API session
                # to integrations that key off the process exit status.
                raise typer.Exit(code=2)
            # iowarp/clio-relay#276 B1: only a genuinely usable, attached API
            # session is worth remembering as "the last session for this
            # cluster" -- overwrites any prior record for the same cluster,
            # so `session attach` always finds the newest bring-up.
            #
            # `session_generation_id` is typed `DurableRecordId | None` on
            # OwnedSessionStartResult because a NON-ready result may omit it,
            # but `usable is (state == "ready")` and that same model's own
            # validator requires `session_generation_id is not None` whenever
            # `state == "ready"` (session_wire_models.py's
            # `_validate_start_result`) -- `result.usable` was just checked
            # above, so this is a real, already-proven invariant. Narrowed
            # explicitly here (D1 review) rather than silencing the type
            # checker on a save call whose failure must stay a real, typed
            # refusal, not a suppressed one.
            assert result.session_generation_id is not None, (
                "a usable owned-session start result always carries its generation id"
            )
            save_owned_session_record(
                cluster=cluster,
                session_id=result.session_id,
                session_generation_id=result.session_generation_id,
                remote_api_port=result.remote_api_port,
            )

    cli._run_or_exit(action)


def _finalize_completed_cleanup_receipt_before_start(
    *,
    definition: ClusterDefinition,
    cluster: str,
    session_id: str,
) -> None:
    """Finish the exact teardown commit if reconnect observes its completed receipt.

    ``coordinator_report_bound`` is gated separately from ``cleanup_receipt``
    the same way ``cli_session_teardown_recovery._resolve_teardown_recovery``
    already, correctly, gates its own analogous call: a receipt written by
    the lease-expiry sweep (``endpoint_owner_session_sweep.py``, #277) is
    genuine but deliberately skips the two-sided coordinator-report finalize
    ceremony -- the desktop that ceremony hands a receipt to is exactly the
    thing that is gone when the sweep runs. Without this gate,
    ``read_remote_session_cleanup_report`` always refuses an unbound
    reference, so reconnecting after a sweep-driven (rather than a
    desktop-driven) expiry would brick every ``session start --replace``
    before it ever reached its own start logic
    (iowarp/clio-relay#(twice-expired session brick)). There is nothing
    local to finalize in that case -- the remote generation is already
    closed -- so degrading to a no-op here is correct, not a swallowed
    error: the caller's own start logic proceeds unaffected, and the local
    desktop admission mirror self-heals lazily the next time it is opened
    for a new generation (``mirror_owner_session_generation_open``).
    """
    import clio_relay.cli as cli

    raw_status = session_lifecycle.status_remote_session(
        definition=definition,
        session_id=session_id,
        pre_start_cleanup_probe=True,
    )
    try:
        status = OwnedSessionRecoveryStatus.model_validate(raw_status)
    except ValidationError:
        return
    if not status.cleanup_receipt or not status.coordinator_report_bound:
        return
    report = session_lifecycle.read_remote_session_cleanup_report(
        definition=definition,
        cluster=cluster,
        session_id=session_id,
        status=status,
    )
    report = cli_cleanup_report._verified_finalized_cleanup_report(
        status,
        report=report,
        cluster=cluster,
        session_id=session_id,
    )
    generation_id = cast(str, report.session_generation_id)
    operation_id = report.cleanup_operation_id
    if operation_id is None:
        raise RelayError("completed cleanup receipt omitted its operation identity")
    admission = status.admission_status
    if not isinstance(admission, dict):
        raise RelayError("completed cleanup receipt omitted authoritative admission evidence")
    queue = cli._managed_queue_from_env()
    local_admission_session_id = _desktop_owner_session_admission_id(
        cluster=cluster,
        session_id=session_id,
    )
    local_status = queue.owner_session_generation_status(
        local_admission_session_id,
        session_generation_id=generation_id,
    )
    remote_closed = admission.get("closed") is True
    local_closing = bool(
        local_status.get("closing") is True
        and local_status.get("closing_generation_id") == generation_id
    )
    if not remote_closed and not local_closing:
        raise RelayError(
            "completed remote cleanup receipt has no exact desktop closing mirror; "
            "automatic reconnect recovery was refused before mutation"
        )
    cli_owned_session_recovery._mark_owner_session_closed(
        queue=queue,
        definition=definition,
        cluster=cluster,
        remote_execution=remote_cli.should_execute_on_cluster(definition),
        session_id=session_id,
        local_admission_session_id=local_admission_session_id,
        session_generation_id=generation_id,
        legacy_unversioned_job_ids=[],
        finalized_recovery=status,
        finalized_report=report,
    )
    refreshed = OwnedSessionRecoveryStatus.model_validate(
        session_lifecycle.status_remote_session(definition=definition, session_id=session_id)
    )
    if not (
        refreshed.recovery_verified
        and refreshed.cleanup_receipt
        and refreshed.cleanup_paths_pending is False
        and refreshed.coordinator_report_bound
        and isinstance(refreshed.admission_status, dict)
        and refreshed.admission_status.get("closed") is True
    ):
        raise RelayError(
            "owned session cleanup closure was not authoritative after reconnect recovery"
        )
    if refreshed.coordinator_report_ref != status.coordinator_report_ref:
        raise RelayError("coordinator cleanup report reference changed during reconnect closure")
    cli_cleanup_report._verified_finalized_cleanup_report(
        refreshed,
        report=report,
        cluster=cluster,
        session_id=session_id,
        expected_generation_id=generation_id,
        expected_cleanup_operation_id=operation_id,
        expected_cleanup_policy=report.cleanup_policy,
    )


def _verify_session_start_worker_release_identity(
    definition: ClusterDefinition,
    *,
    dev_mode: bool = False,
    findings: VerificationFindings | None = None,
) -> SessionApiReleaseIdentity:
    """Require one exact live worker/install identity before session mutation.

    ``dev_mode`` (clio-relay#211) downgrades the REMOTE worker's identity
    comparison to a recorded warning instead of raising -- this is the
    "identity_matches_current" wall observed live on a cluster pinned to a
    dev sha. The local desktop identity resolution is unaffected by this
    parameter (it already honors ``CLIO_RELAY_DEV_MODE`` on its own via
    ``installation_info``'s default): session start still requires *a*
    session-API process-attestation identity to bind to, so when the
    downgraded remote receipt has no artifact sha at all, one is derived
    deterministically from what IS known and clearly marked advisory rather
    than silently treated as a real release digest.
    """
    findings = findings if findings is not None else VerificationFindings()
    local_identity = _session_api_release_identity_from_installation(
        installation.installation_info(),
        label="local clio-relay",
    )
    remote_receipt = verify_remote_worker_info(
        cli_remote_worker_probe._remote_worker_info(definition),
        expected_cluster=definition.name,
        expected_version=local_identity.distribution_version,
        expected_software=local_identity.software,
        expected_artifact_sha256=local_identity.artifact_sha256,
        expected_source=None,
        dev_mode=dev_mode,
        findings=findings,
    )
    artifact_sha256 = remote_receipt.artifact_sha256
    if artifact_sha256 is None:
        if not dev_mode:
            raise ConfigurationError("remote worker receipt omitted its artifact SHA-256")
        message = "remote worker receipt omitted its artifact SHA-256"
        findings.record(message)
        artifact_sha256 = hashlib.sha256(
            f"dev-mode-advisory:{remote_receipt.distribution_version}:"
            f"{remote_receipt.install_spec}".encode()
        ).hexdigest()
    return SessionApiReleaseIdentity(
        distribution_version=remote_receipt.distribution_version,
        artifact_sha256=artifact_sha256,
        software=remote_receipt.software,
    )


def _echo_dev_mode_findings(findings: VerificationFindings) -> None:
    """Print the DEV MODE banner + downgraded findings to stderr, when present.

    clio-relay#211 requires every status/info surface a downgraded check
    can reach to carry an unmissable marker; commands that return a typed,
    ``extra="forbid"`` payload (a session start plan, an install receipt)
    cannot have arbitrary keys merged in, so this prints the same banner
    payload as a companion line instead of silently dropping it.
    """
    dev_mode_payload = findings.payload()
    if dev_mode_payload is None:
        return
    typer.echo(json.dumps(dev_mode_payload, indent=2), err=True)


def _session_api_release_identity_from_installation(
    info: dict[str, object],
    *,
    label: str,
) -> SessionApiReleaseIdentity:
    """Validate installation evidence and return its session-API identity."""
    if info.get("receipt_matches_install") is not True and not dev_mode_enabled():
        raise ConfigurationError(f"{label} installation receipt does not match the running package")
    try:
        receipt = InstallReceipt.model_validate(info.get("receipt"))
        software = SoftwareIdentity.model_validate(info.get("software"))
    except ValidationError as exc:
        raise ConfigurationError(f"{label} installation identity is invalid: {exc}") from exc
    version = info.get("distribution_version")
    artifact_sha256 = receipt.artifact_sha256
    if (
        not isinstance(version, str)
        or receipt.distribution_version != version
        or receipt.software != software
        or artifact_sha256 is None
    ) and not dev_mode_enabled():
        raise ConfigurationError(f"{label} installation receipt does not match the running package")
    return SessionApiReleaseIdentity(
        distribution_version=str(version or receipt.distribution_version),
        artifact_sha256=artifact_sha256 or "0" * 64,
        software=software,
    )
