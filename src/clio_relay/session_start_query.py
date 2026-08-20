"""Owned-session start planning, single-observation query, and bounded watch (#231 rework).

Extracted from ``session_lifecycle.py``: the read-only exact-selector planner
(``plan_remote_session_start``), the remote single-observation start-status
call and its typed-result projection, the one-shot query wrapper, the bounded
poll/watch loop, and the SSH-authenticated identity challenge. None of these
call back into session_lifecycle.py -- the still-resident
remote_session_*/start_remote_session_durable entry points call into this
module instead.
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, cast
from uuid import uuid4

import clio_relay.session_remote_command as session_remote_command
import clio_relay.session_remote_scripts as session_remote_scripts
from clio_relay.errors import RelayError, RemoteExecutableMissingError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.session_validation import _validate_durable_session_identity, _validate_session
from clio_relay.session_wire_models import (
    MAX_SESSION_START_ERROR_CHARS,
    OwnedSessionInputPolicy,
    OwnedSessionRecoveryStatus,
    OwnedSessionStartPlan,
    OwnedSessionStartResult,
    OwnedSessionStartRetrySelector,
    OwnedSessionStartStatusSelector,
)

if TYPE_CHECKING:
    from clio_relay.cluster_config import ClusterDefinition
    from clio_relay.identifiers import DurableRecordId

_REMOTE_SESSION_START_RECOVERY_TIMEOUT_SECONDS = 15.0
# One start watch is a bounded server-side wait, never a client redial loop.
# The cap matches the ordinary remote-command budget, so the CLI's default
# 120-second watch costs exactly one connection; a longer watch costs one more
# per cap rather than one per polling interval.
MAX_REMOTE_SESSION_START_WAIT_SECONDS = 120.0
_REMOTE_SESSION_START_WAIT_TRANSPORT_MARGIN_SECONDS = 15.0


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
