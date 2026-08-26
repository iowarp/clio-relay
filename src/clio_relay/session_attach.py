"""``session attach``/``session reconnect`` business logic (iowarp/clio-relay#276, lane R-B).

Mirrors :mod:`clio_relay.service_runtime_attach`'s challenge-owned +
resume-in-place semantics -- reverify an already-live resource and resume it
in place, or recreate it -- at the CLIENT connection layer instead of the
gateway-session layer. There is no separate "challenge" step to write here:
:meth:`~clio_relay.remote_connection.RemoteConnection._establish` already
proves ownership on every bring-up, by running ``session recovery-status`` +
``session challenge-owned`` over the same held forward
(:func:`clio_relay.control_channel.owned_session_channel_bootstrap_script`)
and cross-checking the result against the exact session/generation this
connection is pinned to (``RemoteConnection._verify_bootstrap``). Attaching
is therefore exactly the ordinary connection bring-up path, reached with the
identity resolved from the durable record instead of a session the caller
already had open in this process.

Three cases, one function (:func:`attach_owned_session`):

* No connection is held for this cluster yet (a fresh process, or the first
  attach after the previous one exited) -- bring one up. One new SSH dial,
  the ordinary challenge-owned handshake.
* A connection is held and still alive (nothing was lost since the last
  operation) -- reuse it untouched. Zero new dials: "resume in place".
* A connection is held but its channel dropped
  (:attr:`~clio_relay.remote_connection.RemoteConnection.state` is
  ``"authorization_required"``) -- this attach call IS the one explicit,
  user-authorized reconnect the 2FA doctrine requires
  (docs/connection-model.md:141-157): exactly one new dial via
  :meth:`~clio_relay.remote_connection.RemoteConnectionRegistry.reconnect`,
  never a silent redial from inside an operation.

A remote session that is dead, torn down, or owned by someone else fails
:func:`~clio_relay.remote_connection.RemoteConnection._establish`'s bootstrap
verification with a typed ``RelayError``; this module re-raises that as the
one typed refusal callers discriminate on, :class:`SessionNotAttachableError`
(``reason == "session_not_attachable"``), carrying the underlying detail
rather than swallowing it (no-silent-fallback).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

import clio_relay.cli_owned_relay_jobs as cli_owned_relay_jobs
import clio_relay.core_queue as core_queue
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.owned_session_record import (
    default_owned_session_record_path,
    load_owned_session_record,
)
from clio_relay.remote_connection import (
    RemoteConnection,
    RemoteConnectionRegistry,
    connection_registry,
    resolve_remote_api_port,
)

SESSION_ATTACH_REPORT_SCHEMA = "clio-relay.session-attach-report.v1"


class NoDurableSessionRecordError(ConfigurationError):
    """No durable last-session record exists, and the environment did not
    fully name a session either.

    Typed so a caller (the CLI, an MCP surface) can render a precise recovery
    instruction instead of a generic configuration failure.
    """

    reason = "no_durable_session_record"


class SessionNotAttachableError(RelayError):
    """The resolved remote owned session could not be verified as live and owned.

    Raised for a dead process, a torn-down session, a generation that moved
    on, or any other bring-up/bootstrap-verification failure encountered
    while attaching -- ``detail`` always carries the exact underlying typed
    error's message, never a generic replacement (no-silent-fallback).
    """

    reason = "session_not_attachable"

    def __init__(self, *, cluster: str, session_id: str, detail: str) -> None:
        self.cluster = cluster
        self.session_id = session_id
        self.detail = detail
        super().__init__(
            f"owned session {session_id!r} on cluster {cluster!r} is not attachable: {detail}"
        )


@dataclass(frozen=True)
class AttachTarget:
    """The exact session identity ``session attach`` resolved to connect to."""

    session_id: str
    session_generation_id: str
    remote_api_port: int
    identity_source: Literal["environment_override", "durable_record"]


class SessionAttachReport(BaseModel):
    """The typed result ``session attach``/``session reconnect`` prints."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = SESSION_ATTACH_REPORT_SCHEMA
    cluster: str
    session_id: str
    session_generation_id: str
    remote_api_port: int
    transport_mode: str
    identity_source: Literal["environment_override", "durable_record"]
    channel_reestablished: bool
    connected: bool
    running_jobs: list[dict[str, object]]


def _environment_attach_target(
    *,
    cluster: str,
    settings: RelaySettings,
) -> AttachTarget | None:
    """Return the environment's own session identity, only when it fully names one.

    ``CLIO_RELAY_OWNER_SESSION_ID``/``CLIO_RELAY_SESSION_GENERATION_ID``/
    ``CLIO_RELAY_OWNER_SESSION_CLUSTER`` remain an explicit per-invocation
    override (iowarp/clio-relay#276 B1's design point 1): only when they do
    NOT together name a session for this exact cluster does resolution fall
    back to the durable record.
    """
    if settings.owner_session_id is None or settings.owner_session_generation_id is None:
        return None
    if settings.resolved_owner_session_cluster() != cluster:
        return None
    return AttachTarget(
        session_id=settings.owner_session_id,
        session_generation_id=settings.owner_session_generation_id,
        remote_api_port=resolve_remote_api_port(settings=settings),
        identity_source="environment_override",
    )


def resolve_attach_target(
    *,
    cluster: str,
    settings: RelaySettings,
    record_path: Path | None = None,
) -> AttachTarget:
    """Resolve the exact session identity ``session attach`` should connect to.

    Raises :class:`NoDurableSessionRecordError` when neither the environment
    nor the durable record names a session for this cluster -- a typed
    refusal naming the exact recovery action, never a bare ``KeyError``/
    ``None`` propagating into a confusing downstream failure.
    """
    override = _environment_attach_target(cluster=cluster, settings=settings)
    if override is not None:
        return override
    record = load_owned_session_record(cluster, path=record_path)
    if record is None:
        raise NoDurableSessionRecordError(
            f"no durable session record for cluster {cluster!r}; run `clio-relay session "
            f"start --cluster {cluster} ...` to create one, or set "
            "CLIO_RELAY_OWNER_SESSION_ID / CLIO_RELAY_SESSION_GENERATION_ID / "
            "CLIO_RELAY_OWNER_SESSION_CLUSTER explicitly"
        )
    return AttachTarget(
        session_id=record.session_id,
        session_generation_id=record.session_generation_id,
        remote_api_port=record.remote_api_port,
        identity_source="durable_record",
    )


def attach_owned_session(
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
    record_path: Path | None = None,
    registry: RemoteConnectionRegistry | None = None,
) -> tuple[RemoteConnection, AttachTarget, bool]:
    """Resume or re-establish this cluster's owned-session channel.

    Returns the held connection, the resolved attach target, and whether this
    call performed a new SSH dial (``channel_reestablished``): ``False`` only
    when an already-live channel for the exact resolved identity was reused
    untouched ("resume in place"); ``True`` for both a brand-new bring-up
    (fresh process / first attach after a crash) and an explicit reconnect of
    a dropped channel -- either way, exactly one new dial, never more.
    """
    target = resolve_attach_target(
        cluster=definition.name,
        settings=settings,
        record_path=record_path,
    )
    effective_settings = settings.model_copy(
        update={
            "owner_session_id": target.session_id,
            "owner_session_generation_id": target.session_generation_id,
            "owner_session_cluster": definition.name,
        }
    )
    active_registry = registry or connection_registry()
    existing = active_registry.get(definition.name)
    try:
        if existing is not None and existing.matches(
            settings=effective_settings,
            remote_api_port=target.remote_api_port,
        ):
            if existing.connected:
                return existing, target, False
            return active_registry.reconnect(definition.name), target, True
        connection = active_registry.connection(
            definition=definition,
            settings=effective_settings,
            remote_api_port=target.remote_api_port,
        )
        return connection, target, True
    except RelayError as exc:
        raise SessionNotAttachableError(
            cluster=definition.name,
            session_id=target.session_id,
            detail=str(exc),
        ) from exc


def build_attach_report(
    *,
    connection: RemoteConnection,
    target: AttachTarget,
    channel_reestablished: bool,
    definition: ClusterDefinition,
    queue: core_queue.ClioCoreQueue,
) -> SessionAttachReport:
    """Enumerate the attached session's running jobs and render the attach report.

    Job discovery mirrors ``session detach``'s own local-vs-remote branch
    (``cli_session.py``'s ``session_detach``): remote execution reads through
    ``queue owner-jobs`` over the cluster-targeted CLI exec, local/dev-mode
    execution reads the local queue's
    :meth:`~clio_relay.queue_jobs.ClioCoreQueue.list_owner_session_jobs_page`
    directly -- the same primitive the design calls out as "today wired only
    into teardown", now also wired into attach.
    """
    remote_execution = remote_cli.should_execute_on_cluster(definition)
    if remote_execution:
        owned_jobs = cli_owned_relay_jobs._list_remote_owned_active_cluster_jobs(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            definition,
            definition.name,
            owner_session_id=target.session_id,
            owner_session_generation_id=target.session_generation_id,
        )
    else:
        owned_jobs = cli_owned_relay_jobs._list_owned_active_cluster_jobs(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue,
            definition.name,
            owner_session_id=target.session_id,
            owner_session_generation_id=target.session_generation_id,
            scheduler_provider=definition.scheduler_provider,
        )
    return SessionAttachReport(
        cluster=definition.name,
        session_id=target.session_id,
        session_generation_id=target.session_generation_id,
        remote_api_port=target.remote_api_port,
        transport_mode=connection.transport_mode,
        identity_source=target.identity_source,
        channel_reestablished=channel_reestablished,
        connected=connection.connected,
        running_jobs=[
            {
                "job_id": job.job_id,
                "state": job.relay_state.value,
                "scheduler_job_ids": list(job.scheduler_job_ids),
            }
            for job in owned_jobs
        ],
    )


__all__ = [
    "SESSION_ATTACH_REPORT_SCHEMA",
    "AttachTarget",
    "NoDurableSessionRecordError",
    "SessionAttachReport",
    "SessionNotAttachableError",
    "attach_owned_session",
    "build_attach_report",
    "default_owned_session_record_path",
    "resolve_attach_target",
]
