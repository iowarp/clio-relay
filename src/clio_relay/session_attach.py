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
connection is pinned to (``remote_connection_registry.verify_bootstrap``). Attaching
is therefore exactly the ordinary connection bring-up path, reached with the
identity resolved from the durable record instead of a session the caller
already had open in this process.

Three cases, one function (:func:`attach_owned_session`), branching on the
held connection's own typed :attr:`~clio_relay.remote_connection.
RemoteConnection.state`:

* No connection is held for this cluster yet (a fresh process, or the first
  attach after the previous one exited) -- bring one up. One new SSH dial,
  the ordinary challenge-owned handshake.
* ``state == "connected"`` (nothing was lost since the last operation) --
  reuse it untouched. Zero new dials: "resume in place".
* ``state == "authorization_required"`` (the channel dropped) -- this attach
  call IS the one explicit, user-authorized reconnect the 2FA doctrine
  requires (docs/connection-model.md:141-157): exactly one new dial via
  :meth:`~clio_relay.remote_connection.RemoteConnectionRegistry.reconnect`,
  never a silent redial from inside an operation.

A remote session that is dead, torn down, or owned by someone else fails
:func:`~clio_relay.remote_connection.RemoteConnection._establish`'s bootstrap
verification with a typed ``RelayError``; this module re-raises that as the
one typed refusal callers discriminate on, :class:`SessionNotAttachableError`
(``reason == "session_not_attachable"``), carrying the underlying detail
rather than swallowing it (no-silent-fallback). :func:`build_attach_report`
raises the same typed error when a REUSED channel (``channel_reestablished``
False) fails its own live cross-check: unlike a fresh bring-up or an
authorized reconnect, "resume in place" never re-runs bootstrap
verification, so without an explicit check here a forward that stayed open
to a remote session that had since died or been torn down would be reported
as live (iowarp/clio-relay#276 review D3).

Job enumeration rides the SAME held channel every other owned-session
operation does -- ``GET /queue``, which auto-scopes to the owner session
named by the ``OWNER_SESSION_ID_HEADER``/``SESSION_GENERATION_ID_HEADER``
headers every channel request already carries
(``http_api_routes_queue.py:95-125``). There is deliberately no separate
remote-vs-local branch here (iowarp/clio-relay#276 review D2): a per-page
``remote_cli`` ssh exec for a report enumeration would be an unbudgeted,
2FA-visible dial this module's own docstring's "exactly one new dial, never
more" claim must not quietly violate.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, ValidationError

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.control_channel import ChannelDropped
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import RelayJob
from clio_relay.owned_session_record import (
    default_owned_session_record_path,
    load_owned_session_record,
)
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS
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
    while attaching or while re-verifying a reused channel -- ``detail``
    always carries the exact underlying typed error's message, never a
    generic replacement (no-silent-fallback).
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


class AttachJobRow(BaseModel):
    """One non-terminal job the attach report found running under this session."""

    model_config = ConfigDict(extra="forbid")

    job_id: str
    cluster: str
    kind: str
    state: str


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
    running_jobs: list[AttachJobRow]


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
    target: AttachTarget | None = None,
) -> tuple[RemoteConnection, AttachTarget, bool]:
    """Resume or re-establish this cluster's owned-session channel.

    Returns the held connection, the resolved attach target, and whether this
    call performed a new SSH dial (``channel_reestablished``): ``False`` only
    when an already-live channel (``state == "connected"``) for the exact
    resolved identity was reused untouched ("resume in place"); ``True`` for
    both a brand-new bring-up (fresh process / first attach after a crash)
    and an explicit reconnect of a dropped channel (``state ==
    "authorization_required"``) -- either way, exactly one new dial, never
    more.

    ``target`` lets a caller that already resolved (and locked on) an attach
    target pin THIS call to that exact identity: the CLI keys its
    ``owner_session_transition_lock`` on the resolved session id, so an
    internal re-resolution here could race a concurrent ``session start
    --replace`` into working on a session the lock does not cover
    (iowarp/clio-relay#276 review residual 2). When omitted, the target is
    resolved here.
    """
    if target is None:
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
            if existing.state == "connected":
                return existing, target, False
            if existing.state == "authorization_required":
                return active_registry.reconnect(definition.name), target, True
            # "not_established" is unreachable for a connection the registry
            # already holds (RemoteConnectionRegistry.connection() always
            # calls connect() before recording it) -- fall through to the
            # ordinary bring-up path below rather than assuming it away.
        connection = active_registry.connection(
            definition=definition,
            settings=effective_settings,
            remote_api_port=target.remote_api_port,
        )
        return connection, target, True
    except ChannelDropped as exc:
        # A drop in the narrow window between the state read and the dial is
        # a distinct cause from an unattachable session: name it, and name
        # the recovery (re-running attach IS the one authorized reconnect).
        raise SessionNotAttachableError(
            cluster=definition.name,
            session_id=target.session_id,
            detail=(
                "the held channel dropped while attaching; re-run "
                "'clio-relay session attach' to authorize one reconnect "
                f"({exc})"
            ),
        ) from exc
    except RelayError as exc:
        raise SessionNotAttachableError(
            cluster=definition.name,
            session_id=target.session_id,
            detail=str(exc),
        ) from exc


def _list_owned_jobs_over_channel(
    connection: RemoteConnection,
    *,
    definition: ClusterDefinition,
) -> list[AttachJobRow]:
    """Enumerate the attached session's non-terminal jobs over the held channel.

    ``GET /queue`` auto-scopes to the owner session named by the same
    ``OWNER_SESSION_ID_HEADER``/``SESSION_GENERATION_ID_HEADER`` headers
    every request over this channel already carries
    (``http_api_routes_queue.py:95-125``'s ``ctx.resolved.owner_session_id``
    branch) -- this rides the SAME pooled stream as any other owned-session
    operation. Zero new dials, and the identical code path whether this
    connection is ``ssh_forward`` or a dev-mode transport: there is no
    separate remote-vs-local branch here (iowarp/clio-relay#276 review D2).
    """
    rows: list[AttachJobRow] = []
    cursor = 1
    while True:
        document = connection.request_json(
            method="GET",
            path="/queue",
            query={
                "cluster": definition.name,
                "include_terminal": False,
                "cursor": cursor,
                "limit": MAX_RESPONSE_PAGE_RECORDS,
                "scan_limit": MAX_RESPONSE_PAGE_RECORDS,
            },
        )
        if not isinstance(document, dict):
            raise RelayError("owned session queue listing response is not a JSON object")
        page = cast(dict[str, object], document)
        if page.get("visibility_filter") != "exact_owner_session_generation":
            raise RelayError(
                "owned session queue listing was not scoped to the attached owner session"
            )
        raw_jobs = page.get("jobs")
        if not isinstance(raw_jobs, list):
            raise RelayError("owned session queue listing omitted its jobs array")
        for raw_entry in cast(list[object], raw_jobs):
            if not isinstance(raw_entry, dict):
                raise RelayError("owned session queue listing returned a non-object job entry")
            raw_job = cast(dict[str, object], raw_entry).get("job")
            if not isinstance(raw_job, dict):
                raise RelayError("owned session queue listing entry omitted its job")
            try:
                job = RelayJob.model_validate(raw_job)
            except ValidationError as exc:
                raise RelayError("owned session queue listing returned an invalid job") from exc
            rows.append(
                AttachJobRow(
                    job_id=job.job_id,
                    cluster=job.cluster,
                    kind=job.kind.value,
                    state=job.state.value,
                )
            )
        next_cursor = page.get("source_next_cursor")
        if next_cursor is None:
            break
        if (
            isinstance(next_cursor, bool)
            or not isinstance(next_cursor, int)
            or next_cursor <= cursor
        ):
            raise RelayError("owned session queue listing returned an invalid page cursor")
        cursor = next_cursor
    return rows


def build_attach_report(
    *,
    connection: RemoteConnection,
    target: AttachTarget,
    channel_reestablished: bool,
    definition: ClusterDefinition,
) -> SessionAttachReport:
    """Cross-check the held channel and enumerate its running jobs.

    ``connection.session_status()`` (zero new dials) cross-checks the exact
    owner/cluster/session/generation before anything is reported: a fresh
    bring-up or authorized reconnect already proved this out of band, but a
    channel reused in place (``channel_reestablished`` False) never did, so
    this is the one place that verification happens for that path
    (iowarp/clio-relay#276 review D3). A stale/dead/mismatched remote session
    surfaces as the same typed :class:`SessionNotAttachableError` a failed
    bring-up does.
    """
    try:
        connection.session_status()
    except ChannelDropped as exc:
        # Distinguish the true cause: the channel died between the reuse
        # decision and this cross-check. The remote session may be fine --
        # the recovery is the one authorized reconnect, not teardown
        # (iowarp/clio-relay#276 review residual 4).
        raise SessionNotAttachableError(
            cluster=definition.name,
            session_id=target.session_id,
            detail=(
                "the held channel dropped during attach verification; re-run "
                "'clio-relay session attach' to authorize one reconnect "
                f"({exc})"
            ),
        ) from exc
    except RelayError as exc:
        raise SessionNotAttachableError(
            cluster=definition.name,
            session_id=target.session_id,
            detail=str(exc),
        ) from exc
    running_jobs = _list_owned_jobs_over_channel(connection, definition=definition)
    return SessionAttachReport(
        cluster=definition.name,
        session_id=target.session_id,
        session_generation_id=target.session_generation_id,
        remote_api_port=target.remote_api_port,
        transport_mode=connection.transport_mode,
        identity_source=target.identity_source,
        channel_reestablished=channel_reestablished,
        connected=connection.connected,
        running_jobs=running_jobs,
    )


__all__ = [
    "SESSION_ATTACH_REPORT_SCHEMA",
    "AttachJobRow",
    "AttachTarget",
    "NoDurableSessionRecordError",
    "SessionAttachReport",
    "SessionNotAttachableError",
    "attach_owned_session",
    "build_attach_report",
    "default_owned_session_record_path",
    "resolve_attach_target",
]
