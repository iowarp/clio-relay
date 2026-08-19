"""The generic ``gateway`` record CRUD command group (iowarp/clio-relay#231).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names ``gateway_app`` as a mechanical extraction: 12 commands spanning 1,107
lines, past the 800-line new-file cap (SS2 ground rule 6) once module
overhead is added, so this splits by seam rather than forcing all twelve
into one file: this module owns the canonical ``gateway_app = typer.Typer(...)``
instance plus the five generic-record CRUD commands -- ``create``/``list``/
``get``/``update``/``close`` (plain ``GatewaySession`` reads/writes, no
scheduler or transport lifecycle) -- while ``cli_gateway_runtime.py`` owns
all seven runtime-lifecycle commands (``start-runtime``/``resume-runtime``/
``browser-attach``/``browser-detach``/``detach-runtime``/``attach-runtime``/
``stop-runtime``), registered onto this module's ``gateway_app`` via
``@cli_gateway.gateway_app.command(...)`` -- the same two-file-one-Typer
pattern ``cli_queue.py``/``cli_queue_maintenance.py`` and ``cli_session.py``/
``cli_session_owned.py`` established. (``start-runtime`` was originally
drafted into this file alongside the CRUD five since it is textually
adjacent in old ``cli.py`` and shares no CRUD-exclusive helper, but doing so
put this file at 819 lines -- over the cap -- so it moved to
``cli_gateway_runtime.py`` instead, which needed the room far less than this
one did.) Unlike ``session_app``, this group has no giant single-function/
closure entanglement, so every one of its twelve commands (and their
exclusive private helpers) leaves ``cli.py`` entirely -- no "stays behind"
exception.

**What moved as exclusive private helpers.** ``_reject_generic_cli_gateway_
runtime_fields`` (plus its three backing frozensets,
``_GENERIC_GATEWAY_RUNTIME_KEYS``/``_GENERIC_GATEWAY_CONNECTOR_KEYS``/
``_GENERIC_GATEWAY_OWNER_METADATA_KEYS``) sat just above ``create`` in
``cli.py`` and is called only from ``create``/``update``, both here.
``_local_gateway_session``/``_local_gateway_queue`` (interleaved after
``create``) and ``_should_query_remote_cluster``/``_parse_gateway_page``
(interleaved after ``list``) are each called only from commands that land in
this file. ``_try_remote_gateway_session_passthrough`` lived far below in
``cli.py``'s shared helper zone (former line 14386), but every one of its
four call sites was inside ``create``/``get``/``update``/``close`` -- all
moving here -- so despite the physical distance it is exclusive too, and
moves with its callers rather than staying behind as dead code.

**Domain logic stays where it lives.** ``core_queue.ClioCoreQueue`` and
``remote_cli.*`` keep additional call sites elsewhere in ``cli.py`` (mostly
inside ``session_start``/``session_teardown``, which stay put per
``cli_session.py``'s own docstring), so those keep caller ``cli`` unchanged
in ``AUDITED_COLLABORATORS`` (where audited at all).

**The import-cycle discipline.** ``cli`` is never bound as a module-level
name here, matching every prior extraction: it is imported function-locally,
as the first statement of each command body that needs a cross-cutting
``cli.py`` collaborator (``_require_cluster``/``_run_or_exit``/
``_json_object``/``_json_text_from_option``/``_public_json``).
"""

# `cli.<symbol>` references below are intentional cross-module access to a
# name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring).
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Annotated, cast

import typer
from pydantic import ValidationError

import clio_relay.core_queue as core_queue
import clio_relay.remote_cli as remote_cli
from clio_relay.config import RelaySettings
from clio_relay.errors import NotFoundError, RelayError
from clio_relay.models import GatewaySession, GatewaySessionState
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.public_records import public_gateway_session

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring and `cli_relay_host.py`'s identical discipline.
gateway_app = typer.Typer(no_args_is_help=True)


_GENERIC_GATEWAY_RUNTIME_KEYS = frozenset(
    {
        "runtime_spec",
        "jarvis_runtime_binding",
        "browser_attachment",
        "ownership_intents",
        "teardown_intent",
        "teardown",
        "detach",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)
_GENERIC_GATEWAY_CONNECTOR_KEYS = frozenset(
    {
        "browser_proxy",
        "desktop_connector",
        "remote_connector",
    }
)
_GENERIC_GATEWAY_OWNER_METADATA_KEYS = frozenset(
    {
        "owner",
        "owner_session_id",
        "owner_session_generation_id",
        "owner_session_admission_id",
        "runtime_kind",
        "binding_source",
        "source_relay_job_id",
        "source_relay_artifact_id",
        "jarvis_execution_id",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)


def _reject_generic_cli_gateway_runtime_fields(
    *,
    gateway: dict[str, object],
    metadata: dict[str, object],
) -> None:
    """Keep generic CLI gateway writes outside supervisor-owned runtime identity."""
    protected = [f"gateway.{key}" for key in sorted(_GENERIC_GATEWAY_RUNTIME_KEYS & gateway.keys())]
    transport = gateway.get("transport")
    if isinstance(transport, dict):
        protected.extend(
            f"gateway.transport.{key}"
            for key in sorted(_GENERIC_GATEWAY_CONNECTOR_KEYS & transport.keys())
        )
    protected.extend(
        f"metadata.{key}" for key in sorted(_GENERIC_GATEWAY_OWNER_METADATA_KEYS & metadata.keys())
    )
    if protected:
        raise typer.BadParameter(
            "generic gateway commands cannot write relay-managed runtime fields: "
            + ", ".join(protected)
        )


@gateway_app.command("create")
def gateway_create(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Human-readable session name.")],
    state: Annotated[
        GatewaySessionState,
        typer.Option(help="Initial gateway session state."),
    ] = GatewaySessionState.CREATED,
    queue_state: Annotated[str | None, typer.Option(help="Scheduler queue state.")] = None,
    node: Annotated[str | None, typer.Option(help="Allocated node or host.")] = None,
    stdout_uri: Annotated[str | None, typer.Option(help="Gateway stdout log URI.")] = None,
    stderr_uri: Annotated[str | None, typer.Option(help="Gateway stderr log URI.")] = None,
    log_uri: Annotated[
        list[str] | None,
        typer.Option(help="Additional log URI; repeat for multiple logs."),
    ] = None,
    artifact: Annotated[
        list[str] | None,
        typer.Option(help="Artifact URI or id; repeat for multiple artifacts."),
    ] = None,
    gateway_json: Annotated[
        str,
        typer.Option(help="JSON object with gateway endpoint metadata."),
    ] = "{}",
    gateway_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with gateway endpoint metadata."),
    ] = None,
    resources_json: Annotated[
        str,
        typer.Option(help="JSON object with requested resource metadata."),
    ] = "{}",
    resources_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with requested resource metadata."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata for this gateway session."),
    ] = "{}",
    metadata_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object metadata file."),
    ] = None,
) -> None:
    """Create a durable scheduler-backed gateway service session."""
    import clio_relay.cli as cli

    gateway_source = cli._json_text_from_option(gateway_json, gateway_json_file)
    resources_source = cli._json_text_from_option(resources_json, resources_json_file)
    metadata_source = cli._json_text_from_option(metadata_json, metadata_json_file)
    gateway_payload = cli._json_object(gateway_source)
    metadata_payload = cli._json_object(metadata_source)
    _reject_generic_cli_gateway_runtime_fields(
        gateway=gateway_payload,
        metadata=metadata_payload,
    )
    remote_args = [
        "gateway",
        "create",
        "--cluster",
        cluster,
        "--name",
        name,
        "--state",
        state.value,
        "--gateway-json",
        gateway_source,
        "--resources-json",
        resources_source,
        "--metadata-json",
        metadata_source,
    ]
    if queue_state is not None:
        remote_args.extend(["--queue-state", queue_state])
    if node is not None:
        remote_args.extend(["--node", node])
    if stdout_uri is not None:
        remote_args.extend(["--stdout-uri", stdout_uri])
    if stderr_uri is not None:
        remote_args.extend(["--stderr-uri", stderr_uri])
    for value in log_uri or []:
        remote_args.extend(["--log-uri", value])
    for value in artifact or []:
        remote_args.extend(["--artifact", value])
    if _try_remote_gateway_session_passthrough(cluster, remote_args):
        return
    session = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir).create_gateway_session(
        GatewaySession(
            cluster=cluster,
            name=name,
            state=state,
            queue_state=queue_state,
            node=node,
            stdout_uri=stdout_uri,
            stderr_uri=stderr_uri,
            log_uris=log_uri or [],
            gateway=gateway_payload,
            artifacts=artifact or [],
            requested_resources=cli._json_object(resources_source),
            metadata=metadata_payload,
        )
    )
    typer.echo(cli._public_json(public_gateway_session(session)))


def _local_gateway_session(
    session_id: str,
    *,
    cluster: str | None,
) -> GatewaySession | None:
    """Return a desktop-owned gateway record before considering remote passthrough."""
    queue = _local_gateway_queue()
    try:
        session = queue.get_gateway_session(session_id)
    except NotFoundError:
        return None
    if cluster is not None and session.cluster != cluster:
        return None
    return session


def _local_gateway_queue() -> core_queue.ClioCoreQueue:
    """Open the desktop queue without resolving unrelated executable settings."""
    configured = os.getenv("CLIO_RELAY_CORE_DIR")
    if configured:
        core_dir = Path(configured).expanduser().resolve()
    else:
        bootstrap_dir = Path.home() / ".local" / "share" / "clio-relay" / "core"
        core_dir = bootstrap_dir.resolve() if bootstrap_dir.exists() else Path(".clio-relay/core")
    return core_queue.ClioCoreQueue(core_dir)


@gateway_app.command("list")
def gateway_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Optional configured cluster filter."),
    ] = None,
    cursor: Annotated[
        int,
        typer.Option(help="One-based global gateway source cursor.", min=1),
    ] = 1,
    limit: Annotated[
        int,
        typer.Option(
            help="Maximum gateway source positions read.",
            min=1,
            max=MAX_RESPONSE_PAGE_RECORDS,
        ),
    ] = DEFAULT_RESPONSE_PAGE_RECORDS,
    desktop_cursor: Annotated[
        int | None,
        typer.Option(help="Optional desktop-owned gateway source cursor.", min=1),
    ] = None,
    cluster_cursor: Annotated[
        int | None,
        typer.Option(help="Optional cluster-owned gateway source cursor.", min=1),
    ] = None,
) -> None:
    """List bounded desktop and cluster gateway source windows."""
    import clio_relay.cli as cli

    def action() -> None:
        resolved_desktop_cursor = desktop_cursor or cursor
        resolved_cluster_cursor = cluster_cursor or cursor
        remote_args = [
            "gateway",
            "list",
            "--cursor",
            str(resolved_cluster_cursor),
            "--limit",
            str(limit),
        ]
        if cluster is not None:
            remote_args.extend(["--cluster", cluster])
        queue = _local_gateway_queue()
        desktop_sessions, desktop_next_cursor, desktop_total = queue.list_gateway_sessions_page(
            cursor=resolved_desktop_cursor,
            limit=limit,
            cluster=cluster,
        )
        cluster_sessions: list[GatewaySession] = []
        cluster_next_cursor: int | None = None
        cluster_total = 0
        query_remote = cluster is not None and _should_query_remote_cluster(cluster)
        if query_remote:
            assert cluster is not None
            definition = cli._require_cluster(cluster)
            cluster_sessions, cluster_next_cursor, cluster_total = _parse_gateway_page(
                remote_cli.run_remote_clio(definition, remote_args),
                limit=limit,
                expected_cluster=cluster,
            )
        combined = {session.session_id: session for session in cluster_sessions}
        combined.update({session.session_id: session for session in desktop_sessions})
        sessions = sorted(
            combined.values(),
            key=lambda session: (session.created_at, session.session_id),
        )
        typer.echo(
            cli._public_json(
                {
                    "gateway_sessions": [public_gateway_session(session) for session in sessions],
                    "source_cursor": cursor,
                    "source_limit": limit,
                    "source_next_cursor": (
                        (
                            desktop_next_cursor
                            if desktop_next_cursor == cluster_next_cursor
                            else None
                        )
                        if query_remote
                        else desktop_next_cursor
                    ),
                    "source_next_cursors": {
                        "desktop": desktop_next_cursor,
                        "cluster": cluster_next_cursor,
                    },
                    "source_cursors": {
                        "desktop": resolved_desktop_cursor,
                        "cluster": resolved_cluster_cursor,
                    },
                    "source_totals": {
                        "desktop": desktop_total,
                        "cluster": cluster_total,
                    },
                    "source_total": desktop_total + cluster_total,
                    "source_total_semantics": "sum_of_independent_gateway_source_high_waters",
                    "aggregate_record_limit": limit * 2,
                    "filters_apply_within_source_window": True,
                }
            )
        )

    cli._run_or_exit(action)


def _should_query_remote_cluster(cluster: str) -> bool:
    """Return whether a CLI read should include the configured remote store."""
    import clio_relay.cli as cli

    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    return remote_cli.should_execute_on_cluster(cli._require_cluster(cluster))


def _parse_gateway_page(
    payload: str,
    *,
    limit: int,
    expected_cluster: str,
) -> tuple[list[GatewaySession], int | None, int]:
    """Validate a bounded current or legacy remote gateway-list response."""
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RelayError("remote gateway list did not return valid JSON") from exc
    if isinstance(decoded, list):
        raw_sessions = cast(list[object], decoded)
        next_cursor: int | None = None
        total = len(raw_sessions)
    elif isinstance(decoded, dict):
        page = cast(dict[str, object], decoded)
        raw = page.get("gateway_sessions")
        if not isinstance(raw, list):
            raise RelayError("remote gateway page omitted gateway_sessions")
        raw_sessions = cast(list[object], raw)
        raw_next_cursor = page.get("source_next_cursor")
        if raw_next_cursor is not None and not isinstance(raw_next_cursor, int):
            raise RelayError("remote gateway page has an invalid next cursor")
        next_cursor = raw_next_cursor
        raw_total = page.get("source_total")
        if not isinstance(raw_total, int) or raw_total < len(raw_sessions):
            raise RelayError("remote gateway page has an invalid source total")
        total = raw_total
    else:
        raise RelayError("remote gateway list must return an object or legacy array")
    if len(raw_sessions) > limit:
        raise RelayError(f"remote gateway page exceeds the requested {limit}-record limit")
    sessions = [GatewaySession.model_validate(item) for item in raw_sessions]
    if any(session.cluster != expected_cluster for session in sessions):
        raise RelayError("remote gateway page returned a different cluster")
    return sessions, next_cursor, total


@gateway_app.command("get")
def gateway_get(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to inspect over SSH."),
    ] = None,
) -> None:
    """Read a gateway service session."""
    import clio_relay.cli as cli

    local_session = _local_gateway_session(session_id, cluster=cluster)
    if local_session is not None:
        typer.echo(cli._public_json(public_gateway_session(local_session)))
        return
    remote_args = ["gateway", "get", session_id]
    if _try_remote_gateway_session_passthrough(cluster, remote_args):
        return
    session = core_queue.ClioCoreQueue(RelaySettings.from_env().core_dir).get_gateway_session(
        session_id
    )
    typer.echo(cli._public_json(public_gateway_session(session)))


@gateway_app.command("update")
def gateway_update(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to update over SSH."),
    ] = None,
    state: Annotated[
        GatewaySessionState | None,
        typer.Option(help="Updated gateway session state."),
    ] = None,
    queue_state: Annotated[str | None, typer.Option(help="Scheduler queue state.")] = None,
    node: Annotated[str | None, typer.Option(help="Allocated node or host.")] = None,
    stdout_uri: Annotated[str | None, typer.Option(help="Gateway stdout log URI.")] = None,
    stderr_uri: Annotated[str | None, typer.Option(help="Gateway stderr log URI.")] = None,
    log_uri: Annotated[
        list[str] | None,
        typer.Option(help="Additional log URI; repeat for multiple logs."),
    ] = None,
    artifact: Annotated[
        list[str] | None,
        typer.Option(help="Artifact URI or id; repeat for multiple artifacts."),
    ] = None,
    resources_json: Annotated[
        str | None,
        typer.Option(help="JSON object with requested resource metadata."),
    ] = None,
    resources_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with requested resource metadata."),
    ] = None,
    gateway_json: Annotated[
        str | None,
        typer.Option(help="JSON object with gateway endpoint metadata."),
    ] = None,
    gateway_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object with gateway endpoint metadata."),
    ] = None,
    metadata_json: Annotated[
        str,
        typer.Option(help="JSON object metadata to merge into this session."),
    ] = "{}",
    metadata_json_file: Annotated[
        Path | None,
        typer.Option(help="Path to a JSON object metadata file."),
    ] = None,
) -> None:
    """Update a gateway service session."""
    import clio_relay.cli as cli

    if gateway_json is not None and gateway_json_file is not None:
        raise typer.BadParameter("use either --gateway-json or --gateway-json-file, not both")
    if resources_json is not None and resources_json_file is not None:
        raise typer.BadParameter("use either --resources-json or --resources-json-file, not both")
    gateway_source = None
    if gateway_json is not None or gateway_json_file is not None:
        gateway_source = cli._json_text_from_option(gateway_json or "{}", gateway_json_file)
    resources_source = None
    if resources_json is not None or resources_json_file is not None:
        resources_source = cli._json_text_from_option(resources_json or "{}", resources_json_file)
    metadata_source = cli._json_text_from_option(metadata_json, metadata_json_file)
    gateway_payload = cli._json_object(gateway_source) if gateway_source is not None else None
    metadata_payload = cli._json_object(metadata_source)
    _reject_generic_cli_gateway_runtime_fields(
        gateway=gateway_payload or {},
        metadata=metadata_payload,
    )
    remote_args = ["gateway", "update", session_id]
    if state is not None:
        remote_args.extend(["--state", state.value])
    if queue_state is not None:
        remote_args.extend(["--queue-state", queue_state])
    if node is not None:
        remote_args.extend(["--node", node])
    if stdout_uri is not None:
        remote_args.extend(["--stdout-uri", stdout_uri])
    if stderr_uri is not None:
        remote_args.extend(["--stderr-uri", stderr_uri])
    for value in log_uri or []:
        remote_args.extend(["--log-uri", value])
    for value in artifact or []:
        remote_args.extend(["--artifact", value])
    if resources_source is not None:
        remote_args.extend(["--resources-json", resources_source])
    if gateway_source is not None:
        remote_args.extend(["--gateway-json", gateway_source])
    remote_args.extend(["--metadata-json", metadata_source])
    local_session = _local_gateway_session(session_id, cluster=cluster)
    if local_session is None and _try_remote_gateway_session_passthrough(cluster, remote_args):
        return
    updates: dict[str, object] = {}
    if queue_state is not None:
        updates["queue_state"] = queue_state
    if node is not None:
        updates["node"] = node
    if stdout_uri is not None:
        updates["stdout_uri"] = stdout_uri
    if stderr_uri is not None:
        updates["stderr_uri"] = stderr_uri
    if log_uri is not None:
        updates["log_uris"] = log_uri
    if artifact is not None:
        updates["artifacts"] = artifact
    if resources_source is not None:
        updates["requested_resources"] = cli._json_object(resources_source)
    if gateway_payload is not None:
        updates["gateway"] = gateway_payload
    cli._run_or_exit(
        lambda: typer.echo(
            cli._public_json(
                public_gateway_session(
                    core_queue.ClioCoreQueue(
                        RelaySettings.from_env().core_dir
                    ).update_gateway_session(
                        session_id,
                        state=state,
                        metadata=metadata_payload,
                        reject_relay_managed_fields=True,
                        **updates,
                    )
                )
            )
        )
    )


@gateway_app.command("close")
def gateway_close(
    session_id: str,
    cluster: Annotated[
        str | None,
        typer.Option(help="Configured cluster to update over SSH."),
    ] = None,
) -> None:
    """Mark a gateway service session closed."""
    import clio_relay.cli as cli

    local_session = _local_gateway_session(session_id, cluster=cluster)
    if local_session is None and _try_remote_gateway_session_passthrough(
        cluster, ["gateway", "close", session_id]
    ):
        return
    cli._run_or_exit(
        lambda: typer.echo(
            cli._public_json(
                public_gateway_session(
                    core_queue.ClioCoreQueue(
                        RelaySettings.from_env().core_dir
                    ).close_gateway_session(session_id)
                )
            )
        )
    )


def _try_remote_gateway_session_passthrough(cluster: str | None, args: list[str]) -> bool:
    """Render a validated remote gateway record through the local public projection."""
    import clio_relay.cli as cli

    if cluster is None:
        return False
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    definition = cli._require_cluster(cluster)
    if not remote_cli.should_execute_on_cluster(definition):
        return False

    def action() -> None:
        payload = remote_cli.run_remote_clio(definition, args)
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RelayError("remote gateway command did not return valid JSON") from exc
        try:
            session = GatewaySession.model_validate(decoded)
        except ValidationError as exc:
            raise RelayError("remote gateway command returned an invalid session") from exc
        if session.cluster != cluster:
            raise RelayError("remote gateway command returned a different cluster")
        typer.echo(cli._public_json(public_gateway_session(session)))

    cli._run_or_exit(action)
    return True
