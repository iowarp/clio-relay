"""The ``remote-mcp`` command group's register/unregister/list/reload/refresh
commands (iowarp/clio-relay#231 cli.py decomposition).

``docs/design/relay-architecture-2026-08.md`` SS5's target owner-module map
names ``remote_mcp_app`` -- ~1,430 lines, the largest single Typer sub-app
still cli.py-resident after the top-level command-module extractions -- for
a split: this module owns its own ``typer.Typer()`` instance (the
``relay-host``/``release`` precedent) and five of its six commands
(``register``/``unregister``/``list``/``reload``/``refresh``); ``validate``
-- at 314 body lines with its own ~980-line business-logic engine (route
resolution, the durable virtual-MCP call, the fresh-Spack-install
configuration-tree observation) -- moves separately into the sibling
``cli_remote_mcp_validate.py``, so this module stays comfortably under the
800-line cap, matching the same two-way split ``cli_jarvis_mcp.py``/
``cli_jarvis_mcp_validate.py`` already established. The engine itself moved
to the new ``remote_mcp_validation.py`` -- a real owner module, not another
cli.py-shaped command wrapper, per ground rule 2.

**Domain logic stays where it lives.** ``register``/``unregister``/``list``/
``reload`` delegate to ``ClusterRegistry.mutate``/``RemoteMcpSchemaCache``
(already-correct owners); ``refresh`` composes ``resolve_registered_remote_
mcp_admission``/``cache_entry_from_discovery_artifact`` the same way it did
inside cli.py.

**Exclusive helpers moved with their only caller.** ``_remote_mcp_cache_
status`` (``list``'s only call site) and ``_read_remote_mcp_result_
artifact``/``_read_local_mcp_result_artifact`` (``refresh``'s only call
sites) had exactly one caller each in the whole of cli.py -- all three move
here outright, no forwarder needed.

**Collaborators reached through cli.py's own name (not moved here).**
``_environment_references``, ``_managed_queue_from_env``, ``_json_output``,
``_require_discovery_success``, ``_last_nonempty_line``, ``_run_or_exit``,
and the artifact-reading family the moved helpers above call
(``_remote_artifact_records``, ``_artifact_record``, ``_decode_artifact_
envelope``, ``_complete_local_artifact_records``) are all confirmed shared
with the jarvis-mcp engine or cli.py-resident session code -- reached the
established way, through cli.py's own name via the function-local ``import
clio_relay.cli as cli`` discipline.

**Reassigned patch-seam caller.** ``mcp_server.load_registered_remote_mcp_
catalog`` had exactly three call sites in the whole of cli.py -- ``reload``
and ``refresh`` here, plus ``remote_mcp_validate``'s own preflight in the
sibling ``cli_remote_mcp_validate.py``. This slice reassigns its ``caller``
entry in ``AUDITED_COLLABORATORS`` from ``"cli"`` to ``"cli_remote_mcp"``
(this module owns two of its three call sites); the sibling module reaches
it the same module-attribute way without being separately tracked, the same
"second, untracked caller" shape established elsewhere in this campaign for
an audited symbol with more than one real caller.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, cast
from uuid import uuid4

import typer
from pydantic import ValidationError

import clio_relay.mcp_server as mcp_server_module
import clio_relay.relay_ops as relay_ops
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    RemoteMcpContract,
    RemoteMcpProfile,
    RemoteMcpServerConfig,
    default_registry_path,
)
from clio_relay.errors import RelayError
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    JobKind,
    McpCallSpec,
    McpOperation,
    RelayJob,
)
from clio_relay.relay_ops import read_artifact_bytes
from clio_relay.remote_cli import staged_remote_cluster_registry
from clio_relay.remote_mcp import (
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    cache_entry_from_discovery_artifact,
    default_remote_mcp_cache_path,
    remote_mcp_execution_fingerprint,
    resolve_registered_remote_mcp_admission,
)

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level -- see
# this module's own docstring for the import-cycle discipline this supports.
# pyright: reportPrivateUsage=false

remote_mcp_app = typer.Typer(no_args_is_help=True)


def _remote_mcp_cache_status(
    registration: RemoteMcpServerConfig,
    entry: RemoteMcpSchemaCacheEntry | None,
) -> dict[str, object]:
    if entry is None:
        return {"state": "missing", "fresh": False}
    execution_matches = entry.execution_fingerprint == remote_mcp_execution_fingerprint(
        registration
    )
    fresh = entry.is_fresh()
    if fresh and execution_matches:
        state = "fresh"
    elif not execution_matches:
        state = "command_changed"
    else:
        state = "stale"
    return {
        "state": state,
        "fresh": fresh,
        "execution_matches": execution_matches,
        "discovered_at": entry.discovered_at.isoformat(),
        "expires_at": entry.expires_at.isoformat(),
        "schema_digest": entry.schema_digest,
        "tool_names": sorted(tool.name for tool in entry.tools),
        "provenance": entry.provenance.model_dump(mode="json"),
    }


def _read_remote_mcp_result_artifact(
    definition: ClusterDefinition,
    job_id: str,
) -> tuple[dict[str, object], bytes]:
    import clio_relay.cli as cli

    artifacts = cli._remote_artifact_records(definition, job_id)
    artifact = cli._artifact_record(artifacts, kind="mcp_result")
    if artifact is None:
        raise RelayError(f"remote MCP discovery job has no mcp_result artifact: {job_id}")
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("remote MCP result artifact has no artifact_id")
    envelope = cli._json_output(
        remote_cli.run_remote_clio(definition, ["job", "read-artifact", artifact_id]),
        "remote discovery artifact payload",
    )
    return artifact, cli._decode_artifact_envelope(envelope)


def _read_local_mcp_result_artifact(
    queue: Any,
    job_id: str,
) -> tuple[dict[str, object], bytes]:
    import clio_relay.cli as cli

    artifacts = cli._complete_local_artifact_records(queue, job_id)
    artifact = cli._artifact_record(artifacts, kind="mcp_result")
    if artifact is None:
        raise RelayError(f"remote MCP discovery job has no mcp_result artifact: {job_id}")
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("local MCP result artifact has no artifact_id")
    envelope = read_artifact_bytes(queue, artifact_id)
    return cast(dict[str, object], artifact), cli._decode_artifact_envelope(envelope)


@remote_mcp_app.command("register")
def remote_mcp_register(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Stable server registration name.")],
    command: Annotated[str, typer.Option(help="Remote stdio MCP executable.")],
    arg: Annotated[
        list[str] | None,
        typer.Option(help="Remote MCP command argument. Repeatable and passed without a shell."),
    ] = None,
    env_from: Annotated[
        list[str] | None,
        typer.Option(
            help=(
                "Child=SOURCE environment reference. Repeatable; values are resolved only "
                "by the endpoint worker."
            )
        ),
    ] = None,
    allow_tool: Annotated[
        list[str] | None,
        typer.Option(
            help="Exact remote tool name to virtualize. Repeatable; '*' explicitly allows all."
        ),
    ] = None,
    profile: Annotated[
        list[str] | None,
        typer.Option(help="Local MCP profile allowed to expose tools: user, admin, or operator."),
    ] = None,
    namespace: Annotated[
        str | None,
        typer.Option(help="Optional stable namespace used in generated local aliases."),
    ] = None,
    contract: Annotated[
        str | None,
        typer.Option(
            help=(
                "Optional audited semantic contract. Supported: clio-kit-spack-user-v2.3 "
                "(current, 5 tools), clio-kit-spack-user-v2.1 (compatibility, 3 tools; "
                "accepted against a live v2.3 server as a subset), clio-kit-spack-user-v2 "
                "(compatibility), clio-kit-scientific-catalog-user-v1.1 (current), "
                "clio-kit-scientific-catalog-user-v1 (compatibility)."
            )
        ),
    ] = None,
    schema_cache_ttl_seconds: Annotated[
        int,
        typer.Option(help="Maximum age of a discovered schema before tools are hidden.", min=1),
    ] = 86_400,
    call_timeout_seconds: Annotated[
        int,
        typer.Option(
            help="Maximum duration of each virtual tools/call execution.",
            min=1,
            max=86_400,
        ),
    ] = 300,
    enabled: Annotated[
        bool,
        typer.Option("--enabled/--disabled", help="Enable this remote MCP registration."),
    ] = True,
    replace: Annotated[
        bool,
        typer.Option(help="Replace an existing registration with the same cluster and name."),
    ] = False,
) -> None:
    """Register an allowlisted remote MCP server for one cluster."""
    import clio_relay.cli as cli

    registry_path = default_registry_path()
    try:
        registration = RemoteMcpServerConfig(
            command=command,
            args=arg or [],
            env_from=cli._environment_references(env_from),
            namespace=namespace,
            contract=cast(RemoteMcpContract | None, contract),
            allow_tools=allow_tool or [],
            profiles=cast(list[RemoteMcpProfile], profile or ["admin"]),
            schema_cache_ttl_seconds=schema_cache_ttl_seconds,
            call_timeout_seconds=call_timeout_seconds,
            enabled=enabled,
        )
    except ValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc

    def update_registry(registry: ClusterRegistry) -> None:
        definition = registry.require(cluster)
        if name in definition.remote_mcp_servers and not replace:
            raise typer.BadParameter(
                f"remote MCP server is already registered for {cluster}: {name}; use --replace"
            )
        definition.remote_mcp_servers[name] = registration

    ClusterRegistry.mutate(registry_path, update_registry)
    cache = RemoteMcpSchemaCache.load(default_remote_mcp_cache_path(registry_path=registry_path))
    cached = cache.entry_for(cluster, name)
    typer.echo(
        json.dumps(
            {
                "cluster": cluster,
                "server_name": name,
                "registration": registration.model_dump(mode="json"),
                "execution_fingerprint": remote_mcp_execution_fingerprint(registration),
                "cache_reusable": (
                    cached is not None
                    and cached.execution_fingerprint
                    == remote_mcp_execution_fingerprint(registration)
                ),
                "reload_semantics": (
                    "configuration is read on the next local MCP tools/list; run remote-mcp "
                    "refresh before exposure when the cache is missing, stale, or command-changed"
                ),
            },
            indent=2,
        )
    )


@remote_mcp_app.command("unregister")
def remote_mcp_unregister(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Remote MCP server registration name.")],
) -> None:
    """Remove a remote MCP registration and its local schema cache entry."""
    registry_path = default_registry_path()

    def update_registry(registry: ClusterRegistry) -> None:
        definition = registry.require(cluster)
        if name not in definition.remote_mcp_servers:
            raise typer.BadParameter(f"remote MCP server is not registered for {cluster}: {name}")
        del definition.remote_mcp_servers[name]

    ClusterRegistry.mutate(registry_path, update_registry)
    cache_path = default_remote_mcp_cache_path(registry_path=registry_path)
    RemoteMcpSchemaCache.remove_entry(cache_path, cluster, name)
    typer.echo(
        json.dumps(
            {
                "cluster": cluster,
                "server_name": name,
                "registered": False,
                "cache_removed": True,
            },
            indent=2,
        )
    )


@remote_mcp_app.command("list")
def remote_mcp_list(
    cluster: Annotated[
        str | None,
        typer.Option(help="Optional configured cluster filter."),
    ] = None,
) -> None:
    """List registrations and cache freshness/provenance as JSON."""
    registry_path = default_registry_path()
    registry = ClusterRegistry.load(registry_path)
    if cluster is not None:
        registry.require(cluster)
    cache_path = default_remote_mcp_cache_path(registry_path=registry_path)
    cache = RemoteMcpSchemaCache.load(cache_path)
    registrations: list[dict[str, object]] = []
    for cluster_name, definition in sorted(registry.clusters.items()):
        if cluster is not None and cluster_name != cluster:
            continue
        for server_name, registration in sorted(definition.remote_mcp_servers.items()):
            entry = cache.entry_for(cluster_name, server_name)
            registrations.append(
                {
                    "cluster": cluster_name,
                    "server_name": server_name,
                    "registration": registration.model_dump(mode="json"),
                    "cache": _remote_mcp_cache_status(registration, entry),
                }
            )
    typer.echo(
        json.dumps(
            {
                "registry_path": str(registry_path),
                "cache_path": str(cache_path),
                "registrations": registrations,
            },
            indent=2,
        )
    )


@remote_mcp_app.command("reload")
def remote_mcp_reload(
    profile: Annotated[
        str,
        typer.Option(help="Local MCP profile to render: user, admin, operator, or all."),
    ] = "user",
) -> None:
    """Reload local config/cache and report the exact next tools/list catalog."""
    if profile not in {"user", "admin", "operator", "all"}:
        raise typer.BadParameter("--profile must be user, admin, operator, or all")
    catalog = mcp_server_module.load_registered_remote_mcp_catalog(profile)
    typer.echo(
        json.dumps(
            {
                "profile": profile,
                "catalog_revision": catalog.revision,
                "tools": catalog.tool_definitions(),
                "issues": [issue.model_dump(mode="json") for issue in catalog.issues],
                "remote_discovery_performed": False,
                "mcp_server_restart_required": False,
                "client_action": "request tools/list again to observe this catalog revision",
            },
            indent=2,
        )
    )


@remote_mcp_app.command("refresh")
def remote_mcp_refresh(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    name: Annotated[str, typer.Option(help="Remote MCP server registration name.")],
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Optional timeout for the remote MCP protocol session.", min=1),
    ] = 120,
    wait_timeout_seconds: Annotated[
        float,
        typer.Option(help="Maximum time to wait for the durable discovery job.", min=1),
    ] = 600,
    poll_seconds: Annotated[
        float,
        typer.Option(help="Durable discovery job polling interval.", min=0.05),
    ] = 2,
    idempotency_key: Annotated[
        str | None,
        typer.Option(help="Optional discovery submission idempotency key."),
    ] = None,
) -> None:
    """Discover a registered server through a durable MCP tools/list relay job."""
    import clio_relay.cli as cli

    registry_path = default_registry_path()
    registry = ClusterRegistry.load(registry_path)
    definition = registry.require(cluster)
    try:
        registration = definition.remote_mcp_servers[name]
    except KeyError as exc:
        raise typer.BadParameter(
            f"remote MCP server is not registered for {cluster}: {name}"
        ) from exc
    if not registration.enabled:
        raise typer.BadParameter(f"remote MCP server is disabled for {cluster}: {name}")
    key = idempotency_key or f"remote-mcp-discovery:{cluster}:{name}:{uuid4().hex}"

    def action() -> None:
        if remote_cli.should_execute_on_cluster(definition):
            remote_args = [
                "mcp-call",
                "--cluster",
                cluster,
                "--server",
                registration.command,
                "--operation",
                McpOperation.TOOLS_LIST.value,
                "--idempotency-key",
                key,
            ]
            if timeout_seconds is not None:
                remote_args.extend(["--timeout-seconds", str(timeout_seconds)])
            for item in registration.args:
                remote_args.extend(["--server-arg", item])
            for child_name, source_name in sorted(registration.env_from.items()):
                remote_args.extend(["--env-from", f"{child_name}={source_name}"])
            with staged_remote_cluster_registry(definition) as remote_registry_path:
                job_id = cli._last_nonempty_line(
                    remote_cli.run_remote_clio(
                        definition,
                        remote_args,
                        cluster_registry_path=remote_registry_path,
                    )
                )
            wait_result = cli._json_output(
                remote_cli.run_remote_clio(
                    definition,
                    [
                        "job",
                        "wait",
                        job_id,
                        "--timeout-seconds",
                        str(wait_timeout_seconds),
                        "--poll-seconds",
                        str(poll_seconds),
                    ],
                ),
                "remote discovery wait",
            )
            cli._require_discovery_success(wait_result, job_id)
            artifact, artifact_payload = _read_remote_mcp_result_artifact(
                definition,
                job_id,
            )
        else:
            queue = cli._managed_queue_from_env()
            admission_class, admission_authority = resolve_registered_remote_mcp_admission(
                queue=queue,
                definition=definition,
                cluster=cluster,
                server=registration.command,
                server_args=registration.args,
                env_from=registration.env_from,
                operation=McpOperation.TOOLS_LIST,
                tool=None,
                expected_server_artifact_digest=None,
                evidence=None,
                timeout_seconds=timeout_seconds,
            )
            metadata = (
                {}
                if admission_authority is None
                else {
                    MCP_ADMISSION_AUTHORITY_METADATA_KEY: admission_authority.model_dump(
                        mode="json"
                    )
                }
            )
            job = queue.submit_job(
                RelayJob(
                    cluster=cluster,
                    kind=JobKind.MCP_CALL,
                    spec=McpCallSpec(
                        server=registration.command,
                        server_args=registration.args,
                        env_from=registration.env_from,
                        admission_class=admission_class,
                        operation=McpOperation.TOOLS_LIST,
                        timeout_seconds=timeout_seconds,
                    ),
                    idempotency_key=key,
                    metadata=metadata,
                )
            )
            terminal = relay_ops.wait_for_terminal(
                queue,
                job.job_id,
                timeout_seconds=wait_timeout_seconds,
                poll_seconds=poll_seconds,
            )
            cli._require_discovery_success(terminal.model_dump(mode="json"), job.job_id)
            artifact, artifact_payload = _read_local_mcp_result_artifact(queue, job.job_id)
            job_id = job.job_id
        entry = cache_entry_from_discovery_artifact(
            cluster=cluster,
            server_name=name,
            registration=registration,
            discovery_job_id=job_id,
            artifact_id=str(artifact["artifact_id"]),
            artifact_sha256=cast(str | None, artifact.get("sha256")),
            artifact_payload=artifact_payload,
        )
        cache_path = default_remote_mcp_cache_path(registry_path=registry_path)
        RemoteMcpSchemaCache.update_entry(cache_path, entry)
        catalogs = {
            profile_name: mcp_server_module.load_registered_remote_mcp_catalog(profile_name)
            for profile_name in registration.profiles
        }
        typer.echo(
            json.dumps(
                {
                    "cluster": cluster,
                    "server_name": name,
                    "discovery_job_id": job_id,
                    "cache_path": str(cache_path),
                    "cache_entry": entry.model_dump(mode="json"),
                    "profiles": {
                        profile_name: {
                            "catalog_revision": catalog.revision,
                            "virtual_tools": sorted(catalog.tools),
                            "registration_virtual_tools": sorted(
                                alias
                                for alias, tool in catalog.tools.items()
                                if (route := tool.routes.get(cluster)) is not None
                                and route.server_name == name
                            ),
                        }
                        for profile_name, catalog in catalogs.items()
                    },
                    "mcp_server_restart_required": False,
                    "client_action": "request tools/list again to load the refreshed schemas",
                },
                indent=2,
                default=str,
            )
        )

    cli._run_or_exit(action)
