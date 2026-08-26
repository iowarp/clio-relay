"""The ``relay-host`` command group (iowarp/clio-relay#231, R8(ii)).

``docs/design/relay-architecture-2026-08.md`` §5's ``relay-host`` command-
module row names this as the first real command-module extraction off
``cli.py``: the seven original ``relay_host_app`` commands (frps/frpc config
rendering, the live frpc login check, and the three transport acceptance
probes) move out of the 19k-line monolith into their own capped module, per
ground rule 2 (§2) -- ``cli.py`` parses and renders only; this module does
the same for its own commands and nothing more. clio-relay#279 adds three
more (``install-proxy``/``teardown-proxy``/``proxy-status``): the cluster-
side frpc proxy bring-up path, the missing product half of #188's TCP
transport modes -- see ``frpc_unit.py``'s module docstring for the design.

**Domain logic stays where it lives.** The original seven commands delegate
to ``relay_host.py`` (frp TOML rendering: ``render_frps_config``,
``render_frpc_config``, ``render_frpc_visitor_config``) and
``transport_probe.py`` (the three live probes: ``run_frp_http_probe``,
``run_frp_direct_http_probe``, ``run_ssh_forward_http_probe``) exactly as
they did inside ``cli.py`` -- both are already-correct owner modules, so the
move only changes which file holds the thin command wrapper, not who does
the work. ``transport_probe`` is imported module-attribute style
(``import clio_relay.transport_probe as transport_probe``) because it is one
of R8(i)'s audited patch-seam collaborators (``tests/test_cli_patch_seam.py``);
``relay_host``'s renderers are pure, deterministic, and never monkeypatched,
so they are imported directly. The three #279 commands delegate to
``frpc_proxy_bringup.py`` (the ssh-executing bring-up/teardown/status
functions), imported the SAME module-attribute way as ``transport_probe``
for the same reason: tests replace its ssh-dialing function with a fake
rather than ever dialing anywhere.

**What does NOT move here.** Two categories of logic this group's commands
call are NOT owned by any of the three modules above, and this slice does
not relocate them:

- ``_run_transport_validation``/``_run_frpc_connection_validation``/
  ``_require_frp_server_addr``/``_echo_lines`` (the validation-report
  bookkeeping, the cluster-config guard, and the report-line printer) still
  live in ``cli.py`` itself. §5's target map gives ``_run_transport_validation``
  its own future, unsequenced row (folding into ``frp_transport.py`` once
  that becomes its rightful owner); moving its body here now would just
  relocate §2 ground rule 2's violation from one file to another rather than
  fix it. ``_require_frp_server_addr`` and ``_run_frpc_connection_validation``
  have no other callers, but they share ``_attach_verified_remote_worker`` (a
  `cli.py` helper also used by session teardown, well outside this group) as
  a private collaborator, so splitting them out on their own would just add
  a second cross-module hop for no structural gain.
- The cross-cutting helpers named in §4.1's fan-out table (``_run_or_exit``,
  ``_require_cluster``, ``_write_failed_acceptance_report``,
  ``_resolve_env_secret``, ``_acceptance_report_command``, plus
  ``default_report_path``) moved to ``cli_support.py`` in this same slice
  (see that module's docstring). This group applies the marker decorator,
  ``@cli_support._acceptance_report_command``, straight from its true owner
  -- a real attribute read that fires at this module's own import time, so it
  cannot go through ``cli.py`` without recreating the cycle this module's
  import discipline (below) exists to avoid. The other four reach
  ``cli.py``'s thin forwarders at call time, same as the ``cli.py``-owned
  helpers above, and ``default_report_path`` is imported straight from its
  true owner, ``validation_report.py``, since it never moved.

**The import-cycle discipline.** ``cli.py`` imports this module to register
``relay_host_app``, and this module reaches back into ``cli.py`` for the
collaborators named above -- a real cycle between the two files. It resolves
cleanly because ``cli`` is never bound as a module-level name here at all
(runtime or ``TYPE_CHECKING`` -- see the comment above ``relay_host_app``
for why not even the latter earns its keep in this file): it is imported
function-locally, as the first statement of each of this module's seven
command bodies (``import clio_relay.cli as cli``, then ``cli.<symbol>(...)``),
which defers the real import until a command actually runs and ``cli.py``
has finished loading. There are **no** module-level attribute reads on
``cli`` anywhere in this file -- the marker decorator above reads
``cli_support`` instead, for exactly this reason. Never use a bare
``from clio_relay.cli import <symbol>``, which would silently un-patch every
test targeting the owner and break the moment a future slice moves the
symbol again (the coupling ``tests/test_cli_patch_seam.py`` polices, R8(i)).
``tests/test_cli_relay_host.py``'s subprocess regression test is the guard
for this discipline: in a fresh interpreter it asserts that
``import clio_relay.cli_relay_host`` succeeds first, that
``import clio_relay.cli`` succeeds first, and that ``python -m
clio_relay.cli`` exits 0.
"""

# Every `cli.<symbol>` reference below is intentional cross-module access to
# a name `cli.py` keeps underscore-prefixed on purpose (see this module's own
# docstring): the R8(i) patch seam requires calling collaborators through the
# module they are looked up on, and cli.py's re-exports for the R8(ii)
# cli_support.py split (`_run_or_exit`, `_require_cluster`, ...) keep their
# original names for that exact reason. `http_api.py` sets the equivalent
# `reportUnusedFunction=false` for its own decorator-registered-only
# handlers; this is the same "pyright can't see the real caller" shape, one
# rule over.
# Scope (F6, iowarp/clio-relay#231 R8(ii) review): covers only the audited
# `cli.<seam>` private-attribute reads inside the seven command bodies below.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

import clio_relay.cli_support as cli_support
import clio_relay.frpc_proxy_bringup as frpc_proxy_bringup
import clio_relay.transport_probe as transport_probe
from clio_relay.config import RelaySettings
from clio_relay.frp_proxy_naming import canonical_proxy_name
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpsConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
    render_frps_config,
)
from clio_relay.validation_report import default_report_path

# `cli` (`clio_relay.cli`) is deliberately NOT imported at module level, not
# even under `TYPE_CHECKING`: nothing in this file uses `cli` in a type
# annotation position, only as `cli.<symbol>(...)` expressions inside
# function bodies -- each of the seven command bodies below re-imports it
# locally as their first statement (`import clio_relay.cli as cli`), which
# both binds it at runtime and gives pyright everything it needs within that
# function's scope. A module-level `TYPE_CHECKING` import was tried and
# measured dead weight here (both ruff `reportUnusedImport`/F401 and pyright
# `reportUnusedImport` flag it, since there is no annotation consumer to
# keep it alive) -- see the module docstring for the discipline this
# supports.
relay_host_app = typer.Typer(no_args_is_help=True)


@relay_host_app.command("render-frps-config")
def render_frps(
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to CLIO_RELAY_FRP_TOKEN."),
    ] = None,
    bind_port: Annotated[int, typer.Option(help="frps bind port.")] = 7000,
    transport_protocol: Annotated[
        FrpTransportProtocol,
        typer.Option(help="frpc-to-frps transport protocol."),
    ] = FrpTransportProtocol.WSS,
    dashboard_port: Annotated[
        int | None,
        typer.Option(help="Optional frps dashboard port."),
    ] = None,
) -> None:
    """Render an frps config with no relay application state."""
    import clio_relay.cli as cli

    cli._run_or_exit(
        lambda: typer.echo(
            render_frps_config(
                FrpsConfig(
                    bind_port=bind_port,
                    token=cli._resolve_env_secret(token, "CLIO_RELAY_FRP_TOKEN", "frp token"),
                    transport_protocol=transport_protocol,
                    dashboard_port=dashboard_port,
                )
            )
        )
    )


@relay_host_app.command("render-frpc-config")
def render_frpc(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_port: Annotated[int, typer.Option(help="Local relay endpoint port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    proxy_name: Annotated[
        str | None,
        typer.Option(
            help=(
                "stcp proxy name. Defaults to the canonical <cluster>-owned-session "
                "form (clio-relay#279) -- the SAME name frp_transport.py's desktop "
                "brokered_tcp/udp_rendezvous transports and `relay-host install-proxy` "
                "resolve for this cluster. Overriding this without also overriding the "
                "desktop visitor's --server-name reintroduces the relay-stcp mismatch "
                "trap this default used to fall into."
            )
        ),
    ] = None,
) -> None:
    """Render an frpc config using the cluster's configured frp transport."""
    import clio_relay.cli as cli

    def action() -> None:
        definition = cli._require_cluster(cluster)
        transport = definition.frp_transport
        server_addr = cli._require_frp_server_addr(transport.server_addr, cluster)
        resolved_proxy_name = proxy_name or canonical_proxy_name(definition, cluster=cluster)
        typer.echo(
            render_frpc_config(
                FrpcConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=cli._resolve_env_secret(token, transport.token_env, "frp token"),
                    transport_protocol=FrpTransportProtocol(transport.protocol),
                    proxy_name=resolved_proxy_name,
                    local_port=local_port,
                    secret_key=cli._resolve_env_secret(
                        secret_key,
                        transport.stcp_secret_env,
                        "stcp secret",
                    ),
                )
            )
        )

    cli._run_or_exit(action)


@relay_host_app.command("test-frpc-connection")
@cli_support._acceptance_report_command
def test_frpc(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_port: Annotated[int, typer.Option(help="Local relay endpoint port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    proxy_name: Annotated[str, typer.Option(help="stcp proxy name.")] = "relay-stcp-live-check",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds frpc must stay connected before success."),
    ] = 10.0,
    validation_report: Annotated[
        Path | None,
        typer.Option(
            help="Canonical frpc connection validation JSON path. Defaults under .clio-relay."
        ),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run a live frpc login check and persist canonical success or failure evidence."""
    import clio_relay.cli as cli
    import clio_relay.cli_transport_validation as cli_transport_validation

    canonical_report_path = validation_report or default_report_path(cluster)

    try:
        settings = RelaySettings.from_env()
        definition = cli._require_cluster(cluster)
        transport = definition.frp_transport
        server_addr = cli._require_frp_server_addr(transport.server_addr, cluster)
        config = FrpcConfig(
            server_addr=server_addr,
            server_port=transport.server_port,
            token=cli._resolve_env_secret(token, transport.token_env, "frp token"),
            transport_protocol=FrpTransportProtocol(transport.protocol),
            proxy_name=proxy_name,
            local_port=local_port,
            secret_key=cli._resolve_env_secret(
                secret_key,
                transport.stcp_secret_env,
                "stcp secret",
            ),
        )
    except BaseException as exc:
        cli._write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.frpc-connection.preflight",
            summary="validate frpc connection acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise

    def action() -> None:
        cli._echo_lines(
            cli_transport_validation._run_frpc_connection_validation(
                cluster=cluster,
                proxy_name=proxy_name,
                frpc_bin=settings.frpc_bin,
                config=config,
                timeout_seconds=timeout_seconds,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
            )
        )

    cli._run_or_exit(action)


@relay_host_app.command("render-frpc-visitor-config")
def render_frpc_visitor(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    bind_port: Annotated[int, typer.Option(help="Local desktop visitor bind port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    server_name: Annotated[str, typer.Option(help="Cluster-side stcp proxy name.")] = "relay-stcp",
    visitor_name: Annotated[
        str,
        typer.Option(help="Desktop-side stcp visitor name."),
    ] = "relay-stcp-visitor",
    bind_addr: Annotated[
        str,
        typer.Option(help="Local desktop visitor bind address."),
    ] = "127.0.0.1",
) -> None:
    """Render a desktop-side frpc STCP visitor config."""
    import clio_relay.cli as cli

    def action() -> None:
        definition = cli._require_cluster(cluster)
        transport = definition.frp_transport
        server_addr = cli._require_frp_server_addr(transport.server_addr, cluster)
        typer.echo(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=cli._resolve_env_secret(token, transport.token_env, "frp token"),
                    transport_protocol=FrpTransportProtocol(transport.protocol),
                    visitor_name=visitor_name,
                    server_name=server_name,
                    bind_addr=bind_addr,
                    bind_port=bind_port,
                    secret_key=cli._resolve_env_secret(
                        secret_key,
                        transport.stcp_secret_env,
                        "stcp secret",
                    ),
                )
            )
        )

    cli._run_or_exit(action)


@relay_host_app.command("test-http-transport")
@cli_support._acceptance_report_command
def test_http_transport(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_bind_port: Annotated[int, typer.Option(help="Local desktop visitor bind port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    proxy_name: Annotated[str, typer.Option(help="stcp proxy/server name.")] = "relay-http",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds to wait for healthz through the transport."),
    ] = 30.0,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical transport validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run an end-to-end HTTP health check through frp STCP."""
    import clio_relay.cli as cli
    import clio_relay.cli_transport_validation as cli_transport_validation

    canonical_report_path = validation_report or default_report_path(cluster)
    try:
        settings = RelaySettings.from_env()
        definition = cli._require_cluster(cluster)
    except BaseException as exc:
        cli._write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.preflight",
            summary="validate HTTP transport acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    cli._run_or_exit(
        lambda: cli._echo_lines(
            cli_transport_validation._run_transport_validation(
                cluster=cluster,
                transport_mode="frp-relay",
                resource_id=proxy_name,
                resource_role="frp_stcp_probe",
                retain_remote_session=False,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
                probe=lambda: transport_probe.run_frp_http_probe(
                    cluster=cluster,
                    definition=definition,
                    frpc_bin=settings.frpc_bin,
                    token=cli._resolve_env_secret(
                        token,
                        definition.frp_transport.token_env,
                        "frp token",
                    ),
                    secret_key=cli._resolve_env_secret(
                        secret_key,
                        definition.frp_transport.stcp_secret_env,
                        "stcp secret",
                    ),
                    local_bind_port=local_bind_port,
                    remote_api_port=remote_api_port,
                    proxy_name=proxy_name,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                ),
            )
        )
    )


@relay_host_app.command("test-direct-transport")
@cli_support._acceptance_report_command
def test_direct_transport(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_bind_port: Annotated[int, typer.Option(help="Local desktop visitor bind port.")],
    token: Annotated[
        str | None,
        typer.Option(help="frp authentication token. Defaults to cluster token_env."),
    ] = None,
    secret_key: Annotated[
        str | None,
        typer.Option(help="stcp/xtcp shared secret. Defaults to cluster stcp_secret_env."),
    ] = None,
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    proxy_name: Annotated[
        str,
        typer.Option(help="xtcp proxy/server name."),
    ] = "relay-http-direct",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds to wait for healthz through direct transport."),
    ] = 30.0,
    allow_stcp_fallback: Annotated[
        bool,
        typer.Option(
            "--allow-stcp-fallback/--no-allow-stcp-fallback",
            help="Allow fallback to STCP if XTCP fails.",
        ),
    ] = False,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical transport validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run an end-to-end HTTP health check through frp XTCP direct transport."""
    import clio_relay.cli as cli
    import clio_relay.cli_transport_validation as cli_transport_validation

    canonical_report_path = validation_report or default_report_path(cluster)
    try:
        settings = RelaySettings.from_env()
        definition = cli._require_cluster(cluster)
    except BaseException as exc:
        cli._write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.preflight",
            summary="validate direct transport acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    cli._run_or_exit(
        lambda: cli._echo_lines(
            cli_transport_validation._run_transport_validation(
                cluster=cluster,
                transport_mode="frp-direct",
                resource_id=proxy_name,
                resource_role="frp_xtcp_probe",
                retain_remote_session=False,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
                probe=lambda: transport_probe.run_frp_direct_http_probe(
                    cluster=cluster,
                    definition=definition,
                    frpc_bin=settings.frpc_bin,
                    token=cli._resolve_env_secret(
                        token,
                        definition.frp_transport.token_env,
                        "frp token",
                    ),
                    secret_key=cli._resolve_env_secret(
                        secret_key,
                        definition.frp_transport.stcp_secret_env,
                        "stcp/xtcp secret",
                    ),
                    local_bind_port=local_bind_port,
                    remote_api_port=remote_api_port,
                    proxy_name=proxy_name,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                    allow_stcp_fallback=allow_stcp_fallback,
                ),
            )
        )
    )


@relay_host_app.command("test-ssh-transport")
@cli_support._acceptance_report_command
def test_ssh_transport(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    local_bind_port: Annotated[int, typer.Option(help="Local desktop SSH-forward bind port.")],
    remote_api_port: Annotated[int, typer.Option(help="Remote cluster API port.")] = 8765,
    session_id: Annotated[
        str,
        typer.Option(help="Owned remote relay session id for this probe."),
    ] = "relay-ssh-forward-test",
    timeout_seconds: Annotated[
        float,
        typer.Option(help="Seconds to wait for healthz through the SSH forward."),
    ] = 30.0,
    detach_remote: Annotated[
        bool,
        typer.Option(
            "--detach-remote/--teardown-remote",
            help="Leave the remote API session running after the local SSH probe exits.",
        ),
    ] = False,
    validation_report: Annotated[
        Path | None,
        typer.Option(help="Canonical transport validation JSON path. Defaults under .clio-relay."),
    ] = None,
    validation_launcher: Annotated[
        str | None,
        typer.Option(help="Launcher evidence, such as uv-tool."),
    ] = None,
    validation_install_source: Annotated[
        str | None,
        typer.Option(help="Explicit kind:reference install evidence."),
    ] = None,
    validation_artifact: Annotated[
        Path | None,
        typer.Option(
            help="Optional wheel whose SHA-256 is recorded in transport evidence.",
            exists=True,
            dir_okay=False,
        ),
    ] = None,
) -> None:
    """Run an end-to-end HTTP health check through SSH local port forwarding."""
    import clio_relay.cli as cli
    import clio_relay.cli_transport_validation as cli_transport_validation

    canonical_report_path = validation_report or default_report_path(cluster)
    try:
        settings = RelaySettings.from_env()
        definition = cli._require_cluster(cluster)
    except BaseException as exc:
        cli._write_failed_acceptance_report(
            path=canonical_report_path,
            scenario="transport",
            cluster=cluster,
            check_id="transport.preflight",
            summary="validate SSH transport acceptance inputs",
            error=exc,
            launcher=validation_launcher,
            install_source=validation_install_source,
            artifact=validation_artifact,
        )
        raise
    cli._run_or_exit(
        lambda: cli._echo_lines(
            cli_transport_validation._run_transport_validation(
                cluster=cluster,
                transport_mode="ssh-forward",
                resource_id=session_id,
                resource_role="ssh_forward_probe",
                retain_remote_session=detach_remote,
                validation_report=canonical_report_path,
                validation_launcher=validation_launcher,
                validation_install_source=validation_install_source,
                validation_artifact=validation_artifact,
                probe=lambda: transport_probe.run_ssh_forward_http_probe(
                    cluster=cluster,
                    definition=definition,
                    local_bind_port=local_bind_port,
                    remote_api_port=remote_api_port,
                    session_id=session_id,
                    api_token=settings.api_token,
                    timeout_seconds=timeout_seconds,
                    detach_remote=detach_remote,
                ),
            )
        )
    )


# --- clio-relay#279: cluster-side frpc proxy bring-up ----------------------
#
# The missing product half of #188's TCP transport modes: `relay_host.py`'s
# renderers produce the frpc proxy TOML, but nothing installed/ran/kept it
# alive on a cluster. These three commands do that, riding their own single
# ssh pass each (never a second dial) via `frpc_proxy_bringup.py`.


@relay_host_app.command("install-proxy")
def install_proxy(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
    remote_port: Annotated[
        int,
        typer.Option(help="Cluster-local port the frpc proxy exposes (the relay API port)."),
    ] = frpc_proxy_bringup.DEFAULT_FRPC_PROXY_REMOTE_PORT,
) -> None:
    """Render, install, enable, and start the cluster-side frpc proxy unit.

    One ssh pass: writes the proxy TOML (secrets env-templated, never
    inline), the 0600 secrets env file, and the systemd user unit, then
    ``daemon-reload``/``enable``/starts and verifies it is active before
    returning the typed bring-up receipt.
    """
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    cli._run_or_exit(
        lambda: typer.echo(
            frpc_proxy_bringup.install_frpc_proxy_over_ssh(
                cluster=cluster,
                definition=definition,
                ssh_host=ssh_host or definition.ssh_host,
                remote_port=remote_port,
            ).model_dump_json(indent=2)
        )
    )


@relay_host_app.command("teardown-proxy")
def teardown_proxy(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
) -> None:
    """Disable, stop, and remove the cluster-side frpc proxy unit, TOML, and env file.

    One ssh pass; self-cleaning regardless of whether the unit was already
    installed (a not-installed unit is a no-op, not an error).
    """
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    cli._run_or_exit(
        lambda: typer.echo(
            frpc_proxy_bringup.teardown_frpc_proxy_over_ssh(
                cluster=cluster,
                ssh_host=ssh_host or definition.ssh_host,
            ).model_dump_json(indent=2)
        )
    )


@relay_host_app.command("proxy-status")
def proxy_status(
    cluster: Annotated[str, typer.Option(help="Configured cluster name.")],
    ssh_host: Annotated[
        str | None,
        typer.Option(help="Override SSH host alias for this run."),
    ] = None,
) -> None:
    """Report the cluster-side frpc proxy unit's diagnosable status.

    One read-only ssh pass: ``systemctl --user`` load/active/enabled state
    plus the unit's last journal lines, classified into a typed status
    document -- frpc down, frps unreachable, and a rejected token all
    surface here as "installed and enabled but inactive", with the journal
    tail carrying the actual cause.
    """
    import clio_relay.cli as cli

    definition = cli._require_cluster(cluster)
    cli._run_or_exit(
        lambda: typer.echo(
            frpc_proxy_bringup.frpc_proxy_status_over_ssh(
                cluster=cluster,
                ssh_host=ssh_host or definition.ssh_host,
            ).model_dump_json(indent=2)
        )
    )
