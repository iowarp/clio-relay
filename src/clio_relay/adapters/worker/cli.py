"""The worker adapter's operator commands (RL3).

``worker_app`` holds every operator command of the worker adapter: cluster
setup and bootstrap, the endpoint worker, the owned api and session, the
relay host, the queue and the scheduler. The relay CLI mounts it as
``clio-relay worker``. Commands the relay runs on a site over ssh go through
the same group (``remote_cli`` adds the ``worker`` prefix), so the command
lines are the same on both sides.

``app`` is a root that mounts ``worker_app`` under ``worker``. It is what
``clio-relay`` runs until the cutover moves the entry point to the new
surface, which mounts ``worker_app`` itself.
"""

from __future__ import annotations

import json
import os
import re
import subprocess  # noqa: F401 -- test_cli.py monkeypatches cli.subprocess.run directly
import sys
from collections.abc import Callable
from pathlib import Path

import typer
from filelock import FileLock

import clio_relay.adapters.worker.cli_api as cli_api
import clio_relay.adapters.worker.cli_cluster as cli_cluster
import clio_relay.adapters.worker.cli_endpoint as cli_endpoint
import clio_relay.adapters.worker.cli_init as cli_init
import clio_relay.adapters.worker.cli_installation_receipt as cli_installation_receipt
import clio_relay.adapters.worker.cli_jarvis_runtime_authority as cli_jarvis_runtime_authority
import clio_relay.adapters.worker.cli_queue as cli_queue
import clio_relay.adapters.worker.cli_relay_host as cli_relay_host
import clio_relay.adapters.worker.cli_scheduler as cli_scheduler
import clio_relay.adapters.worker.cli_session as cli_session
import clio_relay.adapters.worker.cli_support as cli_support
import clio_relay.adapters.worker.cli_worker as cli_worker
import clio_relay.adapters.worker.site_server as site_server
import clio_relay.remote_cli as remote_cli
from clio_relay.adapters.worker import (
    cli_cluster_deploy,  # noqa: F401 -- registers cluster_app's deployment commands
    cli_queue_maintenance,  # noqa: F401 -- registers queue_app's maintenance commands
    cli_session_attach,  # noqa: F401 -- registers session_app's attach/reconnect commands  # pyright: ignore[reportUnusedImport]
    cli_session_owned,  # noqa: F401 -- registers session_app's owned-session commands
    cli_session_start,  # noqa: F401 -- registers session_app's start command
    cli_session_teardown,  # noqa: F401 -- registers session_app's teardown command
)
from clio_relay.adapters.worker.storage_runtime import StorageManagedQueue
from clio_relay.adapters.worker.worker_concurrency import parse_kind_concurrency_options
from clio_relay.cluster_config import (
    ClusterDefinition,
    # test_cli.py reaches it as
    # `cli.release_private_configuration_windows_parent_guard` to undo a
    # Windows configuration-parent guard a fixture acquired directly.
    release_private_configuration_windows_parent_guard,  # noqa: F401
)
from clio_relay.config import RelaySettings  # noqa: F401 -- test_cli.py uses cli.RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.installation import (
    InstallReceipt,  # noqa: F401 -- test_cli_api.py builds cli.InstallReceipt directly
)
from clio_relay.models import JobKind
from clio_relay.owner_session_admission import (
    desktop_owner_session_admission_id as _desktop_owner_session_admission_id,  # noqa: F401
)
from clio_relay.owner_session_admission import (
    owner_session_transition_lock,
)
from clio_relay.pagination import (
    MAX_RESPONSE_PAGE_RECORDS,  # noqa: F401 -- test_cli.py paginates via cli.MAX_RESPONSE_PAGE_RECORDS
)
from clio_relay.process_containment import consume_broker_child_environment
from clio_relay.validation_report import (
    LiveValidationReport,
    redact_sensitive_values,
)

worker_app = typer.Typer(
    no_args_is_help=True,
    help="Set up and run the worker adapter: clusters, endpoint, session, queue, scheduler.",
)

worker_app.add_typer(cli_endpoint.endpoint_app, name="endpoint")
worker_app.add_typer(cli_relay_host.relay_host_app, name="relay-host")
worker_app.add_typer(cli_cluster.cluster_app, name="cluster")
worker_app.add_typer(cli_api.api_app, name="api")
worker_app.add_typer(cli_session.session_app, name="session")
worker_app.add_typer(cli_queue.queue_app, name="queue")
worker_app.add_typer(cli_scheduler.scheduler_app, name="scheduler")

worker_app.command("status")(cli_worker.worker_status_command)
worker_app.command()(cli_init.init)
worker_app.command("install-frp")(cli_init.install_frp)
worker_app.command("installation-write-receipt")(
    cli_installation_receipt.installation_write_receipt
)
worker_app.command("installation-info")(cli_installation_receipt.show_installation_info)
worker_app.command("bootstrap-inspect", hidden=True)(cli_installation_receipt.bootstrap_inspect)
worker_app.command("jarvis-runtime-authority", hidden=True)(
    cli_jarvis_runtime_authority.jarvis_runtime_authority
)
worker_app.command("site-serve", hidden=True)(site_server.site_serve_command)


@worker_app.callback()
def main() -> None:
    """Set up and run the worker adapter."""
    consume_broker_child_environment()


app = typer.Typer(no_args_is_help=True)
app.add_typer(worker_app, name="worker")


@app.callback()
def _root() -> None:  # pyright: ignore[reportUnusedFunction]
    """Run clio-relay commands."""


def _none_if_blank(value: str | None) -> str | None:
    if value is None or value.strip() == "":
        return None
    return value


def _parse_age_seconds(value: str) -> int:
    """Parse a positive operator age threshold such as ``30m`` or ``2h``."""
    match = re.fullmatch(r"(?P<count>[1-9][0-9]*)(?P<unit>[smhd]?)", value.strip().lower())
    if match is None:
        raise typer.BadParameter("age threshold must be a positive integer with s, m, h, or d")
    multiplier = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86_400}[match.group("unit")]
    return int(match.group("count")) * multiplier


def _public_json(value: object) -> str:
    """Serialize operator-facing JSON without exposing durable credentials."""
    return json.dumps(redact_sensitive_values(value), indent=2)


# #231 cli.py decomposition, shared-plumbing relocation pass: this symbol's
# real body moved to cli_support.py -- see that module's own docstring. A
# thin forwarder, not a bare object re-binding, for the same F3/F4 reason
# every other forwarder below carries: re-reading `cli_support.<symbol>` on
# every call keeps both `monkeypatch.setattr(cli_support, ...)` and the
# pre-existing `monkeypatch.setattr(cli, ...)` patch points effective.
def _managed_queue_from_env() -> StorageManagedQueue:
    return cli_support._managed_queue_from_env()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


# R8(ii) interim seam (docs/design/relay-architecture-2026-08.md §4.1/§5):
# this symbol's real body moved to cli_support.py -- the doc's cli_support.py
# row for cli.py's shared-plumbing fan-out. cli.py keeps it defined under its
# original name at its original definition site so its own ~200 existing
# bare-name call sites keep working unchanged; migrating cli.py's other 15
# sub-apps onto `cli_support.X(...)` directly is separate, unsequenced future
# work, not something this slice's `relay-host` extraction should absorb as
# a side effect.
#
# F3/F4 fix (iowarp/clio-relay#231 R8(ii) review): this is a thin forwarder,
# not a bare object re-binding (`_write_failed_acceptance_report =
# cli_support._write_failed_acceptance_report`). The bare form captures the
# owner's function object at import time, so
# `monkeypatch.setattr(cli_support, "_write_failed_acceptance_report", ...)`
# never reaches callers here -- a silent no-op patch that only
# `monkeypatch.setattr(cli, "_write_failed_acceptance_report", ...)` could
# see. Re-reading `cli_support.<symbol>` on every call restores both patch
# directions.
def _write_failed_acceptance_report(
    *,
    path: Path,
    scenario: str,
    cluster: str,
    check_id: str,
    summary: str,
    error: BaseException,
    launcher: str | None,
    install_source: str | None,
    artifact: Path | None,
    partial_report: LiveValidationReport | None = None,
) -> None:
    cli_support._write_failed_acceptance_report(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        path=path,
        scenario=scenario,
        cluster=cluster,
        check_id=check_id,
        summary=summary,
        error=error,
        launcher=launcher,
        install_source=install_source,
        artifact=artifact,
        partial_report=partial_report,
    )


def _echo_lines(lines: list[str]) -> None:
    for line in lines:
        typer.echo(_console_safe_text(line))


def _console_safe_text(value: str) -> str:
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    return value.encode(encoding, errors="replace").decode(encoding, errors="replace")


def _try_remote_cluster_passthrough(cluster: str | None, args: list[str]) -> bool:
    if cluster is None:
        return False
    if os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower() == "local":
        return False
    definition = _require_cluster(cluster)
    if not remote_cli.should_execute_on_cluster(definition):
        return False
    _run_remote_or_exit(definition, args)
    return True


def _run_remote_or_exit(
    definition: ClusterDefinition,
    args: list[str],
    *,
    cluster_registry_path: str | None = None,
) -> None:
    if cluster_registry_path is None:
        _run_or_exit(
            lambda: typer.echo(
                _console_safe_text(remote_cli.run_remote_clio(definition, args)),
                nl=False,
            )
        )
        return
    _run_or_exit(
        lambda: typer.echo(
            _console_safe_text(
                remote_cli.run_remote_clio(
                    definition,
                    args,
                    cluster_registry_path=cluster_registry_path,
                )
            ),
            nl=False,
        )
    )


# F3/F4 fix (iowarp/clio-relay#231 R8(ii) review): thin forwarder, not a bare
# object re-binding -- see the longer note beside
# `_write_failed_acceptance_report`'s forwarder above. Re-reading
# `cli_support._require_cluster` on every call keeps both
# `monkeypatch.setattr(cli_support, "_require_cluster", ...)` and
# `monkeypatch.setattr(cli, "_require_cluster", ...)` effective.
def _require_cluster(cluster: str) -> ClusterDefinition:
    return cli_support._require_cluster(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster
    )


def _session_transition_lock(*, cluster: str, session_id: str) -> FileLock:
    """Return the shared cluster-scoped owner-session transition lock."""
    return owner_session_transition_lock(cluster=cluster, session_id=session_id)


def _require_durable_session_identity(value: str, *, field: str) -> str:
    """Validate a session identity before it reaches local or remote persistence."""
    try:
        return validate_durable_record_id(value)
    except ValueError as error:
        raise RelayError(f"invalid {field}: {error}") from error


def _kind_concurrency_options(
    items: list[str] | None,
    *,
    param_hint: str = "--kind-concurrency",
) -> dict[JobKind, int]:
    try:
        return parse_kind_concurrency_options(items)
    except ConfigurationError as exc:
        raise typer.BadParameter(
            str(exc),
            param_hint=param_hint,
        ) from exc


def _require_frp_server_addr(  # pyright: ignore[reportUnusedFunction]
    server_addr: str, cluster: str
) -> str:
    if server_addr.strip():
        return server_addr
    raise ConfigurationError(
        f"frp server address is not configured for cluster {cluster}; "
        "set it with `clio-relay worker cluster add --frp-server-addr ...`"
    )


# F3/F4 fix (iowarp/clio-relay#231 R8(ii) review): thin forwarder, not a bare
# object re-binding -- see the longer note beside
# `_write_failed_acceptance_report`'s forwarder above. Re-reading
# `cli_support._resolve_env_secret` on every call keeps both
# `monkeypatch.setattr(cli_support, "_resolve_env_secret", ...)` and
# `monkeypatch.setattr(cli, "_resolve_env_secret", ...)` effective.
def _resolve_env_secret(value: str | None, env_name: str, label: str) -> str:
    return cli_support._resolve_env_secret(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        value, env_name, label
    )


# F3/F4 fix (iowarp/clio-relay#231 R8(ii) review): thin forwarder, not a bare
# object re-binding -- see the longer note beside
# `_write_failed_acceptance_report`'s forwarder above. Re-reading
# `cli_support._run_or_exit` on every call keeps both
# `monkeypatch.setattr(cli_support, "_run_or_exit", ...)` and
# `monkeypatch.setattr(cli, "_run_or_exit", ...)` effective.
def _run_or_exit(action: Callable[[], None]) -> None:
    cli_support._run_or_exit(action)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
