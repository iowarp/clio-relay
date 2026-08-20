"""Command-line interface for clio-relay."""

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

import clio_relay.cli_agent as cli_agent
import clio_relay.cli_api as cli_api
import clio_relay.cli_cluster as cli_cluster
import clio_relay.cli_cluster_deploy as cli_cluster_deploy  # noqa: F401 -- registers cluster_app's deployment commands
import clio_relay.cli_diagnostics as cli_diagnostics
import clio_relay.cli_endpoint as cli_endpoint
import clio_relay.cli_gateway as cli_gateway
import clio_relay.cli_gateway_runtime as cli_gateway_runtime  # noqa: F401 -- registers gateway_app's runtime commands
import clio_relay.cli_init as cli_init
import clio_relay.cli_installation_receipt as cli_installation_receipt
import clio_relay.cli_jarvis_mcp as cli_jarvis_mcp
import clio_relay.cli_jarvis_mcp_validate as cli_jarvis_mcp_validate
import clio_relay.cli_job as cli_job
import clio_relay.cli_job_records as cli_job_records  # noqa: F401 -- registers job_app's records commands
import clio_relay.cli_monitor as cli_monitor
import clio_relay.cli_queue as cli_queue
import clio_relay.cli_queue_maintenance as cli_queue_maintenance  # noqa: F401 -- registers queue_app's maintenance commands
import clio_relay.cli_relay_host as cli_relay_host
import clio_relay.cli_release as cli_release
import clio_relay.cli_remote_mcp as cli_remote_mcp
import clio_relay.cli_remote_mcp_validate as cli_remote_mcp_validate  # noqa: F401 -- registers remote_mcp_app's validate command
import clio_relay.cli_scheduler as cli_scheduler
import clio_relay.cli_session as cli_session
import clio_relay.cli_session_owned as cli_session_owned  # noqa: F401 -- registers session_app's owned-session commands
import clio_relay.cli_session_start as cli_session_start  # noqa: F401 -- registers session_app's start command
import clio_relay.cli_session_teardown as cli_session_teardown  # noqa: F401 -- registers session_app's teardown command
import clio_relay.cli_storage as cli_storage
import clio_relay.cli_support as cli_support
import clio_relay.cli_worker as cli_worker
import clio_relay.remote_cli as remote_cli
from clio_relay.cluster_config import (
    ClusterDefinition,
    # cli.py has no internal caller of its own; kept here only because
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
from clio_relay.jarvis_mcp import (
    # test_cli.py builds a fake binding via
    # cli.jarvis_mcp_artifact_binding_from_entry
    jarvis_mcp_artifact_binding_from_entry,  # noqa: F401
)
from clio_relay.mcp_stdio_validation import (
    PackagedMcpStdioSession,  # noqa: F401 -- test_cli.py types fakes as cli.PackagedMcpStdioSession
)
from clio_relay.models import (
    ArtifactUse,
    JobKind,
    RelayJob,
)
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
from clio_relay.remote_mcp import (
    # cli_cluster_deploy.py and its tests reach both as
    # cli.RemoteMcpSchemaCache / cli.default_remote_mcp_cache_path.
    RemoteMcpSchemaCache,  # noqa: F401
    default_remote_mcp_cache_path,  # noqa: F401
)
from clio_relay.storage_runtime import StorageAdmissionError, StorageManagedQueue
from clio_relay.validation_report import (
    LiveValidationReport,
    load_validation_report,
    redact_sensitive_values,
)
from clio_relay.worker_concurrency import parse_kind_concurrency_options

# R8(ii) interim seam (docs/design/relay-architecture-2026-08.md §4.1/§5):
# these two symbols' real bodies moved to cli_support.py -- see the longer
# note beside `_write_failed_acceptance_report`'s re-export below. Bound
# here, under this exact name, purely for cli.py's own ~15 other command
# groups' `@_acceptance_report_command` decorator applications (a real
# attribute lookup evaluated at each of *those* def sites' module-load time,
# so this must be defined before the first one runs). `cli_relay_host.py`
# does NOT reach this through `cli.py` -- its own four commands apply
# `@cli_support._acceptance_report_command` straight from the owner (see
# that module's docstring), specifically to avoid the import-cycle hazard
# a `cli.<symbol>` module-level decorator read would create.
_ACCEPTANCE_REPORT_COMMAND_ATTRIBUTE = (
    cli_support._ACCEPTANCE_REPORT_COMMAND_ATTRIBUTE  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_acceptance_report_command = (
    cli_support._acceptance_report_command  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


app = typer.Typer(no_args_is_help=True)

app.add_typer(cli_endpoint.endpoint_app, name="endpoint")
app.add_typer(cli_relay_host.relay_host_app, name="relay-host")
app.add_typer(cli_job.job_app, name="job")
app.add_typer(cli_cluster.cluster_app, name="cluster")
app.add_typer(cli_agent.agent_app, name="agent")
app.add_typer(cli_monitor.monitor_app, name="monitor")
app.add_typer(cli_api.api_app, name="api")
app.add_typer(cli_session.session_app, name="session")
app.add_typer(cli_gateway.gateway_app, name="gateway")
app.add_typer(cli_queue.queue_app, name="queue")
app.add_typer(cli_worker.worker_app, name="worker")
app.add_typer(cli_scheduler.scheduler_app, name="scheduler")
app.add_typer(cli_remote_mcp.remote_mcp_app, name="remote-mcp")
app.add_typer(cli_release.release_app, name="release")
app.add_typer(cli_storage.storage_app, name="storage")

# Flat, un-namespaced top-level commands whose bodies live in a sibling
# module (docs/design/relay-architecture-2026-08.md SS5's cli.py top-level
# command-module row): `app` is defined here, in cli.py, not owned by a
# sub-Typer the sibling module could decorate directly onto, so cli.py
# imports each sibling for its plain, fully-annotated function object and
# applies the registration itself -- one line per command, ground rule 2.
app.command("doctor")(cli_diagnostics.doctor)
app.command("live-test")(cli_diagnostics.live_test)
app.command()(cli_init.init)
app.command("install-frp")(cli_init.install_frp)
app.command("installation-write-receipt")(cli_installation_receipt.installation_write_receipt)
app.command("installation-info")(cli_installation_receipt.show_installation_info)
app.command("bootstrap-inspect", hidden=True)(cli_installation_receipt.bootstrap_inspect)
app.command("jarvis-runtime-authority", hidden=True)(cli_jarvis_mcp.jarvis_runtime_authority)
app.command("mcp-call")(cli_jarvis_mcp.mcp_call)
app.command("jarvis-mcp-call")(cli_jarvis_mcp.jarvis_mcp_call)
app.command("jarvis-mcp-refresh")(cli_jarvis_mcp.jarvis_mcp_refresh)
app.command("mcp-server")(cli_jarvis_mcp.mcp_server)
app.command("jarvis-mcp-validate")(cli_jarvis_mcp_validate.jarvis_mcp_validate)


@app.callback()
def main() -> None:
    """Run clio-relay commands."""
    consume_broker_child_environment()


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


def _submit_managed_job(job: RelayJob) -> RelayJob:
    return cli_support._submit_managed_job(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        job
    )


# F3/F4 fix (iowarp/clio-relay#231 R8(ii) review): a bare object re-binding
# (`_echo_storage_admission_error = cli_support._echo_storage_admission_error`)
# captures the owner's function object at import time, so
# `monkeypatch.setattr(cli_support, "_echo_storage_admission_error", ...)`
# never reaches this module's own two call sites -- a silent no-op patch.
# A thin forwarder re-reads `cli_support.<symbol>` on every call, so both
# `monkeypatch.setattr(cli_support, ...)` and the pre-existing
# `monkeypatch.setattr(cli, ...)` patch points bite.
def _echo_storage_admission_error(error: StorageAdmissionError) -> None:
    cli_support._echo_storage_admission_error(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        error
    )


def _json_object(value: str) -> dict[str, object]:
    return cli_support._json_object(value)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def _json_text_from_option(source: str, source_file: Path | None) -> str:
    return cli_support._json_text_from_option(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        source, source_file
    )


_JARVIS_NONTERMINAL_VALIDATION_CHECKS = frozenset(
    {
        "remote-mcp.jarvis-live-progress",
        "remote-mcp.jarvis-execution-query",
    }
)
_JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA_V1 = "clio-relay.jarvis-mcp-validation-resume.v1"
_JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA = "clio-relay.jarvis-mcp-validation-resume.v2"
_JARVIS_VALIDATION_PHASE_INTENT = "jarvis_run_intent"
_JARVIS_VALIDATION_PHASE_DISPATCH = "jarvis_run_dispatch"
_JARVIS_VALIDATION_PHASE_QUERY = "execution_query"


_MAX_JARVIS_EXECUTION_QUERY_OBSERVATIONS = 512
_JARVIS_QUERY_INTEGRITY_KEY = "relay_query_integrity"
_JARVIS_QUERY_INTEGRITY_SCHEMA = "clio-relay.jarvis-query-integrity.v1"
_JARVIS_VERIFIED_GAP_KEY = "relay_query_verified_gap"
_JARVIS_VERIFIED_GAP_SCHEMA = "clio-relay.jarvis-query-verified-gap.v1"
_JARVIS_EXECUTION_STATE_RANK = {
    "preparing": 0,
    "scripted": 1,
    "submitting": 2,
    "submitted": 3,
    "running": 4,
    "completed": 5,
    "failed": 5,
    "canceled": 5,
}
_JARVIS_PACKAGE_PROGRESS_STATES = frozenset(
    {"pending", "starting", "running", "ready", "completed", "failed", "canceled"}
)


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


def _load_current_acceptance_report(
    path: Path,
    *,
    expected_report_id: str,
) -> LiveValidationReport | None:
    """Load strict evidence only when it belongs to the current CLI invocation."""
    try:
        report = load_validation_report(path)
    except ConfigurationError:
        return None
    return report if report.report_id == expected_report_id else None


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
        "set it with `clio-relay cluster add --frp-server-addr ...`"
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


# #231 cli.py decomposition, shared-plumbing relocation pass: these four
# symbols' real bodies moved to cli_support.py -- see that module's own
# docstring. Thin forwarders, not bare object re-bindings, for the same
# F3/F4 reason every other forwarder in this file carries.
def _environment_references(items: list[str] | None) -> dict[str, str]:
    return cli_support._environment_references(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        items
    )


def _artifact_use_refs(items: list[str] | None) -> list[ArtifactUse]:
    return cli_support._artifact_use_refs(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        items
    )


def _artifact_use_cli_value(ref: ArtifactUse) -> str:
    return cli_support._artifact_use_cli_value(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ref
    )


def _artifact_use_idempotency_suffix(refs: list[ArtifactUse]) -> str:
    return cli_support._artifact_use_idempotency_suffix(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        refs
    )


# F3/F4 fix (iowarp/clio-relay#231 R8(ii) review): thin forwarder, not a bare
# object re-binding -- see the longer note beside
# `_write_failed_acceptance_report`'s forwarder above. Re-reading
# `cli_support._run_or_exit` on every call keeps both
# `monkeypatch.setattr(cli_support, "_run_or_exit", ...)` and
# `monkeypatch.setattr(cli, "_run_or_exit", ...)` effective.
def _run_or_exit(action: Callable[[], None]) -> None:
    cli_support._run_or_exit(action)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
