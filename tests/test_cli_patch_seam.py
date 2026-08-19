"""Guard the extraction-stable patch seam cli.py's collaborators use
(iowarp/clio-relay#231, R8(i)/R8(ii); doc
`docs/design/relay-architecture-2026-08.md` SS4.6/SS9).

`tests/test_cli.py` and its siblings patch these collaborator symbols on the
*owning* module (e.g. `monkeypatch.setattr(transport_probe,
"run_frp_http_probe", ...)`), not on the caller's own namespace. That only
works because the caller resolves them through a module-attribute lookup
(`import clio_relay.transport_probe as transport_probe`, then
`transport_probe.run_frp_http_probe(...)`) rather than binding the bare name
into its own namespace (`from clio_relay.transport_probe import
run_frp_http_probe`, then `run_frp_http_probe(...)`). The bare-name form is
what `docs/design/relay-architecture-2026-08.md` SS4.6 calls "the coupling
that makes extracting logic out of cli.py expensive" -- a future edit that
quietly reintroduces it doesn't just add a stylistic wart, it silently
un-patches every test that targets the owner module (the fake is never
invoked) or breaks loudly with an AttributeError once the symbol leaves the
caller's namespace during a real command-module extraction.

This test locks in the R8(i) inventory: every (owner module, symbol) pair
that slice moved off the bare-import seam, plus R8(ii)'s update -- three of
those pairs (`transport_probe`'s three probe entry points) moved caller from
`cli.py` to the new `cli_relay_host.py` when the `relay-host` command group
was extracted, per that module's own docstring. It is deliberately
independent of any future refactor's own bookkeeping -- it reads the live
AST of the guarded source files, so a regression is caught the moment it
lands, without needing anyone to remember this list exists.

**F3/F4 sabotage guard (iowarp/clio-relay#231 R8(ii) review).** The static
AST checks above prove `cli.py` and `cli_relay_host.py` never bare-import an
audited collaborator; they do not prove a *forwarder* actually forwards. The
five ``cli_support.py`` collaborators `cli.py` still exposes under their
original names (`_run_or_exit`, `_require_cluster`,
`_write_failed_acceptance_report`, `_resolve_env_secret`,
`_echo_storage_admission_error`) used to be bound as bare object re-exports
(`_run_or_exit = cli_support._run_or_exit`), which capture the owner's
function *object* at import time -- `monkeypatch.setattr(cli_support,
"_run_or_exit", fake)` after that point never reaches a caller holding the
old reference, a silent no-op. They are now thin forwarders that re-read
`cli_support.<symbol>` on every call. The tests below prove both patch
directions bite on a real command's actual call path (not a synthetic direct
call): `monkeypatch.setattr(cli_support, "<name>", fake)` and
`monkeypatch.setattr(cli, "<name>", fake)` must each change what a real
`relay-host` (or, for `_echo_storage_admission_error`, `agent run`) command
does. This is why `test_cli_patch_seam` grew from 63 parametrized cases
(R8(i), one guarded caller) to 124 (R8(ii) added `cli_relay_host` as a
second guarded caller to the negative-half AST check) to 124 + these 10
new sabotage cases in this fix.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

import clio_relay.cli_support as cli_support
from clio_relay import cli
from clio_relay.cli import app
from clio_relay.errors import ConfigurationError
from clio_relay.storage_policy import StorageDecision, StorageReason
from clio_relay.storage_runtime import StorageAdmissionError
from tests.test_cli import (
    _write_test_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

_SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "clio_relay"

# Every cli.py-family module this guard polices, plus the file it audits.
# A collaborator's `caller` field below must name one of these keys.
_GUARDED_CALLERS: dict[str, Path] = {
    "cli": _SRC_ROOT / "cli.py",
    "cli_relay_host": _SRC_ROOT / "cli_relay_host.py",
    "cli_monitor": _SRC_ROOT / "cli_monitor.py",
    "cli_agent": _SRC_ROOT / "cli_agent.py",
    "cli_api": _SRC_ROOT / "cli_api.py",
    "cli_worker": _SRC_ROOT / "cli_worker.py",
    "cli_release": _SRC_ROOT / "cli_release.py",
    "cli_endpoint": _SRC_ROOT / "cli_endpoint.py",
}

# (owner module short name, real symbol name as defined on that module,
# caller -- the `_GUARDED_CALLERS` key that must reach it by module-attribute
# import) -- the R8(i) audit inventory (docs/design/relay-architecture-
# 2026-08.md SS4.6/SS9), updated by R8(ii): the three `transport_probe`
# entries' caller moved from `cli` to `cli_relay_host` when the `relay-host`
# command group was extracted (`transport_probe.run_frp_http_probe`/
# `run_frp_direct_http_probe`/`run_ssh_forward_http_probe` are now called
# from that module's `test-http-transport`/`test-direct-transport`/
# `test-ssh-transport` commands, not from `cli.py` itself). `real symbol
# name` is the name as it exists on the owner module, not any local alias a
# caller or a test file might give it (e.g. `relay_ops`'s `job_status` was
# locally aliased to `get_job_status` in cli.py's old bare-import form -- the
# guard checks for `job_status`, since re-importing it under any alias
# reintroduces the same coupling).
AUDITED_COLLABORATORS: tuple[tuple[str, str, str], ...] = (
    ("session_lifecycle", "status_remote_session", "cli"),
    ("session_lifecycle", "teardown_remote_session", "cli"),
    ("remote_cli", "run_remote_clio", "cli"),
    ("remote_cli", "should_execute_on_cluster", "cli"),
    ("mcp_stdio_validation", "run_packaged_mcp_stdio_session", "cli"),
    ("session_lifecycle", "detach_remote_session", "cli"),
    ("installation", "installation_info", "cli"),
    ("session_lifecycle", "start_remote_session", "cli"),
    ("bootstrap", "package_source_root", "cli"),
    ("installation", "worker_runtime_info", "cli"),
    # #231 cli.py decomposition: moved caller cli -> cli_endpoint with the
    # endpoint command-group extraction (EndpointWorker's only cli.py call
    # site was endpoint_start).
    ("endpoint", "EndpointWorker", "cli_endpoint"),
    ("scheduler_providers", "provider_for_scheduler", "cli"),
    ("bootstrap", "bootstrap_cluster_over_ssh", "cli"),
    ("jarvis_mcp_validation", "build_jarvis_mcp_validation_report", "cli"),
    ("frp_check", "run_frpc_connection_check", "cli"),
    ("live_acceptance", "run_live_acceptance", "cli"),
    ("bootstrap_reconcile", "bootstrap_invocation_lock", "cli"),
    ("session_lifecycle", "finalize_remote_session_cleanup_report", "cli"),
    ("session_lifecycle", "read_remote_session_cleanup_report", "cli"),
    ("session_lifecycle", "inspect_owned_session_recovery_status", "cli"),
    # #231 cli.py decomposition: moved caller cli -> cli_release with the
    # release command-group extraction (run_local_release_validation's only
    # cli.py call site was release_validate_local).
    ("release_validation", "run_local_release_validation", "cli_release"),
    # R8(ii): moved caller cli -> cli_relay_host with the relay-host extraction.
    ("transport_probe", "run_frp_http_probe", "cli_relay_host"),
    ("core_queue", "ClioCoreQueue", "cli"),
    ("bootstrap_reconcile", "inspect_exact_bootstrap_noop", "cli"),
    ("bounded_process", "run_bounded_process", "cli"),
    ("storage_runtime", "storage_managed_queue", "cli"),
    ("service_runtime", "ServiceRuntimeSupervisor", "cli"),
    ("deployment", "install_endpoint_user_service_over_ssh", "cli"),
    # R8(ii): moved caller cli -> cli_relay_host with the relay-host extraction.
    ("transport_probe", "run_frp_direct_http_probe", "cli_relay_host"),
    ("transport_probe", "run_ssh_forward_http_probe", "cli_relay_host"),
    ("mcp_server", "load_registered_remote_mcp_catalog", "cli"),
    ("relay_ops", "wait_for_terminal", "cli"),
    ("bootstrap_reconcile", "write_bootstrap_receipt", "cli"),
    ("bootstrap_reconcile", "proven_active_generation_mismatch", "cli"),
    ("installation", "write_self_install_receipt", "cli"),
    ("relay_ops", "observe_until_terminal", "cli"),
    ("scheduler_providers", "validation_provider_for_scheduler", "cli"),
    ("cluster_config", "open_private_atomic_file", "cli"),
    ("session_lifecycle", "start_remote_session_durable", "cli"),
    # #231 cli.py decomposition: moved caller cli -> cli_api with the api
    # command-group extraction (api_start was each symbol's only cli.py call
    # site).
    ("installation", "verified_session_api_install_receipt", "cli_api"),
    ("session_lifecycle", "publish_owned_session_api_startup_receipt", "cli_api"),
    ("session_api", "submit_owned_session_job", "cli"),
    ("validation_report", "write_validation_report", "cli"),
    ("remote_cli", "remote_command_timeout", "cli"),
    ("application_profiles", "install_cluster_app_over_ssh", "cli"),
    ("owner_session_admission", "owner_session_gateway_admission", "cli"),
    ("fastmcp_server", "run_fastmcp_stdio", "cli"),
    ("fastmcp_server", "run_fastmcp_http", "cli"),
    ("endpoint_service_status", "endpoint_service_readiness_over_ssh", "cli"),
    ("deployment", "restart_endpoint_user_service_over_ssh", "cli"),
    ("relay_ops", "job_status", "cli"),
    ("cluster_config", "acquire_private_configuration_windows_parent_guard", "cli"),
    ("scheduler_providers", "allocation_connector_provider_for_scheduler", "cli"),
    ("bootstrap_acceptance", "bootstrap_reuse_acceptance_evidence", "cli"),
    ("remote_mcp", "build_remote_mcp_acceptance_report", "cli"),
    ("jarvis_mcp", "jarvis_mcp_server", "cli"),
    ("remote_cli", "remove_remote_file", "cli"),
    ("queue_validation", "run_queue_management_validation", "cli"),
    ("remote_cli", "run_remote_shell", "cli"),
    ("scheduler_validation", "run_scheduler_lifecycle_validation", "cli"),
    ("remote_cli", "write_remote_file", "cli"),
)


def _tree(caller: str) -> ast.Module:
    path = _GUARDED_CALLERS[caller]
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _bare_imported_names(tree: ast.Module, module: str) -> set[str]:
    """Every real (unaliased) name bare-imported from `clio_relay.<module>`."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == f"clio_relay.{module}":
            names.update(alias.name for alias in node.names)
    return names


def _module_attribute_imports(tree: ast.Module) -> set[str]:
    """Every `clio_relay.<module>` short name reached via `import clio_relay.X as Y`."""
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if (
                    alias.name.startswith("clio_relay.")
                    and "." not in alias.name[len("clio_relay.") :]
                ):
                    modules.add(alias.name.split(".", 1)[1])
    return modules


_AUDITED_MODULE_SYMBOLS: tuple[tuple[str, str], ...] = tuple(
    dict.fromkeys((module, symbol) for module, symbol, _caller in AUDITED_COLLABORATORS)
)


@pytest.mark.parametrize(
    "guarded_caller,module,symbol",
    [
        (guarded_caller, module, symbol)
        for guarded_caller in _GUARDED_CALLERS
        for module, symbol in _AUDITED_MODULE_SYMBOLS
    ],
)
def test_no_bare_import_of_an_audited_collaborator(
    guarded_caller: str, module: str, symbol: str
) -> None:
    """Neither `cli.py` nor `cli_relay_host.py` may bind an audited
    collaborator's real name into its own namespace via
    `from clio_relay.<module> import <symbol>` -- doing so silently
    reintroduces the bare-name-lookup coupling SS4.6 describes: a test
    patching the owner module's attribute would stop taking effect, and a
    real command-module extraction would break the call outright. Checked
    against every guarded caller file, not just the one an entry is
    currently assigned to (the positive-half test below checks that a
    *working* call path exists at the assigned caller; this half checks
    that *no* guarded file reintroduces the bare-name form, regardless of
    which one is currently responsible for calling it).
    """
    tree = _tree(guarded_caller)
    bare_names = _bare_imported_names(tree, module)
    assert symbol not in bare_names, (
        f"{_GUARDED_CALLERS[guarded_caller].name} bare-imports `{symbol}` from "
        f"clio_relay.{module} again (`from clio_relay.{module} import {symbol}`) -- "
        f"this un-patches every test that targets `{module}.{symbol}` directly. Call "
        f"it as `{module}.{symbol}(...)` through the module-attribute import instead "
        "(see docs/design/relay-architecture-2026-08.md SS4.6)."
    )


def test_every_audited_owner_module_is_reached_by_module_attribute_import() -> None:
    """The positive half of the guard: every owner module an audited
    collaborator lives in must actually be reachable as `module.symbol(...)`
    from its assigned caller file -- i.e. that file imports the module
    itself (`import clio_relay.X as X`), not just avoids bare-importing the
    symbol.
    """
    caller_trees = {caller: _tree(caller) for caller in _GUARDED_CALLERS}
    required: dict[str, set[str]] = {caller: set() for caller in _GUARDED_CALLERS}
    for module, _symbol, caller in AUDITED_COLLABORATORS:
        required[caller].add(module)
    for caller, audited_modules in required.items():
        module_imports = _module_attribute_imports(caller_trees[caller])
        missing = audited_modules - module_imports
        assert not missing, (
            f"{_GUARDED_CALLERS[caller].name} no longer imports these owner modules by "
            f"module-attribute form (`import clio_relay.X as X`): {sorted(missing)}. "
            "Without it, the audited collaborators living there have no working call path."
        )


def test_audited_collaborators_cover_every_family_named_in_the_design_doc() -> None:
    """Sanity: the inventory isn't accidentally empty or truncated."""
    assert len(AUDITED_COLLABORATORS) == 61
    assert len({module for module, _symbol, _caller in AUDITED_COLLABORATORS}) == 32


# ---------------------------------------------------------------------------
# F3/F4 sabotage guard: the five cli.py forwarders for cli_support.py's
# collaborators (see the module docstring). Each pair below drives a real
# command through `CliRunner` -- not a synthetic direct call -- and proves
# the patched fake, not the real body, actually ran.
# ---------------------------------------------------------------------------


def _storage_admission_error() -> StorageAdmissionError:
    decision = StorageDecision(
        allowed=False, reason=StorageReason.CORE_HIGH_WATER, message="storage refused"
    )
    return StorageAdmissionError(decision)


def test_sabotage_run_or_exit_via_cli_support(monkeypatch: MonkeyPatch) -> None:
    """Patching `cli_support._run_or_exit` must reach `relay-host
    render-frps-config`'s real call path through `cli.py`'s forwarder."""
    calls: list[str] = []

    def fake_run_or_exit(action: object) -> None:
        del action  # the real action (which would render the config) is never called
        calls.append("fake")

    monkeypatch.setattr(cli_support, "_run_or_exit", fake_run_or_exit)
    result = CliRunner().invoke(app, ["relay-host", "render-frps-config"])
    assert calls == ["fake"], result.output
    assert "bindPort" not in result.output


def test_sabotage_run_or_exit_via_cli(monkeypatch: MonkeyPatch) -> None:
    """The pre-existing patch direction (`monkeypatch.setattr(cli, ...)`)
    must still bite after the object re-export became a forwarder."""
    calls: list[str] = []

    def fake_run_or_exit(action: object) -> None:
        del action
        calls.append("fake")

    monkeypatch.setattr(cli, "_run_or_exit", fake_run_or_exit)
    result = CliRunner().invoke(app, ["relay-host", "render-frps-config"])
    assert calls == ["fake"], result.output
    assert "bindPort" not in result.output


def _fake_resolve_env_secret_cli_support(value: str | None, env_name: str, label: str) -> str:
    del value, env_name, label
    return "SABOTAGE-CLI-SUPPORT-TOKEN"


def _fake_resolve_env_secret_cli(value: str | None, env_name: str, label: str) -> str:
    del value, env_name, label
    return "SABOTAGE-CLI-TOKEN"


def test_sabotage_resolve_env_secret_via_cli_support(monkeypatch: MonkeyPatch) -> None:
    """Patching `cli_support._resolve_env_secret` must reach `relay-host
    render-frps-config`'s rendered output through `cli.py`'s forwarder."""
    monkeypatch.setattr(cli_support, "_resolve_env_secret", _fake_resolve_env_secret_cli_support)
    result = CliRunner().invoke(app, ["relay-host", "render-frps-config"])
    assert result.exit_code == 0, result.output
    assert "SABOTAGE-CLI-SUPPORT-TOKEN" in result.output


def test_sabotage_resolve_env_secret_via_cli(monkeypatch: MonkeyPatch) -> None:
    """The pre-existing patch direction must still bite."""
    monkeypatch.setattr(cli, "_resolve_env_secret", _fake_resolve_env_secret_cli)
    result = CliRunner().invoke(app, ["relay-host", "render-frps-config"])
    assert result.exit_code == 0, result.output
    assert "SABOTAGE-CLI-TOKEN" in result.output


def test_sabotage_require_cluster_via_cli_support(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Patching `cli_support._require_cluster` must reach `relay-host
    render-frpc-config`'s real call path through `cli.py`'s forwarder."""
    monkeypatch.chdir(tmp_path)

    def fake_require_cluster(cluster: str) -> object:
        raise ConfigurationError(f"SABOTAGE-CLI-SUPPORT-REQUIRE-CLUSTER:{cluster}")

    monkeypatch.setattr(cli_support, "_require_cluster", fake_require_cluster)
    result = CliRunner().invoke(
        app,
        ["relay-host", "render-frpc-config", "--cluster", "ares", "--local-port", "1"],
    )
    assert result.exit_code == 1
    assert "SABOTAGE-CLI-SUPPORT-REQUIRE-CLUSTER:ares" in result.output


def test_sabotage_require_cluster_via_cli(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """The pre-existing patch direction must still bite."""
    monkeypatch.chdir(tmp_path)

    def fake_require_cluster(cluster: str) -> object:
        raise ConfigurationError(f"SABOTAGE-CLI-REQUIRE-CLUSTER:{cluster}")

    monkeypatch.setattr(cli, "_require_cluster", fake_require_cluster)
    result = CliRunner().invoke(
        app,
        ["relay-host", "render-frpc-config", "--cluster", "ares", "--local-port", "1"],
    )
    assert result.exit_code == 1
    assert "SABOTAGE-CLI-REQUIRE-CLUSTER:ares" in result.output


def test_sabotage_write_failed_acceptance_report_via_cli_support(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Patching `cli_support._write_failed_acceptance_report` must reach
    `relay-host test-frpc-connection`'s real preflight-failure call path
    through `cli.py`'s forwarder: a genuine unknown-cluster failure drives
    the real except-block call, and the fake's own report content -- not the
    real canonical failure envelope -- lands on disk."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    report_path = tmp_path / "report.json"

    def fake_write(**kwargs: object) -> None:
        path = kwargs["path"]
        assert isinstance(path, Path)
        path.write_text(json.dumps({"sentinel": "cli_support-sabotage"}), encoding="utf-8")

    monkeypatch.setattr(cli_support, "_write_failed_acceptance_report", fake_write)
    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-frpc-connection",
            "--cluster",
            "does-not-exist",
            "--local-port",
            "1",
            "--validation-report",
            str(report_path),
        ],
    )
    assert result.exit_code == 1, result.output
    assert json.loads(report_path.read_text(encoding="utf-8")) == {
        "sentinel": "cli_support-sabotage"
    }


def test_sabotage_write_failed_acceptance_report_via_cli(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """The pre-existing patch direction must still bite."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    report_path = tmp_path / "report.json"

    def fake_write(**kwargs: object) -> None:
        path = kwargs["path"]
        assert isinstance(path, Path)
        path.write_text(json.dumps({"sentinel": "cli-sabotage"}), encoding="utf-8")

    monkeypatch.setattr(cli, "_write_failed_acceptance_report", fake_write)
    result = CliRunner().invoke(
        app,
        [
            "relay-host",
            "test-frpc-connection",
            "--cluster",
            "does-not-exist",
            "--local-port",
            "1",
            "--validation-report",
            str(report_path),
        ],
    )
    assert result.exit_code == 1, result.output
    assert json.loads(report_path.read_text(encoding="utf-8")) == {"sentinel": "cli-sabotage"}


def test_sabotage_echo_storage_admission_error_via_cli_support(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """`_echo_storage_admission_error` has no `relay-host` caller (see
    `cli_relay_host.py`'s own docstring) -- its two real callers are
    elsewhere in `cli.py` (`_submit_managed_job` and the JARVIS MCP call
    path). `agent run` is the lightest real command that reaches
    `_submit_managed_job`; a fake managed queue forces the real
    `StorageAdmissionError` handling path without needing live storage
    infrastructure. Patching `cli_support._echo_storage_admission_error`
    must reach it through `cli.py`'s forwarder."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text("hi", encoding="utf-8")

    class _FakeQueue:
        def submit_job(self, job: object) -> object:
            del job
            raise _storage_admission_error()

    def _fake_echo_cli_support(error: StorageAdmissionError) -> None:
        del error
        print("SABOTAGE-CLI-SUPPORT-ECHO")  # noqa: T201

    monkeypatch.setattr(cli, "_managed_queue_from_env", lambda: _FakeQueue())
    monkeypatch.setattr(cli_support, "_echo_storage_admission_error", _fake_echo_cli_support)
    result = CliRunner().invoke(
        app, ["agent", "run", "--cluster", "ares", "--prompt", str(prompt_file)]
    )
    assert result.exit_code == 1
    assert "SABOTAGE-CLI-SUPPORT-ECHO" in result.output
    assert "storage_admission_denied" not in result.output


def test_sabotage_echo_storage_admission_error_via_cli(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """The pre-existing patch direction must still bite."""
    monkeypatch.chdir(tmp_path)
    _write_test_cluster(tmp_path)
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")
    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text("hi", encoding="utf-8")

    class _FakeQueue:
        def submit_job(self, job: object) -> object:
            del job
            raise _storage_admission_error()

    def _fake_echo_cli(error: StorageAdmissionError) -> None:
        del error
        print("SABOTAGE-CLI-ECHO")  # noqa: T201

    monkeypatch.setattr(cli, "_managed_queue_from_env", lambda: _FakeQueue())
    monkeypatch.setattr(cli, "_echo_storage_admission_error", _fake_echo_cli)
    result = CliRunner().invoke(
        app, ["agent", "run", "--cluster", "ares", "--prompt", str(prompt_file)]
    )
    assert result.exit_code == 1
    assert "SABOTAGE-CLI-ECHO" in result.output
    assert "storage_admission_denied" not in result.output
