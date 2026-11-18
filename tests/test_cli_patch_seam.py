"""Guard the extraction-stable patch seam cli.py's collaborators use
(iowarp/clio-relay#231, R8(i)/R8(ii); doc
`docs/design/relay-architecture-2026-08.md` SS4.6/SS9).

`tests/test_cli.py` and its siblings patch these collaborator symbols on the
*owning* module (e.g. `monkeypatch.setattr(transport_probe,
"run_frp_http_probe", ...)`), not on the caller's own namespace. That only
works because the caller resolves them through a module-attribute lookup
(`import clio_relay.adapters.worker.transport_probe as transport_probe`, then
`transport_probe.run_frp_http_probe(...)`) rather than binding the bare name
into its own namespace (`from clio_relay.adapters.worker.transport_probe import
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

import clio_relay.adapters.worker.cli_support as cli_support
from clio_relay.adapters.worker import cli
from clio_relay.adapters.worker.cli import worker_app as app
from clio_relay.errors import ConfigurationError
from tests.test_cli import (
    _write_test_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

_SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "clio_relay"
_WORKER_ROOT = _SRC_ROOT / "adapters" / "worker"

# Every cli.py-family module this guard polices, plus the file it audits.
# A collaborator's `caller` field below must name one of these keys.
_GUARDED_CALLERS: dict[str, Path] = {
    "cli": _WORKER_ROOT / "cli.py",
    "cli_relay_host": _WORKER_ROOT / "cli_relay_host.py",
    "cli_api": _WORKER_ROOT / "cli_api.py",
    "cli_worker": _WORKER_ROOT / "cli_worker.py",
    "cli_endpoint": _WORKER_ROOT / "cli_endpoint.py",
    "cli_scheduler": _WORKER_ROOT / "cli_scheduler.py",
    "cli_queue": _WORKER_ROOT / "cli_queue.py",
    "cli_queue_maintenance": _WORKER_ROOT / "cli_queue_maintenance.py",
    "cli_session": _WORKER_ROOT / "cli_session.py",
    "cli_cluster_deploy": _WORKER_ROOT / "cli_cluster_deploy.py",
    "cli_installation_receipt": _WORKER_ROOT / "cli_installation_receipt.py",
    # #231 wave-2 (session start/teardown + JARVIS execution-query engine
    # extraction): the audited collaborators these six moved-into modules
    # now call directly, replacing "cli" as the entries' `caller` below.
    "cli_session_start": _WORKER_ROOT / "cli_session_start.py",
    "cli_session_teardown": _WORKER_ROOT / "cli_session_teardown.py",
    # split/cli-session-teardown-w3: cli_session_teardown.py's own further
    # decomposition into a facade plus phase-owner modules. Each guarded
    # here so a future edit to any of them cannot silently reintroduce a
    # bare import of an audited collaborator.
    "cli_session_teardown_state": _WORKER_ROOT / "cli_session_teardown_state.py",
    "cli_session_teardown_recovery": _WORKER_ROOT / "cli_session_teardown_recovery.py",
    "cli_session_teardown_jobs": _WORKER_ROOT / "cli_session_teardown_jobs.py",
    "cli_session_teardown_finalize": _WORKER_ROOT / "cli_session_teardown_finalize.py",
    "cli_session_teardown_report": _WORKER_ROOT / "cli_session_teardown_report.py",
    "cli_session_teardown_action": _WORKER_ROOT / "cli_session_teardown_action.py",
    "cli_owned_session_recovery": _WORKER_ROOT / "cli_owned_session_recovery.py",
    "cli_transport_validation": _WORKER_ROOT / "cli_transport_validation.py",
    "cli_cleanup_evidence": _WORKER_ROOT / "cli_cleanup_evidence.py",
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
    ("session_lifecycle", "status_remote_session", "cli_session"),
    # split/cli-session-teardown-w3: moved caller cli_session_teardown ->
    # cli_session_teardown_finalize with the coordinator-call/closure phase
    # extraction (teardown_remote_session's only call site).
    ("session_lifecycle", "teardown_remote_session", "cli_session_teardown_finalize"),
    ("remote_cli", "run_remote_clio", "cli"),
    ("remote_cli", "should_execute_on_cluster", "cli"),
    # #231 cli.py decomposition: moved caller cli -> cli_session with the
    # session command-group extraction (detach_remote_session's only cli.py
    # call site was session_detach).
    ("session_lifecycle", "detach_remote_session", "cli_session"),
    ("installation", "installation_info", "cli_installation_receipt"),
    ("session_lifecycle", "start_remote_session", "cli_session_start"),
    # #231 cli.py decomposition: moved caller cli -> cli_cluster_deploy with
    # the cluster deployment command-group extraction (cluster_bootstrap's
    # only cli.py call site).
    ("bootstrap", "package_source_root", "cli_cluster_deploy"),
    # #231 cli.py decomposition: moved caller cli -> cli_installation_receipt
    # with the installation/receipt command-group extraction (bootstrap_
    # inspect's only cli.py call site).
    ("installation", "worker_runtime_info", "cli_installation_receipt"),
    # #231 cli.py decomposition: moved caller cli -> cli_endpoint with the
    # endpoint command-group extraction (EndpointWorker's only cli.py call
    # site was endpoint_start).
    ("endpoint", "EndpointWorker", "cli_endpoint"),
    # #231 cli.py decomposition: moved caller cli -> cli_cluster_deploy with
    # the cluster deployment command-group extraction (cluster_bootstrap's
    # only cli.py call site).
    ("bootstrap", "bootstrap_cluster_over_ssh", "cli_cluster_deploy"),
    ("frp_check", "run_frpc_connection_check", "cli_transport_validation"),
    # #231 cli.py decomposition: moved caller cli -> cli_installation_receipt
    # with the installation/receipt command-group extraction (bootstrap_
    # inspect's own serialization lock; its only cli.py call site).
    ("bootstrap_reconcile", "bootstrap_invocation_lock", "cli_installation_receipt"),
    # split/cli-session-teardown-w3: moved caller cli_session_teardown ->
    # cli_session_teardown_state with _persist_verified_cleanup_report_
    # before_closure's extraction (its only call site for each).
    ("session_lifecycle", "finalize_remote_session_cleanup_report", "cli_session_teardown_state"),
    ("session_lifecycle", "read_remote_session_cleanup_report", "cli_session_teardown_state"),
    ("session_lifecycle", "inspect_owned_session_recovery_status", "cli_owned_session_recovery"),
    # R8(ii): moved caller cli -> cli_relay_host with the relay-host extraction.
    ("transport_probe", "run_frp_http_probe", "cli_relay_host"),
    ("core_queue", "ClioCoreQueue", "cli_installation_receipt"),
    # #231 cli.py decomposition: moved caller cli -> cli_installation_receipt
    # with the installation/receipt command-group extraction (bootstrap_
    # inspect's only cli.py call site).
    ("bootstrap_reconcile", "inspect_exact_bootstrap_noop", "cli_installation_receipt"),
    # #231 cli.py decomposition: moved caller cli -> cli_installation_receipt
    # with the installation/receipt command-group extraction (bootstrap_
    # inspect's only cli.py call site).
    ("bounded_process", "run_bounded_process", "cli_installation_receipt"),
    ("storage_runtime", "storage_managed_queue", "cli_queue_maintenance"),
    # #231 cli.py decomposition: moved caller cli -> cli_cluster_deploy with
    # the cluster deployment command-group extraction (cluster_install_
    # endpoint_service's only cli.py call site).
    ("deployment", "install_endpoint_user_service_over_ssh", "cli_cluster_deploy"),
    # R8(ii): moved caller cli -> cli_relay_host with the relay-host extraction.
    ("transport_probe", "run_frp_direct_http_probe", "cli_relay_host"),
    ("transport_probe", "run_ssh_forward_http_probe", "cli_relay_host"),
    # #231 cli.py decomposition: moved caller cli -> cli_installation_receipt
    # with the installation/receipt command-group extraction (bootstrap_
    # inspect's only cli.py call site).
    ("bootstrap_reconcile", "write_bootstrap_receipt", "cli_installation_receipt"),
    # #231 cli.py decomposition: moved caller cli -> cli_installation_receipt
    # with the installation/receipt command-group extraction (bootstrap_
    # inspect's only cli.py call site).
    ("bootstrap_reconcile", "proven_active_generation_mismatch", "cli_installation_receipt"),
    # #231 cli.py decomposition: moved caller cli -> cli_installation_receipt
    # with the installation/receipt command-group extraction (installation_
    # write_receipt's only cli.py call site).
    ("installation", "write_self_install_receipt", "cli_installation_receipt"),
    ("cluster_config", "open_private_atomic_file", "cli_cleanup_evidence"),
    ("session_lifecycle", "start_remote_session_durable", "cli_session_start"),
    # #231 cli.py decomposition: moved caller cli -> cli_api with the api
    # command-group extraction (api_start was each symbol's only cli.py call
    # site).
    ("installation", "verified_session_api_install_receipt", "cli_api"),
    ("session_lifecycle", "publish_owned_session_api_startup_receipt", "cli_api"),
    # #231 cli.py decomposition: moved caller cli -> cli_session with the
    # session command-group extraction (submit_owned_session_job's only
    # cli.py call site was session_submit_jarvis).
    ("session_api", "submit_owned_session_job", "cli_session"),
    ("validation_report", "write_validation_report", "cli_session_teardown"),
    # #231 cli.py decomposition: moved caller cli -> cli_cluster_deploy with
    # the cluster deployment command-group extraction (cluster_install_app's
    # only cli.py call site).
    ("application_profiles", "install_cluster_app_over_ssh", "cli_cluster_deploy"),
    # #231 cli.py decomposition: moved caller cli -> cli_cluster_deploy with
    # the cluster deployment command-group extraction (cluster_endpoint_
    # service_status's only cli.py call site).
    ("endpoint_service_status", "endpoint_service_readiness_over_ssh", "cli_cluster_deploy"),
    # #231 cli.py decomposition: moved caller cli -> cli_cluster_deploy with
    # the cluster deployment command-group extraction (cluster_restart_
    # endpoint_service's only cli.py call site).
    ("deployment", "restart_endpoint_user_service_over_ssh", "cli_cluster_deploy"),
    (
        "cluster_config",
        "acquire_private_configuration_windows_parent_guard",
        "cli_cleanup_evidence",
    ),
    # #231 cli.py decomposition: moved caller cli -> cli_cluster_deploy with
    # the cluster deployment command-group extraction (cluster_bootstrap's
    # only cli.py call site).
    ("bootstrap_acceptance", "bootstrap_reuse_acceptance_evidence", "cli_cluster_deploy"),
)


# The worker modules moved into clio_relay.adapters.worker; the rest stay at the root.
_OWNER_PACKAGES = ("clio_relay.adapters.worker", "clio_relay")


def _qualified(module: str) -> set[str]:
    return {f"{package}.{module}" for package in _OWNER_PACKAGES}


def _tree(caller: str) -> ast.Module:
    path = _GUARDED_CALLERS[caller]
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _bare_imported_names(tree: ast.Module, module: str) -> set[str]:
    """Every real (unaliased) name bare-imported from `clio_relay.<module>`."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _qualified(module):
            names.update(alias.name for alias in node.names)
    return names


def _module_attribute_imports(tree: ast.Module) -> set[str]:
    """Every owner module short name a caller binds as a module.

    That is `import clio_relay.X as Y`, `import clio_relay.adapters.worker.X as Y`
    or `from clio_relay.adapters.worker import X`.
    """
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                for prefix in _OWNER_PACKAGES:
                    short = alias.name.removeprefix(f"{prefix}.")
                    if short != alias.name and "." not in short:
                        modules.add(short)
                        break
        elif isinstance(node, ast.ImportFrom) and node.module in _OWNER_PACKAGES:
            modules.update(alias.name for alias in node.names)
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
    assert len(AUDITED_COLLABORATORS) == 38
    assert len({module for module, _symbol, _caller in AUDITED_COLLABORATORS}) == 18


# ---------------------------------------------------------------------------
# F3/F4 sabotage guard: the five cli.py forwarders for cli_support.py's
# collaborators (see the module docstring). Each pair below drives a real
# command through `CliRunner` -- not a synthetic direct call -- and proves
# the patched fake, not the real body, actually ran.
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# #231 shared-plumbing relocation pass: `_managed_queue_from_env`'s own
# sabotage guard. Unlike the five R8(ii) forwarders above, this symbol has
# two distinct call shapes worth distinguishing: `cli_monitor.py`'s
# `monitor run-once` reaches it as `cli._managed_queue_from_env()` -- a
# fresh module-attribute lookup at call time, so both `monkeypatch.setattr
# (cli, ...)` and `monkeypatch.setattr(cli_support, ...)` reach it (the
# forwarder re-reads `cli_support.<symbol>` on every call). The sabotage
# tests above for `_echo_storage_admission_error`, by contrast, patch their
# `_managed_queue_from_env` *setup* only via `cli_support`: `_submit_managed_
# job`'s own bare internal call to `_managed_queue_from_env()` resolves
# through cli_support.py's own module namespace (both symbols are
# cli_support.py-resident now), so a patch on `cli.py`'s forwarder is never
# consulted there -- that boundary is deliberate and documented on those
# tests, not a second bug to fix here.
# ---------------------------------------------------------------------------
