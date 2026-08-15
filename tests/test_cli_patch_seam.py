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
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "clio_relay"

# Every cli.py-family module this guard polices, plus the file it audits.
# A collaborator's `caller` field below must name one of these keys.
_GUARDED_CALLERS: dict[str, Path] = {
    "cli": _SRC_ROOT / "cli.py",
    "cli_relay_host": _SRC_ROOT / "cli_relay_host.py",
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
    ("endpoint", "EndpointWorker", "cli"),
    ("scheduler_providers", "provider_for_scheduler", "cli"),
    ("bootstrap", "bootstrap_cluster_over_ssh", "cli"),
    ("jarvis_mcp_validation", "build_jarvis_mcp_validation_report", "cli"),
    ("frp_check", "run_frpc_connection_check", "cli"),
    ("live_acceptance", "run_live_acceptance", "cli"),
    ("bootstrap_reconcile", "bootstrap_invocation_lock", "cli"),
    ("session_lifecycle", "finalize_remote_session_cleanup_report", "cli"),
    ("session_lifecycle", "read_remote_session_cleanup_report", "cli"),
    ("session_lifecycle", "inspect_owned_session_recovery_status", "cli"),
    ("release_validation", "run_local_release_validation", "cli"),
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
    ("installation", "verified_session_api_install_receipt", "cli"),
    ("session_lifecycle", "publish_owned_session_api_startup_receipt", "cli"),
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
    {(module, symbol) for module, symbol, _caller in AUDITED_COLLABORATORS}
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
