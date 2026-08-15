"""Guard the extraction-stable patch seam cli.py's collaborators use
(iowarp/clio-relay#231, R8(i); doc `docs/design/relay-architecture-2026-08.md`
SS4.6/SS9).

`tests/test_cli.py` and its siblings patch these collaborator symbols on the
*owning* module (e.g. `monkeypatch.setattr(transport_probe,
"run_frp_http_probe", ...)`), not on `cli`'s own namespace. That only works
because `cli.py` resolves them through a module-attribute lookup
(`import clio_relay.transport_probe as transport_probe`, then
`transport_probe.run_frp_http_probe(...)`) rather than binding the bare name
into its own namespace (`from clio_relay.transport_probe import
run_frp_http_probe`, then `run_frp_http_probe(...)`). The bare-name form is
what `docs/design/relay-architecture-2026-08.md` SS4.6 calls "the coupling
that makes extracting logic out of cli.py expensive" -- a future edit that
quietly reintroduces it doesn't just add a stylistic wart, it silently
un-patches every test that targets the owner module (the fake is never
invoked) or breaks loudly with an AttributeError once the symbol leaves
`cli`'s namespace during a real command-module extraction.

This test locks in the R8(i) inventory: every (owner module, symbol) pair
this slice moved off the bare-import seam. It is deliberately independent of
any future `cli.py` refactor's own bookkeeping -- it reads the live AST of
`src/clio_relay/cli.py`, so a regression is caught the moment it lands,
without needing anyone to remember this list exists.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# (owner module short name, real symbol name as defined on that module) --
# the R8(i) audit inventory (docs/design/relay-architecture-2026-08.md SS4.6/
# SS9). `real symbol name` is the name as it exists on the owner module, not
# any local alias cli.py or a test file might give it (e.g. `relay_ops`'s
# `job_status` was locally aliased to `get_job_status` in cli.py's old
# bare-import form -- the guard checks for `job_status`, since re-importing
# it under any alias reintroduces the same coupling).
AUDITED_COLLABORATORS: tuple[tuple[str, str], ...] = (
    ("session_lifecycle", "status_remote_session"),
    ("session_lifecycle", "teardown_remote_session"),
    ("remote_cli", "run_remote_clio"),
    ("remote_cli", "should_execute_on_cluster"),
    ("mcp_stdio_validation", "run_packaged_mcp_stdio_session"),
    ("session_lifecycle", "detach_remote_session"),
    ("installation", "installation_info"),
    ("session_lifecycle", "start_remote_session"),
    ("bootstrap", "package_source_root"),
    ("installation", "worker_runtime_info"),
    ("endpoint", "EndpointWorker"),
    ("scheduler_providers", "provider_for_scheduler"),
    ("bootstrap", "bootstrap_cluster_over_ssh"),
    ("jarvis_mcp_validation", "build_jarvis_mcp_validation_report"),
    ("frp_check", "run_frpc_connection_check"),
    ("live_acceptance", "run_live_acceptance"),
    ("bootstrap_reconcile", "bootstrap_invocation_lock"),
    ("session_lifecycle", "finalize_remote_session_cleanup_report"),
    ("session_lifecycle", "read_remote_session_cleanup_report"),
    ("session_lifecycle", "inspect_owned_session_recovery_status"),
    ("release_validation", "run_local_release_validation"),
    ("transport_probe", "run_frp_http_probe"),
    ("core_queue", "ClioCoreQueue"),
    ("bootstrap_reconcile", "inspect_exact_bootstrap_noop"),
    ("bounded_process", "run_bounded_process"),
    ("storage_runtime", "storage_managed_queue"),
    ("service_runtime", "ServiceRuntimeSupervisor"),
    ("deployment", "install_endpoint_user_service_over_ssh"),
    ("transport_probe", "run_frp_direct_http_probe"),
    ("transport_probe", "run_ssh_forward_http_probe"),
    ("mcp_server", "load_registered_remote_mcp_catalog"),
    ("relay_ops", "wait_for_terminal"),
    ("bootstrap_reconcile", "write_bootstrap_receipt"),
    ("bootstrap_reconcile", "proven_active_generation_mismatch"),
    ("installation", "write_self_install_receipt"),
    ("relay_ops", "observe_until_terminal"),
    ("scheduler_providers", "validation_provider_for_scheduler"),
    ("cluster_config", "open_private_atomic_file"),
    ("session_lifecycle", "start_remote_session_durable"),
    ("installation", "verified_session_api_install_receipt"),
    ("session_lifecycle", "publish_owned_session_api_startup_receipt"),
    ("session_api", "submit_owned_session_job"),
    ("validation_report", "write_validation_report"),
    ("remote_cli", "remote_command_timeout"),
    ("application_profiles", "install_cluster_app_over_ssh"),
    ("owner_session_admission", "owner_session_gateway_admission"),
    ("fastmcp_server", "run_fastmcp_stdio"),
    ("fastmcp_server", "run_fastmcp_http"),
    ("endpoint_service_status", "endpoint_service_readiness_over_ssh"),
    ("deployment", "restart_endpoint_user_service_over_ssh"),
    ("relay_ops", "job_status"),
    ("cluster_config", "acquire_private_configuration_windows_parent_guard"),
    ("scheduler_providers", "allocation_connector_provider_for_scheduler"),
    ("bootstrap_acceptance", "bootstrap_reuse_acceptance_evidence"),
    ("remote_mcp", "build_remote_mcp_acceptance_report"),
    ("jarvis_mcp", "jarvis_mcp_server"),
    ("remote_cli", "remove_remote_file"),
    ("queue_validation", "run_queue_management_validation"),
    ("remote_cli", "run_remote_shell"),
    ("scheduler_validation", "run_scheduler_lifecycle_validation"),
    ("remote_cli", "write_remote_file"),
)


def _cli_tree() -> ast.Module:
    cli_path = Path(__file__).resolve().parents[1] / "src" / "clio_relay" / "cli.py"
    return ast.parse(cli_path.read_text(encoding="utf-8"), filename=str(cli_path))


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


@pytest.mark.parametrize("module,symbol", AUDITED_COLLABORATORS)
def test_no_bare_import_of_an_audited_collaborator(module: str, symbol: str) -> None:
    """cli.py must not bind an audited collaborator's real name into its own
    namespace via `from clio_relay.<module> import <symbol>` -- doing so
    silently reintroduces the bare-name-lookup coupling SS4.6 describes: a
    test patching the owner module's attribute would stop taking effect, and
    a real command-module extraction would break the call outright.
    """
    tree = _cli_tree()
    bare_names = _bare_imported_names(tree, module)
    assert symbol not in bare_names, (
        f"cli.py bare-imports `{symbol}` from clio_relay.{module} again "
        f"(`from clio_relay.{module} import {symbol}`) -- this un-patches "
        f"every test that targets `{module}.{symbol}` directly. Call it as "
        f"`{module}.{symbol}(...)` through the module-attribute import "
        f"instead (see docs/design/relay-architecture-2026-08.md SS4.6)."
    )


def test_every_audited_owner_module_is_reached_by_module_attribute_import() -> None:
    """The positive half of the guard: every owner module the audited
    collaborators live in must actually be reachable as `module.symbol(...)`
    -- i.e. cli.py imports the module itself (`import clio_relay.X as X`),
    not just avoids bare-importing the symbol.
    """
    tree = _cli_tree()
    module_imports = _module_attribute_imports(tree)
    audited_modules = {module for module, _ in AUDITED_COLLABORATORS}
    missing = audited_modules - module_imports
    assert not missing, (
        f"cli.py no longer imports these owner modules by module-attribute "
        f"form (`import clio_relay.X as X`): {sorted(missing)}. Without it, "
        f"the audited collaborators living there have no working call path."
    )


def test_audited_collaborators_cover_every_family_named_in_the_design_doc() -> None:
    """Sanity: the inventory isn't accidentally empty or truncated."""
    assert len(AUDITED_COLLABORATORS) == 61
    assert len({module for module, _ in AUDITED_COLLABORATORS}) == 32
