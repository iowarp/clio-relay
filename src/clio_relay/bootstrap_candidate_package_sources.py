"""The candidate-overlay package manifest: exactly which sources travel.

Split from :mod:`clio_relay.bootstrap_candidate_uv_install_source` during the
integrate-w2 merge train (2026-08-20), which pushed that module's own
line count over the 800-line new-file cap once the manifest below grew to
cover two newly-split facade families (see the comment on
:data:`_BOOTSTRAP_CANDIDATE_SOURCE_NAMES`). `_bootstrap_candidate_package_
sources()` identifies exactly which repository-relative sources travel with
a candidate overlay -- both process_containment.py's and bootstrap_
reconcile.py's own owner-module dependencies must be listed here, or a
standalone deployment of just the facade breaks: (1) a plain import of the
facade with only its own name on sys.path raises ImportError for the
missing siblings, and (2) process_containment's isolated containment-broker
subprocess resolves its own module root with no namespace-package fallback
available, so a facade shipped without its siblings can silently resolve
against a stale/legacy install instead and fail closed.
"""

from __future__ import annotations

from pathlib import Path

_BOOTSTRAP_CANDIDATE_PACKAGE_OVERLAY = (
    b"\nfrom importlib import metadata as _clio_relay_metadata\n"
    b"from pkgutil import extend_path\n\n"
    b"__path__ = extend_path(__path__, __name__)\n"
    b"try:\n"
    b"    __version__ = _clio_relay_metadata.version('clio-relay')\n"
    b"except _clio_relay_metadata.PackageNotFoundError:\n"
    b"    pass\n"
)
_BOOTSTRAP_CANDIDATE_SOURCE_NAMES = (
    "bootstrap_full_activation_staging.py",
    "bootstrap_jarvis_staging.py",
    "bootstrap_provider_build_info.py",
    "bootstrap_reconcile.py",
    # integrate-w2 merge-train fix (post-#231 split/bootstrap-reconcile-w2 +
    # split/process-containment-w2): bootstrap_reconcile.py and
    # process_containment.py stopped being self-contained single files and
    # became thin facades over sibling owner modules. Deploying the facade
    # alone breaks two real candidate-overlay paths: (1) a standalone import
    # of the facade with only these names on sys.path raises ImportError for
    # the missing siblings, and (2) worse, process_containment's own isolated
    # containment-broker subprocess (`_BROKER_SCRIPT` in
    # process_containment_broker_script.py) computes its `module_root` from
    # `Path(__file__).resolve().parent.parent` of whichever file actually
    # defines `_spawn_broker` -- if that resolves to a legacy/partial install
    # missing this candidate's own process_containment.py, the broker's
    # isolated `sys.path.insert(0, module_root)` (no namespace-package
    # fallback available there, unlike the normal import path) can no longer
    # find it and exits SystemExit(125) before the child-readiness handshake.
    # Both owner-module families must travel with the candidate so the
    # facade's own directory is always the one that wins import resolution.
    "bootstrap_reconcile_activation_paths.py",
    "bootstrap_reconcile_builtin_repos.py",
    "bootstrap_reconcile_constants.py",
    "bootstrap_reconcile_execution_identity.py",
    "bootstrap_reconcile_generation_staging.py",
    "bootstrap_reconcile_inspection.py",
    "bootstrap_reconcile_jarvis_wrapper_binding.py",
    "bootstrap_reconcile_locks.py",
    "bootstrap_reconcile_models.py",
    "bootstrap_reconcile_planning.py",
    "bootstrap_reconcile_planning_support.py",
    "bootstrap_reconcile_primitives.py",
    "bootstrap_reconcile_readiness.py",
    "bootstrap_reconcile_receipt.py",
    "bootstrap_reconcile_replacement_provider.py",
    "bootstrap_reconcile_repository.py",
    "bootstrap_reconcile_transaction.py",
    "bootstrap_recovery.py",
    "bounded_process.py",
    "errors.py",
    "process_containment.py",
    "process_containment_broker.py",
    "process_containment_broker_script.py",
    "process_containment_environment.py",
    "process_containment_popen.py",
    "process_containment_posix.py",
    "process_containment_recorded.py",
    "process_containment_registry.py",
    "process_containment_spawn.py",
    "process_containment_systemd_core.py",
    "process_containment_systemd_query.py",
    "process_containment_systemd_scope.py",
    "process_containment_termination.py",
    "process_containment_types.py",
    "process_containment_windows.py",
    "safe_archive.py",
)


def _bootstrap_candidate_package_sources() -> dict[str, bytes]:
    """Return the exact sources overlaid during candidate reconciliation."""
    package_root = Path(__file__).parent
    sources = {
        "__init__.py": (package_root / "__init__.py").read_bytes()
        + _BOOTSTRAP_CANDIDATE_PACKAGE_OVERLAY
    }
    for name in _BOOTSTRAP_CANDIDATE_SOURCE_NAMES:
        sources[name] = (package_root / name).read_bytes()
    return sources
