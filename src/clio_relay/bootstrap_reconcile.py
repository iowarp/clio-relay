"""Crash-safe desired-state reconciliation primitives for cluster bootstrap.

The SSH bootstrap shell is intentionally a small transaction driver.  This
module owns the durable contract used by that driver: canonical desired-state
identity, read-only no-op verification, JARVIS state preservation evidence,
and the fsync-backed transaction journal.

Owner-module re-exports (iowarp/clio-relay#255 split/bootstrap-reconcile).
Each extracted concern is re-imported here under its original name so every
existing ``from clio_relay.bootstrap_reconcile import X`` caller (the SSH
bootstrap shell's embedded heredocs, ``bootstrap.py``, ``cli_installation_
receipt.py``, ``endpoint.py``, the test suite's ``import clio_relay.
bootstrap_reconcile as bootstrap_reconcile_module``) and every qualified
``bootstrap_reconcile.X``/monkeypatch access keeps resolving unchanged -- a
pure move, not a behavior change. See each owner module's own docstring for
what it owns:

* ``bootstrap_reconcile_constants`` -- shared schema ids and platform primitives
* ``bootstrap_reconcile_primitives`` -- filesystem/identity primitives
* ``bootstrap_reconcile_models`` -- desired-state/inspection/plan pydantic models
* ``bootstrap_reconcile_transaction`` -- the transaction journal state machine
* ``bootstrap_reconcile_locks`` -- the private bootstrap invocation lock
* ``bootstrap_reconcile_execution_identity`` -- venv/JARVIS-state identity
* ``bootstrap_reconcile_readiness`` -- binary/queue/worker readiness checks
* ``bootstrap_reconcile_activation_paths`` -- pre-fence capture + atomic activation
* ``bootstrap_reconcile_inspection`` -- read-only exact-noop inspection
* ``bootstrap_reconcile_jarvis_wrapper_binding`` -- JARVIS wrapper/launcher binding
* ``bootstrap_reconcile_generation_staging`` -- staged-generation reverification
* ``bootstrap_reconcile_replacement_provider`` -- replacement-provider attestation
* ``bootstrap_reconcile_planning`` -- ``plan_bootstrap_reconcile``
* ``bootstrap_reconcile_planning_support`` -- its legacy-reuse helpers
* ``bootstrap_reconcile_receipt`` -- acceptance receipt construction
* ``bootstrap_reconcile_builtin_repos`` -- JARVIS builtin-repository provenance
* ``bootstrap_reconcile_repository`` -- exact-path repository reconciliation
"""

from __future__ import annotations

import logging
import os  # noqa: F401 -- back-compat surface: bootstrap_reconcile_module.os.*

from clio_relay.bootstrap_reconcile_activation_paths import (
    _activation_identity_matches_after_rename,  # noqa: F401
    _activation_path_identity,  # noqa: F401
    _activation_symlink_lexical_target,  # noqa: F401
    _capture_activation_object,  # noqa: F401
    _capture_activation_path,  # noqa: F401
    _capture_reconcile_activation_paths,  # noqa: F401
    _is_generation_repository_target,  # noqa: F401
    _reconcile_activation_symlink,  # noqa: F401
    _verify_stable_symlink,  # noqa: F401
    reconcile_staged_activation_links,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_builtin_repos import (
    _is_python_library_directory,  # noqa: F401
    _jarvis_cd_builtin_repository,  # noqa: F401
    _jarvis_cd_metadata,  # noqa: F401
    _jarvis_site_package_directories,  # noqa: F401
    _record_installs_jarvis_builtin,  # noqa: F401
    _relay_owned_jarvis_builtin_repositories,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_constants import (
    _AT_FDCWD,  # noqa: F401
    _FCHMOD,  # noqa: F401
    _GETUID,  # noqa: F401
    _O_BINARY,  # noqa: F401
    _O_NOFOLLOW,  # noqa: F401
    _RENAME_EXCHANGE,  # noqa: F401
    BOOTSTRAP_DESIRED_STATE_SCHEMA,  # noqa: F401
    BOOTSTRAP_LOCK_TIMEOUT_SECONDS,  # noqa: F401
    BOOTSTRAP_RECEIPT_SCHEMA,  # noqa: F401
    BOOTSTRAP_TRANSACTION_SCHEMA,  # noqa: F401
    LEGACY_MANAGED_JARVIS_REPO_PATH,  # noqa: F401
    MANAGED_JARVIS_REPO_PATH,  # noqa: F401
    MAX_JARVIS_CONFIG_BYTES,  # noqa: F401
    MAX_JARVIS_DISTRIBUTION_METADATA_BYTES,  # noqa: F401
    MAX_JARVIS_DISTRIBUTION_RECORD_BYTES,  # noqa: F401
    MAX_JARVIS_GRAPH_BYTES,  # noqa: F401
    MAX_JARVIS_REPOS_BYTES,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_execution_identity import (
    execution_environment_identity,  # noqa: F401
    inspect_jarvis_state,  # noqa: F401
    jarvis_wrapper_payload,  # noqa: F401
    write_jarvis_wrapper,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_generation_staging import (
    finish_staged_activation,  # noqa: F401
    inspect_prepared_generation,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_inspection import (
    _inspect_active_generation,  # noqa: F401
    _inspect_installation_identity,  # noqa: F401
    inspect_exact_bootstrap_noop,  # noqa: F401
    proven_active_generation_mismatch,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_jarvis_wrapper_binding import (
    _relay_managed_jarvis_launcher_selected,  # noqa: F401
    _verify_active_generation_jarvis_wrapper,  # noqa: F401
    resolve_receipt_bound_jarvis_python,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_locks import (
    bootstrap_invocation_lock,  # noqa: F401
    repair_legacy_cursor_permissions_for_upgrade,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_models import (
    BootstrapActivationPath,  # noqa: F401
    BootstrapActivationPathIdentity,  # noqa: F401
    BootstrapDesiredState,  # noqa: F401
    BootstrapInspection,  # noqa: F401
    BootstrapPersistentUvToolIdentity,  # noqa: F401
    BootstrapReadinessEvidence,  # noqa: F401
    BootstrapReconcilePlan,  # noqa: F401
    BootstrapReplacementProviderEvidence,  # noqa: F401
    JarvisStateEvidence,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_planning import plan_bootstrap_reconcile  # noqa: F401
from clio_relay.bootstrap_reconcile_planning_support import (
    _bounded_subprocess,  # noqa: F401
    _full_plan,  # noqa: F401
    _managed_generation_jarvis_environment,  # noqa: F401
    _verify_jarvis_util_reuse,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_primitives import (
    _atomic_exchange_paths,  # noqa: F401
    _atomic_json,  # noqa: F401
    _canonical_path_preserving_final,  # noqa: F401
    _cross_handle_stat_identity,  # noqa: F401
    _expand_home,  # noqa: F401
    _fsync_directory,  # noqa: F401
    _identity_matches_after_rename,  # noqa: F401
    _is_sha256,  # noqa: F401
    _path_is_directory_alias,  # noqa: F401
    _read_bounded,  # noqa: F401
    _read_regular_bounded,  # noqa: F401
    _read_regular_bounded_with_identity,  # noqa: F401
    _require_sha256,  # noqa: F401
    _stat_identity,  # noqa: F401
    _yaml_mapping,  # noqa: F401
    canonical_json_sha256,  # noqa: F401
    verify_atomic_exchange_support,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_readiness import (
    _queue_readiness_verified,  # noqa: F401
    _uv_version_output_matches,  # noqa: F401
    _verify_binary,  # noqa: F401
    _verify_uv,  # noqa: F401
    _worker_readiness_verified,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_receipt import (
    _default_noop_components,  # noqa: F401
    make_bootstrap_receipt,  # noqa: F401
    validate_jarvis_builtin_result,  # noqa: F401
    write_bootstrap_receipt,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_replacement_provider import (
    _verify_bootstrap_replacement_provider,  # noqa: F401
    prove_bootstrap_replacement_provider,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_repository import (
    _managed_repository_payload,  # noqa: F401
    reconcile_managed_jarvis_repository,  # noqa: F401
    repair_managed_jarvis_binding,  # noqa: F401
)
from clio_relay.bootstrap_reconcile_transaction import (
    _TRANSACTION_TRANSITIONS,  # noqa: F401
    BootstrapOwnedPath,  # noqa: F401
    BootstrapOwnedPathIdentity,  # noqa: F401
    BootstrapTransactionJournal,  # noqa: F401
    BootstrapTransactionState,  # noqa: F401
)

# Back-compat surface only: the pre-split module had these as plain
# top-of-file imports, reachable (and monkeypatched) as
# ``bootstrap_reconcile.installation_info``/``.run_bounded_process``. Each
# owner module that actually calls them holds its own independent import, so
# a collaborator patch that must affect one owner's behavior has to target
# that owner module directly (see the split's test updates) -- patching the
# copy here is inert by construction, exactly like patching any other
# already-imported name from a third module.
from clio_relay.bounded_process import run_bounded_process  # noqa: F401
from clio_relay.installation import installation_info  # noqa: F401

logger = logging.getLogger(__name__)
