"""AST guards for the staged ``core_queue`` owner split."""

from __future__ import annotations

import ast
import hashlib
import importlib
import inspect
import json
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from pytest import MonkeyPatch

from clio_relay import (
    queue_artifact_lineage,
    queue_browser_attachments,
    queue_endpoints,
    queue_events,
    queue_execution_cleanup,
    queue_gateway_indexes,
    queue_gateways,
    queue_gc_storage,
    queue_idempotency,
    queue_index_migration,
    queue_index_state,
    queue_input_ingest,
    queue_job_gc,
    queue_job_gc_protections,
    queue_jobs,
    queue_layout,
    queue_lease_admission,
    queue_lease_capacity_audit,
    queue_lease_indexes,
    queue_lease_records,
    queue_leases,
    queue_legacy_audit,
    queue_legacy_output_audit,
    queue_legacy_output_codec,
    queue_order_index,
    queue_owner_session_lifecycle,
    queue_owner_session_records,
    queue_progress,
    queue_scheduler_cancel_records,
    queue_scheduler_cancel_state,
    queue_startup,
    queue_store_lock,
    queue_store_read,
    queue_store_write,
    queue_tasks,
    queue_transitions,
)
from clio_relay.browser_gateway import BrowserAttachmentRecord
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    INPUT_INGEST_POLICY_METADATA_KEY,
    ArtifactRef,
    ArtifactUse,
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    InputArtifactIngestPolicy,
    InputArtifactSpec,
    JarvisRunSpec,
    JobKind,
    JobState,
    ProgressRecord,
    RelayJob,
    RelayMcpTaskProjection,
    RelayMcpTaskRecord,
    RelayTask,
    UsedArtifactRef,
    deterministic_input_artifact_id,
    new_id,
)
from clio_relay.queue_jarvis_inputs import QueueJarvisInputsMixin
from clio_relay.queue_layout import QueueLayout

_SOURCE_ROOT = Path(__file__).parents[1] / "src" / "clio_relay"
_TESTS_ROOT = Path(__file__).parent
_NON_OWNER_QUEUE_MODULES = frozenset({"queue_management", "queue_validation"})
_OWNER_RANK = {
    "queue_context": 0,
    "queue_jarvis_inputs": 1,
    "queue_layout": 2,
    "queue_store_lock": 3,
    "queue_store_read": 4,
    "queue_store_write": 5,
    "queue_lease_records": 6,
    "queue_scheduler_cancel_records": 7,
    "queue_legacy_output_codec": 8,
    "queue_index_state": 9,
    "queue_legacy_output_audit": 10,
    "queue_legacy_output_migration": 11,
    "queue_legacy_audit": 12,
    "queue_order_index": 13,
    "queue_events": 14,
    "queue_owner_session_records": 15,
    "queue_owner_session_lifecycle": 16,
    "queue_idempotency": 17,
    "queue_endpoints": 18,
    "queue_artifact_lineage": 19,
    # queue_gateway_indexes lands here, ahead of every caller that self-calls
    # into it (queue_artifacts/queue_input_ingest/queue_jobs/queue_tasks,
    # each already landed at the time of the original per-line inventory in
    # section 1 of the design doc). Its own real dependencies are just the
    # base store/layout family (ranks 0-5); it has no forward need for any
    # of CQ7/CQ9/CQ10/CQ12/CQ14 despite those appearing in the design doc's
    # CQ16 predecessor list -- that list describes queue_gateways' and
    # queue_monitor_rules' real needs (owner-session intake, append_event,
    # get_job), not queue_gateway_indexes'. Ledger §9.3/§10.2 precedent:
    # resolve a reverse-rank self-call edge by hoisting the collaborator
    # earlier, not by re-ranking its callers.
    "queue_gateway_indexes": 20,
    "queue_artifacts": 21,
    "queue_scheduler_cancel_state": 22,
    # queue_execution_cleanup (the shard-layout/flat-to-shard-migration/
    # detection half of CQ17, typed deviation CQ17-EC-01) lands here,
    # immediately before its earliest caller. queue_jobs.write_job (CQ12,
    # already landed with TYPE_CHECKING stubs anticipating exactly this)
    # calls its _migrate_execution_cleanup_shard_unlocked/_execution_cleanup_
    # shard directly on every canonical job write, so this owner must rank
    # before queue_jobs. Its own real dependencies are just the base store/
    # layout family (ranks 2-5); the former self.get_job(...) call is
    # replaced with the shared queue_store_read.read_required_job primitive
    # (ledger §9.3 precedent), so it has no forward need for queue_jobs at
    # all. See queue_execution_cleanup.py's own module docstring for the
    # full two-owner-split account.
    "queue_execution_cleanup": 23,
    "queue_jobs": 24,
    "queue_input_ingest": 25,
    "queue_progress": 26,
    "queue_tasks": 27,
    # queue_execution_cleanup_markers (the durable-marker-mutation half of
    # CQ17: register/acknowledge/migrate_plan/stage_sidecar) lands here,
    # immediately after queue_tasks. Every one of its methods persists an
    # updated RelayTask through queue_tasks._sync_task_retention_indexes_
    # unlocked, so it must rank after queue_tasks -- which itself must rank
    # after queue_jobs (CQ12) and therefore after queue_execution_cleanup
    # above. A single combined owner cannot satisfy both "before queue_jobs"
    # and "after queue_tasks" at once, hence the CQ17-EC-01 split.
    "queue_execution_cleanup_markers": 28,
    "queue_lease_indexes": 29,
    "queue_lease_capacity_state": 30,
    "queue_lease_capacity_audit": 31,
    "queue_lease_recovery": 32,
    "queue_lease_admission": 33,
    "queue_leases": 34,
    "queue_scheduler_cancel_claims": 35,
    "queue_gateways": 36,
    "queue_browser_attachments": 37,
    "queue_monitor_rules": 38,
    # CQ18: queue_gc_storage (pure GC quarantine-tree filesystem primitives,
    # no dependency on job_gc or its protections -- appended last purely
    # because nothing else needs it earlier), queue_job_gc_protections (the
    # fail-closed eligibility gate, typed deviation CQ18-JG-01), queue_job_gc
    # (the phased trash-staging orchestration that reads the protections
    # gate -- must rank after it). See queue_job_gc_protections.py's own
    # module docstring for the split's full account.
    "queue_gc_storage": 39,
    "queue_job_gc_protections": 40,
    "queue_job_gc": 41,
    # CQ19: index discovery (the three bounded, no-history-scan migration-
    # state repairs -- schema upgrade, capacity-gate reconciliation, state
    # extension -- none has an inbound edge from any other owner) lands
    # first at 42. queue_startup (initialize plus its locked-core/
    # permission-repair helpers) lands at 43, immediately after: initialize
    # self-calls index_discovery's three methods. queue_index_migration
    # (the bounded v0.9-to-indexed migration batch driver) lands at 44 --
    # CQ19-ST-01 typed deviation, not the doc's naive "discovery/migration
    # ahead of startup" order: migrate_indexes_batch/index_migration_status
    # both self-call self.initialize() as their first line, a real,
    # pre-existing edge that requires queue_startup to land before it.
    # queue_transitions (the transition-intent applier,
    # _reconcile_transition_intents_unlocked) lands last at 45: every kind
    # branch dispatches into an already-landed owner's real mutation
    # primitive, so it must rank after all of them. See each module's own
    # docstring for the full account, including CQ19-TI-01 (the write-
    # ahead-log primitives that stay facade-resident because many earlier-
    # ranked owners already self-call them).
    "queue_index_discovery": 42,
    "queue_startup": 43,
    "queue_index_migration": 44,
    "queue_transitions": 45,
}
_OWNER_BUDGETS = {
    "queue_context": 70,
    # CQ20-JI-01: cap raised 300 -> 340. Dissolving CQ1's instance-composition
    # deviation into a real QueueJarvisInputsMixin absorbed the eight public
    # delegator bodies core_queue.py used to hold (net new lines here), and
    # ruff's line-length wrap of `self._store_adapter.storage_root / ...`
    # (longer than the old `self._store.storage_root / ...`) added a few
    # more physical lines across four methods.
    "queue_jarvis_inputs": 340,
    "queue_layout": 410,
    "queue_store_lock": 270,
    # CQ18: +2 real lines (migration_batch_paths, the shared primitive
    # queue_job_gc._trash_job_references_unlocked hoists here -- ledger
    # §9.3/§10.2 precedent -- and the not-yet-extracted index-migration
    # facade code both need it). A justified, minimal ratchet-up.
    "queue_store_read": 420,
    "queue_store_write": 230,
    "queue_lease_records": 680,
    "queue_scheduler_cancel_records": 260,
    "queue_legacy_output_codec": 500,
    "queue_index_state": 270,
    "queue_legacy_output_audit": 520,
    "queue_legacy_output_migration": 250,
    "queue_legacy_audit": 650,
    "queue_order_index": 450,
    "queue_events": 270,
    "queue_owner_session_records": 690,
    "queue_owner_session_lifecycle": 350,
    "queue_idempotency": 270,
    "queue_endpoints": 340,
    "queue_artifact_lineage": 500,
    "queue_artifacts": 220,
    "queue_scheduler_cancel_state": 450,
    "queue_execution_cleanup": 380,
    "queue_jobs": 800,
    "queue_input_ingest": 715,
    "queue_progress": 190,
    "queue_tasks": 420,
    "queue_execution_cleanup_markers": 360,
    "queue_lease_indexes": 620,
    "queue_lease_capacity_state": 490,
    "queue_lease_capacity_audit": 600,
    "queue_lease_recovery": 620,
    "queue_lease_admission": 590,
    "queue_leases": 360,
    "queue_scheduler_cancel_claims": 560,
    "queue_gateway_indexes": 540,
    "queue_gateways": 400,
    "queue_browser_attachments": 420,
    "queue_monitor_rules": 230,
    "queue_gc_storage": 280,
    "queue_job_gc_protections": 320,
    "queue_job_gc": 720,
    "queue_index_discovery": 380,
    "queue_startup": 550,
    "queue_index_migration": 720,
    "queue_transitions": 280,
}
_CQ4_CODEC_OWNERS = frozenset(
    {
        "queue_lease_records",
        "queue_scheduler_cancel_records",
    }
)
_JARVIS_INPUT_SYMBOLS = (
    "get_jarvis_package_input_contract",
    "put_jarvis_package_input_contract",
    "get_jarvis_pipeline_input_lineage",
    "get_jarvis_pipeline_input_bindings",
    "update_jarvis_pipeline_input_bindings",
    "get_jarvis_run_input_manifest",
    "put_jarvis_run_input_manifest",
    "merge_jarvis_pipeline_input_lineage",
)
_LAYOUT_METHODS = {
    # CQ20 dissolved the other four members this dict used to check
    # (``_storage_root_stat``, ``_durable_key``, ``_require_durable_record_
    # id``, ``_label_key``): each was a facade-resident single-call forward
    # whose one remaining production caller was rewired to call
    # ``queue_layout``/``queue_store_write`` module-qualified directly (two
    # of the four had zero production callers left at all). Only
    # ``_job_record_path`` survives as a facade-resident delegator -- it has
    # 26 real callers spanning the full rank range (CQ20-FA-01) -- so this
    # signature-parity check now covers just that one member.
    "_job_record_path": "job_record_path",
}


class _CodecLookupSabotage(RuntimeError):
    """Raised only when a CQ4 owner decoder lookup is live."""


class _IndexStateLookupSabotage(RuntimeError):
    """Raised only when the CQ5 owner store-read lookup is live."""


class _LegacyOutputPathLookupSabotage(RuntimeError):
    """Raised only when a CQ6 caller reaches the codec path lookup."""


class _EventIndexLookupSabotage(RuntimeError):
    """Raised only when CQ7 event append reaches the order-index owner."""


class _IdempotencyStoreLookupSabotage(RuntimeError):
    """Raised only when CQ8 idempotency reaches its store-read owner."""


class _EndpointStoreLookupSabotage(RuntimeError):
    """Raised only when CQ8 endpoint registration reaches its store-write owner."""


class _ArtifactLineageWriteLookupSabotage(RuntimeError):
    """Raised only when CQ9 submission-edge validation reaches its write owner."""


class _OwnerSessionWriteLookupSabotage(RuntimeError):
    """Raised only when CQ10 closure reaches its store-write owner."""


class _SchedulerCancelStateWriteLookupSabotage(RuntimeError):
    """Raised only when CQ11 pending-state creation reaches its store-write owner."""


class _JobsOrderIndexLookupSabotage(RuntimeError):
    """Raised only when CQ12 submission reaches the order-index owner through queue_jobs."""


class _JobsWriteLookupSabotage(RuntimeError):
    """Raised only when CQ12 submission reaches the queue_jobs write_job seam."""


class _InputIngestWriteJobLookupSabotage(RuntimeError):
    """Raised only when CQ13's begin/fail/complete paths reach queue_jobs.write_job."""


class _PutMcpTaskCompositionSabotage(RuntimeError):
    """Raised only when ``ClioCoreQueue.put_mcp_task`` resolves through the
    inherited ``QueueTasksMixin`` body (the CQ14 owner-composition proof --
    not a collaborator store-lookup sabotage)."""


class _ProgressIndexStateLookupSabotage(RuntimeError):
    """Raised only when CQ14 latest-progress reaches its index-state owner."""


class _LeaseCapacityAuditSyncLookupSabotage(RuntimeError):
    """Raised only when CQ15 repair reaches queue_lease_indexes.sync_operational_indexes
    through the ``queue_lease_capacity_audit`` owner's module-qualified lookup."""


class _LeaseAdmissionWriteJobLookupSabotage(RuntimeError):
    """Raised only when CQ15 lease acquisition (``queue_lease_admission``) reaches
    queue_jobs.write_job through its owner seam."""


class _LeaseRecoveryWriteJobLookupSabotage(RuntimeError):
    """Raised only when CQ15 stale-lease recovery (``queue_lease_recovery``) reaches
    queue_jobs.write_job through its owner seam."""


class _BrowserAttachmentCasLookupSabotage(RuntimeError):
    """Raised only when CQ16 browser-attachment CAS transitions
    (``queue_browser_attachments``) reach ``queue_gateways.write_gateway_session``
    through the owner's module-qualified collaborator-attribute lookup."""


class _GatewayBacklinkSyncLookupSabotage(RuntimeError):
    """Raised only when a CQ16 canonical gateway write (``queue_gateways``) reaches
    ``queue_gateway_indexes.sync_gateway_session_derived`` through the owner's
    module-qualified collaborator-attribute lookup."""


class _ExecutionCleanupShardReadLookupSabotage(RuntimeError):
    """Raised only when CQ17 flat-to-shard migration reaches
    ``queue_store_read.read_json_file`` through the ``queue_execution_cleanup``
    owner's module-qualified lookup."""


class _ExecutionCleanupShardWriteLookupSabotage(RuntimeError):
    """Raised only when CQ17 flat-to-shard migration reaches
    ``queue_store_write.write_json`` through the ``queue_execution_cleanup``
    owner's module-qualified lookup."""


class _JobGcProtectionsCompositionSabotage(RuntimeError):
    """Raised only when ``ClioCoreQueue._terminal_job_gc_protections``
    resolves through the inherited ``QueueJobGcProtectionsMixin`` body (the
    CQ18-JG-01 composition proof -- not a collaborator lookup sabotage)."""


class _JobGcExecutionCleanupLookupSabotage(RuntimeError):
    """Raised only when CQ18 eligibility protections reach
    ``_job_has_pending_execution_cleanup_unlocked`` through the
    ``queue_job_gc_protections`` owner's protection-owner lookup."""


class _JobGcOwnerSessionClosureLookupSabotage(RuntimeError):
    """Raised only when CQ18 eligibility protections reach
    ``get_owner_session_closed`` through the ``queue_job_gc_protections``
    owner's protection-owner lookup."""


class _JobGcStorageMoveLookupSabotage(RuntimeError):
    """Raised only when CQ18 trash-staging reaches
    ``queue_gc_storage.move_gc_path`` through the ``queue_job_gc`` owner's
    module-qualified collaborator-attribute lookup."""


class _IndexMigrationBatchPathsLookupSabotage(RuntimeError):
    """Raised only when CQ19's ``migrate_indexes_batch`` reaches
    ``queue_store_read.migration_batch_paths`` through the
    ``queue_index_migration`` owner's module-qualified lookup (the design
    row's "one domain-migration lookup")."""


class _TransitionApplierBoundedPathsLookupSabotage(RuntimeError):
    """Raised only when CQ19's transition-intent applier
    (``_reconcile_transition_intents_unlocked``) reaches
    ``queue_store_read.bounded_json_record_paths`` through the
    ``queue_transitions`` owner's module-qualified lookup (the design row's
    "one transition-applier lookup")."""


class _StartupAuditBeforeInitializationLookupSabotage(RuntimeError):
    """Raised only when CQ19's ``queue_startup.initialize`` reaches
    ``queue_legacy_audit.audit_before_initialization`` through the owner's
    module-qualified lookup (the design row's exact named seam:
    "queue_startup.queue_legacy_audit.audit_before_initialization")."""


@dataclass(frozen=True)
class _OwnerDependency:
    caller: str
    collaborator: str


def _discover_owner_manifest(source_root: Path) -> tuple[str, ...]:
    return tuple(
        sorted(
            path.stem
            for path in source_root.glob("queue_*.py")
            if path.stem not in _NON_OWNER_QUEUE_MODULES
        )
    )


_OWNER_MANIFEST = _discover_owner_manifest(_SOURCE_ROOT)


def _owner_tree(owner: str) -> ast.Module:
    return ast.parse(
        (_SOURCE_ROOT / f"{owner}.py").read_text(encoding="utf-8"),
        filename=f"{owner}.py",
    )


def _imported_owner(
    module: str | None,
    *,
    owners: tuple[str, ...] = _OWNER_MANIFEST,
) -> str | None:
    if module is None:
        return None
    normalized = module.removeprefix("clio_relay.")
    return normalized if normalized in owners else None


def _core_import_lines(tree: ast.Module) -> list[int]:
    violations: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name in {"core_queue", "clio_relay.core_queue"} for alias in node.names):
                violations.append(node.lineno)
        elif isinstance(node, ast.ImportFrom) and (
            node.module in {"core_queue", "clio_relay.core_queue"}
            or (
                node.module in {None, "clio_relay"}
                and any(alias.name == "core_queue" for alias in node.names)
            )
        ):
            violations.append(node.lineno)
    return violations


def _bare_owner_import_lines(
    tree: ast.Module,
    *,
    caller: str,
    functions_by_owner: dict[str, set[str]],
    owners: tuple[str, ...] = _OWNER_MANIFEST,
) -> list[int]:
    """Return lines that bind a cross-owner function to an unqualified name.

    Two syntaxes create the same hazard: ``from collaborator import func``
    (an ``ImportFrom``) and ``alias = collaborator.func`` (an ``Assign`` or
    ``AnnAssign``). Both bind a bare local name at import time that a
    monkeypatch on ``collaborator.func`` can no longer intercept -- F5
    (block-2 review): the original guard only walked ``ImportFrom`` and
    missed the assignment form entirely. N12 (closing-round review): the
    assignment form itself only matched a depth-1 chain
    (``collaborator.func``) -- ``alias = collaborator.SomeClass.method``
    (``queue_artifacts.py``'s real ``_require_durable_record_id =
    queue_layout.QueueLayout.require_durable_record_id``) has an
    ``ast.Attribute`` nested inside the attribute, not a bare ``ast.Name``,
    so it walked straight past. Walking the chain to its root ``Name`` (via
    ``_flatten_attribute_chain``, shared with the §4 monkeypatch audit)
    catches both depths uniformly against ``functions_by_owner``'s
    class-qualified (``"SomeClass.method"``) entries.
    """
    violations: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            collaborator = _imported_owner(node.module, owners=owners)
            if collaborator is None or collaborator == caller:
                continue
            if any(
                alias.name in functions_by_owner.get(collaborator, set()) for alias in node.names
            ):
                violations.append(node.lineno)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            if node.value is None:
                continue
            chain = _flatten_attribute_chain(node.value)
            if chain is None or len(chain) < 2:
                continue
            root, *rest = chain
            collaborator = _imported_owner(root, owners=owners)
            if collaborator is None or collaborator == caller:
                continue
            if ".".join(rest) in functions_by_owner.get(collaborator, set()):
                violations.append(node.lineno)
    return violations


def _mixin_classes(tree: ast.Module) -> list[ast.ClassDef]:
    """Return the owner's composed ``*Mixin`` class definitions.

    Every class actually mixed into ``ClioCoreQueue`` (directly, or as a base
    of another mixin) is named ``...Mixin`` in this codebase -- confirmed by
    the full class inventory across ``src/clio_relay/queue_*.py``. Plain
    record/protocol/helper classes (e.g. ``QueueLayout``, ``LeaseIndexIdentity``)
    are held by composition (``self._layout``), not MRO, so ``self.<name>()``
    can never resolve into them and they are excluded here.
    """
    return [
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name.endswith("Mixin")
    ]


def _real_method_names(class_node: ast.ClassDef) -> set[str]:
    """Return the class's directly-defined method names (real bodies only).

    ``if TYPE_CHECKING:`` stub declarations are not walked here on purpose:
    a stub documents an *expected* cross-owner edge for the type checker, it
    is never itself the real definition. Using only real definitions makes
    the manifest reflect where a name is actually implemented.
    """
    return {
        item.name
        for item in class_node.body
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _mixin_method_owner_manifest(
    owners: tuple[str, ...] = _OWNER_MANIFEST,
) -> dict[str, str]:
    """Map each unambiguous mixin method name to its one defining owner."""
    owner_by_method: dict[str, str] = {}
    ambiguous: set[str] = set()
    for owner in owners:
        for class_node in _mixin_classes(_owner_tree(owner)):
            for name in _real_method_names(class_node):
                if name in owner_by_method and owner_by_method[name] != owner:
                    ambiguous.add(name)
                owner_by_method[name] = owner
    for name in ambiguous:
        del owner_by_method[name]
    return owner_by_method


_MIXIN_METHOD_OWNERS = _mixin_method_owner_manifest()


def _self_call_edges(
    caller: str,
    tree: ast.Module,
    *,
    method_owners: dict[str, str] = _MIXIN_METHOD_OWNERS,
) -> set[_OwnerDependency]:
    """Return caller->collaborator edges for ``self.<name>(...)`` calls.

    These resolve only through the fully composed ``ClioCoreQueue`` MRO at
    runtime -- invisible to a static per-module import scan. F4 (block-2
    review): five such calls to ``self.get_job(...)`` created reverse-rank
    edges onto the later-landed ``queue_jobs`` owner, undetected until now.
    """
    dependencies: set[_OwnerDependency] = set()
    for class_node in _mixin_classes(tree):
        own_methods = _real_method_names(class_node)
        for node in ast.walk(class_node):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "self"
            ):
                continue
            name = node.func.attr
            if name in own_methods:
                continue
            collaborator = method_owners.get(name)
            if collaborator is not None and collaborator != caller:
                dependencies.add(_OwnerDependency(caller, collaborator))
    return dependencies


def _owner_dependencies() -> tuple[_OwnerDependency, ...]:
    dependencies: set[_OwnerDependency] = set()
    for caller in _OWNER_MANIFEST:
        tree = _owner_tree(caller)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                collaborator = _imported_owner(node.module)
                if collaborator is not None and collaborator != caller:
                    dependencies.add(_OwnerDependency(caller, collaborator))
                if node.module in {None, "clio_relay"}:
                    for alias in node.names:
                        if alias.name in _OWNER_MANIFEST and alias.name != caller:
                            dependencies.add(_OwnerDependency(caller, alias.name))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    collaborator = _imported_owner(alias.name)
                    if collaborator is not None and collaborator != caller:
                        dependencies.add(_OwnerDependency(caller, collaborator))
        dependencies |= _self_call_edges(caller, tree)
    return tuple(sorted(dependencies, key=lambda edge: (edge.caller, edge.collaborator)))


# --- §4 audit extension: dynamically-built (f-string/loop) monkeypatch seams ---
#
# The static `monkeypatch.setattr("clio_relay.module.attr", ...)` string-literal
# sites are covered by the design §4 table by hand. A patch target built as
# `f"{module_name}.attr"` inside a `for module_name in (...)` loop is invisible
# to that hand-audit: F1/F2 (block-2 review) found a real dead seam this way --
# `queue_legacy_audit.py` moved its real call off `clio_relay.core_queue`, but
# three test loops kept patching `clio_relay.core_queue.ensure_private_configuration_directory`,
# which either raised AttributeError (no `raising=False`) or silently patched
# nothing at all (`raising=False`). Neither is caught by the string-literal
# audit. This extractor + guard makes that class of break fail loudly here
# instead of only at a flaky/silent runtime seam.
_KNOWN_DYNAMIC_SEAM_EXEMPTIONS: frozenset[str] = frozenset()
"""Explicitly justified (module, attr) dynamic targets allowed to not resolve.

Empty by design: every dynamic loop-built monkeypatch target discovered in
``tests/`` must resolve on the real module it names. An addition here must
carry its own justification in the same change that adds it -- this is not a
default escape hatch (design doc §4: "a temporary re-export ... is never an
injection seam").
"""


def _loop_string_literals(loop: ast.For) -> tuple[str, ...] | None:
    """Return the loop's iterated string literals, or None if not all-literal."""
    if not isinstance(loop.target, ast.Name):
        return None
    if not isinstance(loop.iter, (ast.Tuple, ast.List)):
        return None
    literals: list[str] = []
    for element in loop.iter.elts:
        if not (isinstance(element, ast.Constant) and isinstance(element.value, str)):
            return None
        literals.append(element.value)
    return tuple(literals)


def _fstring_loop_variable_suffix(joined: ast.JoinedStr, *, loop_variable: str) -> str | None:
    """Return the static suffix of ``f"{loop_variable}<suffix>"``, or None."""
    if len(joined.values) != 2:
        return None
    head, tail = joined.values
    if not (
        isinstance(head, ast.FormattedValue)
        and isinstance(head.value, ast.Name)
        and head.value.id == loop_variable
    ):
        return None
    if not (isinstance(tail, ast.Constant) and isinstance(tail.value, str)):
        return None
    return tail.value


def _setattr_raising_is_false(call: ast.Call) -> bool:
    return any(
        keyword.arg == "raising"
        and isinstance(keyword.value, ast.Constant)
        and keyword.value.value is False
        for keyword in call.keywords
    )


def _dynamic_setattr_targets(tree: ast.Module) -> tuple[tuple[str, bool], ...]:
    """Return every ``(module.attr, raising)`` pair built from a string-literal
    loop feeding an f-string into ``monkeypatch.setattr(...)``."""
    targets: list[tuple[str, bool]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.For):
            continue
        literals = _loop_string_literals(node)
        if literals is None or not isinstance(node.target, ast.Name):
            continue
        loop_variable = node.target.id
        for call in ast.walk(node):
            if not (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "setattr"
                and call.args
                and isinstance(call.args[0], ast.JoinedStr)
            ):
                continue
            suffix = _fstring_loop_variable_suffix(call.args[0], loop_variable=loop_variable)
            if suffix is None:
                continue
            raising_false = _setattr_raising_is_false(call)
            targets.extend((f"{literal}{suffix}", raising_false) for literal in literals)
    return tuple(targets)


def _resolve_dotted_attribute_chain(dotted: str) -> bool:
    """Return True when ``dotted`` resolves to a real attribute.

    Imports the longest possible module prefix, then ``getattr``-walks the
    remainder. This reaches attribute chains a single ``rpartition`` cannot:
    ``clio_relay.queue_store_write.cluster_config.open_private_atomic_file``
    is not an importable module path (``cluster_config`` is an attribute
    ``queue_store_write`` re-exports, not a submodule of it) -- it only
    resolves by importing ``clio_relay.queue_store_write`` and then
    attribute-walking ``cluster_config``, ``open_private_atomic_file``.
    """
    parts = dotted.split(".")
    for split in range(len(parts), 0, -1):
        module_path = ".".join(parts[:split])
        try:
            target_object: object = importlib.import_module(module_path)
        except ImportError:
            continue
        for attribute in parts[split:]:
            if not hasattr(target_object, attribute):
                return False
            target_object = getattr(target_object, attribute)
        return True
    return False


def _resolve_dynamic_target(target: str) -> bool:
    """Return True when ``module.path.attr`` names a real, existing attribute."""
    return _resolve_dotted_attribute_chain(target)


def test_dynamic_fstring_loop_monkeypatch_targets_resolve_or_are_registered() -> None:
    """Every loop-built monkeypatch target across ``tests/`` must name a real
    attribute on the real module, or be an explicitly justified exemption.

    This is the §4 audit extension (F3, block-2 review): the hand-maintained
    string-literal patch table cannot see targets assembled as
    ``f"{module_name}.attr"`` inside a ``for module_name in (...)`` loop. Two
    sites used exactly that shape and went dead when CQ6 moved the real call
    off ``clio_relay.core_queue`` without updating them: the three loops in
    ``tests/test_fastmcp_server.py`` (F1) and the one in
    ``tests/test_service_runtime.py`` (F2).
    """
    violations: list[str] = []
    for test_file in sorted(_TESTS_ROOT.glob("test_*.py")):
        tree = ast.parse(test_file.read_text(encoding="utf-8"), filename=test_file.name)
        for target, raising_false in _dynamic_setattr_targets(tree):
            if not target.startswith("clio_relay."):
                continue
            if target in _KNOWN_DYNAMIC_SEAM_EXEMPTIONS:
                continue
            if not _resolve_dynamic_target(target):
                violations.append(f"{test_file.name}: {target} (raising=False: {raising_false})")
    assert violations == []


def test_guard_detects_a_dead_fstring_loop_seam_fixture() -> None:
    """Fixture proof the extractor+resolver catch F1/F2's exact break shape,
    including the ``raising=False`` variant that produces no test failure on
    its own (F2)."""
    source = (
        "def f(monkeypatch):\n"
        "    for module_name in (\n"
        '        "clio_relay.cluster_config",\n'
        '        "clio_relay.core_queue",\n'
        "    ):\n"
        "        monkeypatch.setattr(\n"
        '            f"{module_name}.ensure_private_configuration_directory",\n'
        "            lambda *a, **k: None,\n"
        "            raising=False,\n"
        "        )\n"
    )
    targets = _dynamic_setattr_targets(ast.parse(source))

    assert targets == (
        ("clio_relay.cluster_config.ensure_private_configuration_directory", True),
        ("clio_relay.core_queue.ensure_private_configuration_directory", True),
    )
    # cluster_config really defines the function; core_queue does not -- this
    # is the live F1/F2 seam, byte-for-byte.
    assert _resolve_dynamic_target(targets[0][0]) is True
    assert _resolve_dynamic_target(targets[1][0]) is False


# --- §4 audit extension: plain-object monkeypatch.setattr(module, "name", ...) seams ---
#
# The f-string-loop extractor above only sees a target built from a string
# literal fed through an f-string. It is blind to the far more common shape
# ``monkeypatch.setattr(some_module_or_attr_chain, "literal_name", value, ...)``
# -- exactly the shape F3 (closing-round review) found dead in
# ``tests/test_queue.py``: ``monkeypatch.setattr(core_queue_module,
# "open_private_atomic_file", open_test_file, raising=False)`` kept patching a
# facade attribute the CQ-split moved off ``core_queue`` months ago, and
# ``raising=False`` made the break invisible -- the assertion that actually
# proved the behavior lived entirely on a different, still-correct patch two
# lines later. The same review pass found this shape live-broken a second time
# in ``tests/test_jarvis_handle_first_admission.py`` (no ``raising=False`` --
# that one simply errored, but nothing had run it end-to-end since the facade
# collapse) and unnecessarily flagged in ``tests/test_browser_gateway.py``
# (``raising=False`` on an attribute that has always resolved).


def _flatten_attribute_chain(node: ast.expr) -> tuple[str, ...] | None:
    """Flatten a bare ``Name``/``Attribute`` chain root-first: ``a.b.c`` -> ("a", "b", "c").

    Returns None for anything else (string literals, calls, subscripts, ...) --
    those are either the string-literal form (hand-audited) or not a
    statically resolvable target at all.
    """
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        parts.reverse()
        return tuple(parts)
    return None


def _import_local_name_map(tree: ast.Module) -> dict[str, str]:
    """Map every name one file's imports bind to its dotted source path."""
    mapping: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    mapping[alias.asname] = alias.name
                else:
                    top = alias.name.split(".")[0]
                    mapping[top] = top
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                mapping[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return mapping


def _plain_object_setattr_targets(tree: ast.Module) -> tuple[tuple[str, bool], ...]:
    """Return every ``(dotted_target, raising_false)`` pair from a
    ``monkeypatch.setattr(<module-or-attribute-chain>, "literal_attr", value, ...)``
    call whose first argument is a bare module/attribute-chain expression
    resolvable through this file's own imports."""
    import_map = _import_local_name_map(tree)
    targets: list[tuple[str, bool]] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "setattr"
            and len(node.args) >= 2
        ):
            continue
        target_expr, attr_expr = node.args[0], node.args[1]
        if not (isinstance(attr_expr, ast.Constant) and isinstance(attr_expr.value, str)):
            continue
        chain = _flatten_attribute_chain(target_expr)
        if not chain:
            continue
        root, *rest = chain
        if root not in import_map:
            continue
        dotted = ".".join([import_map[root], *rest, attr_expr.value])
        targets.append((dotted, _setattr_raising_is_false(node)))
    return tuple(targets)


_KNOWN_RAISING_FALSE_CROSS_PLATFORM_EXEMPTIONS: dict[str, str] = {
    "clio_relay.cluster_config.os.getuid": (
        "test fakes os.name = 'posix' on Windows to exercise the POSIX "
        "ownership branch; os.getuid must be created, not just overridden"
    ),
    "clio_relay.service_runtime_connector_identity.signal.SIGKILL": (
        "POSIX-only signal constant, absent on Windows"
    ),
    "clio_relay.service_runtime.os.pidfd_open": (
        "Linux-only API (glibc >= 2.26), absent on Windows and older POSIX"
    ),
    "clio_relay.service_runtime_connector_identity.signal.pidfd_send_signal": (
        "Linux-only API (glibc >= 2.26), absent on Windows and older POSIX"
    ),
    "clio_relay.service_runtime.os.killpg": "POSIX-only process-group signal, absent on Windows",
}
"""Dotted ``module...attr`` targets where ``raising=False`` is load-bearing
because the attribute genuinely does not exist on every platform this suite
runs on. Never a default escape hatch (design doc §4: "a temporary
re-export ... is never an injection seam") -- an addition here must carry its
own one-line reason in the same change that adds it."""


def test_plain_object_monkeypatch_targets_resolve_and_raising_false_is_pinned() -> None:
    """Every ``monkeypatch.setattr(<module-or-attr-chain>, "literal", ...)`` call
    across ``tests/`` must name a real, resolvable attribute; ``raising=False``
    on a target that resolves right now is a hard error unless the target is
    pinned to the cross-platform allowlist above with a reason (F3,
    closing-round review). This is the plain-object sibling of
    ``test_dynamic_fstring_loop_monkeypatch_targets_resolve_or_are_registered``
    above -- together they cover every shape a monkeypatch target can take
    except the hand-audited string-literal path table.
    """
    unresolved: list[str] = []
    unjustified_raising_false: list[str] = []
    for test_file in sorted(_TESTS_ROOT.glob("test_*.py")):
        tree = ast.parse(test_file.read_text(encoding="utf-8"), filename=test_file.name)
        for dotted, raising_false in _plain_object_setattr_targets(tree):
            if not dotted.startswith("clio_relay."):
                continue
            exempt = dotted in _KNOWN_RAISING_FALSE_CROSS_PLATFORM_EXEMPTIONS
            resolves = _resolve_dotted_attribute_chain(dotted)
            if not resolves and not exempt:
                unresolved.append(f"{test_file.name}: {dotted}")
                continue
            if raising_false and resolves and not exempt:
                unjustified_raising_false.append(f"{test_file.name}: {dotted}")
    assert unresolved == []
    assert unjustified_raising_false == []


def test_guard_detects_a_dead_plain_object_seam_fixture() -> None:
    """Fixture proof the plain-object extractor catches F3's exact break shape:
    a real module target whose literal attribute name no longer exists."""
    source = (
        "def f(monkeypatch):\n"
        "    monkeypatch.setattr(\n"
        "        core_queue_module,\n"
        '        "open_private_atomic_file",\n'
        "        lambda *a, **k: None,\n"
        "        raising=False,\n"
        "    )\n"
    )
    tree = ast.parse(source)
    tree_with_import = ast.parse("import clio_relay.core_queue as core_queue_module\n" + source)
    targets = _plain_object_setattr_targets(tree)
    assert targets == ()  # no import in this file -> the root name is unresolvable, skipped
    targets_with_import = _plain_object_setattr_targets(tree_with_import)
    assert targets_with_import == (("clio_relay.core_queue.open_private_atomic_file", True),)
    assert _resolve_dotted_attribute_chain(targets_with_import[0][0]) is False


def test_split_owners_never_import_the_core_queue_facade() -> None:
    """An extracted owner must never create a callback edge to ``core_queue``."""
    violations = [
        f"{owner}:{line}"
        for owner in _OWNER_MANIFEST
        for line in _core_import_lines(_owner_tree(owner))
    ]
    assert violations == []


def _owner_functions_and_class_methods(owner: str) -> set[str]:
    """Return an owner's module-level function names, plus every class-level
    method as ``"ClassName.method"`` (N12: a static/class method reached
    through a depth-2 chain -- ``module.Class.method`` -- is the same bare-
    alias hazard as a bare module function; ``functions_by_owner`` must name
    it too, or a depth-2 alias like ``queue_artifacts.py``'s real
    ``_require_durable_record_id = queue_layout.QueueLayout.
    require_durable_record_id`` is invisible to the guard)."""
    tree = _owner_tree(owner)
    names = {
        node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    for class_node in tree.body:
        if not isinstance(class_node, ast.ClassDef):
            continue
        names.update(
            f"{class_node.name}.{member.name}"
            for member in class_node.body
            if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
        )
    return names


def test_split_owners_never_bare_import_cross_owner_functions() -> None:
    """Collaborator functions stay qualified by their owner module."""
    functions_by_owner = {
        owner: _owner_functions_and_class_methods(owner) for owner in _OWNER_MANIFEST
    }
    violations: list[str] = []
    for caller in _OWNER_MANIFEST:
        violations.extend(
            f"{caller}:{line} imports a cross-owner function bare"
            for line in _bare_owner_import_lines(
                _owner_tree(caller),
                caller=caller,
                functions_by_owner=functions_by_owner,
            )
        )
    assert violations == []


def _core_queue_module_tree() -> ast.Module:
    return ast.parse(
        (_SOURCE_ROOT / "core_queue.py").read_text(encoding="utf-8"),
        filename="core_queue.py",
    )


def _core_queue_alias_assignments(tree: ast.Module) -> dict[str, str]:
    """Return ``{alias_name: "owner_module.attr"}`` for every module-level
    ``NAME = owner_module.attr`` re-export in ``core_queue.py`` (skips
    anything that is not a bare ``Name = <attribute chain on an imported
    module>`` shape at module scope -- real facade logic lives in methods,
    never module-level statements)."""
    import_map = _import_local_name_map(tree)
    aliases: dict[str, str] = {}
    for node in tree.body:
        if not (isinstance(node, ast.Assign) and len(node.targets) == 1):
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        chain = _flatten_attribute_chain(node.value)
        if chain is None or len(chain) < 2:
            continue
        root, *rest = chain
        if root not in import_map:
            continue
        aliases[target.id] = ".".join([import_map[root], *rest])
    return aliases


def test_core_queue_module_aliases_each_have_a_named_production_consumer() -> None:
    """Every ``core_queue.py`` module-level re-export must have a real
    consumer (F5, closing-round review): an audit of the pre-review 75 found
    54 with zero consumers anywhere and 11 with test-only consumers -- both
    classes deleted (the 11 retargeted to their real owner module in the
    handful of tests that used them). A production consumer imports the name
    directly from ``clio_relay.core_queue`` or accesses it as an attribute of
    a module bound to ``clio_relay.core_queue``; a facade-internal consumer
    is an unqualified reference inside ``core_queue.py``'s own body. A name
    with neither is a dead re-export and must not re-accrete.
    """
    core_queue_tree = _core_queue_module_tree()
    aliases = _core_queue_alias_assignments(core_queue_tree)
    alias_def_lines = {
        node.targets[0].lineno  # type: ignore[union-attr]
        for node in core_queue_tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id in aliases
    }
    facade_internal = {
        node.id
        for node in ast.walk(core_queue_tree)
        if isinstance(node, ast.Name)
        and isinstance(node.ctx, ast.Load)
        and node.id in aliases
        and node.lineno not in alias_def_lines
    }

    production_consumers: set[str] = set()
    for py_file in sorted(_SOURCE_ROOT.glob("*.py")) + sorted(_SOURCE_ROOT.rglob("*/*.py")):
        if py_file.name == "core_queue.py":
            continue
        file_tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=py_file.name)
        for node in ast.walk(file_tree):
            if isinstance(node, ast.ImportFrom) and node.module in {
                "clio_relay.core_queue",
                "core_queue",
            }:
                production_consumers.update(
                    alias.name for alias in node.names if alias.name in aliases
                )
        module_bound = _local_names_bound_to_core_queue(file_tree)
        if module_bound:
            for node in ast.walk(file_tree):
                if (
                    isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)
                    and node.value.id in module_bound
                    and node.attr in aliases
                ):
                    production_consumers.add(node.attr)

    violations = sorted(
        name for name in aliases if name not in facade_internal and name not in production_consumers
    )
    assert violations == []


def _local_names_bound_to_core_queue(tree: ast.Module) -> set[str]:
    """Return local names one file binds to the ``core_queue`` module object
    (``import clio_relay.core_queue as X`` / ``from clio_relay import
    core_queue as X``), for resolving ``X.NAME`` attribute-access consumers."""
    bound: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in {"clio_relay.core_queue", "core_queue"}:
                    bound.add(alias.asname or alias.name.split(".")[-1])
        elif isinstance(node, ast.ImportFrom) and node.module == "clio_relay":
            for alias in node.names:
                if alias.name == "core_queue":
                    bound.add(alias.asname or "core_queue")
    return bound


def test_split_owner_dependencies_follow_the_migration_topology() -> None:
    """Every recorded owner dependency points to an earlier landed owner."""
    violations = [
        f"{edge.caller} -> {edge.collaborator}"
        for edge in _owner_dependencies()
        if _OWNER_RANK.get(edge.collaborator, len(_OWNER_RANK))
        >= _OWNER_RANK.get(edge.caller, len(_OWNER_RANK))
    ]
    assert violations == []


def test_cq4_codecs_are_store_independent() -> None:
    """Record-only CQ4 owners depend on codecs/layout only."""
    store_owners = {"queue_store_lock", "queue_store_read", "queue_store_write"}
    violations = [
        f"{edge.caller} -> {edge.collaborator}"
        for edge in _owner_dependencies()
        if edge.caller in _CQ4_CODEC_OWNERS and edge.collaborator in store_owners
    ]
    assert violations == []


def test_all_landed_owners_are_discovered_and_within_design_budgets() -> None:
    """Every landed owner is discovered and reads its cap from one table."""
    assert set(_OWNER_MANIFEST) == set(_OWNER_BUDGETS)
    for owner in _OWNER_MANIFEST:
        budget = _OWNER_BUDGETS[owner]
        line_count = len((_SOURCE_ROOT / f"{owner}.py").read_text(encoding="utf-8").splitlines())
        assert line_count <= budget, f"{owner}: {line_count} > {budget}"


def test_non_owner_exemption_and_owner_rank_are_pinned_to_the_manifest() -> None:
    """F9 (block-2 review): both exemption tables must be loud diffs, never
    silent. ``_OWNER_RANK.get(x, len(_OWNER_RANK))`` (used by the topology
    check) treats an owner missing from the rank table as "ranked last" --
    a genuine omission would pass silently instead of failing loudly. And
    nothing previously stopped ``_NON_OWNER_QUEUE_MODULES`` from growing to
    quietly exempt a real owner from every guard in this file. Both are
    pinned here against the real filesystem manifest.
    """
    # 1. The exemption set is pinned to its exact, reviewed contents -- a
    #    change to the source constant must show as a diff on this line too.
    assert frozenset({"queue_management", "queue_validation"}) == _NON_OWNER_QUEUE_MODULES

    # 2. Every exempted module is substantively non-owner: it composes no
    #    ``*Mixin`` class, so it structurally cannot join ClioCoreQueue's
    #    MRO. This is not just trusting the name is on the list.
    for module in _NON_OWNER_QUEUE_MODULES:
        assert _mixin_classes(_owner_tree(module)) == [], (
            f"{module} is exempted as non-owner but defines a *Mixin class"
        )

    # 3. Every module actually on disk is exactly either a ranked owner or an
    #    exempted non-owner -- no third, silently-ignored category.
    all_queue_modules = frozenset(path.stem for path in _SOURCE_ROOT.glob("queue_*.py"))
    assert all_queue_modules == set(_OWNER_MANIFEST) | _NON_OWNER_QUEUE_MODULES
    assert set(_OWNER_MANIFEST) & _NON_OWNER_QUEUE_MODULES == set()

    # 4. _OWNER_RANK is keyed on exactly the discovered owners -- an owner
    #    missing here would otherwise fall back to _OWNER_RANK.get(x,
    #    len(_OWNER_RANK)) in the topology check and never be flagged.
    assert set(_OWNER_RANK) == set(_OWNER_MANIFEST)

    # 5. Ranks are a dense 0..N-1 permutation: no duplicate or skipped rank.
    assert sorted(_OWNER_RANK.values()) == list(range(len(_OWNER_RANK)))


@pytest.mark.parametrize("caller", ["auxiliary", "event_audit"])
def test_cq6_legacy_output_callers_use_the_codec_path_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caller: str,
) -> None:
    """Both historical callers resolve path scans through the CQ6 owner seam."""

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _LegacyOutputPathLookupSabotage(caller)

    monkeypatch.setattr(
        queue_legacy_output_audit.queue_legacy_output_codec,
        "iter_legacy_event_paths",
        sabotage,
    )
    queue = ClioCoreQueue(tmp_path / caller)

    with pytest.raises(_LegacyOutputPathLookupSabotage, match=caller):
        if caller == "auxiliary":
            list(queue._iter_legacy_output_auxiliary_paths("legacy_output_archives"))  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        else:
            queue._audit_legacy_output_state_before_initialization()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_cq7_event_append_uses_the_order_index_increment_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Job-event append must resolve index advancement through the CQ7 owner seam."""
    queue = ClioCoreQueue(tmp_path)

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _EventIndexLookupSabotage("queue_events order-index increment lookup engaged")

    def discard_event_write(*_args: object) -> None:
        return None

    monkeypatch.setattr(queue_store_write, "write_model", discard_event_write)
    monkeypatch.setattr(queue_events.queue_order_index, "increment_job_index", sabotage)

    with pytest.raises(
        _EventIndexLookupSabotage,
        match="queue_events order-index increment lookup engaged",
    ):
        queue.append_event("job-cq7", "cq7.sabotage", "CQ7 sabotage", locked=True)


def test_cq8_idempotency_uses_its_store_read_lookup_and_typed_mixin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Idempotency resolution must execute the typed CQ8 owner and its read seam."""

    def sabotage(_path: Path) -> object:
        raise _IdempotencyStoreLookupSabotage("queue_idempotency store-read lookup engaged")

    def accept_complete_index(_root: Path) -> None:
        return None

    isolated_store_read = SimpleNamespace(
        **{
            **vars(queue_store_read),
            "read_json_document": sabotage,
        }
    )
    monkeypatch.setattr(queue_idempotency, "queue_store_read", isolated_store_read)
    isolated_index_state = SimpleNamespace(
        **{
            **vars(queue_index_state),
            "require_index_migration_complete": accept_complete_index,
        }
    )
    monkeypatch.setattr(queue_idempotency, "queue_index_state", isolated_index_state)
    queue = ClioCoreQueue(tmp_path)
    monkeypatch.setattr(queue, "initialize", lambda: None)
    monkeypatch.setattr(queue, "_require_index_migration_complete", lambda: None)
    monkeypatch.setattr(queue, "_lock", nullcontext())
    job = RelayJob(
        cluster="cluster-cq8",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="cq8-owner-read",
    )

    with pytest.raises(
        _IdempotencyStoreLookupSabotage,
        match="queue_idempotency store-read lookup engaged",
    ):
        queue.resolve_idempotent_submission(job)

    assert (
        ClioCoreQueue.resolve_idempotent_submission
        is queue_idempotency.QueueIdempotencyMixin.resolve_idempotent_submission
    )


def test_cq8_endpoint_registration_uses_its_store_write_lookup_and_typed_mixin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Endpoint registration must execute the typed CQ8 owner and its write seam."""

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _EndpointStoreLookupSabotage("queue_endpoints store-write lookup engaged")

    def accept_complete_index(_root: Path) -> None:
        return None

    def ignore_global_order(_family: str, _record_id: str) -> int:
        return 1

    def ignore_write(_path: Path, _record: object) -> None:
        return None

    def ignore_fresh_index(_endpoint: EndpointRegistration) -> None:
        return None

    isolated_store_write = SimpleNamespace(
        **{
            **vars(queue_store_write),
            "write_model": sabotage,
        }
    )
    monkeypatch.setattr(queue_endpoints, "queue_store_write", isolated_store_write)
    queue = ClioCoreQueue(tmp_path)
    isolated_index_state = SimpleNamespace(
        **{
            **vars(queue_index_state),
            "require_index_migration_complete": accept_complete_index,
        }
    )
    monkeypatch.setattr(queue_endpoints, "queue_index_state", isolated_index_state)
    monkeypatch.setattr(queue, "initialize", lambda: None)
    monkeypatch.setattr(queue, "_require_index_migration_complete", lambda: None)
    monkeypatch.setattr(queue, "_lock", nullcontext())
    monkeypatch.setattr(queue, "_ensure_global_order_entry_unlocked", ignore_global_order)
    monkeypatch.setattr(queue, "_write", ignore_write)
    monkeypatch.setattr(queue, "_index_fresh_endpoint_unlocked", ignore_fresh_index)
    observed_at = datetime(2026, 8, 15, 12, tzinfo=UTC)
    endpoint = EndpointRegistration(
        endpoint_id="endpoint-cq8",
        role=EndpointRole.WORKER,
        cluster="cluster-cq8",
        hostname="worker-cq8",
        pid=238,
        registered_at=observed_at,
        last_seen_at=observed_at,
    )

    with pytest.raises(
        _EndpointStoreLookupSabotage,
        match="queue_endpoints store-write lookup engaged",
    ):
        queue.register_endpoint(endpoint)

    assert ClioCoreQueue.register_endpoint is queue_endpoints.QueueEndpointsMixin.register_endpoint


def test_cq9_submission_edge_validation_uses_the_lineage_write_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Submission-edge validation must write through the CQ9 lineage owner seam."""
    queue = ClioCoreQueue(tmp_path)
    consumer = RelayJob(
        cluster="cluster-cq9",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="cq9-consumer",
        used_artifact_refs=[ArtifactUse(artifact_id="artifact-cq9", sha256="9" * 64)],
    )
    record = UsedArtifactRef(
        artifact_id="artifact-cq9",
        consumer_job_id=consumer.job_id,
        producer_job_id="producer-cq9",
        sequence=1,
        sha256="9" * 64,
        created_at=datetime(2026, 8, 15, 12, tzinfo=UTC),
    )

    def artifact_records(
        _job: RelayJob,
        *,
        allocate_sequences: bool,
    ) -> list[UsedArtifactRef]:
        del allocate_sequences
        return [record]

    def ignore_optional(*_args: object) -> None:
        return None

    def ignore_write(*_args: object) -> None:
        return None

    monkeypatch.setattr(
        queue,
        "_artifact_use_records_unlocked",
        artifact_records,
    )
    monkeypatch.setattr(queue, "_read_optional", ignore_optional)
    monkeypatch.setattr(queue, "_write", ignore_write)

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _ArtifactLineageWriteLookupSabotage("queue_artifact_lineage write lookup engaged")

    monkeypatch.setattr(queue_artifact_lineage, "write_immutable_use_record", sabotage)

    with pytest.raises(
        _ArtifactLineageWriteLookupSabotage,
        match="queue_artifact_lineage write lookup engaged",
    ):
        queue._ensure_artifact_use_indexes_unlocked(consumer)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert (
        ClioCoreQueue._ensure_artifact_use_indexes_unlocked  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        is queue_artifact_lineage.QueueArtifactLineageMixin._ensure_artifact_use_indexes_unlocked  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )


def test_cq10_owner_session_closure_uses_the_records_write_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Owner-session closure must persist through the CQ10 records owner seam."""
    queue = ClioCoreQueue(tmp_path)
    owner_session_id = "session-cq10"
    generation_id = "generation-cq10"
    closing_path = (
        queue._storage_root  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        / "owner_sessions"
        / f"{QueueLayout.label_key(owner_session_id, domain='owner-session')}.closing.json"
    )
    closing_path.parent.mkdir(parents=True)
    closing_path.write_text(
        json.dumps(
            {
                "owner_session_id": owner_session_id,
                "session_generation_id": generation_id,
                "closing": True,
            }
        ),
        encoding="utf-8",
    )

    def active_generation(_owner_session_id: str) -> str:
        return generation_id

    # F10 (block-2 review): CQ10's real read/write path calls
    # queue_store_read.read_json_document / queue_store_read.read_optional /
    # queue_store_write.write_model directly (module-qualified) rather than
    # through self._read_json_document / self._read_optional / self._write --
    # patching those three facade instance methods was inert scaffolding that
    # never fired. Only the closing-marker file on disk (written above) and
    # the active-generation patch below are on the real call path; the
    # write_model sabotage below is what the assertion actually exercises.
    monkeypatch.setattr(queue, "initialize", lambda: None)
    monkeypatch.setattr(queue, "_lock", nullcontext())
    monkeypatch.setattr(queue, "_owner_session_active_generation", active_generation)

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _OwnerSessionWriteLookupSabotage(
            "queue_owner_session_records store-write lookup engaged"
        )

    isolated_store_write = SimpleNamespace(
        **{
            **vars(queue_store_write),
            "write_model": sabotage,
        }
    )
    monkeypatch.setattr(
        queue_owner_session_records,
        "queue_store_write",
        isolated_store_write,
    )

    with pytest.raises(
        _OwnerSessionWriteLookupSabotage,
        match="queue_owner_session_records store-write lookup engaged",
    ):
        queue.set_owner_session_closed(
            owner_session_id,
            session_generation_id=generation_id,
        )

    assert (
        ClioCoreQueue.set_owner_session_closed
        is queue_owner_session_records.QueueOwnerSessionRecordsMixin.set_owner_session_closed
    )
    owner_status = queue_owner_session_lifecycle.QueueOwnerSessionLifecycleMixin.owner_session_generation_status  # noqa: E501
    assert ClioCoreQueue.owner_session_generation_status is owner_status


def test_cq11_scheduler_cancel_pending_creation_uses_the_state_write_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Ensuring pending scheduler-cancellation state must write through the CQ11 owner seam."""
    queue = ClioCoreQueue(tmp_path)
    job = RelayJob(
        cluster="cluster-cq11",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="cq11-pending",
    )
    queue.submit_job(job)

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _SchedulerCancelStateWriteLookupSabotage(
            "queue_scheduler_cancel_state store-write lookup engaged"
        )

    isolated_store_write = SimpleNamespace(
        **{
            **vars(queue_store_write),
            "write_model": sabotage,
        }
    )
    monkeypatch.setattr(
        queue_scheduler_cancel_state,
        "queue_store_write",
        isolated_store_write,
    )

    with pytest.raises(
        _SchedulerCancelStateWriteLookupSabotage,
        match="queue_scheduler_cancel_state store-write lookup engaged",
    ):
        queue.ensure_scheduler_cancel_pending(job.job_id, reason="operator_request")

    owner_ensure_pending = (
        queue_scheduler_cancel_state.QueueSchedulerCancelStateMixin.ensure_scheduler_cancel_pending
    )  # noqa: E501
    assert ClioCoreQueue.ensure_scheduler_cancel_pending is owner_ensure_pending


@pytest.mark.parametrize("branch", ["initial_submission", "idempotent_replay"])
def test_cq12_submit_job_uses_the_order_index_ensure_global_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    branch: str,
) -> None:
    """Job submission's global-order entry must resolve through the CQ12 owner
    seam on both designed call sites (design §3 CQ12 row): the first-ever
    submission (``queue_jobs.py:283``) and the idempotent-resubmission REPLAY
    branch that finds an already-written job for the same key
    (``queue_jobs.py:234``). F6 (block-2 review): only the first site was
    covered before this parametrization.
    """
    queue = ClioCoreQueue(tmp_path)
    job = RelayJob(
        cluster="cluster-cq12",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=f"cq12-order-{branch}",
    )
    if branch == "idempotent_replay":
        queue.submit_job(job)

    def sabotage(*_args: object, **_kwargs: object) -> int:
        raise _JobsOrderIndexLookupSabotage("queue_jobs order-index ensure_global lookup engaged")

    isolated_order_index = SimpleNamespace(
        **{
            **vars(queue_order_index),
            "ensure_global": sabotage,
        }
    )
    monkeypatch.setattr(queue_jobs, "queue_order_index", isolated_order_index)

    with pytest.raises(
        _JobsOrderIndexLookupSabotage,
        match="queue_jobs order-index ensure_global lookup engaged",
    ):
        queue.submit_job(job)

    assert ClioCoreQueue.submit_job is queue_jobs.QueueJobsMixin.submit_job


@pytest.mark.parametrize("branch", ["initial_submission", "idempotent_replay"])
def test_cq12_submit_job_uses_the_write_job_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    branch: str,
) -> None:
    """Job submission's canonical write must resolve through the CQ12
    ``write_job`` seam on both designed call sites (design §3 CQ12 row): the
    first-ever submission (``queue_jobs.py:286``) and the idempotent-
    resubmission REPLAY branch (``queue_jobs.py:239``). F6 (block-2 review):
    only the first site was covered before this parametrization.
    """
    queue = ClioCoreQueue(tmp_path)
    job = RelayJob(
        cluster="cluster-cq12",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=f"cq12-write-{branch}",
    )
    if branch == "idempotent_replay":
        queue.submit_job(job)

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _JobsWriteLookupSabotage("queue_jobs write_job lookup engaged")

    monkeypatch.setattr(queue_jobs, "write_job", sabotage)

    with pytest.raises(
        _JobsWriteLookupSabotage,
        match="queue_jobs write_job lookup engaged",
    ):
        queue.submit_job(job)

    assert ClioCoreQueue.submit_job is queue_jobs.QueueJobsMixin.submit_job


def _cq13_input_ingest_policy_metadata(
    *, max_count: int = 16, max_total_bytes: int = 4_194_304
) -> dict[str, object]:
    return {
        "owner": "clio-relay",
        "owner_session_id": "session-cq13",
        "owner_session_generation_id": "generation-cq13",
        INPUT_INGEST_POLICY_METADATA_KEY: InputArtifactIngestPolicy(
            max_file_count=max_count,
            max_total_bytes=max_total_bytes,
        ).model_dump(mode="json"),
    }


def _cq13_submit_input_ingest_job(
    queue: ClioCoreQueue,
    *,
    idempotency_key: str,
    payload: bytes,
) -> RelayJob:
    queue.prepare_owner_session_start(
        "session-cq13",
        recorded_generation_id=None,
        candidate_generation_id="generation-cq13",
    )
    spec = InputArtifactSpec(
        logical_name="input.txt",
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    return queue.submit_job(
        RelayJob(
            cluster="cluster-cq13",
            kind=JobKind.INPUT_INGEST,
            spec=spec,
            idempotency_key=idempotency_key,
            metadata=_cq13_input_ingest_policy_metadata(),
        )
    )


def _cq13_isolated_write_job_sabotage(monkeypatch: MonkeyPatch) -> None:
    """F11 (block-2 review) pattern: rebind ``queue_input_ingest``'s own
    reference to an isolated ``queue_jobs`` copy instead of mutating the
    real, shared ``queue_jobs`` module object in place."""

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _InputIngestWriteJobLookupSabotage("queue_input_ingest write_job lookup engaged")

    isolated_queue_jobs = SimpleNamespace(**{**vars(queue_jobs), "write_job": sabotage})
    monkeypatch.setattr(queue_input_ingest, "queue_jobs", isolated_queue_jobs)


def test_cq13_begin_input_ingest_uses_the_write_job_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Claiming a synchronous ingest attempt must resolve through the CQ13
    ``write_job`` seam (design §3 CQ13 row: "Patch ``queue_input_ingest.
    queue_jobs.write_job`` on begin/fail/complete paths")."""
    queue = ClioCoreQueue(tmp_path)
    job = _cq13_submit_input_ingest_job(queue, idempotency_key="cq13-begin", payload=b"payload")
    _cq13_isolated_write_job_sabotage(monkeypatch)

    with pytest.raises(
        _InputIngestWriteJobLookupSabotage,
        match="queue_input_ingest write_job lookup engaged",
    ):
        queue.begin_input_ingest(job.job_id, attempt_id=new_id("ingest_attempt"))

    assert (
        ClioCoreQueue.begin_input_ingest
        is queue_input_ingest.QueueInputIngestMixin.begin_input_ingest
    )


def test_cq13_fail_input_ingest_uses_the_write_job_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Terminalizing a failed ingest attempt must resolve through the CQ13
    ``write_job`` seam (design §3 CQ13 row)."""
    queue = ClioCoreQueue(tmp_path)
    job = _cq13_submit_input_ingest_job(queue, idempotency_key="cq13-fail", payload=b"payload")
    attempt_id = new_id("ingest_attempt")
    queue.begin_input_ingest(job.job_id, attempt_id=attempt_id)
    _cq13_isolated_write_job_sabotage(monkeypatch)

    with pytest.raises(
        _InputIngestWriteJobLookupSabotage,
        match="queue_input_ingest write_job lookup engaged",
    ):
        queue.fail_input_ingest(job.job_id, attempt_id=attempt_id, error="cq13 sabotage")

    assert (
        ClioCoreQueue.fail_input_ingest
        is queue_input_ingest.QueueInputIngestMixin.fail_input_ingest
    )


def test_cq13_complete_input_ingest_uses_the_write_job_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Idempotent ingest completion must resolve through the CQ13
    ``write_job`` seam (design §3 CQ13 row)."""
    queue = ClioCoreQueue(tmp_path)
    payload = b"payload"
    job = _cq13_submit_input_ingest_job(queue, idempotency_key="cq13-complete", payload=payload)
    attempt_id = new_id("ingest_attempt")
    running, _claimed = queue.begin_input_ingest(job.job_id, attempt_id=attempt_id)
    artifact = ArtifactRef(
        artifact_id=deterministic_input_artifact_id(job.job_id),
        job_id=job.job_id,
        uri=f"file:///{job.job_id}/input.txt",
        kind="input",
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        created_at=running.created_at,
        metadata={
            "schema_version": "clio-relay.input-artifact.v1",
            "logical_name": "input.txt",
        },
    )
    queue.reconcile_input_artifact(artifact, attempt_id=attempt_id)
    _cq13_isolated_write_job_sabotage(monkeypatch)

    with pytest.raises(
        _InputIngestWriteJobLookupSabotage,
        match="queue_input_ingest write_job lookup engaged",
    ):
        queue.complete_input_ingest(job.job_id, attempt_id=attempt_id)

    assert (
        ClioCoreQueue.complete_input_ingest
        is queue_input_ingest.QueueInputIngestMixin.complete_input_ingest
    )


def test_cq14_put_mcp_task_resolves_through_the_queue_tasks_mixin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """``put_mcp_task`` must be the inherited ``QueueTasksMixin`` method, not a
    facade-local body (design §5/§9 row: "Patch ``queue_tasks.QueueTasksMixin.
    put_mcp_task`` and assert ``ClioCoreQueue`` resolves that inherited lookup
    before wiring; FastMCP tests are acceptance only"). Unlike every other
    sabotage test in this file, this one patches the owner method itself
    rather than a collaborator store lookup -- it is the composition proof,
    demonstrably red while ``ClioCoreQueue`` still defines and executes the
    old facade body (patching ``QueueTasksMixin.put_mcp_task`` has no effect
    on a name resolved directly on ``ClioCoreQueue``'s own class dict), green
    only after that body is deleted and the mixin is composed into the MRO.
    """
    task_id = new_id("mcp_task")
    task = RelayMcpTaskRecord(
        task_id=task_id,
        job_id=task_id,
        state=JobState.QUEUED,
        projection=RelayMcpTaskProjection(
            tool_name="relay_submit_agent",
            profile="user",
            arguments={},
            initial_result={"job_id": task_id, "state": "queued", "terminal": False},
        ),
    )

    def sabotage(self: object, submitted: RelayMcpTaskRecord) -> RelayMcpTaskRecord:
        del self, submitted
        raise _PutMcpTaskCompositionSabotage("queue_tasks.QueueTasksMixin.put_mcp_task engaged")

    monkeypatch.setattr(queue_tasks.QueueTasksMixin, "put_mcp_task", sabotage)
    queue = ClioCoreQueue(tmp_path)

    with pytest.raises(
        _PutMcpTaskCompositionSabotage,
        match="queue_tasks.QueueTasksMixin.put_mcp_task engaged",
    ):
        queue.put_mcp_task(task)

    assert ClioCoreQueue.put_mcp_task is queue_tasks.QueueTasksMixin.put_mcp_task


def test_cq14_latest_job_progress_uses_the_index_state_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Latest-progress resolution must resolve its indexed count through the
    CQ14 owner seam onto the CQ5 index-state owner (standard store-lookup
    sabotage, isolated-namespace pattern -- design row: "the standard
    store-lookup sabotage for queue_progress")."""
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="cluster-cq14",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cq14-progress",
        )
    )
    queue.append_progress(ProgressRecord(job_id=job.job_id, label="cq14-progress"))

    def sabotage(*_args: object, **_kwargs: object) -> int:
        raise _ProgressIndexStateLookupSabotage("queue_progress index-state lookup engaged")

    isolated_index_state = SimpleNamespace(**{**vars(queue_index_state), "index_integer": sabotage})
    monkeypatch.setattr(queue_progress, "queue_index_state", isolated_index_state)

    with pytest.raises(
        _ProgressIndexStateLookupSabotage,
        match="queue_progress index-state lookup engaged",
    ):
        queue.latest_job_progress(job.job_id)

    assert (
        ClioCoreQueue.latest_job_progress is queue_progress.QueueProgressMixin.latest_job_progress
    )


def test_cq15_repair_uses_the_lease_indexes_sync_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Lease-operational-index repair must resolve convergence through the
    CQ15 owner seam onto ``queue_lease_indexes.sync_operational_indexes``
    (isolated-namespace pattern -- design row: "Patch ``queue_lease_capacity_
    audit.queue_lease_indexes.sync_operational_indexes``, then each
    lifecycle/recovery job-write lookup.")."""
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="cluster-cq15",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cq15-repair",
        )
    )
    lease = queue.acquire_job(job.job_id, "worker-cq15", cluster="cluster-cq15")
    assert lease is not None

    def sabotage(*_args: object, **_kwargs: object) -> object:
        raise _LeaseCapacityAuditSyncLookupSabotage(
            "queue_lease_capacity_audit lease-indexes sync lookup engaged"
        )

    isolated_lease_indexes = SimpleNamespace(
        **{**vars(queue_lease_indexes), "sync_operational_indexes": sabotage}
    )
    monkeypatch.setattr(queue_lease_capacity_audit, "queue_lease_indexes", isolated_lease_indexes)

    with pytest.raises(
        _LeaseCapacityAuditSyncLookupSabotage,
        match="queue_lease_capacity_audit lease-indexes sync lookup engaged",
    ):
        queue.repair_lease_operational_indexes()

    assert (
        ClioCoreQueue.repair_lease_operational_indexes
        is queue_lease_capacity_audit.QueueLeaseCapacityAuditMixin.repair_lease_operational_indexes
    )


def test_cq15_lease_acquisition_uses_the_write_job_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Worker-lane lease acquisition (``queue_lease_admission``) must resolve
    its canonical job write through the ``queue_jobs.write_job`` seam
    (design row: "then each lifecycle/recovery job-write lookup" -- the
    lifecycle half)."""
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="cluster-cq15",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cq15-acquire",
        )
    )

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _LeaseAdmissionWriteJobLookupSabotage(
            "queue_lease_admission write_job lookup engaged"
        )

    # queue_lease_admission calls self._write_job_unlocked(...), the inherited
    # QueueJobsMixin wrapper whose body calls the bare ``write_job`` name
    # resolved from queue_jobs.py's own module globals -- the real lookup
    # site, matching the CQ12 precedent (test_cq12_submit_job_uses_the_
    # write_job_lookup patches this exact same global).
    monkeypatch.setattr(queue_jobs, "write_job", sabotage)

    with pytest.raises(
        _LeaseAdmissionWriteJobLookupSabotage,
        match="queue_lease_admission write_job lookup engaged",
    ):
        queue.acquire_job(job.job_id, "worker-cq15", cluster="cluster-cq15")

    assert ClioCoreQueue.acquire_job is queue_lease_admission.QueueLeaseAdmissionMixin.acquire_job


def test_cq15_stale_recovery_uses_the_write_job_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Stale-lease recovery (``queue_lease_recovery``) must resolve its
    requeue/fail job write through the ``queue_jobs.write_job`` seam
    (design row: "then each lifecycle/recovery job-write lookup" -- the
    recovery half)."""
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="cluster-cq15",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cq15-recover",
        )
    )
    lease = queue.acquire_job(job.job_id, "worker-cq15", cluster="cluster-cq15", ttl_seconds=-1)
    assert lease is not None

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _LeaseRecoveryWriteJobLookupSabotage("queue_lease_recovery write_job lookup engaged")

    monkeypatch.setattr(queue_jobs, "write_job", sabotage)

    with pytest.raises(
        _LeaseRecoveryWriteJobLookupSabotage,
        match="queue_lease_recovery write_job lookup engaged",
    ):
        queue.recover_stale_job(job.job_id, cluster="cluster-cq15")

    assert queue_leases.QueueLeasesMixin.recover_stale_job is ClioCoreQueue.recover_stale_job


def _owned_ready_gateway(queue: ClioCoreQueue, *, cluster: str, name: str) -> GatewaySession:
    return queue.create_gateway_session(
        GatewaySession(
            cluster=cluster,
            name=name,
            state=GatewaySessionState.READY,
            gateway={
                "runtime_spec": {"deployment_driver": "jarvis-bound"},
                "jarvis_runtime_binding": {"schema_version": "binding"},
            },
            metadata={"owner": "clio-relay"},
        )
    )


def test_cq16_browser_attachment_cas_uses_the_write_gateway_session_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Browser-attachment CAS transitions must resolve their canonical write
    through the ``queue_gateways.write_gateway_session`` seam (isolated-namespace
    pattern -- design row: "Patch each caller owner's collaborator attribute
    for browser CAS and backlink synchronization" (the browser-CAS half))."""
    queue = ClioCoreQueue(tmp_path)
    session = _owned_ready_gateway(queue, cluster="cluster-cq16", name="paraview-cas")

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _BrowserAttachmentCasLookupSabotage(
            "queue_browser_attachments write_gateway_session lookup engaged"
        )

    isolated_gateways = SimpleNamespace(
        **{**vars(queue_gateways), "write_gateway_session": sabotage}
    )
    monkeypatch.setattr(queue_browser_attachments, "queue_gateways", isolated_gateways)

    attachment = BrowserAttachmentRecord(
        attachment_id="browser-cq16",
        state="starting",
        issued_at="2026-08-18T00:00:00+00:00",
        expires_at="2026-08-18T00:30:00+00:00",
        token_sha256="a" * 64,
        bind_port=28791,
        revocation_path="C:/runtime/browser-cq16.revoked",
    )
    intent: dict[str, object] = {
        "schema_version": "clio-relay.gateway-ownership-intent.v1",
        "state": "starting",
        "attachment_id": attachment.attachment_id,
        "owner_token": "owner-token-cq16",
        "connector_generation_id": "generation-cq16",
        "config_path": "C:/runtime/browser-cq16.json",
    }

    with pytest.raises(
        _BrowserAttachmentCasLookupSabotage,
        match="queue_browser_attachments write_gateway_session lookup engaged",
    ):
        queue.prepare_gateway_browser_attachment(
            session.session_id,
            attachment=attachment,
            browser_proxy_intent=intent,
        )

    assert (
        ClioCoreQueue.prepare_gateway_browser_attachment
        is queue_browser_attachments.QueueBrowserAttachmentsMixin.prepare_gateway_browser_attachment
    )


def test_cq16_gateway_canonical_write_uses_the_backlink_sync_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Every canonical gateway write must resolve backlink convergence through
    the ``queue_gateway_indexes.sync_gateway_session_derived`` seam (design
    row: "... and backlink synchronization" (the backlink-synchronization
    half))."""
    queue = ClioCoreQueue(tmp_path)

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _GatewayBacklinkSyncLookupSabotage("queue_gateways backlink sync lookup engaged")

    isolated_gateway_indexes = SimpleNamespace(
        **{**vars(queue_gateway_indexes), "sync_gateway_session_derived": sabotage}
    )
    monkeypatch.setattr(queue_gateways, "queue_gateway_indexes", isolated_gateway_indexes)

    with pytest.raises(
        _GatewayBacklinkSyncLookupSabotage,
        match="queue_gateways backlink sync lookup engaged",
    ):
        _owned_ready_gateway(queue, cluster="cluster-cq16", name="paraview-backlink")

    assert (
        ClioCoreQueue.create_gateway_session
        is queue_gateways.QueueGatewaysMixin.create_gateway_session
    )


def _legacy_execution_cleanup_marker(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    job_id: str,
) -> Path:
    """Plant one flat (pre-shard-migration) execution-cleanup marker on disk."""
    marker = RelayTask(job_id=job_id, name="legacy-cleanup", metadata={"cluster": cluster})
    shard = queue._execution_cleanup_shard(job_id)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    shard_path = queue._execution_cleanup_shard_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster, shard
    )
    shard_path.mkdir(parents=True, exist_ok=True)
    legacy_path = shard_path / f"{marker.task_id}.json"
    legacy_path.write_text(marker.model_dump_json(), encoding="utf-8")
    return legacy_path


def test_cq17_execution_cleanup_migration_uses_the_store_read_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Flat-to-shard execution-cleanup migration must resolve its legacy-marker
    read through the ``queue_store_read.read_json_file`` seam (isolated-namespace
    pattern -- design row: "Patch its shard read/write lookup and prove
    flat-to-shard migration delegates" (the read half)). Calls the migration
    primitive directly (not through ``scan_execution_cleanup``'s outer shard
    loop) so the sabotage exercises the migration's own read, not a later,
    unrelated read of the already-migrated marker."""
    queue = ClioCoreQueue(tmp_path)
    queue.initialize()
    cluster = "cluster-cq17"
    job_id = new_id("job")
    _legacy_execution_cleanup_marker(queue, cluster=cluster, job_id=job_id)
    shard = queue._execution_cleanup_shard(job_id)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def sabotage(*_args: object, **_kwargs: object) -> RelayTask:
        raise _ExecutionCleanupShardReadLookupSabotage(
            "queue_execution_cleanup store-read lookup engaged"
        )

    isolated_store_read = SimpleNamespace(**{**vars(queue_store_read), "read_json_file": sabotage})
    monkeypatch.setattr(queue_execution_cleanup, "queue_store_read", isolated_store_read)

    with pytest.raises(
        _ExecutionCleanupShardReadLookupSabotage,
        match="queue_execution_cleanup store-read lookup engaged",
    ):
        queue._migrate_execution_cleanup_shard_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster, shard, limit=10
        )

    assert (
        ClioCoreQueue.scan_execution_cleanup
        is queue_execution_cleanup.QueueExecutionCleanupMixin.scan_execution_cleanup
    )


def test_cq17_execution_cleanup_migration_uses_the_store_write_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Flat-to-shard execution-cleanup migration must resolve its completion
    receipt write through the ``queue_store_write.write_json`` seam
    (isolated-namespace pattern -- design row: "Patch its shard read/write
    lookup and prove flat-to-shard migration delegates" (the write half)).
    Calls the migration primitive directly for the same reason as the read
    half above."""
    queue = ClioCoreQueue(tmp_path)
    queue.initialize()
    cluster = "cluster-cq17"
    job_id = new_id("job")
    _legacy_execution_cleanup_marker(queue, cluster=cluster, job_id=job_id)
    shard = queue._execution_cleanup_shard(job_id)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _ExecutionCleanupShardWriteLookupSabotage(
            "queue_execution_cleanup store-write lookup engaged"
        )

    isolated_store_write = SimpleNamespace(**{**vars(queue_store_write), "write_json": sabotage})
    monkeypatch.setattr(queue_execution_cleanup, "queue_store_write", isolated_store_write)

    with pytest.raises(
        _ExecutionCleanupShardWriteLookupSabotage,
        match="queue_execution_cleanup store-write lookup engaged",
    ):
        queue._migrate_execution_cleanup_shard_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster, shard, limit=10
        )

    assert (
        ClioCoreQueue.scan_execution_cleanup
        is queue_execution_cleanup.QueueExecutionCleanupMixin.scan_execution_cleanup
    )


def _cq18_terminal_job(queue: ClioCoreQueue, key: str, **metadata: object) -> RelayJob:
    """Submit and terminalize one clean job for CQ18 GC-eligibility tests."""
    submitted = queue.submit_job(
        RelayJob(
            cluster="cluster-cq18",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key=key,
            metadata=metadata,
        )
    )
    return queue.update_job_state(submitted.job_id, JobState.SUCCEEDED)


def test_cq18_terminal_job_gc_protections_resolve_through_the_protections_mixin(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """``plan_terminal_job_gc`` must resolve its eligibility gate through the
    inherited ``QueueJobGcProtectionsMixin`` body, not a residual facade
    implementation (CQ18-JG-01 composition proof, mirroring CQ14's
    ``put_mcp_task`` pattern -- patching the owner method itself rather than
    a collaborator lookup)."""
    queue = ClioCoreQueue(tmp_path)
    job = _cq18_terminal_job(queue, "cq18-composition")

    def sabotage(self: object, job: RelayJob) -> list[str]:
        del self, job
        raise _JobGcProtectionsCompositionSabotage(
            "queue_job_gc_protections.QueueJobGcProtectionsMixin._terminal_job_gc_protections "
            "engaged"
        )

    monkeypatch.setattr(
        queue_job_gc_protections.QueueJobGcProtectionsMixin,
        "_terminal_job_gc_protections",
        sabotage,
    )

    with pytest.raises(
        _JobGcProtectionsCompositionSabotage,
        match="_terminal_job_gc_protections engaged",
    ):
        queue.plan_terminal_job_gc(job.job_id)

    assert ClioCoreQueue.plan_terminal_job_gc is queue_job_gc.QueueJobGcMixin.plan_terminal_job_gc
    assert (
        ClioCoreQueue._terminal_job_gc_protections  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        is queue_job_gc_protections.QueueJobGcProtectionsMixin._terminal_job_gc_protections  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )


def test_cq18_protections_use_the_execution_cleanup_pending_check_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The eligibility gate must resolve its pending-cleanup check through
    the inherited ``queue_execution_cleanup`` protection-owner lookup
    (design row: "patch each protection-owner lookup in queue_job_gc")."""
    queue = ClioCoreQueue(tmp_path)
    job = _cq18_terminal_job(queue, "cq18-execution-cleanup-protection")

    def sabotage(*_args: object, **_kwargs: object) -> bool:
        raise _JobGcExecutionCleanupLookupSabotage(
            "queue_job_gc_protections execution-cleanup pending-check lookup engaged"
        )

    monkeypatch.setattr(queue, "_job_has_pending_execution_cleanup_unlocked", sabotage)

    with pytest.raises(
        _JobGcExecutionCleanupLookupSabotage,
        match="execution-cleanup pending-check lookup engaged",
    ):
        queue.plan_terminal_job_gc(job.job_id)

    assert (
        ClioCoreQueue._terminal_job_gc_protections  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        is queue_job_gc_protections.QueueJobGcProtectionsMixin._terminal_job_gc_protections  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )


def test_cq18_protections_use_the_owner_session_closure_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The eligibility gate must resolve its owner-session closure check
    through the inherited ``queue_owner_session_records`` protection-owner
    lookup (design row: "patch each protection-owner lookup in
    queue_job_gc")."""
    queue = ClioCoreQueue(tmp_path)
    job = _cq18_terminal_job(
        queue,
        "cq18-owner-session-protection",
        owner_session_id="owner-session-cq18",
        owner_session_generation_id="generation-cq18",
    )

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _JobGcOwnerSessionClosureLookupSabotage(
            "queue_job_gc_protections owner-session closure lookup engaged"
        )

    monkeypatch.setattr(queue, "get_owner_session_closed", sabotage)

    with pytest.raises(
        _JobGcOwnerSessionClosureLookupSabotage,
        match="owner-session closure lookup engaged",
    ):
        queue.plan_terminal_job_gc(job.job_id)

    assert (
        ClioCoreQueue._terminal_job_gc_protections  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        is queue_job_gc_protections.QueueJobGcProtectionsMixin._terminal_job_gc_protections  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )


def test_cq18_job_gc_uses_the_gc_storage_move_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Terminal-job trash-staging must resolve its quarantine move through
    the ``queue_gc_storage.move_gc_path`` seam (isolated-namespace pattern --
    design row: "then queue_job_gc.queue_gc_storage.move_gc_path")."""
    queue = ClioCoreQueue(tmp_path)
    job = _cq18_terminal_job(queue, "cq18-gc-storage-move")

    def sabotage(*_args: object, **_kwargs: object) -> bool:
        raise _JobGcStorageMoveLookupSabotage("queue_job_gc queue_gc_storage.move_gc_path engaged")

    isolated_gc_storage = SimpleNamespace(**{**vars(queue_gc_storage), "move_gc_path": sabotage})
    monkeypatch.setattr(queue_job_gc, "queue_gc_storage", isolated_gc_storage)

    with pytest.raises(
        _JobGcStorageMoveLookupSabotage,
        match="queue_gc_storage.move_gc_path engaged",
    ):
        queue.collect_terminal_job(
            job.job_id,
            execute=True,
            external_quarantine_id=f"cq18-quarantine:{job.job_id}",
        )

    assert ClioCoreQueue.collect_terminal_job is queue_job_gc.QueueJobGcMixin.collect_terminal_job


def test_cq19_index_migration_uses_the_migration_batch_paths_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The bounded v0.9-to-indexed migration batch driver must resolve its
    per-family record-path listing through the ``queue_store_read.
    migration_batch_paths`` seam (isolated-namespace pattern -- design row:
    "Patch one domain-migration lookup"). A flat legacy job record (written
    directly, bypassing ``ClioCoreQueue``) keeps the fresh-seed migration
    checkpoint incomplete so the batch driver's first per-family loop
    iteration reaches the lookup instead of returning early."""
    (tmp_path / "jobs").mkdir(parents=True)
    legacy_job = RelayJob(
        cluster="cq19-migration",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key="cq19-legacy-job",
    )
    (tmp_path / "jobs" / f"{legacy_job.job_id}.json").write_text(
        legacy_job.model_dump_json(indent=2), encoding="utf-8"
    )
    queue = ClioCoreQueue(tmp_path)

    def sabotage(*_args: object, **_kwargs: object) -> tuple[list[Path], bool]:
        raise _IndexMigrationBatchPathsLookupSabotage(
            "queue_index_migration store-read migration_batch_paths lookup engaged"
        )

    isolated_store_read = SimpleNamespace(
        **{**vars(queue_store_read), "migration_batch_paths": sabotage}
    )
    monkeypatch.setattr(queue_index_migration, "queue_store_read", isolated_store_read)

    with pytest.raises(
        _IndexMigrationBatchPathsLookupSabotage,
        match="migration_batch_paths lookup engaged",
    ):
        queue.migrate_indexes_batch(batch_size=1)

    assert (
        ClioCoreQueue.migrate_indexes_batch
        is queue_index_migration.QueueIndexMigrationMixin.migrate_indexes_batch
    )


def test_cq19_transition_applier_uses_the_bounded_json_record_paths_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The transition-intent applier must resolve its journal listing through
    the ``queue_store_read.bounded_json_record_paths`` seam (isolated-
    namespace pattern -- design row: "one transition-applier lookup"). Calls
    ``_reconcile_transition_intents_unlocked`` directly (through the public
    ``reconcile_pending_transitions`` wrapper, after the queue is already
    initialized) so the sabotage exercises the applier's own journal read,
    not initialization's separate bootstrap path."""
    queue = ClioCoreQueue(tmp_path)
    queue.initialize()

    def sabotage(*_args: object, **_kwargs: object) -> list[Path]:
        raise _TransitionApplierBoundedPathsLookupSabotage(
            "queue_transitions store-read bounded_json_record_paths lookup engaged"
        )

    isolated_store_read = SimpleNamespace(
        **{**vars(queue_store_read), "bounded_json_record_paths": sabotage}
    )
    monkeypatch.setattr(queue_transitions, "queue_store_read", isolated_store_read)

    with pytest.raises(
        _TransitionApplierBoundedPathsLookupSabotage,
        match="bounded_json_record_paths lookup engaged",
    ):
        queue.reconcile_pending_transitions()

    assert (
        ClioCoreQueue._reconcile_transition_intents_unlocked  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        is queue_transitions.QueueTransitionsMixin._reconcile_transition_intents_unlocked  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    )


def test_cq19_startup_uses_the_legacy_audit_before_initialization_lookup(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Queue startup must resolve its pre-initialization legacy-state audit
    through the ``queue_legacy_audit.audit_before_initialization`` seam
    (isolated-namespace pattern -- design row's exact named CQ19 seam:
    "queue_startup.queue_legacy_audit.audit_before_initialization"). A fresh
    queue's first ``initialize()`` call always takes the exclusive-lifetime,
    missing-seal path that reaches this lookup."""

    def sabotage(*_args: object, **_kwargs: object) -> object:
        raise _StartupAuditBeforeInitializationLookupSabotage(
            "queue_startup legacy-audit audit_before_initialization lookup engaged"
        )

    isolated_legacy_audit = SimpleNamespace(
        **{**vars(queue_legacy_audit), "audit_before_initialization": sabotage}
    )
    monkeypatch.setattr(queue_startup, "queue_legacy_audit", isolated_legacy_audit)

    with pytest.raises(
        _StartupAuditBeforeInitializationLookupSabotage,
        match="audit_before_initialization lookup engaged",
    ):
        ClioCoreQueue(tmp_path).initialize()

    # CQ19-ST-02 typed deviation: unlike every other CQ19 seam, ``initialize``
    # is deliberately NOT a ``QueueStartupMixin`` method (it stays a thin
    # facade-resident dispatch to the module-level ``queue_startup.
    # initialize`` function) -- see that module's docstring for why.
    assert not hasattr(queue_startup.QueueStartupMixin, "initialize")
    assert ClioCoreQueue.initialize.__module__ == "clio_relay.core_queue"


def test_guard_rejects_absolute_bare_owner_import_fixture() -> None:
    """A top-level absolute owner import cannot bypass bare-function checks."""
    tree = ast.parse("from queue_layout import validate_canonical_access\n")
    violations = _bare_owner_import_lines(
        tree,
        caller="queue_store_read",
        functions_by_owner={"queue_layout": {"validate_canonical_access"}},
    )

    assert violations == [1]


def test_guard_rejects_bare_owner_assign_alias_fixture() -> None:
    """F5: an ``Assign``/``AnnAssign`` re-export is the same hazard as a bare
    ``ImportFrom`` -- both must be caught. This is exactly the shape of the
    live ``queue_order_index.index_integer = queue_index_state.index_integer``
    bug this widened guard found and this batch fixed."""
    functions_by_owner = {"queue_layout": {"validate_canonical_access"}}
    assign_tree = ast.parse("alias = queue_layout.validate_canonical_access\n")
    ann_assign_tree = ast.parse("alias: object = queue_layout.validate_canonical_access\n")

    assert _bare_owner_import_lines(
        assign_tree,
        caller="queue_store_read",
        functions_by_owner=functions_by_owner,
    ) == [1]
    assert _bare_owner_import_lines(
        ann_assign_tree,
        caller="queue_store_read",
        functions_by_owner=functions_by_owner,
    ) == [1]
    # A same-owner assignment (no cross-owner rebinding) is not a violation.
    assert (
        _bare_owner_import_lines(
            assign_tree,
            caller="queue_layout",
            functions_by_owner=functions_by_owner,
        )
        == []
    )


def test_guard_rejects_depth_two_bare_owner_assign_alias_fixture() -> None:
    """N12 (closing-round review): a depth-2 chain (``module.Class.method``)
    is the same bare-alias hazard as ``module.func`` and must be caught too
    -- exactly the live shape at ``queue_artifacts.py``'s ``_require_
    durable_record_id = queue_layout.QueueLayout.require_durable_record_id``,
    which the depth-1-only guard silently missed before this fix."""
    functions_by_owner = {"queue_layout": {"QueueLayout.require_durable_record_id"}}
    tree = ast.parse("alias = queue_layout.QueueLayout.require_durable_record_id\n")

    assert _bare_owner_import_lines(
        tree,
        caller="queue_artifacts",
        functions_by_owner=functions_by_owner,
    ) == [1]
    # A same-owner assignment (no cross-owner rebinding) is not a violation.
    assert (
        _bare_owner_import_lines(
            tree,
            caller="queue_layout",
            functions_by_owner=functions_by_owner,
        )
        == []
    )


def test_guard_rejects_package_from_core_import_fixture() -> None:
    """Package-from syntax cannot bypass the facade callback guard."""
    tree = ast.parse("from clio_relay import core_queue\n")

    assert _core_import_lines(tree) == [1]


def test_guard_discovers_and_rejects_unregistered_owner_fixture(tmp_path: Path) -> None:
    """A newly landed owner is scanned without manual manifest registration."""
    fixture = tmp_path / "queue_future_owner.py"
    fixture.write_text("from . import core_queue\n", encoding="utf-8")
    manifest = _discover_owner_manifest(tmp_path)

    assert manifest == ("queue_future_owner",)
    assert _core_import_lines(ast.parse(fixture.read_text(encoding="utf-8"))) == [1]


def test_queue_store_protocol_is_implemented_by_one_private_adapter(
    tmp_path: Path,
) -> None:
    """CQ3 owners share one private adapter, never the public facade surface.

    CQ20-JI-01 note: ``queue_jarvis_inputs`` used to hold its own private
    reference to this adapter (an instance-composition ``QueueJarvisInputs``
    helper, checked here as ``queue._jarvis_inputs._store is adapter``).
    Dissolving that into a real ``QueueJarvisInputsMixin`` means its methods
    now read ``self._store_adapter`` directly through the composed instance
    -- the exact same attribute this test already asserts is the queue's one
    private adapter, so there is no second reference left to check.
    """
    queue = ClioCoreQueue(tmp_path)

    adapter = queue._store_adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert adapter is not queue
    assert queue._layout._store is adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def _callable_member_names(klass: type, *, public: bool) -> set[str]:
    """Return every routine/property member name visible on ``klass``.

    N11 (closing-round review): ``inspect.isfunction(member)`` alone is
    blind to classmethods and staticmethods -- ``inspect.getmembers``
    resolves a classmethod through its descriptor into a bound ``method``
    object, which ``isfunction`` returns False for (``_scan_many`` silently
    escaped both prior copies of this filter this way). ``inspect.isroutine``
    covers functions, bound methods, staticmethods, and
    ``functools.cached_property`` uniformly; ``isinstance(member, property)``
    catches properties, which ``isroutine`` does not.
    """
    return {
        name
        for name, member in inspect.getmembers(klass)
        if name.startswith("_") != public
        and (inspect.isroutine(member) or isinstance(member, property))
    }


# CQ20's own acceptance test (design doc §3 row: "Run a reflection/MRO test
# over every public method before deleting the last body"). The full public
# surface is pinned by NAME, not just count (N11: a same-count swap -- one
# method renamed while another moved on -- would slip past a bare
# ``len(...) == 128`` check unnoticed).
_FACADE_PUBLIC_METHOD_NAMES: tuple[str, ...] = (
    "acknowledge_execution_cleanup",
    "acknowledge_job_cancellation",
    "acquire_job",
    "acquire_next_job",
    "active_job_capacity",
    "append_artifact",
    "append_event",
    "append_monitor_rule",
    "append_progress",
    "append_task",
    "append_task_event",
    "audit_lease_capacity",
    "begin_gateway_browser_attachment_revoke",
    "begin_input_ingest",
    "cancel_job_if_active",
    "claim_scheduler_cancel_attempt",
    "claim_scheduler_cancel_confirmation",
    "clear_owner_session_closing",
    "close_gateway_session",
    "collect_terminal_job",
    "complete_gateway_browser_attachment",
    "complete_input_ingest",
    "complete_scheduler_cancel_identity_scan",
    "create_gateway_session",
    "drain_events",
    "drain_task_events",
    "ensure_scheduler_cancel_pending",
    "fail_input_ingest",
    "finalize_scheduler_cancel_identities",
    "finish_gateway_browser_attachment_revoke",
    "get_artifact",
    "get_endpoint",
    "get_gateway_session",
    "get_jarvis_package_input_contract",
    "get_jarvis_pipeline_input_bindings",
    "get_jarvis_pipeline_input_lineage",
    "get_jarvis_run_input_manifest",
    "get_job",
    "get_job_tombstone",
    "get_mcp_task",
    "get_owner_session_cleanup_intent",
    "get_owner_session_closed",
    "get_scheduler_cancel_disposition",
    "get_scheduler_cancel_pending",
    "get_task",
    "get_transform_ref",
    "index_migration_status",
    "initialize",
    "job_artifact_count",
    "job_has_pending_execution_cleanup",
    "latest_job_event",
    "latest_job_progress",
    "lease_admission_capacity_snapshot",
    "list_artifact_users_page",
    "list_artifacts",
    "list_artifacts_page",
    "list_endpoints",
    "list_endpoints_page",
    "list_gateway_sessions",
    "list_gateway_sessions_page",
    "list_jobs",
    "list_jobs_page",
    "list_leases",
    "list_monitor_rules",
    "list_monitor_rules_page",
    "list_owner_session_jobs_page",
    "list_progress",
    "list_progress_page",
    "list_tasks",
    "list_tasks_page",
    "list_used_artifacts_page",
    "merge_jarvis_pipeline_input_lineage",
    "migrate_execution_cleanup_plan",
    "migrate_indexes_batch",
    "mirror_owner_session_generation_open",
    "owner_session_generation_status",
    "owner_session_is_closing",
    "plan_terminal_job_gc",
    "prepare_gateway_browser_attachment",
    "prepare_gateway_teardown_intent",
    "prepare_owner_session_start",
    "put_jarvis_package_input_contract",
    "put_jarvis_run_input_manifest",
    "put_mcp_task",
    "read_event_page",
    "readiness_info",
    "reconcile_input_artifact",
    "reconcile_pending_transitions",
    "record_scheduler_cancel_attempt",
    "record_scheduler_cancel_observation",
    "record_transform_ref",
    "recover_abandoned_input_ingests",
    "recover_stale_job",
    "recover_stale_jobs",
    "register_endpoint",
    "register_execution_cleanup",
    "register_scheduler_cancel_identity",
    "register_scheduler_cancel_identity_once",
    "release_lease",
    "renew_lease",
    "reopen_owner_session",
    "repair_lease_operational_indexes",
    "resolve_idempotent_submission",
    "scan_active_jobs",
    "scan_due_scheduler_cancellations",
    "scan_endpoints",
    "scan_execution_cleanup",
    "scan_fresh_endpoints",
    "scan_fresh_endpoints_read_only",
    "scan_gateway_sessions",
    "scan_job_leases",
    "scan_job_tasks",
    "scan_jobs",
    "scan_leases",
    "scan_monitor_rules",
    "set_owner_session_closed",
    "set_owner_session_closing",
    "stage_execution_cleanup_sidecar",
    "submit_and_acquire_job",
    "submit_job",
    "update_gateway_session",
    "update_jarvis_pipeline_input_bindings",
    "update_job_metadata",
    "update_job_state",
    "update_mcp_task_projection",
    "update_monitor_rule",
    "update_task_metadata",
    "update_task_state",
)


def test_facade_public_method_set_stays_at_the_128_method_base() -> None:
    """Private owner wiring must not grow the public queue facade."""
    public_methods = _callable_member_names(ClioCoreQueue, public=True)

    assert public_methods == set(_FACADE_PUBLIC_METHOD_NAMES)
    assert len(_FACADE_PUBLIC_METHOD_NAMES) == 128


# The two facade-legitimate exceptions to "every public method resolves to a
# composed owner" are pinned here by name, each with its own typed-deviation
# citation; an addition to core_queue.py that grows this set must be added
# to the allowlist explicitly in the same change, or the test below goes red.
_FACADE_RESIDENT_PUBLIC_METHODS: frozenset[str] = frozenset(
    {
        # CQ19-ST-02: a thin dispatch to the bare module-level
        # ``queue_startup.initialize`` function, kept off the owner mixin
        # manifest on purpose -- see that method's own docstring and
        # ``queue_startup.py``'s module docstring for the full account.
        "initialize",
        # CQ19-TI-01: a thin dispatch over the write-ahead-log primitives
        # that themselves stay facade-resident (``_recover_pending_
        # transitions_unlocked`` and friends) -- see ``queue_transitions.
        # py``'s module docstring for the full account.
        "reconcile_pending_transitions",
    }
)


# N10 (closing-round review): the private sibling of
# ``_FACADE_RESIDENT_PUBLIC_METHODS`` above, pinned at the measured
# post-F5/N6/N8/N9 set -- every private (``_``-prefixed) name still defined
# directly on ``ClioCoreQueue`` rather than inherited from a composed
# ``*Mixin`` owner. Each carries its own citation; an addition here without
# one is a regrowth of the facade this whole closing round exists to stop,
# even while ``core_queue.py`` itself stays comfortably under the 800-line
# gate (a line-count cap alone cannot see a name creeping back onto the
# class body it was supposed to leave).
_FACADE_RESIDENT_PRIVATE_METHODS: frozenset[str] = frozenset(
    {
        # Every class defines its own constructor; there is no owner to
        # inherit it from.
        "__init__",
        # CQ20-FA-01 store-adapter hub family (§15.2): `_write`/
        # `_read_optional` are called by `_QueueStoreAdapter` as
        # `self._queue._write(...)` etc, and routing through the *instance*
        # rather than the bare module function is what keeps a real,
        # live `monkeypatch.setattr(queue, "_write", ...)`-style test seam
        # working. `_job_record_path` (26 callers) and `_scan_many`
        # (inherited directly by `storage_runtime.StorageManagedQueue`)
        # are the family's other two hub members.
        "_write",
        "_read_optional",
        "_job_record_path",
        "_scan_many",
        # CQ13-IO-01/CQ19-TI-01 write-ahead-log family (queue_transitions.py
        # module docstring): each self-called from many already-landed
        # owners spanning the full rank range, so none carry an
        # architecture-guard edge regardless of rank.
        "_write_transition_intent_unlocked",
        "_recover_pending_transitions_unlocked",
        "_read_index_migration_state",
        "_write_index_migration_state",
        "_require_index_migration_complete",
        "_lease_capacity_migration_complete_unlocked",
        "_assert_input_ingest_quota_unlocked",
    }
)


def _mro_defining_class(name: str) -> type:
    """Return the first class in ``ClioCoreQueue.__mro__`` that defines ``name``."""
    for klass in ClioCoreQueue.__mro__:
        if name in vars(klass):
            return klass
    raise AssertionError(f"{name!r} is not defined anywhere in the ClioCoreQueue MRO")


def _owner_module_name(klass: type) -> str:
    """Return ``klass``'s module stem (``clio_relay.queue_x`` -> ``queue_x``)."""
    return klass.__module__.rsplit(".", 1)[-1]


def test_every_public_method_resolves_to_an_owner_mixin_or_the_pinned_allowlist() -> None:
    """CQ20's MRO proof: the facade composes owners, it does not implement them.

    Walks every public method on the pinned 128-method surface (``test_
    facade_public_method_set_stays_at_the_128_method_base``) and asserts
    each one's real defining class -- found by walking ``ClioCoreQueue.
    __mro__`` in resolution order, the same lookup Python itself performs --
    is a composed owner mixin, never ``ClioCoreQueue`` itself, except the
    two names in ``_FACADE_RESIDENT_PUBLIC_METHODS`` above. Those two are
    checked in the opposite direction: each must still genuinely be defined
    directly on ``ClioCoreQueue``, so the allowlist cannot silently go stale
    if a future slice moves one of them into a real owner. N11: "owner
    mixin" is proven by ``__module__`` membership in ``_OWNER_MANIFEST``,
    not a ``*Mixin`` name-suffix guess -- a class can rename without ever
    tripping this proof either way.
    """
    public_methods = _callable_member_names(ClioCoreQueue, public=True)
    assert public_methods == set(_FACADE_PUBLIC_METHOD_NAMES)

    for name in sorted(public_methods - _FACADE_RESIDENT_PUBLIC_METHODS):
        defining_class = _mro_defining_class(name)
        assert defining_class is not ClioCoreQueue, (
            f"{name!r} is a core_queue-defined body, not inherited from an "
            "owner mixin -- move it to its owner, or add it to "
            "_FACADE_RESIDENT_PUBLIC_METHODS with a typed-deviation citation "
            "if it is genuinely facade-legitimate."
        )
        assert _owner_module_name(defining_class) in _OWNER_MANIFEST, (
            f"{name!r} resolves to {defining_class!r}, whose module "
            f"{defining_class.__module__!r} is not a discovered owner."
        )

    for name in sorted(_FACADE_RESIDENT_PUBLIC_METHODS):
        assert _mro_defining_class(name) is ClioCoreQueue, (
            f"{name!r} is pinned in _FACADE_RESIDENT_PUBLIC_METHODS as a "
            "facade-resident deviation, but it is no longer defined on "
            "ClioCoreQueue -- remove it from the allowlist."
        )


def test_facade_private_method_set_stays_pinned() -> None:
    """N10 (closing-round review): pin the facade's private residents.

    A god-file regrowth doesn't have to add physical lines to be a
    regression -- a private method quietly re-landing directly on
    ``ClioCoreQueue`` instead of its owner mixin regrows the facade while
    ``core_queue.py`` can stay comfortably under the 800-line gate the whole
    time. This pins the exact measured set (post-F5/N6/N8/N9) by name, the
    private sibling of ``_FACADE_RESIDENT_PUBLIC_METHODS``/``test_every_
    public_method_resolves_to_an_owner_mixin_or_the_pinned_allowlist`` above.
    """
    private_methods = _callable_member_names(ClioCoreQueue, public=False)
    resident = {name for name in private_methods if _mro_defining_class(name) is ClioCoreQueue}
    assert resident == _FACADE_RESIDENT_PRIVATE_METHODS


def test_index_completeness_gate_reads_through_the_cq5_owner(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The facade completeness gate must re-resolve the CQ5 store-read lookup."""

    def sabotage(_path: Path) -> object:
        raise _IndexStateLookupSabotage("queue_index_state store-read lookup engaged")

    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "index-v1.json").write_text(
        json.dumps(
            {
                "schema_version": queue_layout.INDEX_MIGRATION_SCHEMA,
                "complete": True,
            }
        ),
        encoding="utf-8",
    )
    isolated_store_read = SimpleNamespace(
        **{
            **vars(queue_store_read),
            "read_json_document": sabotage,
        }
    )
    monkeypatch.setattr(queue_index_state, "queue_store_read", isolated_store_read)

    with pytest.raises(
        _IndexStateLookupSabotage,
        match="queue_index_state store-read lookup engaged",
    ):
        ClioCoreQueue(tmp_path)._require_index_migration_complete()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_cq5_sealed_state_preserves_duplicate_key_rejection(tmp_path: Path) -> None:
    """CQ5 keeps the sealed-state reader's strict duplicate-key contract.

    CQ20 dissolution: the facade's own ``_read_sealed_index_migration_state``
    forward is deleted; ``queue_legacy_audit.QueueLegacyAuditMixin``'s
    byte-identical ``_read_sealed_state`` (already exercised by that owner's
    own audit methods) is the one real implementation left, reached here
    through the composed ``ClioCoreQueue`` MRO.
    """
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "index-v1.json").write_text(
        '{"complete":true,"complete":false}',
        encoding="utf-8",
    )

    with pytest.raises(queue_store_lock.LegacyQueueStateError) as raised:
        ClioCoreQueue(tmp_path)._read_sealed_state()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert isinstance(raised.value.__cause__, QueueConflictError)
    assert "duplicate JSON key 'complete'" in str(raised.value.__cause__)


def test_jarvis_input_methods_are_inherited_from_the_owner_mixin() -> None:
    """CQ20-JI-01: every jarvis-input public method resolves to the real owner.

    CQ1 originally kept these eight methods as facade-resident delegators
    with byte-for-byte matching signatures over a separately composed
    ``QueueJarvisInputs`` helper. CQ20 dissolves that gap: the facade no
    longer defines any of them, so the meaningful check is no longer
    "do the two signatures match" (there is only one definition left) but
    "does each one actually resolve to ``QueueJarvisInputsMixin``, not a
    residual ``ClioCoreQueue``-defined body" -- exactly what the MRO proof
    below also checks, made explicit here per-symbol for this family.
    """
    for symbol in _JARVIS_INPUT_SYMBOLS:
        assert symbol not in vars(ClioCoreQueue), symbol
        assert symbol in vars(QueueJarvisInputsMixin), symbol
        assert getattr(ClioCoreQueue, symbol) is getattr(QueueJarvisInputsMixin, symbol), symbol


def test_layout_facade_signatures_match_the_owner() -> None:
    """CQ2 keeps every layout facade signature byte-for-byte equivalent."""
    for facade_symbol, owner_symbol in _LAYOUT_METHODS.items():
        facade_signature = inspect.signature(getattr(ClioCoreQueue, facade_symbol))
        owner_signature = inspect.signature(getattr(QueueLayout, owner_symbol))
        assert facade_signature == owner_signature, facade_symbol


def test_lease_decoder_lookup_is_owned_by_the_cq4_module(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The CQ15 ``queue_lease_indexes`` owner must re-resolve the CQ4 decoder
    lookup for the lease operational-index manifest it reads.

    Post-CQ15 update: the facade no longer keeps a bare
    ``_lease_index_identity_from_document`` re-export -- both of its former
    callers (``queue_lease_indexes._read_lease_index_identity_by_token`` and
    the recovery-intent replay path) now call ``queue_lease_records.
    lease_index_identity_from_document`` directly. The real call site moved;
    this test follows it (design §4: "patch the module containing the real
    call expression, not ... a dead facade shim").
    """
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="cluster-cq4-lease",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cq4-lease-decoder",
        )
    )
    lease = queue.acquire_job(job.job_id, "worker-cq4-lease", cluster="cluster-cq4-lease")
    assert lease is not None

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _CodecLookupSabotage("queue_lease_records decoder lookup engaged")

    monkeypatch.setattr(queue_lease_records, "lease_index_identity_from_document", sabotage)

    with pytest.raises(
        _CodecLookupSabotage,
        match="queue_lease_records decoder lookup engaged",
    ):
        queue._read_lease_index_identity(lease.lease_id)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_scheduler_decoder_lookup_is_owned_by_the_cq4_module(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """CQ19's operational-record migration must re-resolve the CQ4 owner
    decoder lookup through its real ``queue_index_migration`` call site.
    Corrected from the dead facade wrapper ``core_queue_module.
    _cancellation_requested_at`` (deleted once its only caller,
    ``_migrate_operational_record_unlocked``, moved to ``queue_index_
    migration`` and started calling ``queue_scheduler_cancel_records.
    cancellation_requested_at`` module-qualified directly) -- design doc §4's
    own rule: patch the module containing the real call expression, never a
    dead facade shim."""
    queue = ClioCoreQueue(tmp_path)
    queue.initialize()
    job = queue.submit_job(
        RelayJob(
            cluster="cq4-decoder",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="cq4-decoder-job",
            metadata={
                "cancellation_request": {
                    "schema_version": "clio-relay.cancellation-request.v1",
                    "cancel_scheduler": True,
                    "requested_at": "2026-08-15T12:00:00+00:00",
                }
            },
        )
    )

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _CodecLookupSabotage("queue_scheduler_cancel_records decoder lookup engaged")

    monkeypatch.setattr(queue_scheduler_cancel_records, "cancellation_requested_at", sabotage)

    with pytest.raises(
        _CodecLookupSabotage,
        match="queue_scheduler_cancel_records decoder lookup engaged",
    ):
        queue._migrate_operational_record_unlocked("jobs", job)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_legacy_output_decoder_lookup_is_owned_by_the_cq4_module(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The facade's legacy decoder must re-resolve the CQ4 owner lookup."""

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _CodecLookupSabotage("queue_legacy_output_codec decoder lookup engaged")

    monkeypatch.setattr(queue_legacy_output_codec, "decode_v09_legacy_output_record", sabotage)
    text = "x" * (queue_layout.RECORD_FAMILY_MAX_BYTES["events"] + 1)
    path = tmp_path / "legacy.json"
    path.write_text(
        json.dumps(
            {
                "job_id": "job-cq4",
                "seq": 1,
                "event_type": "stdout.delta",
                "message": text,
                "level": "info",
                "created_at": "2026-08-15T12:00:00Z",
                "payload": {"stream": "stdout", "text": text},
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        _CodecLookupSabotage,
        match="queue_legacy_output_codec decoder lookup engaged",
    ):
        queue_legacy_output_codec.read_v09_legacy_output_record(
            path,
            job_id="job-cq4",
            seq=1,
        )
