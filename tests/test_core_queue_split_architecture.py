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

from clio_relay import core_queue as core_queue_module
from clio_relay import (
    queue_artifact_lineage,
    queue_endpoints,
    queue_events,
    queue_idempotency,
    queue_index_state,
    queue_input_ingest,
    queue_jobs,
    queue_lease_admission,
    queue_lease_capacity_audit,
    queue_lease_indexes,
    queue_lease_records,
    queue_leases,
    queue_legacy_output_audit,
    queue_legacy_output_codec,
    queue_order_index,
    queue_owner_session_lifecycle,
    queue_owner_session_records,
    queue_progress,
    queue_scheduler_cancel_records,
    queue_scheduler_cancel_state,
    queue_store_read,
    queue_store_write,
    queue_tasks,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    INPUT_INGEST_POLICY_METADATA_KEY,
    ArtifactRef,
    ArtifactUse,
    EndpointRegistration,
    EndpointRole,
    InputArtifactIngestPolicy,
    InputArtifactSpec,
    JarvisRunSpec,
    JobKind,
    JobState,
    ProgressRecord,
    RelayJob,
    RelayMcpTaskProjection,
    RelayMcpTaskRecord,
    UsedArtifactRef,
    deterministic_input_artifact_id,
    new_id,
)
from clio_relay.queue_jarvis_inputs import QueueJarvisInputs
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
    "queue_artifacts": 20,
    "queue_scheduler_cancel_state": 21,
    "queue_jobs": 22,
    "queue_input_ingest": 23,
    "queue_progress": 24,
    "queue_tasks": 25,
    "queue_lease_indexes": 26,
    "queue_lease_capacity_state": 27,
    "queue_lease_capacity_audit": 28,
    "queue_lease_recovery": 29,
    "queue_lease_admission": 30,
    "queue_leases": 31,
    "queue_scheduler_cancel_claims": 32,
}
_OWNER_BUDGETS = {
    "queue_context": 70,
    "queue_jarvis_inputs": 300,
    "queue_layout": 410,
    "queue_store_lock": 270,
    "queue_store_read": 410,
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
    "queue_jobs": 800,
    "queue_input_ingest": 715,
    "queue_progress": 190,
    "queue_tasks": 420,
    "queue_lease_indexes": 620,
    "queue_lease_capacity_state": 490,
    "queue_lease_capacity_audit": 600,
    "queue_lease_recovery": 620,
    "queue_lease_admission": 590,
    "queue_leases": 360,
    "queue_scheduler_cancel_claims": 560,
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
    "_storage_root_stat": "storage_root_stat",
    "_job_record_path": "job_record_path",
    "_durable_key": "durable_key",
    "_require_durable_record_id": "require_durable_record_id",
    "_label_key": "label_key",
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
    missed the assignment form entirely.
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
            value = node.value
            if not (isinstance(value, ast.Attribute) and isinstance(value.value, ast.Name)):
                continue
            collaborator = _imported_owner(value.value.id, owners=owners)
            if collaborator is None or collaborator == caller:
                continue
            if value.attr in functions_by_owner.get(collaborator, set()):
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


def _resolve_dynamic_target(target: str) -> bool:
    """Return True when ``module.path.attr`` names a real, existing attribute."""
    module_path, _, attribute = target.rpartition(".")
    if not module_path:
        return False
    try:
        module = importlib.import_module(module_path)
    except ImportError:
        return False
    return hasattr(module, attribute)


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


def test_split_owners_never_import_the_core_queue_facade() -> None:
    """An extracted owner must never create a callback edge to ``core_queue``."""
    violations = [
        f"{owner}:{line}"
        for owner in _OWNER_MANIFEST
        for line in _core_import_lines(_owner_tree(owner))
    ]
    assert violations == []


def test_split_owners_never_bare_import_cross_owner_functions() -> None:
    """Collaborator functions stay qualified by their owner module."""
    functions_by_owner = {
        owner: {
            node.name
            for node in _owner_tree(owner).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for owner in _OWNER_MANIFEST
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
        / f"{queue._label_key(owner_session_id, domain='owner-session')}.closing.json"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
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
    """CQ3 owners share one private adapter, never the public facade surface."""
    queue = ClioCoreQueue(tmp_path)

    adapter = queue._store_adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert adapter is not queue
    assert queue._layout._store is adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert queue._jarvis_inputs._store is adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_facade_public_method_set_stays_at_the_128_method_base() -> None:
    """Private owner wiring must not grow the public queue facade."""
    public_methods = {
        name
        for name, member in inspect.getmembers(ClioCoreQueue)
        if not name.startswith("_") and (inspect.isfunction(member) or isinstance(member, property))
    }

    assert len(public_methods) == 128


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
                "schema_version": core_queue_module.INDEX_MIGRATION_SCHEMA,
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
    """CQ5 keeps the sealed-state reader's strict duplicate-key contract."""
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "index-v1.json").write_text(
        '{"complete":true,"complete":false}',
        encoding="utf-8",
    )

    with pytest.raises(core_queue_module.LegacyQueueStateError) as raised:
        ClioCoreQueue(tmp_path)._read_sealed_index_migration_state()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert isinstance(raised.value.__cause__, QueueConflictError)
    assert "duplicate JSON key 'complete'" in str(raised.value.__cause__)


def test_jarvis_input_facade_signatures_match_the_owner() -> None:
    """CQ1 keeps every public facade signature byte-for-byte equivalent."""
    for symbol in _JARVIS_INPUT_SYMBOLS:
        facade_signature = inspect.signature(getattr(ClioCoreQueue, symbol))
        owner_signature = inspect.signature(getattr(QueueJarvisInputs, symbol))
        assert facade_signature == owner_signature, symbol


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
    monkeypatch: MonkeyPatch,
) -> None:
    """The facade's scheduler decoder must re-resolve the CQ4 owner lookup."""

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _CodecLookupSabotage("queue_scheduler_cancel_records decoder lookup engaged")

    monkeypatch.setattr(queue_scheduler_cancel_records, "cancellation_requested_at", sabotage)

    with pytest.raises(
        _CodecLookupSabotage,
        match="queue_scheduler_cancel_records decoder lookup engaged",
    ):
        core_queue_module._cancellation_requested_at(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            {"requested_at": "2026-08-15T12:00:00+00:00"}
        )


def test_legacy_output_decoder_lookup_is_owned_by_the_cq4_module(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """The facade's legacy decoder must re-resolve the CQ4 owner lookup."""

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _CodecLookupSabotage("queue_legacy_output_codec decoder lookup engaged")

    monkeypatch.setattr(queue_legacy_output_codec, "decode_v09_legacy_output_record", sabotage)
    text = "x" * (core_queue_module.RECORD_FAMILY_MAX_BYTES["events"] + 1)
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
