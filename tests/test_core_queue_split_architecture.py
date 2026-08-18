"""AST guards for the staged ``core_queue`` owner split."""

from __future__ import annotations

import ast
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
    queue_lease_records,
    queue_legacy_output_audit,
    queue_legacy_output_codec,
    queue_owner_session_lifecycle,
    queue_owner_session_records,
    queue_scheduler_cancel_records,
    queue_store_read,
    queue_store_write,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    ArtifactUse,
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    RelayJob,
    UsedArtifactRef,
)
from clio_relay.queue_jarvis_inputs import QueueJarvisInputs
from clio_relay.queue_layout import QueueLayout

_SOURCE_ROOT = Path(__file__).parents[1] / "src" / "clio_relay"
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
}
_OWNER_BUDGETS = {
    "queue_context": 70,
    "queue_jarvis_inputs": 300,
    "queue_layout": 410,
    "queue_store_lock": 270,
    "queue_store_read": 350,
    "queue_store_write": 230,
    "queue_lease_records": 680,
    "queue_scheduler_cancel_records": 260,
    "queue_legacy_output_codec": 500,
    "queue_index_state": 270,
    "queue_legacy_output_audit": 520,
    "queue_legacy_output_migration": 210,
    "queue_legacy_audit": 650,
    "queue_order_index": 450,
    "queue_events": 270,
    "queue_owner_session_records": 690,
    "queue_owner_session_lifecycle": 350,
    "queue_idempotency": 270,
    "queue_endpoints": 340,
    "queue_artifact_lineage": 500,
    "queue_artifacts": 220,
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
    violations: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        collaborator = _imported_owner(node.module, owners=owners)
        if collaborator is None or collaborator == caller:
            continue
        if any(alias.name in functions_by_owner.get(collaborator, set()) for alias in node.names):
            violations.append(node.lineno)
    return violations


def _owner_dependencies() -> tuple[_OwnerDependency, ...]:
    dependencies: set[_OwnerDependency] = set()
    for caller in _OWNER_MANIFEST:
        for node in ast.walk(_owner_tree(caller)):
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
    return tuple(sorted(dependencies, key=lambda edge: (edge.caller, edge.collaborator)))


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
    saved: dict[Path, object] = {}

    def read_closing(path: Path) -> object:
        if path == closing_path:
            return {
                "owner_session_id": owner_session_id,
                "session_generation_id": generation_id,
                "closing": True,
            }
        return None

    def active_generation(_owner_session_id: str) -> str:
        return generation_id

    def read_optional(path: Path, _model: type[object]) -> object | None:
        return saved.get(path)

    def save_write(path: Path, record: object) -> None:
        saved[path] = record

    monkeypatch.setattr(queue, "initialize", lambda: None)
    monkeypatch.setattr(queue, "_lock", nullcontext())
    monkeypatch.setattr(queue, "_read_json_document", read_closing)
    monkeypatch.setattr(queue, "_owner_session_active_generation", active_generation)
    monkeypatch.setattr(queue, "_read_optional", read_optional)
    monkeypatch.setattr(queue, "_write", save_write)

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


def test_guard_rejects_absolute_bare_owner_import_fixture() -> None:
    """A top-level absolute owner import cannot bypass bare-function checks."""
    tree = ast.parse("from queue_layout import validate_canonical_access\n")
    violations = _bare_owner_import_lines(
        tree,
        caller="queue_store_read",
        functions_by_owner={"queue_layout": {"validate_canonical_access"}},
    )

    assert violations == [1]


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


def test_lease_decoder_lookup_is_owned_by_the_cq4_module(monkeypatch: MonkeyPatch) -> None:
    """The facade's lease decoder must re-resolve the CQ4 owner lookup."""

    def sabotage(*_args: object, **_kwargs: object) -> None:
        raise _CodecLookupSabotage("queue_lease_records decoder lookup engaged")

    monkeypatch.setattr(queue_lease_records, "lease_index_identity_from_document", sabotage)
    document = {
        "schema_version": "clio-relay.lease-operational-index.v2",
        "lease_id": "lease-cq4",
        "job_id": "job-cq4",
        "endpoint_id": "endpoint-cq4",
        "cluster": "cluster-cq4",
        "job_kind": JobKind.JARVIS.value,
        "expires_at": datetime(2026, 8, 15, 12, tzinfo=UTC).isoformat(),
    }

    with pytest.raises(
        _CodecLookupSabotage,
        match="queue_lease_records decoder lookup engaged",
    ):
        core_queue_module._lease_index_identity_from_document(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            document,
            label="CQ4 lease",
        )


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
