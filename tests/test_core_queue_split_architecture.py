"""AST guards for the staged ``core_queue`` owner split."""

from __future__ import annotations

import ast
import inspect
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from pytest import MonkeyPatch

from clio_relay import core_queue as core_queue_module
from clio_relay import (
    queue_index_state,
    queue_lease_records,
    queue_legacy_output_codec,
    queue_scheduler_cancel_records,
    queue_store_read,
)
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import JobKind
from clio_relay.queue_jarvis_inputs import QueueJarvisInputs
from clio_relay.queue_layout import QueueLayout

_SOURCE_ROOT = Path(__file__).parents[1] / "src" / "clio_relay"
_OWNER_ORDER = (
    "queue_context",
    "queue_jarvis_inputs",
    "queue_layout",
    "queue_store_lock",
    "queue_store_read",
    "queue_store_write",
    "queue_lease_records",
    "queue_scheduler_cancel_records",
    "queue_legacy_output_codec",
    "queue_index_state",
)
_CQ4_CODEC_OWNERS = {
    "queue_lease_records": 680,
    "queue_scheduler_cancel_records": 260,
    "queue_legacy_output_codec": 500,
}
_CQ5_INDEX_OWNER = {"queue_index_state": 270}
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


@dataclass(frozen=True)
class _OwnerDependency:
    caller: str
    collaborator: str


def _owner_tree(owner: str) -> ast.Module:
    return ast.parse(
        (_SOURCE_ROOT / f"{owner}.py").read_text(encoding="utf-8"),
        filename=f"{owner}.py",
    )


def _imported_owner(module: str | None) -> str | None:
    if module is None or not module.startswith("clio_relay.queue_"):
        return None
    return module.removeprefix("clio_relay.")


def _owner_dependencies() -> tuple[_OwnerDependency, ...]:
    dependencies: set[_OwnerDependency] = set()
    for caller in _OWNER_ORDER:
        for node in ast.walk(_owner_tree(caller)):
            if isinstance(node, ast.ImportFrom):
                collaborator = _imported_owner(node.module)
                if collaborator in _OWNER_ORDER and collaborator != caller:
                    dependencies.add(_OwnerDependency(caller, collaborator))
                if node.module == "clio_relay":
                    for alias in node.names:
                        if alias.name in _OWNER_ORDER and alias.name != caller:
                            dependencies.add(_OwnerDependency(caller, alias.name))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    collaborator = _imported_owner(alias.name)
                    if collaborator in _OWNER_ORDER and collaborator != caller:
                        dependencies.add(_OwnerDependency(caller, collaborator))
    return tuple(sorted(dependencies, key=lambda edge: (edge.caller, edge.collaborator)))


def test_split_owners_never_import_the_core_queue_facade() -> None:
    """An extracted owner must never create a callback edge to ``core_queue``."""
    violations: list[str] = []
    for owner in _OWNER_ORDER:
        for node in ast.walk(_owner_tree(owner)):
            if isinstance(node, ast.Import):
                if any(alias.name == "clio_relay.core_queue" for alias in node.names):
                    violations.append(f"{owner}:{node.lineno}")
            elif isinstance(node, ast.ImportFrom) and node.module == "clio_relay.core_queue":
                violations.append(f"{owner}:{node.lineno}")
    assert violations == []


def test_split_owners_never_bare_import_cross_owner_functions() -> None:
    """Collaborator functions stay qualified by their owner module."""
    functions_by_owner = {
        owner: {
            node.name
            for node in _owner_tree(owner).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for owner in _OWNER_ORDER
    }
    violations: list[str] = []
    for caller in _OWNER_ORDER:
        for node in ast.walk(_owner_tree(caller)):
            if not isinstance(node, ast.ImportFrom):
                continue
            collaborator = _imported_owner(node.module)
            if collaborator is None or collaborator == caller:
                continue
            for alias in node.names:
                if alias.name in functions_by_owner.get(collaborator, set()):
                    violations.append(
                        f"{caller}:{node.lineno} imports {collaborator}.{alias.name} bare"
                    )
    assert violations == []


def test_split_owner_dependencies_follow_the_migration_topology() -> None:
    """Every recorded owner dependency points to an earlier CQ1 owner."""
    order = {owner: index for index, owner in enumerate(_OWNER_ORDER)}
    violations = [
        f"{edge.caller} -> {edge.collaborator}"
        for edge in _owner_dependencies()
        if order[edge.collaborator] >= order[edge.caller]
    ]
    assert violations == []


def test_cq4_codecs_are_store_independent_and_within_design_budgets() -> None:
    """CQ4 owners depend on codecs/layout only and honor their planned caps."""
    store_owners = {"queue_store_lock", "queue_store_read", "queue_store_write"}
    violations = [
        f"{edge.caller} -> {edge.collaborator}"
        for edge in _owner_dependencies()
        if edge.caller in _CQ4_CODEC_OWNERS and edge.collaborator in store_owners
    ]
    assert violations == []
    for owner, budget in _CQ4_CODEC_OWNERS.items():
        line_count = len((_SOURCE_ROOT / f"{owner}.py").read_text(encoding="utf-8").splitlines())
        assert line_count <= budget, f"{owner}: {line_count} > {budget}"


def test_cq5_index_state_follows_predecessors_and_stays_within_budget() -> None:
    """CQ5 depends only on landed owners and honors its planned cap."""
    for owner, budget in _CQ5_INDEX_OWNER.items():
        line_count = len((_SOURCE_ROOT / f"{owner}.py").read_text(encoding="utf-8").splitlines())
        assert line_count <= budget, f"{owner}: {line_count} > {budget}"


def test_queue_store_protocol_is_implemented_by_the_concrete_queue(
    tmp_path: Path,
) -> None:
    """CQ3 removes the temporary facade adapter from the JARVIS owner binding."""
    queue = ClioCoreQueue(tmp_path)

    assert queue._jarvis_input_store is queue  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


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
        ClioCoreQueue(tmp_path)._read_v09_legacy_output_record(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            path,
            job_id="job-cq4",
            seq=1,
        )
