"""AST guards for the staged ``core_queue`` owner split."""

from __future__ import annotations

import ast
import inspect
from dataclasses import dataclass
from pathlib import Path

from clio_relay.core_queue import ClioCoreQueue
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


def test_queue_store_protocol_is_implemented_by_the_concrete_queue(
    tmp_path: Path,
) -> None:
    """CQ3 removes the temporary facade adapter from the JARVIS owner binding."""
    queue = ClioCoreQueue(tmp_path)

    assert queue._jarvis_input_store is queue  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


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
