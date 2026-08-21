"""Mirror-identity coverage for the mcp_call runner's owner modules.

``jarvis-packages/clio_relay/clio_relay/mcp_call/runner.py`` (iowarp/clio-relay
#231/#775 decomposition wave 3) re-exports its public surface from twenty
owner modules that live at the ``clio_relay`` top level and one shared
reach-back registry, ``clio_relay._mcp_call_runner_facade``. They live there
-- not nested under ``mcp_call/`` -- because ``clio_relay.mcp_call`` is
force-included into the wheel but is not part of an editable dev install, so
each is deliberately vendored byte-identical into
``jarvis-packages/clio_relay/clio_relay/`` for the standalone JARVIS worker
deployment tree: the same precedent
``test_process_containment.py::test_embedded_containment_source_is_an_exact_isolated_runtime_mirror``
and ``test_bounded_payload.py::test_the_worker_vendored_copy_is_an_exact_mirror``
already established for ``process_containment.py``/``bounded_payload.py``.
"""

from __future__ import annotations

from pathlib import Path

from pytest import mark

_OWNER_MODULE_NAMES = (
    "_mcp_call_runner_facade",
    "bounded_file_io",
    "clio_kit_runtime_identity",
    "clio_kit_wheel_archive",
    "constants",
    "jarvis_artifact_documents",
    "jarvis_cd_lock_binding",
    "jarvis_execution_query",
    "jarvis_native_execution_documents",
    "params_and_manifest",
    "process_environment",
    "progress_bridge",
    "protocol_messages",
    "python_console_distribution",
    "python_external_distribution",
    "result_document",
    "server_artifact_identity",
    "session_runtime",
    "stdio_io",
    "wheel_private_launch",
    "wheel_snapshot_identity",
)


@mark.parametrize("module_name", _OWNER_MODULE_NAMES)
def test_owner_module_vendored_copy_is_an_exact_mirror(module_name: str) -> None:
    root = Path(__file__).parents[1]
    source = root / "src" / "clio_relay" / f"{module_name}.py"
    embedded = root / "jarvis-packages" / "clio_relay" / "clio_relay" / f"{module_name}.py"

    assert source.is_file(), f"missing canonical owner module: {source}"
    assert embedded.is_file(), f"missing vendored worker copy: {embedded}"
    assert embedded.read_bytes() == source.read_bytes()
