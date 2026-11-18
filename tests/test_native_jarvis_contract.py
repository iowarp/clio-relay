from __future__ import annotations

from typing import cast

import pytest

import clio_relay.native_jarvis_contract as native_jarvis_contract_module
from clio_relay.errors import ConfigurationError
from clio_relay.installation_receipt_models import NativeJarvisExecutionCapability
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_user_contract,
    jarvis_user_contract_titles,
)
from clio_relay.native_jarvis_contract import (
    CLIO_KIT_JARVIS_CONTRACT_ID,
    JARVIS_CD_NATIVE_OPERATIONS,
    _native_capability_matches_component,  # pyright: ignore[reportPrivateUsage]
    native_capabilities_agree,
    probe_clio_kit_native_execution_contract,
)


def _clio_kit_jarvis_contract_document() -> dict[str, object]:
    titles = jarvis_user_contract_titles()
    tools = [
        {
            "name": name,
            "title": titles[name],
            "description": definition["description"],
            "inputSchema": definition["inputSchema"],
            "outputSchema": definition["outputSchema"],
            "annotations": definition["annotations"],
        }
        for name, definition in sorted(jarvis_user_contract().items())
    ]
    return {
        "schema_version": "clio-kit.mcp-user-contract.v1",
        "contract_id": CLIO_KIT_JARVIS_CONTRACT_ID,
        "contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
        "tools": tools,
    }


def test_clio_kit_probe_requires_unified_progress_and_artifact_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = _clio_kit_jarvis_contract_document()

    def probe(_command: list[str], *, label: str) -> dict[str, object]:
        assert label == "clio-kit native execution contract"
        return document

    monkeypatch.setattr(
        native_jarvis_contract_module,
        "run_json_probe",
        probe,
    )

    capability = probe_clio_kit_native_execution_contract(
        ["/home/user/.local/bin/clio-kit", "mcp-server", "jarvis"]
    )

    assert capability.operations == ["jarvis_get_execution", "jarvis_run"]
    assert capability.contract_sha256 == CLIO_KIT_JARVIS_USER_CONTRACT_SHA256


def test_clio_kit_probe_rejects_execution_query_without_artifact_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = _clio_kit_jarvis_contract_document()
    tools = cast(list[dict[str, object]], document["tools"])
    query = next(tool for tool in tools if tool["name"] == "jarvis_get_execution")
    input_schema = cast(dict[str, object], query["inputSchema"])
    properties = cast(dict[str, object], input_schema["properties"])
    properties.pop("artifacts")

    def probe(_command: list[str], *, label: str) -> dict[str, object]:
        assert label == "clio-kit native execution contract"
        return document

    monkeypatch.setattr(
        native_jarvis_contract_module,
        "run_json_probe",
        probe,
    )

    with pytest.raises(ConfigurationError, match="query surface did not match"):
        probe_clio_kit_native_execution_contract(
            ["/home/user/.local/bin/clio-kit", "mcp-server", "jarvis"]
        )


_RETIRED_OPERATION = "execution_store.resolve_service_runtime_authority"


def _jarvis_cd_capability(operations: list[str]) -> NativeJarvisExecutionCapability:
    return NativeJarvisExecutionCapability(operations=sorted(operations))


def test_jarvis_cd_contract_no_longer_requires_the_runtime_authority_operation() -> None:
    """Relay no longer calls ExecutionStore.resolve_service_runtime_authority."""
    assert _RETIRED_OPERATION not in JARVIS_CD_NATIVE_OPERATIONS
    current = _jarvis_cd_capability(list(JARVIS_CD_NATIVE_OPERATIONS))

    assert _native_capability_matches_component(current, component_name="jarvis-cd")


def test_jarvis_cd_receipt_that_lists_the_retired_operation_still_passes() -> None:
    """An installation bootstrapped before the retirement keeps passing."""
    older = _jarvis_cd_capability([*JARVIS_CD_NATIVE_OPERATIONS, _RETIRED_OPERATION])
    current = _jarvis_cd_capability(list(JARVIS_CD_NATIVE_OPERATIONS))

    assert _native_capability_matches_component(older, component_name="jarvis-cd")
    assert native_capabilities_agree(current, older)
    assert native_capabilities_agree(older, current)


def test_jarvis_cd_contract_still_refuses_other_operation_changes() -> None:
    """Only the retired operation is ignored; any other difference still fails."""
    current = _jarvis_cd_capability(list(JARVIS_CD_NATIVE_OPERATIONS))
    missing = _jarvis_cd_capability(list(JARVIS_CD_NATIVE_OPERATIONS[1:]))
    extra = _jarvis_cd_capability([*JARVIS_CD_NATIVE_OPERATIONS, "pipeline.extra"])

    assert not _native_capability_matches_component(missing, component_name="jarvis-cd")
    assert not _native_capability_matches_component(extra, component_name="jarvis-cd")
    assert not native_capabilities_agree(missing, current)
    assert not native_capabilities_agree(extra, current)
