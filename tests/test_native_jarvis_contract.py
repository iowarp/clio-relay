from __future__ import annotations

from typing import cast

import pytest

import clio_relay.native_jarvis_contract as native_jarvis_contract_module
from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_user_contract,
    jarvis_user_contract_titles,
)
from clio_relay.native_jarvis_contract import (
    CLIO_KIT_JARVIS_CONTRACT_ID,
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
