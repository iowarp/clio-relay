"""Cross-repository checks for clio-kit's shipped locked-server contracts."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Any, cast

import pytest

import clio_relay.remote_mcp as remote_mcp
from clio_relay.errors import ConfigurationError
from clio_relay.installation import (
    ComponentArtifactIdentity,
    probe_persistent_uv_tool_identity,
    write_install_receipt,
)
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    CLIO_KIT_JARVIS_USER_WIRE_SHA256,
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_runtime_identity,
    jarvis_user_contract,
)
from clio_relay.remote_mcp import (
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_ARTIFACT_SHA256_BY_ID,
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ARTIFACT_BY_ID,
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID,
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256,
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID,
    CLIO_KIT_SCIENTIFIC_CATALOG_USER_WIRE_SHA256_BY_ID,
    CLIO_KIT_SPACK_USER_ARTIFACT_SHA256_BY_ID,
    CLIO_KIT_SPACK_USER_CONTRACT_ARTIFACT_BY_ID,
    CLIO_KIT_SPACK_USER_CONTRACT_ID,
    CLIO_KIT_SPACK_USER_CONTRACT_SHA256,
    CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID,
    CLIO_KIT_SPACK_USER_WHEEL_VERSION,
    CLIO_KIT_SPACK_USER_WIRE_SHA256_BY_ID,
    RemoteMcpToolSchema,
    remote_mcp_schema_digest,
)

JSON = dict[str, Any]
CONTRACT_INDEX_PATH = "clio_kit/_mcp_contracts/index.json"
CONTRACT_INDEX_SCHEMA = "clio-kit.mcp-user-contract-index.v1"
CONTRACT_SCHEMA = "clio-kit.mcp-user-contract.v1"
CONTRACT_CANONICALIZATION = "json-sort-keys-compact-utf8-v1"
CONTRACT_PROJECTION = "mcp-agent-tool-schema-v1"
MAX_CONTRACT_BYTES = 4 * 1024 * 1024
MAX_PROBE_OUTPUT_BYTES = 16 * 1024 * 1024
EXPECTED_CONTRACTS = {
    "clio-kit-jarvis-user-v3.4": {
        "server_name": "jarvis",
        "artifact": "jarvis-user-v3.4.json",
        "contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
        "tool_names": {
            "jarvis_add_step",
            "jarvis_create_pipeline",
            "jarvis_describe",
            "jarvis_edit_step",
            "jarvis_get_execution",
            "jarvis_run",
        },
    },
    "clio-kit-jarvis-user-v3.3": {
        "server_name": "jarvis",
        "artifact": "jarvis-user-v3.3.json",
        "contract_sha256": "0993ee9b2ee9b3c2b021a3967d9221199c3a6be50d726d4b125812e6b1148115",
        "tool_names": {
            "jarvis_add_step",
            "jarvis_create_pipeline",
            "jarvis_describe",
            "jarvis_edit_step",
            "jarvis_get_execution",
            "jarvis_run",
        },
    },
    "clio-kit-jarvis-user-v3.2": {
        "server_name": "jarvis",
        "artifact": "jarvis-user-v3.2.json",
        "contract_sha256": "12f6d349c9d44d8ce3594943dcd4018ec9b6e01ebb0e59d468bb1bb783a1ad5d",
        "tool_names": {
            "jarvis_add_step",
            "jarvis_create_pipeline",
            "jarvis_describe",
            "jarvis_edit_step",
            "jarvis_get_execution",
            "jarvis_run",
        },
    },
    "clio-kit-jarvis-user-v3.1": {
        "server_name": "jarvis",
        "artifact": "jarvis-user-v3.1.json",
        "contract_sha256": "adc7756025fbcc90b0695bd4eaac00bda5c6cff4eb2f248fd7be263bd90b9b8b",
        "tool_names": {
            "jarvis_add_step",
            "jarvis_create_pipeline",
            "jarvis_describe",
            "jarvis_edit_step",
            "jarvis_get_execution",
            "jarvis_run",
        },
    },
    "clio-kit-slurm-user-v3": {
        "server_name": "slurm",
        "artifact": "slurm-user-v3.json",
        "contract_sha256": "8557f6dbbf5d88ca0a617e06581056d61a363e21ec7fac01f8e31f65e66736a8",
        "tool_names": {
            "slurm_cancel",
            "slurm_cluster",
            "slurm_describe",
            "slurm_list",
            "slurm_submit",
        },
    },
    "clio-kit-spack-user-v2": {
        "server_name": "spack",
        "artifact": "spack-user-v2.json",
        "contract_sha256": CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID["clio-kit-spack-user-v2"],
        "tool_names": {"spack_find", "spack_install", "spack_locate"},
    },
    "clio-kit-spack-user-v2.1": {
        "server_name": "spack",
        "artifact": "spack-user-v2.1.json",
        "contract_sha256": CLIO_KIT_SPACK_USER_CONTRACT_SHA256,
        "tool_names": {"spack_find", "spack_install", "spack_locate"},
    },
    "clio-kit-scientific-catalog-user-v1.1": {
        "server_name": "scientific-catalog",
        "artifact": "scientific-catalog-user-v1.1.json",
        "contract_sha256": CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256,
        "tool_names": {
            "scientific_dataset_describe",
            "scientific_dataset_search",
        },
    },
    "clio-kit-scientific-catalog-user-v1": {
        "server_name": "scientific-catalog",
        "artifact": "scientific-catalog-user-v1.json",
        "contract_sha256": CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID[
            "clio-kit-scientific-catalog-user-v1"
        ],
        "tool_names": {
            "scientific_dataset_describe",
            "scientific_dataset_search",
        },
    },
    "clio-kit-jarvis-user-v3": {
        "server_name": "jarvis",
        "artifact": "jarvis-user-v3.json",
        "contract_sha256": "c70e350d919e0f3fa0c116d7eaf861e23b4087a18a06b2704ddbf7384f8d1f82",
        "tool_names": {
            "jarvis_add_step",
            "jarvis_create_pipeline",
            "jarvis_describe",
            "jarvis_edit_step",
            "jarvis_get_execution",
            "jarvis_run",
        },
    },
}


def _verified_locked_jarvis_runtime() -> dict[str, object]:
    """Return complete receipt evidence for the relay's built-in JARVIS child."""
    expectation = jarvis_cd_lock_binding_expectation()
    return {
        "schema_version": "clio-kit.locked-server.v4",
        "server_name": "jarvis",
        "locked_runtime_verified": True,
        "jarvis_cd_lock_binding": {
            "schema_version": "clio-relay.jarvis-cd-lock-binding.v1",
            "dependency": "jarvis-cd",
            "verified": True,
            "error": None,
            "expected_version": expectation["version"],
            "expected_url": expectation["url"],
            "expected_sha256": expectation["sha256"],
            "observed_version": expectation["version"],
            "observed_source_url": expectation["url"],
            "observed_wheel_url": expectation["url"],
            "observed_wheel_sha256": expectation["sha256"],
            "jarvis_mcp_package_entry_count": 1,
            "resolved_dependency_entry_count": 1,
            "observed_resolved_dependency_entries": [{"name": "jarvis-cd"}],
            "metadata_requirement_entry_count": 1,
            "observed_metadata_requirement_entries": [
                {"name": "jarvis-cd", "url": expectation["url"]}
            ],
            "observed_metadata_requirement_urls": [expectation["url"]],
            "package_entry_count": 1,
            "wheel_entry_count": 1,
        },
    }


ACTIVE_CONTRACT_IDS = frozenset(EXPECTED_CONTRACTS) - {
    "clio-kit-jarvis-user-v3",
    "clio-kit-jarvis-user-v3.1",
    "clio-kit-jarvis-user-v3.2",
    "clio-kit-jarvis-user-v3.3",
    "clio-kit-scientific-catalog-user-v1",
    "clio-kit-spack-user-v2",
}
UV_TOOL_PROBE_VERSION = "0.0.0"


def _build_uv_tool_layout_probe_wheel(tmp_path: Path) -> Path:
    """Build a dependency-free wheel for exercising uv tool layout and provenance."""
    distribution = "clio_kit"
    package = "clio_kit_probe"
    dist_info = f"{distribution}-{UV_TOOL_PROBE_VERSION}.dist-info"
    wheel = tmp_path / f"{distribution}-{UV_TOOL_PROBE_VERSION}-py3-none-any.whl"
    members = {
        f"{package}/__init__.py": (
            b"def main() -> None:\n"
            b'    """Run the uv tool layout probe entry point."""\n'
            b"    return None\n"
        ),
        f"{dist_info}/METADATA": (
            "Metadata-Version: 2.1\n"
            "Name: clio-kit\n"
            f"Version: {UV_TOOL_PROBE_VERSION}\n"
            "Summary: Dependency-free clio-relay uv tool layout probe\n"
            "\n"
        ).encode(),
        f"{dist_info}/WHEEL": (
            b"Wheel-Version: 1.0\n"
            b"Generator: clio-relay-test\n"
            b"Root-Is-Purelib: true\n"
            b"Tag: py3-none-any\n"
            b"\n"
        ),
        f"{dist_info}/entry_points.txt": (
            f"[console_scripts]\nclio-kit = {package}:main\n"
        ).encode(),
    }
    record_path = f"{dist_info}/RECORD"
    record_rows = [
        f"{name},sha256={base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b'=').decode('ascii')},{len(payload)}"
        for name, payload in sorted(members.items())
    ]
    members[record_path] = ("\n".join([*record_rows, f"{record_path},,"]) + "\n").encode()
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in sorted(members.items()):
            archive.writestr(name, payload)
    return wheel


@pytest.fixture(scope="module")
def clio_kit_wheel() -> Path:
    """Return the exact external wheel used for cross-repository verification."""
    configured = os.getenv("CLIO_RELAY_CLIO_KIT_WHEEL")
    if configured is not None:
        wheel = Path(configured).expanduser().resolve(strict=True)
        if not wheel.is_file() or wheel.suffix != ".whl":
            raise AssertionError("CLIO_RELAY_CLIO_KIT_WHEEL must name one built wheel")
    else:
        sibling_dist = Path(__file__).resolve().parents[2] / "clio-kit" / "dist"
        wheels = sorted(sibling_dist.glob("clio_kit-*.whl"))
        if len(wheels) != 1:
            raise AssertionError(
                "set CLIO_RELAY_CLIO_KIT_WHEEL to the exact clio-kit release wheel; "
                f"found {len(wheels)} sibling build artifacts"
            )
        wheel = wheels[0].resolve(strict=True)
    expected_sha256 = os.getenv("CLIO_RELAY_CLIO_KIT_WHEEL_SHA256")
    if expected_sha256 is not None:
        assert hashlib.sha256(wheel.read_bytes()).hexdigest() == expected_sha256
    assert f"-{CLIO_KIT_SPACK_USER_WHEEL_VERSION}-" in wheel.name
    return wheel


@pytest.fixture(scope="module")
def shipped_contracts(clio_kit_wheel: Path) -> dict[str, JSON]:
    """Load and cryptographically verify clio-kit's wheel contract artifacts."""
    return _load_shipped_contracts(clio_kit_wheel)


def test_relay_contract_pins_match_clio_kit_wheel_artifacts(
    shipped_contracts: dict[str, JSON],
) -> None:
    """Bind relay constants and local JARVIS definitions to canonical artifacts."""
    assert set(shipped_contracts) == set(EXPECTED_CONTRACTS)
    for contract_id, expected in EXPECTED_CONTRACTS.items():
        artifact = shipped_contracts[contract_id]
        assert artifact["contract_sha256"] == expected["contract_sha256"]
        assert set(cast(list[str], artifact["tool_names"])) == expected["tool_names"]

    jarvis_tools = _tools_by_name(shipped_contracts["clio-kit-jarvis-user-v3.4"])
    artifact_projection = {
        name: {
            "description": tool.get("description"),
            "inputSchema": tool["inputSchema"],
            "outputSchema": tool.get("outputSchema"),
            "annotations": tool.get("annotations"),
        }
        for name, tool in jarvis_tools.items()
    }
    assert jarvis_user_contract() == artifact_projection

    assert "jarvis_remove_step" not in jarvis_tools
    add_step_input = cast(JSON, jarvis_tools["jarvis_add_step"]["inputSchema"])
    add_step_properties = cast(JSON, add_step_input["properties"])
    assert "do_configure" not in add_step_properties
    assert "canonical setting names exactly" in str(add_step_properties["config"]["description"])
    edit_input = cast(JSON, jarvis_tools["jarvis_edit_step"]["inputSchema"])
    edit_properties = cast(JSON, edit_input["properties"])
    assert cast(JSON, edit_properties["operation"])["enum"] == ["edit", "remove"]
    run_input = cast(JSON, jarvis_tools["jarvis_run"]["inputSchema"])
    run_properties = cast(JSON, run_input["properties"])
    assert "execution_id" in run_properties
    assert "wait" not in run_properties

    describe_input = cast(JSON, jarvis_tools["jarvis_describe"]["inputSchema"])
    describe_properties = cast(JSON, describe_input["properties"])
    assert set(describe_properties) == {
        "target",
        "package_name",
        "query",
        "page_size",
        "cursor",
        "pipeline_id",
        "step_id",
        "include_yaml",
    }
    assert cast(JSON, describe_properties["target"])["enum"] == [
        "packages",
        "package_search",
        "package",
        "pipeline",
        "step",
    ]
    assert describe_properties["page_size"] == {
        "default": 10,
        "description": (
            "Maximum summary matches returned by target='package_search'; bounded to 25."
        ),
        "maximum": 25,
        "minimum": 1,
        "type": "integer",
    }
    query = cast(JSON, describe_properties["query"])
    query_string = next(
        cast(JSON, option)
        for option in cast(list[object], query["anyOf"])
        if isinstance(option, dict) and cast(JSON, option).get("type") == "string"
    )
    assert query_string == {"maxLength": 256, "minLength": 1, "type": "string"}
    cursor = cast(JSON, describe_properties["cursor"])
    cursor_string = next(
        cast(JSON, option)
        for option in cast(list[object], cursor["anyOf"])
        if isinstance(option, dict) and cast(JSON, option).get("type") == "string"
    )
    assert cursor_string == {"maxLength": 1024, "minLength": 1, "type": "string"}

    query = jarvis_tools["jarvis_get_execution"]
    query_input = cast(JSON, query["inputSchema"])
    assert query_input["required"] == ["pipeline_id", "execution_id"]
    query_properties = cast(JSON, query_input["properties"])
    assert set(query_properties) == {
        "pipeline_id",
        "execution_id",
        "include_progress",
        "include_service_runtimes",
        "artifacts",
    }
    assert query_properties["include_progress"] == {"default": True, "type": "boolean"}
    artifact_selector = cast(JSON, query_properties["artifacts"])
    artifact_query = next(
        cast(JSON, option)
        for option in cast(list[object], artifact_selector["anyOf"])
        if isinstance(option, dict) and cast(JSON, option).get("type") == "object"
    )
    artifact_query_properties = cast(JSON, artifact_query["properties"])
    assert set(artifact_query_properties) == {
        "package_id",
        "role",
        "state",
        "artifact_id",
        "page_size",
        "cursor",
    }
    assert artifact_query_properties["page_size"] == {
        "default": 50,
        "description": "Maximum artifacts to return in this page.",
        "maximum": 100,
        "minimum": 1,
        "type": "integer",
    }
    query_output = cast(JSON, query["outputSchema"])
    assert query_output["additionalProperties"] is False
    assert set(cast(list[str], query_output["required"])) == {
        "schema_version",
        "pipeline_id",
        "execution_id",
        "execution_handle",
        "execution_record",
        "runtime_metadata",
        "progress",
        "artifact_page",
        "service_runtimes",
    }
    assert query_output["properties"]["schema_version"] == {
        "const": "clio-kit.jarvis-execution.v2",
        "type": "string",
    }
    assert query_properties["include_service_runtimes"] == {
        "default": False,
        "type": "boolean",
    }
    assert shipped_contracts["clio-kit-jarvis-user-v3.4"]["wire_sha256"] == (
        CLIO_KIT_JARVIS_USER_WIRE_SHA256
    )

    spack_tools = _tools_by_name(shipped_contracts[CLIO_KIT_SPACK_USER_CONTRACT_ID])
    assert "spack_load" not in spack_tools
    locate_output = cast(JSON, spack_tools["spack_locate"]["outputSchema"])
    locate_properties = cast(JSON, locate_output["properties"])
    assert locate_properties["load_spec"] == {"type": "string"}
    assert "load_spec" in cast(list[str], locate_output["required"])
    assert "No matches is a successful result" in cast(
        str, spack_tools["spack_find"]["description"]
    )
    assert "structured not_installed error" in cast(str, spack_tools["spack_locate"]["description"])

    legacy_spack_tools = _tools_by_name(shipped_contracts["clio-kit-spack-user-v2"])
    assert set(legacy_spack_tools) == set(spack_tools)

    scientific_tools = _tools_by_name(
        shipped_contracts[CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ID]
    )
    assert set(scientific_tools) == {
        "scientific_dataset_describe",
        "scientific_dataset_search",
    }
    assert all(
        cast(JSON, tool["annotations"])["readOnlyHint"] is True
        for tool in scientific_tools.values()
    )
    describe_output = cast(JSON, scientific_tools["scientific_dataset_describe"]["outputSchema"])
    describe_properties = cast(JSON, describe_output["properties"])
    dataset_properties = cast(JSON, cast(JSON, describe_properties["dataset"])["properties"])
    assert "dataset_descriptor" in cast(list[str], describe_output["required"])
    assert describe_properties["dataset_descriptor"] == dataset_properties["descriptor"]

    legacy_scientific_tools = _tools_by_name(
        shipped_contracts["clio-kit-scientific-catalog-user-v1"]
    )
    assert set(legacy_scientific_tools) == set(scientific_tools)


@pytest.mark.parametrize("contract_id", sorted(CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID))
def test_relay_vendors_exact_spack_contract_artifacts(contract_id: str) -> None:
    """Keep current and compatibility Spack artifacts immutable in relay wheels."""
    artifact_name = CLIO_KIT_SPACK_USER_CONTRACT_ARTIFACT_BY_ID[contract_id]
    path = Path(remote_mcp.__file__).with_name("_contracts") / artifact_name
    payload = path.read_bytes()
    artifact = _json_object(payload, label=contract_id)
    tools = cast(list[JSON], artifact["tools"])

    assert (
        hashlib.sha256(payload).hexdigest()
        == CLIO_KIT_SPACK_USER_ARTIFACT_SHA256_BY_ID[contract_id]
    )
    assert artifact["contract_id"] == contract_id
    assert artifact["contract_sha256"] == CLIO_KIT_SPACK_USER_CONTRACT_SHA256_BY_ID[contract_id]
    assert artifact["wire_sha256"] == CLIO_KIT_SPACK_USER_WIRE_SHA256_BY_ID[contract_id]
    assert (
        hashlib.sha256(_canonical_json(_contract_projection(tools))).hexdigest()
        == artifact["contract_sha256"]
    )
    assert hashlib.sha256(_canonical_json({"tools": tools})).hexdigest() == artifact["wire_sha256"]


@pytest.mark.parametrize(
    "contract_id", sorted(CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID)
)
def test_relay_vendors_exact_scientific_catalog_contract_artifacts(
    contract_id: str,
) -> None:
    """Keep current and compatibility catalog artifacts immutable in relay wheels."""
    artifact_name = CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_ARTIFACT_BY_ID[contract_id]
    path = Path(remote_mcp.__file__).with_name("_contracts") / artifact_name
    payload = path.read_bytes()
    artifact = _json_object(payload, label=contract_id)
    tools = cast(list[JSON], artifact["tools"])

    assert (
        hashlib.sha256(payload).hexdigest()
        == CLIO_KIT_SCIENTIFIC_CATALOG_USER_ARTIFACT_SHA256_BY_ID[contract_id]
    )
    assert artifact["contract_id"] == contract_id
    assert (
        artifact["contract_sha256"]
        == CLIO_KIT_SCIENTIFIC_CATALOG_USER_CONTRACT_SHA256_BY_ID[contract_id]
    )
    assert (
        artifact["wire_sha256"] == CLIO_KIT_SCIENTIFIC_CATALOG_USER_WIRE_SHA256_BY_ID[contract_id]
    )
    assert (
        hashlib.sha256(_canonical_json(_contract_projection(tools))).hexdigest()
        == artifact["contract_sha256"]
    )
    assert hashlib.sha256(_canonical_json({"tools": tools})).hexdigest() == artifact["wire_sha256"]


def test_install_receipt_exposes_verified_locked_jarvis_dependency_edge(
    tmp_path: Path,
) -> None:
    locked_server_runtime = _verified_locked_jarvis_runtime()
    receipt_path = tmp_path / "install-receipt.json"
    receipt = write_install_receipt(
        install_spec="checkout",
        path=receipt_path,
        component_artifacts={
            "clio-kit": ComponentArtifactIdentity(
                distribution="clio-kit",
                install_spec="clio-kit.whl",
                requested_source="wheel",
                runtime_command=["clio-kit", "mcp-server", "jarvis"],
                locked_server_runtime=locked_server_runtime,
            )
        },
    )

    runtime_identity = jarvis_mcp_runtime_identity(receipt)
    persisted = json.loads(receipt_path.read_text(encoding="utf-8"))

    assert runtime_identity["locked_server_runtime_verified"] is True
    assert runtime_identity["locked_server_runtime"] == locked_server_runtime
    assert (
        persisted["component_artifacts"]["clio-kit"]["locked_server_runtime"]
        == locked_server_runtime
    )

    receipt_runtime = cast(
        dict[str, object],
        receipt.component_artifacts["clio-kit"].locked_server_runtime,
    )
    binding = cast(dict[str, object], receipt_runtime["jarvis_cd_lock_binding"])
    binding["resolved_dependency_entry_count"] = 0
    invalid_runtime_identity = jarvis_mcp_runtime_identity(receipt)
    assert invalid_runtime_identity["locked_server_runtime_verified"] is False
    assert invalid_runtime_identity["artifact_identity_verified"] is False

    binding["resolved_dependency_entry_count"] = 1
    receipt_runtime["schema_version"] = "clio-kit.locked-server.v5"
    assert jarvis_mcp_runtime_identity(receipt)["locked_server_runtime_verified"] is False


def test_real_uv_tool_install_binds_external_launcher_to_receipt_and_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove uv's external launcher is bound without pretending it is in the wheel RECORD."""
    uv = shutil.which("uv")
    assert uv is not None
    probe_wheel = _build_uv_tool_layout_probe_wheel(tmp_path)
    tool_directory = tmp_path / "tools"
    tool_bin_directory = tmp_path / "bin"
    monkeypatch.setenv("UV_TOOL_DIR", str(tool_directory))
    monkeypatch.setenv("UV_TOOL_BIN_DIR", str(tool_bin_directory))
    monkeypatch.setenv("UV_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("UV_LINK_MODE", "copy")
    completed = subprocess.run(
        [
            uv,
            "tool",
            "install",
            "--force",
            "--python",
            sys.executable,
            "--offline",
            "--no-index",
            "--no-config",
            str(probe_wheel),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr
    executable = tool_bin_directory / ("clio-kit.exe" if os.name == "nt" else "clio-kit")
    environment = tool_directory / "clio-kit"
    provider = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")

    identity = probe_persistent_uv_tool_identity(
        uv_executable=uv,
        tool_executable=str(executable),
        provider_interpreter=str(provider),
        source_artifact=probe_wheel,
        distribution="clio-kit",
        distribution_version=UV_TOOL_PROBE_VERSION,
        entry_point="clio-kit",
    )

    assert Path(identity.tool_executable) == executable.absolute()
    assert Path(identity.tool_executable).parent == tool_bin_directory.absolute()
    assert Path(identity.provider_interpreter) == provider.absolute()
    assert Path(identity.distribution_console_script_path).is_relative_to(environment.resolve())
    assert identity.tool_executable_sha256 == identity.distribution_console_script_sha256
    assert Path(identity.uv_receipt_path) == (environment / "uv-receipt.toml").resolve()
    receipt = Path(identity.uv_receipt_path).read_text(encoding="utf-8")
    assert str(executable).replace("\\", "/") in receipt
    assert probe_wheel.as_posix() in receipt

    monkeypatch.delenv("CLIO_RELAY_JARVIS_MCP_COMMAND", raising=False)
    install_receipt = write_install_receipt(
        install_spec="checkout",
        path=tmp_path / "install-receipt.json",
        components={"clio-kit": UV_TOOL_PROBE_VERSION},
        component_artifacts={
            "clio-kit": ComponentArtifactIdentity(
                distribution="clio-kit",
                distribution_version=UV_TOOL_PROBE_VERSION,
                install_spec=str(probe_wheel),
                requested_source="wheel",
                artifact_filename=probe_wheel.name,
                artifact_sha256=hashlib.sha256(probe_wheel.read_bytes()).hexdigest(),
                runtime_artifact_path=str(probe_wheel),
                runtime_command=[str(executable), "mcp-server", "jarvis"],
                runtime_interpreters={"provider": str(provider)},
                runtime_executables={"clio-kit": str(executable), "uv": uv},
                persistent_tool=identity,
                locked_server_runtime=_verified_locked_jarvis_runtime(),
            )
        },
    )
    runtime_identity = jarvis_mcp_runtime_identity(install_receipt)
    assert runtime_identity["persistent_tool_verified"] is True
    assert runtime_identity["artifact_identity_verified"] is True

    internal_launcher = Path(identity.distribution_console_script_path)
    executable.unlink()
    executable.write_bytes(b"substituted-external-launcher")
    if os.name != "nt":
        executable.chmod(0o755)
    with pytest.raises(ConfigurationError, match="launcher"):
        probe_persistent_uv_tool_identity(
            uv_executable=uv,
            tool_executable=str(executable),
            provider_interpreter=str(provider),
            source_artifact=probe_wheel,
            distribution="clio-kit",
            distribution_version=UV_TOOL_PROBE_VERSION,
            entry_point="clio-kit",
        )

    shutil.copy2(internal_launcher, executable)
    receipt_path = Path(identity.uv_receipt_path)
    receipt_payload = receipt_path.read_bytes()
    receipt_text = receipt_payload.decode("utf-8")
    recorded_executable = str(executable).replace("\\", "/")
    substituted_executable = str(tool_bin_directory / "other-clio-kit").replace("\\", "/")
    assert receipt_text.count(recorded_executable) == 1
    receipt_path.write_text(
        receipt_text.replace(recorded_executable, substituted_executable),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="receipt does not own"):
        probe_persistent_uv_tool_identity(
            uv_executable=uv,
            tool_executable=str(executable),
            provider_interpreter=str(provider),
            source_artifact=probe_wheel,
            distribution="clio-kit",
            distribution_version=UV_TOOL_PROBE_VERSION,
            entry_point="clio-kit",
        )
    receipt_path.write_bytes(receipt_payload)

    recorded_source = probe_wheel.as_posix()
    assert receipt_text.count(recorded_source) == 1
    other_wheel = tmp_path / "other" / probe_wheel.name
    other_wheel.parent.mkdir()
    other_wheel.write_bytes(probe_wheel.read_bytes())
    for substituted_source in (
        other_wheel.as_posix(),
        (tmp_path / "missing" / probe_wheel.name).as_posix(),
        "relative-wheel.whl",
        tmp_path.as_posix(),
    ):
        receipt_path.write_text(
            receipt_text.replace(recorded_source, substituted_source),
            encoding="utf-8",
        )
        with pytest.raises(ConfigurationError, match="does not bind the source wheel"):
            probe_persistent_uv_tool_identity(
                uv_executable=uv,
                tool_executable=str(executable),
                provider_interpreter=str(provider),
                source_artifact=probe_wheel,
                distribution="clio-kit",
                distribution_version=UV_TOOL_PROBE_VERSION,
                entry_point="clio-kit",
            )
    receipt_path.write_text("[tool\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="receipt is invalid"):
        probe_persistent_uv_tool_identity(
            uv_executable=uv,
            tool_executable=str(executable),
            provider_interpreter=str(provider),
            source_artifact=probe_wheel,
            distribution="clio-kit",
            distribution_version=UV_TOOL_PROBE_VERSION,
            entry_point="clio-kit",
        )
    receipt_path.write_bytes(receipt_payload)

    module_path = Path(
        subprocess.run(
            [
                str(provider),
                "-I",
                "-c",
                "import clio_kit_probe; print(clio_kit_probe.__file__)",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
    )
    module_path.write_bytes(module_path.read_bytes() + b"\n# substituted\n")
    with pytest.raises(ConfigurationError, match="RECORD member digest mismatch"):
        probe_persistent_uv_tool_identity(
            uv_executable=uv,
            tool_executable=str(executable),
            provider_interpreter=str(provider),
            source_artifact=probe_wheel,
            distribution="clio-kit",
            distribution_version=UV_TOOL_PROBE_VERSION,
            entry_point="clio-kit",
        )


def test_real_uv_tool_install_accepts_canonical_mount_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accept one uv tool whose visible home path resolves through a mount alias."""
    uv = shutil.which("uv")
    assert uv is not None
    built_wheel = _build_uv_tool_layout_probe_wheel(tmp_path)
    canonical_root = tmp_path / "canonical-home"
    canonical_source = canonical_root / "source"
    canonical_source.mkdir(parents=True)
    canonical_wheel = canonical_source / built_wheel.name
    shutil.copy2(built_wheel, canonical_wheel)

    if os.name == "nt":
        intermediate = canonical_root / "lexical-alias"
        intermediate.mkdir()
        visible_root = intermediate / ".."
    else:
        visible_root = tmp_path / "visible-home"
        visible_root.symlink_to(canonical_root, target_is_directory=True)
        assert visible_root.absolute() != visible_root.resolve()

    visible_wheel = visible_root / "source" / built_wheel.name
    tool_directory = visible_root / "tools"
    tool_bin_directory = visible_root / "bin"
    monkeypatch.setenv("UV_TOOL_DIR", str(tool_directory))
    monkeypatch.setenv("UV_TOOL_BIN_DIR", str(tool_bin_directory))
    monkeypatch.setenv("UV_CACHE_DIR", str(visible_root / "cache"))
    monkeypatch.setenv("UV_LINK_MODE", "copy")
    completed = subprocess.run(
        [
            uv,
            "tool",
            "install",
            "--force",
            "--python",
            sys.executable,
            "--offline",
            "--no-index",
            "--no-config",
            str(visible_wheel),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr
    executable = tool_bin_directory / ("clio-kit.exe" if os.name == "nt" else "clio-kit")
    environment = tool_directory / "clio-kit"
    provider = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    direct_url = json.loads(
        subprocess.run(
            [
                str(provider),
                "-I",
                "-c",
                (
                    "from importlib.metadata import distribution; "
                    "print(distribution('clio-kit').read_text('direct_url.json'))"
                ),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
    )
    receipt_text = (environment / "uv-receipt.toml").read_text(encoding="utf-8")
    if os.name != "nt":
        assert direct_url["url"] == visible_wheel.as_uri()
        assert visible_wheel.as_posix() in receipt_text
        assert provider.absolute().is_relative_to(environment.absolute())
        assert provider.parent.resolve().is_relative_to(environment.resolve())

    identity = probe_persistent_uv_tool_identity(
        uv_executable=uv,
        tool_executable=str(executable),
        provider_interpreter=str(provider),
        source_artifact=visible_wheel,
        distribution="clio-kit",
        distribution_version=UV_TOOL_PROBE_VERSION,
        entry_point="clio-kit",
    )

    assert Path(identity.environment_prefix) == environment.resolve()
    assert Path(identity.provider_interpreter).parent.resolve() == provider.parent.resolve()
    assert Path(identity.source_artifact_path) == canonical_wheel.resolve()
    assert Path(identity.uv_receipt_path).is_relative_to(environment.resolve())


@pytest.mark.parametrize("server_name", ["jarvis", "spack"])
def test_relay_runtime_identity_matches_exact_wheel_launcher(
    clio_kit_wheel: Path,
    tmp_path: Path,
    server_name: str,
) -> None:
    """Match relay evidence to the exact v4 identity computed by the wheel launcher."""
    project = _extract_wheel_server_project(clio_kit_wheel, server_name, tmp_path)
    expected = _wheel_launcher_project_identity(clio_kit_wheel, project)
    runner = _load_mcp_call_runner()
    uvx = tmp_path / ("uvx.exe" if os.name == "nt" else "uvx")
    uv = tmp_path / ("uv.exe" if os.name == "nt" else "uv")
    uvx.write_bytes(b"exact-uvx-launcher")
    uv.write_bytes(b"exact-uv-runtime")

    artifact = cast(Any, runner)._server_artifact_identity(
        str(uvx),
        [
            "--refresh",
            "--no-config",
            "--from",
            str(clio_kit_wheel),
            "clio-kit",
            "mcp-server",
            server_name,
        ],
    )
    observed = cast(JSON, artifact["nested_runtime"])

    assert expected["schema_version"] == observed["schema_version"] == ("clio-kit.locked-server.v4")
    assert expected["server_name"] == observed["server_name"] == server_name
    assert expected["project_sha256"] == observed["project_sha256"]
    assert expected["lock_sha256"] == observed["lock_sha256"]
    assert observed["runtime_policy"] == ("uv-run:materialized:frozen:no-editable:no-dev:v3")
    assert observed["contract_source_verified"] is True
    assert observed["locked_runtime_verified"] is True
    assert artifact["server_process_artifact_verified"] is True
    assert artifact["verified"] is True


def test_runner_launches_exact_wheel_only_through_verified_snapshot(
    clio_kit_wheel: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the production runner and exact wheel through its private snapshot."""
    uvx = shutil.which("uvx")
    if uvx is None:
        raise AssertionError("uvx is required for the exact-wheel runner probe")
    runner = _load_mcp_call_runner()
    server_args = [
        "--refresh",
        "--no-config",
        "--from",
        str(clio_kit_wheel),
        "clio-kit",
        "mcp-server",
        "spack",
    ]
    discovery = cast(Any, runner)._server_artifact_identity(uvx, server_args)
    expected_digest = cast(Any, runner)._server_artifact_digest(discovery)
    monkeypatch.chdir(tmp_path)

    return_code = cast(Any, runner).run_mcp_call_from_params(
        {
            "server": uvx,
            "server_args": server_args,
            "operation": "tools/list",
            "timeout_seconds": 180,
            "expected_server_artifact_digest": expected_digest,
        }
    )
    result_payload = (tmp_path / "mcp-result.json").read_text(encoding="utf-8")
    result = _json_object(result_payload.encode("utf-8"), label="runner result")

    assert return_code == 0, result.get("protocol_error")
    assert result["server_args"] == server_args
    assert cast(JSON, result["server_artifact"])["install_spec"] == str(clio_kit_wheel)
    assert result["observed_server_artifact_digest"] == expected_digest
    execution = cast(JSON, result["server_execution_artifact"])
    assert execution["private_snapshot"] is True
    assert execution["source_sha256"] == hashlib.sha256(clio_kit_wheel.read_bytes()).hexdigest()
    assert execution["snapshot_sha256"] == execution["source_sha256"]
    assert execution["snapshot_verified_before_launch"] is True
    assert execution["snapshot_verified_after_launch"] is True
    assert execution["source_verified_after_launch"] is True
    assert execution["cleanup_verified"] is True
    assert "clio-relay-mcp-wheel-" not in result_payload


@pytest.mark.parametrize("contract_id", sorted(ACTIVE_CONTRACT_IDS))
def test_live_locked_stdio_matches_shipped_contract_artifact(
    contract_id: str,
    clio_kit_wheel: Path,
    shipped_contracts: dict[str, JSON],
) -> None:
    """Compare each wheel artifact to its actual locked FastMCP stdio surface."""
    expected = EXPECTED_CONTRACTS[contract_id]
    server_name = cast(str, expected["server_name"])
    observed_tools = _probe_tools_list(clio_kit_wheel, server_name)
    artifact_tools = cast(list[JSON], shipped_contracts[contract_id]["tools"])
    observed_tools.sort(key=lambda tool: cast(str, tool["name"]))

    assert _canonical_json({"tools": observed_tools}) == _canonical_json({"tools": artifact_tools})
    parsed = [_relay_tool_schema(tool) for tool in observed_tools]
    assert remote_mcp_schema_digest(parsed) == expected["contract_sha256"]


def _load_shipped_contracts(wheel: Path) -> dict[str, JSON]:
    with zipfile.ZipFile(wheel) as archive:
        index_payload = _read_bounded_member(archive, CONTRACT_INDEX_PATH)
        index = _json_object(index_payload, label="contract index")
        assert index.get("schema_version") == CONTRACT_INDEX_SCHEMA
        entries = index.get("contracts")
        assert isinstance(entries, list)
        contracts: dict[str, JSON] = {}
        for raw_entry in cast(list[object], entries):
            assert isinstance(raw_entry, dict)
            entry = cast(JSON, raw_entry)
            contract_id = entry.get("contract_id")
            artifact_name = entry.get("artifact")
            assert isinstance(contract_id, str)
            assert isinstance(artifact_name, str)
            expected = EXPECTED_CONTRACTS.get(contract_id)
            if expected is not None:
                assert artifact_name == expected["artifact"]
            artifact_path = f"clio_kit/_mcp_contracts/{artifact_name}"
            artifact_payload = _read_bounded_member(archive, artifact_path)
            assert hashlib.sha256(artifact_payload).hexdigest() == entry["artifact_sha256"]
            artifact = _json_object(artifact_payload, label=contract_id)
            _verify_contract_artifact(entry, artifact)
            if expected is not None:
                contracts[contract_id] = artifact
        assert set(contracts) == set(EXPECTED_CONTRACTS)
    return contracts


def _verify_contract_artifact(entry: JSON, artifact: JSON) -> None:
    assert artifact.get("schema_version") == CONTRACT_SCHEMA
    assert artifact.get("canonicalization") == CONTRACT_CANONICALIZATION
    assert artifact.get("projection") == CONTRACT_PROJECTION
    assert artifact.get("contract_id") == entry.get("contract_id")
    assert artifact.get("server_name") == entry.get("server_name")
    assert artifact.get("profile") == entry.get("profile") == "user"
    tools = artifact.get("tools")
    assert isinstance(tools, list)
    raw_tools = cast(list[object], tools)
    assert all(isinstance(tool, dict) for tool in raw_tools)
    typed_tools = [cast(JSON, tool) for tool in raw_tools]
    contract_digest = hashlib.sha256(_canonical_json(_contract_projection(typed_tools))).hexdigest()
    wire_digest = hashlib.sha256(_canonical_json({"tools": typed_tools})).hexdigest()
    assert artifact.get("contract_sha256") == entry.get("contract_sha256") == contract_digest
    assert artifact.get("wire_sha256") == entry.get("wire_sha256") == wire_digest
    assert artifact.get("tool_names") == [tool["name"] for tool in typed_tools]


def _contract_projection(tools: list[JSON]) -> JSON:
    projected = [
        {
            "annotations": tool.get("annotations"),
            "description": tool.get("description"),
            "input_schema": tool["inputSchema"],
            "name": tool["name"],
            "output_schema": tool.get("outputSchema"),
            "title": tool.get("title"),
        }
        for tool in tools
    ]
    projected.sort(key=lambda tool: cast(str, tool["name"]))
    return {"tools": projected}


def _probe_tools_list(wheel: Path, server_name: str) -> list[JSON]:
    uvx = shutil.which("uvx")
    if uvx is None:
        raise AssertionError("uvx is required for the clio-kit wheel contract probe")
    messages: tuple[JSON, ...] = (
        {
            "jsonrpc": "2.0",
            "id": "initialize",
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "clio-relay-contract-test", "version": "1.0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": "tools-list",
            "method": "tools/list",
            "params": {},
        },
    )
    command = [
        uvx,
        "--refresh",
        "--no-config",
        "--from",
        str(wheel),
        "clio-kit",
        "mcp-server",
        server_name,
    ]
    tools_list = _exchange_tools_list(command, messages, server_name=server_name)
    result = tools_list.get("result")
    if not isinstance(result, dict):
        raise AssertionError(f"clio-kit {server_name} tools/list response is malformed")
    tools = cast(JSON, result).get("tools")
    if not isinstance(tools, list):
        raise AssertionError(f"clio-kit {server_name} tools/list returned invalid tools")
    raw_tools = cast(list[object], tools)
    if not all(isinstance(tool, dict) for tool in raw_tools):
        raise AssertionError(f"clio-kit {server_name} tools/list returned invalid tools")
    return [cast(JSON, tool) for tool in raw_tools]


def _exchange_tools_list(
    command: list[str],
    requests: tuple[JSON, ...],
    *,
    server_name: str,
) -> JSON:
    """Complete the handshake before closing stdin, as a real MCP client does."""
    output_lines: queue.Queue[bytes | None] = queue.Queue(maxsize=1_024)
    deadline = time.monotonic() + 180
    with tempfile.TemporaryFile() as stderr:
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
        )
        if process.stdin is None or process.stdout is None:
            raise AssertionError(f"clio-kit {server_name} stdio pipes are unavailable")
        stdin = process.stdin
        stdout = process.stdout

        def read_stdout() -> None:
            try:
                while line := stdout.readline():
                    output_lines.put(line)
            finally:
                output_lines.put(None)

        reader = threading.Thread(
            target=read_stdout,
            name=f"clio-kit-{server_name}-contract-stdout",
            daemon=True,
        )
        reader.start()
        try:
            stdin.write(_canonical_json(requests[0]) + b"\n")
            stdin.flush()
            initialize = _wait_for_response(
                output_lines,
                response_id="initialize",
                deadline=deadline,
                server_name=server_name,
            )
            if initialize.get("error") is not None:
                raise AssertionError(f"clio-kit {server_name} initialize failed")
            for request in requests[1:]:
                stdin.write(_canonical_json(request) + b"\n")
            stdin.flush()
            tools_list = _wait_for_response(
                output_lines,
                response_id="tools-list",
                deadline=deadline,
                server_name=server_name,
            )
            stdin.close()
            returncode = process.wait(timeout=max(0.1, deadline - time.monotonic()))
            reader.join(timeout=1)
            stderr.seek(0, os.SEEK_END)
            stderr_size = stderr.tell()
            if stderr_size > MAX_PROBE_OUTPUT_BYTES:
                raise AssertionError(f"clio-kit {server_name} exceeded bounded probe output")
            if returncode != 0:
                stderr.seek(max(0, stderr_size - 2_000))
                diagnostic = stderr.read().decode("utf-8", errors="replace")
                raise AssertionError(
                    f"clio-kit {server_name} exited with {returncode}: {diagnostic}"
                )
            return tools_list
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            stdout.close()
            if not stdin.closed:
                stdin.close()
            reader.join(timeout=1)


def _wait_for_response(
    output_lines: queue.Queue[bytes | None],
    *,
    response_id: str,
    deadline: float,
    server_name: str,
) -> JSON:
    total_bytes = 0
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"timed out waiting for clio-kit {server_name} {response_id}")
        try:
            line = output_lines.get(timeout=remaining)
        except queue.Empty as exc:
            raise AssertionError(
                f"timed out waiting for clio-kit {server_name} {response_id}"
            ) from exc
        if line is None:
            raise AssertionError(f"clio-kit {server_name} closed stdout before {response_id}")
        total_bytes += len(line)
        if total_bytes > MAX_PROBE_OUTPUT_BYTES:
            raise AssertionError(f"clio-kit {server_name} exceeded bounded probe output")
        try:
            decoded = cast(object, json.loads(line.decode("utf-8")))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(decoded, dict):
            response = cast(JSON, decoded)
            if response.get("id") == response_id:
                return response


def _extract_wheel_server_project(wheel: Path, server_name: str, root: Path) -> Path:
    """Extract one trusted exact-wheel server project for launcher identity comparison."""
    project = root / "wheel-projects" / server_name
    project.mkdir(parents=True)
    suffix = f"/clio-kit-mcp-servers/{server_name}/uv.lock"
    with zipfile.ZipFile(wheel) as archive:
        lock_names = [
            info.filename
            for info in archive.infolist()
            if info.filename.endswith(suffix)
            or info.filename == f"clio-kit-mcp-servers/{server_name}/uv.lock"
        ]
        assert len(lock_names) == 1
        prefix = lock_names[0][: -len("uv.lock")]
        server_members = [
            info
            for info in archive.infolist()
            if info.filename.startswith(prefix) and info.filename != prefix
        ]
        assert len(server_members) <= 20_000
        assert sum(info.file_size for info in server_members) <= 512 * 1024 * 1024
        for info in server_members:
            relative_text = info.filename[len(prefix) :].rstrip("/")
            relative = PurePosixPath(relative_text)
            assert relative_text and relative.as_posix() == relative_text
            assert not relative.is_absolute() and ".." not in relative.parts
            target = project.joinpath(*relative.parts)
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(info) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination, length=1024 * 1024)
            assert target.stat().st_size == info.file_size
    return project


def _wheel_launcher_project_identity(wheel: Path, project: Path) -> JSON:
    """Ask clio-kit's exact wheel source to compute its own child identity."""
    script = "\n".join(
        [
            "import json",
            "import sys",
            "from pathlib import Path",
            "sys.path.insert(0, sys.argv[1])",
            "import clio_kit",
            "print(json.dumps(clio_kit.locked_server_project_identity(Path(sys.argv[2]))))",
        ]
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(wheel), str(project)],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return _json_object(completed.stdout.encode("utf-8"), label="launcher identity")


def _load_mcp_call_runner() -> ModuleType:
    path = (
        Path(__file__).parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "mcp_call"
        / "runner.py"
    )
    spec = importlib.util.spec_from_file_location("clio_relay_mcp_call_runner_contract", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load MCP call runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _relay_tool_schema(tool: JSON) -> RemoteMcpToolSchema:
    return RemoteMcpToolSchema(
        name=cast(str, tool["name"]),
        title=cast(str | None, tool.get("title")),
        description=cast(str | None, tool.get("description")),
        input_schema=cast(JSON, tool["inputSchema"]),
        output_schema=cast(JSON | None, tool.get("outputSchema")),
        annotations=cast(JSON | None, tool.get("annotations")),
    )


def _tools_by_name(artifact: JSON) -> dict[str, JSON]:
    return {cast(str, tool["name"]): tool for tool in cast(list[JSON], artifact["tools"])}


def _read_bounded_member(archive: zipfile.ZipFile, name: str) -> bytes:
    info = archive.getinfo(name)
    assert not info.is_dir() and info.file_size <= MAX_CONTRACT_BYTES
    with archive.open(info) as stream:
        payload = stream.read(MAX_CONTRACT_BYTES + 1)
    assert len(payload) <= MAX_CONTRACT_BYTES and len(payload) == info.file_size
    return payload


def _json_object(payload: bytes, *, label: str) -> JSON:
    try:
        value = cast(object, json.loads(payload.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AssertionError(f"clio-kit {label} is not UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise AssertionError(f"clio-kit {label} is not a JSON object")
    return cast(JSON, value)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
