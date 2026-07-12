from __future__ import annotations

import json
from typing import Any, cast

import pytest

from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_mcp_server,
    jarvis_mcp_server_args,
    jarvis_user_contract,
    jarvis_user_contract_digest,
    virtual_jarvis_tool_definitions,
)
from clio_relay.jarvis_mcp_validation import build_jarvis_mcp_validation_report
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from clio_relay.runtime_metadata import RUNTIME_METADATA_SCHEMA
from clio_relay.validation_report import ValidationStatus


@pytest.fixture(autouse=True)
def _pinned_jarvis_mcp_wheel(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "CLIO_RELAY_JARVIS_MCP_COMMAND",
        json.dumps(
            [
                "uvx",
                "--from",
                "/opt/wheels/clio_kit-3.0.0-py3-none-any.whl",
                "clio-kit",
                "mcp-server",
                "jarvis",
            ]
        ),
    )


def test_jarvis_mcp_validation_accepts_structured_durable_run() -> None:
    report = build_jarvis_mcp_validation_report(**_acceptance_inputs())

    assert report.status == ValidationStatus.PASSED
    assert {check.check_id for check in report.checks} == {
        "remote-mcp.jarvis-discovery",
        "remote-mcp.jarvis-remote-contract",
        "remote-mcp.jarvis-call",
        "remote-mcp.server-artifact",
        "remote-mcp.durable-result",
        "remote-mcp.jarvis-live-progress",
        "jarvis.spack-runtime-environment",
        "jarvis.structured-runtime-metadata",
    }
    assert {resource.kind for resource in report.resources} == {
        "relay_job",
        "artifact",
        "mcp_server",
        "package_progress_provider",
        "scheduler_job",
    }
    assert (
        next(
            resource for resource in report.resources if resource.role == "runtime_metadata"
        ).resource_id
        == "artifact-runtime_metadata"
    )


def test_bundled_jarvis_contract_matches_pinned_clio_kit_digest() -> None:
    assert jarvis_user_contract_digest() == CLIO_KIT_JARVIS_USER_CONTRACT_SHA256


def test_jarvis_mcp_validation_rejects_legacy_runtime_metadata() -> None:
    inputs = _acceptance_inputs()
    inputs["runtime_metadata"] = {
        "schema_version": RUNTIME_METADATA_SCHEMA,
        "source": "legacy_stdout",
        "pipeline_id": "acceptance",
        "field_sources": {"pipeline_id": "legacy_stdout"},
    }

    report = build_jarvis_mcp_validation_report(**inputs)

    assert report.status == ValidationStatus.FAILED
    structured = next(
        check for check in report.checks if check.check_id == "jarvis.structured-runtime-metadata"
    )
    assert structured.status == ValidationStatus.FAILED


def test_jarvis_mcp_validation_rejects_unattested_nested_server_process() -> None:
    inputs = _acceptance_inputs()
    call_result = cast(dict[str, Any], inputs["mcp_result"])
    call_artifact = cast(dict[str, Any], call_result["server_artifact"])
    call_artifact["nested_launcher"] = True
    call_artifact["server_process_artifact_verified"] = False
    call_artifact["identity_error"] = "nested server environment is not attested"
    call_artifact["verified"] = False
    discovery_result = cast(dict[str, Any], inputs["remote_tools_list_result"])
    discovery_result["server_artifact"] = dict(call_artifact)

    report = build_jarvis_mcp_validation_report(**inputs)

    server_check = next(
        check for check in report.checks if check.check_id == "remote-mcp.server-artifact"
    )
    assert report.status == ValidationStatus.FAILED
    assert server_check.status == ValidationStatus.FAILED


def test_jarvis_mcp_validation_rejects_released_contract_drift() -> None:
    inputs = _acceptance_inputs()
    discovery = cast(dict[str, Any], inputs["remote_tools_list_result"])
    protocol = cast(dict[str, Any], discovery["protocol_result"])
    tools = cast(list[dict[str, Any]], protocol["tools"])
    run = next(tool for tool in tools if tool["name"] == "jarvis_run")
    schema = cast(dict[str, Any], run["inputSchema"])
    properties = cast(dict[str, Any], schema["properties"])
    cast(dict[str, Any], properties["submit"])["default"] = False

    report = build_jarvis_mcp_validation_report(**inputs)

    contract = next(
        check for check in report.checks if check.check_id == "remote-mcp.jarvis-remote-contract"
    )
    assert report.status == ValidationStatus.FAILED
    assert contract.status == ValidationStatus.FAILED
    evidence = contract.evidence[0].metadata
    assert evidence["observed_contract_sha256"] != evidence["expected_contract_sha256"]


def test_jarvis_mcp_validation_rejects_unattributed_scheduler_identity() -> None:
    inputs = _acceptance_inputs()
    runtime = cast(dict[str, Any], inputs["runtime_metadata"])
    sources = cast(dict[str, Any], runtime["field_sources"])
    sources["scheduler_job_id"] = "legacy_stdout"

    report = build_jarvis_mcp_validation_report(**inputs)

    structured = next(
        check for check in report.checks if check.check_id == "jarvis.structured-runtime-metadata"
    )
    assert report.status == ValidationStatus.FAILED
    assert structured.status == ValidationStatus.FAILED


def test_jarvis_mcp_validation_rejects_progress_seen_only_after_terminal() -> None:
    inputs = _acceptance_inputs()
    inputs["live_progress_observation"] = None

    report = build_jarvis_mcp_validation_report(**inputs)

    live_progress = next(
        check for check in report.checks if check.check_id == "remote-mcp.jarvis-live-progress"
    )
    assert report.status == ValidationStatus.FAILED
    assert live_progress.status == ValidationStatus.FAILED
    assert live_progress.evidence[0].metadata["live_observed_while_running"] is False


def test_jarvis_mcp_validation_rejects_unbound_progress_replay() -> None:
    inputs = _acceptance_inputs()
    progress = cast(list[dict[str, Any]], inputs["progress"])
    accepted_metadata = cast(dict[str, Any], progress[-1]["metadata"])
    accepted_metadata["provider_execution_id"] = "attacker-execution"

    report = build_jarvis_mcp_validation_report(**inputs)

    live_progress = next(
        check for check in report.checks if check.check_id == "remote-mcp.jarvis-live-progress"
    )
    assert report.status == ValidationStatus.FAILED
    assert live_progress.status == ValidationStatus.FAILED
    assert live_progress.evidence[0].metadata["accepted_progress_id"] is None


def test_jarvis_mcp_validation_rejects_runner_without_execution_unlock() -> None:
    inputs = _acceptance_inputs()
    mcp_result = cast(dict[str, Any], inputs["mcp_result"])
    bridge = cast(dict[str, Any], mcp_result["package_progress_bridge"])
    bridge["execution_validated"] = False

    report = build_jarvis_mcp_validation_report(**inputs)

    live_progress = next(
        check for check in report.checks if check.check_id == "remote-mcp.jarvis-live-progress"
    )
    assert report.status == ValidationStatus.FAILED
    assert live_progress.status == ValidationStatus.FAILED
    assert live_progress.evidence[0].metadata["bridge_valid"] is False


def _acceptance_inputs() -> dict[str, Any]:
    job_id = "job-jarvis"
    execution_id = "jarvis-execution-acceptance"
    server_artifact = _jarvis_server_artifact()
    server_artifact_digest = remote_mcp_server_artifact_digest(server_artifact)
    local_run = next(
        tool
        for tool in virtual_jarvis_tool_definitions(clusters=["ares"])
        if tool["name"] == "jarvis_run"
    )
    artifacts = [
        {
            "artifact_id": f"artifact-{kind}",
            "kind": kind,
            "uri": f"file:///spool/{kind}.json",
            "sha256": "a" * 64,
        }
        for kind in ("stdout", "stderr", "mcp_result", "provenance", "runtime_metadata")
    ]
    return {
        "cluster": "ares",
        "tool": "jarvis_run",
        "tools_list_response": {
            "jsonrpc": "2.0",
            "id": "list",
            "result": {"tools": [local_run]},
        },
        "call_response": {
            "jsonrpc": "2.0",
            "id": "call",
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": '{"job_id":"job-jarvis","state":"queued"}',
                    }
                ]
            },
        },
        "call_job_id": job_id,
        "call_status": {
            "job": {
                "job_id": job_id,
                "cluster": "ares",
                "kind": "mcp_call",
                "state": "succeeded",
                "spec": {
                    "server": jarvis_mcp_server(),
                    "server_args": jarvis_mcp_server_args(),
                    "operation": "tools/call",
                    "tool": "jarvis_run",
                    "arguments": {
                        "pipeline_id": "acceptance",
                        "spack_specs": ["lammps"],
                    },
                    "expected_server_artifact_digest": server_artifact_digest,
                },
            },
            "terminal": True,
        },
        "artifacts": artifacts,
        "mcp_result": {
            "returncode": 0,
            "operation": "tools/call",
            "tool": "jarvis_run",
            "expected_server_artifact_digest": server_artifact_digest,
            "observed_server_artifact_digest": server_artifact_digest,
            "server_artifact": server_artifact,
            "package_progress_bridge": {
                "schema_version": "clio-relay.mcp-package-progress-bridge.v1",
                "notification_count": 1,
                "notification_bytes": 1024,
                "execution_id": execution_id,
                "pipeline_id": "acceptance",
                "provider": _mcp_progress_provider(),
                "expected_server_artifact_digest": server_artifact_digest,
                "observed_server_artifact_digest": server_artifact_digest,
                "execution_validated": True,
            },
        },
        "provenance": {"job": {"job_id": job_id}},
        "runtime_metadata": {
            "schema_version": RUNTIME_METADATA_SCHEMA,
            "source": "jarvis_mcp",
            "execution_id": execution_id,
            "pipeline_id": "acceptance",
            "scheduler_provider": "slurm",
            "scheduler_job_id": "12345",
            "scheduler_phase": "COMPLETED",
            "field_sources": {
                "pipeline_id": "jarvis_mcp",
                "scheduler_provider": "jarvis_mcp",
                "scheduler_job_id": "jarvis_sidecar",
                "terminal.state": "jarvis_mcp",
            },
            "terminal": {
                "state": "completed",
                "terminal": True,
                "returncode": 0,
            },
            "packages": [
                {
                    "name": "builtin.lammps",
                    "version": "2024.08",
                    "package_type": "builtin.lammps",
                }
            ],
            "details": {
                "producer_contract": {
                    "requested_source": "jarvis_mcp",
                    "producer_schema_version": "jarvis.runtime.v1",
                    "trusted": True,
                    "reason": "producer and scheduler submission contracts matched",
                },
                "runtime_metadata": {
                    "details": {
                        "environment": {
                            "specs": ["lammps"],
                            "persisted": True,
                            "scheduler_reload": "saved_pipeline_environment",
                        }
                    }
                },
            },
        },
        "remote_tools_list_result": {
            "returncode": 0,
            "protocol_result": {
                "tools": [
                    _remote_tool(name, definition)
                    for name, definition in jarvis_user_contract().items()
                ]
            },
            "server_artifact": server_artifact,
        },
        "remote_discovery_job_id": "job-jarvis-discovery",
        "remote_discovery_artifacts": [
            {
                "artifact_id": "artifact-jarvis-schema",
                "kind": "mcp_result",
                "sha256": "e" * 64,
            }
        ],
        "launcher": "uvx",
        "install_source": "wheel:clio-relay.whl",
        "artifact_sha256": "b" * 64,
        "progress": _mcp_progress_records(
            job_id=job_id,
            execution_id=execution_id,
            server_artifact_digest=server_artifact_digest,
        ),
        "live_progress_observation": {
            "progress_id": "progress-warming",
            "job_state": "running",
            "terminal": False,
            "provider_notification_sequence": 1,
        },
    }


def _mcp_progress_provider() -> dict[str, str]:
    return {
        "entry_point": "lammps",
        "entry_point_value": "jarvis_cd.progress.lammps:adapter_from_package",
        "distribution": "jarvis_cd",
        "distribution_version": "2.0.0",
        "adapter": "lammps",
        "package_name": "builtin.lammps",
        "package_version": "2024.08",
        "application_profile": "lammps",
    }


def _mcp_progress_records(
    *,
    job_id: str,
    execution_id: str,
    server_artifact_digest: str,
) -> list[dict[str, object]]:
    provider = _mcp_progress_provider()
    common_metadata: dict[str, object] = {
        "source": "jarvis_package",
        "package_name": provider["package_name"],
        "package_version": provider["package_version"],
        "run_id": job_id,
        "execution_id": job_id,
        "adapter": provider["adapter"],
        "application_profile": provider["application_profile"],
        "provider_entry_point": provider["entry_point"],
        "provider_entry_point_value": provider["entry_point_value"],
        "provider_distribution": provider["distribution"],
        "provider_distribution_version": provider["distribution_version"],
        "provider_source_authority": "mcp_progress_notification",
        "provider_validated": True,
        "provider_execution_id": execution_id,
        "provider_pipeline_id": "acceptance",
        "provider_server_artifact_digest": server_artifact_digest,
        "provider_notification_sequence": 1,
        "provider_transport_source_authority": "package_log",
    }
    common_record: dict[str, object] = {
        "job_id": job_id,
        "label": "timestep",
        "current": 10.0,
        "total": 100.0,
        "unit": "step",
        "message": "LAMMPS timestep 10",
        "source_event_seq": None,
    }
    return [
        {
            **common_record,
            "progress_id": "progress-warming",
            "created_at": "2026-07-11T10:00:00Z",
            "metadata": {
                **common_metadata,
                "acceptance_validated": False,
                "provider_execution_validated": False,
            },
        },
        {
            **common_record,
            "progress_id": "progress-accepted",
            "created_at": "2026-07-11T10:00:01Z",
            "metadata": {
                **common_metadata,
                "acceptance_validated": True,
                "provider_execution_validated": True,
            },
        },
    ]


def _jarvis_server_artifact() -> dict[str, object]:
    install_spec = "/opt/wheels/clio_kit-3.0.0-py3-none-any.whl"
    return {
        "requested_command": jarvis_mcp_server(),
        "resolved_executable": "/home/user/.local/bin/uvx",
        "executable": {
            "path": "/home/user/.local/bin/uvx",
            "filename": "uvx",
            "sha256": "uvx-sha256",
            "size_bytes": 1,
        },
        "install_spec": install_spec,
        "install_source": "wheel",
        "install_artifact_sha256": "d" * 64,
        "input_files": [
            {
                "path": install_spec,
                "filename": "clio_kit-3.0.0-py3-none-any.whl",
                "sha256": "d" * 64,
                "size_bytes": 3,
            }
        ],
        "launcher_artifact_verified": True,
        "nested_launcher": False,
        "server_process_artifact_verified": True,
        "identity_error": None,
        "verified": True,
    }


def _remote_tool(
    name: str,
    definition: dict[str, Any],
) -> dict[str, object]:
    return {
        "name": name,
        "description": definition["description"],
        "inputSchema": definition["inputSchema"],
        "outputSchema": definition["outputSchema"],
        "annotations": definition["annotations"],
    }
