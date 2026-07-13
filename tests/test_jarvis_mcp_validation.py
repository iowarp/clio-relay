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
                "/home/user/.local/bin/clio-kit",
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
        "remote-mcp.jarvis-execution-query",
        "jarvis.spack-runtime-environment",
        "jarvis.structured-runtime-metadata",
    }
    assert {resource.kind for resource in report.resources} == {
        "relay_job",
        "artifact",
        "mcp_server",
        "jarvis_execution_progress",
        "jarvis_generated_artifact",
        "scheduler_job",
    }
    assert (
        next(
            resource for resource in report.resources if resource.role == "runtime_metadata"
        ).resource_id
        == "artifact-runtime_metadata"
    )
    contract = next(
        check for check in report.checks if check.check_id == "remote-mcp.jarvis-remote-contract"
    )
    query = contract.evidence[0].metadata["execution_query"]
    assert query["input_fields"] == [
        "artifacts",
        "execution_id",
        "include_progress",
        "pipeline_id",
    ]
    assert query["artifact_filter_fields"] == [
        "artifact_id",
        "cursor",
        "package_id",
        "page_size",
        "role",
        "state",
    ]
    assert query["progress_schema_version"]["const"] == "jarvis.execution.progress.v1"
    assert query["artifact_page_schema_version"]["const"] == ("jarvis.execution.artifacts.v1")
    execution_query = next(
        check for check in report.checks if check.check_id == "remote-mcp.jarvis-execution-query"
    )
    assertions = execution_query.evidence[0].metadata["assertions"]
    assert assertions == {
        "local_query_surface_verified": True,
        "durable_query_job_verified": True,
        "server_artifact_binding_verified": True,
        "result_transport_verified": True,
        "result_envelope_verified": True,
        "identity_coherent": True,
        "lifecycle_coherent": True,
        "pagination_coherent": True,
        "artifact_filters_coherent": True,
        "runner_semantic_validation_verified": True,
    }
    execution_id = "jarvis-execution-acceptance"
    for check_id in {
        "jarvis.structured-runtime-metadata",
        "remote-mcp.jarvis-execution-query",
        "remote-mcp.jarvis-live-progress",
    }:
        check = next(item for item in report.checks if item.check_id == check_id)
        assert check.evidence[0].metadata["execution_id"] == execution_id
    execution_scoped = [
        resource
        for resource in report.resources
        if resource.kind in {"jarvis_execution_progress", "jarvis_generated_artifact"}
        or resource.role in {"virtual_jarvis_mcp_call", "jarvis_mcp_execution_query"}
    ]
    assert execution_scoped
    assert {resource.metadata.get("execution_id") for resource in execution_scoped} == {
        execution_id
    }


def test_jarvis_mcp_validation_does_not_require_spack_for_non_spack_run() -> None:
    inputs = _acceptance_inputs()
    status = cast(dict[str, Any], inputs["call_status"])
    job = cast(dict[str, Any], status["job"])
    spec = cast(dict[str, Any], job["spec"])
    arguments = cast(dict[str, Any], spec["arguments"])
    arguments.pop("spack_specs")

    report = build_jarvis_mcp_validation_report(**inputs)

    assert report.status == ValidationStatus.PASSED
    assert "jarvis.spack-runtime-environment" not in {check.check_id for check in report.checks}
    assert any(resource.kind == "jarvis_generated_artifact" for resource in report.resources)
    generated = next(
        resource for resource in report.resources if resource.kind == "jarvis_generated_artifact"
    )
    assert generated.references == ["cluster_path:/scratch/acceptance/lammps.out"]


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
    properties["contract_drift"] = {"type": "boolean"}

    report = build_jarvis_mcp_validation_report(**inputs)

    contract = next(
        check for check in report.checks if check.check_id == "remote-mcp.jarvis-remote-contract"
    )
    assert report.status == ValidationStatus.FAILED
    assert contract.status == ValidationStatus.FAILED
    evidence = contract.evidence[0].metadata
    assert evidence["observed_contract_sha256"] != evidence["expected_contract_sha256"]


def test_jarvis_mcp_validation_rejects_split_or_unbounded_execution_query() -> None:
    inputs = _acceptance_inputs()
    discovery = cast(dict[str, Any], inputs["remote_tools_list_result"])
    protocol = cast(dict[str, Any], discovery["protocol_result"])
    tools = cast(list[dict[str, Any]], protocol["tools"])
    query = next(tool for tool in tools if tool["name"] == "jarvis_get_execution")
    query_input = cast(dict[str, Any], query["inputSchema"])
    query_properties = cast(dict[str, Any], query_input["properties"])
    query_properties.pop("artifacts")

    report = build_jarvis_mcp_validation_report(**inputs)

    contract = next(
        check for check in report.checks if check.check_id == "remote-mcp.jarvis-remote-contract"
    )
    assert report.status == ValidationStatus.FAILED
    assert contract.status == ValidationStatus.FAILED
    evidence = contract.evidence[0].metadata
    assert evidence["execution_query"]["artifact_filter_fields"] == []


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
    accepted_metadata["execution_id"] = "attacker-execution"

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


@pytest.mark.parametrize(
    ("path", "replacement", "assertion"),
    [
        (("structured_result", "progress", "execution_id"), "wrong", "identity_coherent"),
        (("structured_result", "artifact_page", "terminal"), False, "lifecycle_coherent"),
        (
            ("structured_result", "artifact_page", "returned_artifact_count"),
            2,
            "pagination_coherent",
        ),
        (
            ("result_validation", "artifact_filters", "page_size"),
            100,
            "runner_semantic_validation_verified",
        ),
    ],
)
def test_jarvis_mcp_validation_rejects_incoherent_execution_query(
    path: tuple[str, ...],
    replacement: object,
    assertion: str,
) -> None:
    inputs = _acceptance_inputs()
    query_result = cast(dict[str, Any], inputs["query_mcp_result"])
    target: dict[str, Any] = query_result
    for key in path[:-1]:
        target = cast(dict[str, Any], target[key])
    target[path[-1]] = replacement

    report = build_jarvis_mcp_validation_report(**inputs)

    query = next(
        check for check in report.checks if check.check_id == "remote-mcp.jarvis-execution-query"
    )
    assert report.status == ValidationStatus.FAILED
    assert query.status == ValidationStatus.FAILED
    assert query.evidence[0].metadata["assertions"][assertion] is False


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
    local_query = next(
        tool
        for tool in virtual_jarvis_tool_definitions(clusters=["ares"])
        if tool["name"] == "jarvis_get_execution"
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
                "schema_version": "clio-relay.mcp-jarvis-progress-bridge.v1",
                "notification_count": 2,
                "notification_bytes": 1024,
                "execution_id": execution_id,
                "pipeline_id": "acceptance",
                "package_sequences": {"package-lammps": 2},
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
            "scheduler_phase": "completed",
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
                    "package_type": "builtin.lammps",
                    "package_id": "package-lammps",
                    "metadata": {"progress_event_count": 2},
                }
            ],
            "details": {
                "producer_contract": {
                    "requested_source": "jarvis_mcp",
                    "contract_kind": "native_execution",
                    "producer_schema_version": "jarvis.execution.record.v1",
                    "handle_schema_version": "jarvis.execution.handle.v1",
                    "progress_schema_version": "jarvis.execution.progress.v1",
                    "trusted": True,
                    "reason": "exact native JARVIS execution documents matched",
                },
                "native_execution": _native_execution_documents(execution_id),
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
        "query_tools_list_response": {
            "jsonrpc": "2.0",
            "id": "query-list",
            "result": {"tools": [local_query]},
        },
        "query_call_response": {
            "jsonrpc": "2.0",
            "id": "query-call",
            "result": {
                "structuredContent": {
                    "job_id": "job-jarvis-query",
                    "state": "queued",
                }
            },
        },
        "query_call_job_id": "job-jarvis-query",
        "query_call_status": {
            "job": {
                "job_id": "job-jarvis-query",
                "cluster": "ares",
                "kind": "mcp_call",
                "state": "succeeded",
                "spec": {
                    "server": jarvis_mcp_server(),
                    "server_args": jarvis_mcp_server_args(),
                    "operation": "tools/call",
                    "tool": "jarvis_get_execution",
                    "arguments": {
                        "pipeline_id": "acceptance",
                        "execution_id": execution_id,
                        "include_progress": True,
                        "artifacts": {"page_size": 25},
                    },
                    "expected_server_artifact_digest": server_artifact_digest,
                },
            },
            "terminal": True,
        },
        "query_artifacts": [
            {
                "artifact_id": f"artifact-query-{kind}",
                "kind": kind,
                "uri": f"file:///spool/query-{kind}.json",
                "sha256": "f" * 64,
            }
            for kind in ("stdout", "stderr", "mcp_result", "provenance")
        ],
        "query_mcp_result": _execution_query_mcp_result(
            execution_id=execution_id,
            server_artifact=server_artifact,
            server_artifact_digest=server_artifact_digest,
        ),
        "query_provenance": {"job": {"job_id": "job-jarvis-query"}},
        "query_initialize_response": None,
        "query_stdio_evidence": None,
    }


def _mcp_progress_records(
    *,
    job_id: str,
    execution_id: str,
    server_artifact_digest: str,
) -> list[dict[str, object]]:
    common_metadata: dict[str, object] = {
        "source": "jarvis_execution",
        "relay_job_id": job_id,
        "execution_id": execution_id,
        "run_id": execution_id,
        "pipeline_id": "acceptance",
        "package_name": "builtin.lammps",
        "package_id": "package-lammps",
        "progress_schema_version": "jarvis.progress.v1",
        "progress_state": "running",
        "progress_observed_at_epoch": 1_788_000_000.0,
        "progress_determinate": True,
        "progress_skipped_event_count": 0,
        "execution_state": "running",
        "execution_terminal": False,
        "server_artifact_digest": server_artifact_digest,
        "provider_source_authority": "jarvis_mcp_progress_notification",
        "producer_validated": True,
    }
    return [
        {
            "job_id": job_id,
            "progress_id": "progress-warming",
            "label": "timestep",
            "current": 10.0,
            "total": 100.0,
            "unit": "step",
            "message": "LAMMPS timestep 10",
            "source_event_seq": None,
            "created_at": "2026-07-11T10:00:00Z",
            "metadata": {
                **common_metadata,
                "progress_sequence": 1,
                "progress_event_count": 1,
                "progress_transport_sequence": 1,
                "execution_binding_validated": False,
            },
        },
        {
            "job_id": job_id,
            "progress_id": "progress-accepted",
            "label": "timestep",
            "current": 20.0,
            "total": 100.0,
            "unit": "step",
            "message": "LAMMPS timestep 20",
            "source_event_seq": None,
            "created_at": "2026-07-11T10:00:01Z",
            "metadata": {
                **common_metadata,
                "progress_sequence": 2,
                "progress_event_count": 2,
                "progress_transport_sequence": 2,
                "execution_binding_validated": True,
            },
        },
    ]


def _execution_query_mcp_result(
    *,
    execution_id: str,
    server_artifact: dict[str, object],
    server_artifact_digest: str,
) -> dict[str, object]:
    native = _native_execution_documents(execution_id)
    artifact = {
        "schema_version": "jarvis.artifact.v1",
        "artifact_id": "art_0000000000000000000001",
        "execution_id": execution_id,
        "package_name": "builtin.lammps",
        "package_id": "package-lammps",
        "logical_name": "lammps-output",
        "kind": "file",
        "role": "output",
        "state": "finalized",
        "structure": "single",
        "ownership": "jarvis",
        "format": "text",
        "location": {
            "kind": "cluster_path",
            "value": "/scratch/acceptance/lammps.out",
        },
        "revision": 1,
        "sequence": 1,
        "observed_at_epoch": 1_788_000_002.0,
        "metadata": {},
    }
    structured = {
        "schema_version": "clio-kit.jarvis-execution.v1",
        "pipeline_id": "acceptance",
        "execution_id": execution_id,
        "execution_handle": native["execution_handle"],
        "execution_record": native["execution_record"],
        "runtime_metadata": {},
        "progress": native["progress"],
        "artifact_page": {
            "producer_schema_version": "jarvis.execution.artifacts.v1",
            "pipeline_id": "acceptance",
            "execution_id": execution_id,
            "execution_state": "completed",
            "terminal": True,
            "artifacts": [artifact],
            "matching_artifact_count": 1,
            "returned_artifact_count": 1,
            "next_cursor": None,
        },
    }
    return {
        "returncode": 0,
        "operation": "tools/call",
        "tool": "jarvis_get_execution",
        "protocol_error": None,
        "structured_result": structured,
        "expected_server_artifact_digest": server_artifact_digest,
        "observed_server_artifact_digest": server_artifact_digest,
        "server_artifact": server_artifact,
        "result_validation": {
            "schema_version": "clio-relay.jarvis-execution-query-validation.v1",
            "pipeline_id": "acceptance",
            "execution_id": execution_id,
            "include_progress": True,
            "progress_included": True,
            "artifacts_requested": True,
            "artifact_filters": {
                "package_id": None,
                "role": None,
                "state": None,
                "artifact_id": None,
                "page_size": 25,
                "cursor": None,
            },
            "returned_artifact_count": 1,
            "next_cursor_present": False,
        },
    }


def _native_execution_documents(execution_id: str) -> dict[str, object]:
    return {
        "execution_handle": {
            "schema_version": "jarvis.execution.handle.v1",
            "execution_id": execution_id,
            "pipeline_id": "acceptance",
            "mode": "scheduler",
            "scheduler_provider": "slurm",
            "scheduler_native_id": "12345",
            "cluster": "linux",
        },
        "execution_record": {
            "schema_version": "jarvis.execution.record.v1",
            "execution_id": execution_id,
            "pipeline_id": "acceptance",
            "pipeline_name": "acceptance",
            "mode": "scheduler",
            "scheduler_provider": "slurm",
            "scheduler_native_id": "12345",
            "cluster": "linux",
            "state": "completed",
            "terminal": True,
            "submitted": True,
            "return_code": 0,
            "error": None,
            "created_at": "2026-07-11T10:00:00Z",
            "updated_at": "2026-07-11T10:00:02Z",
            "metadata": {},
        },
        "progress": {
            "schema_version": "jarvis.execution.progress.v1",
            "execution_id": execution_id,
            "pipeline_id": "acceptance",
            "execution_state": "completed",
            "terminal": True,
            "packages": [
                {
                    "package_id": "package-lammps",
                    "package_name": "builtin.lammps",
                    "event_count": 2,
                    "latest": {
                        "schema_version": "jarvis.progress.v1",
                        "execution_id": execution_id,
                        "package_id": "package-lammps",
                        "package_name": "builtin.lammps",
                        "state": "running",
                        "label": "timestep",
                        "sequence": 2,
                        "observed_at_epoch": 1_788_000_000.0,
                        "determinate": True,
                        "current": 20.0,
                        "total": 100.0,
                        "unit": "step",
                        "message": "LAMMPS timestep 20",
                        "metadata": {},
                    },
                }
            ],
        },
    }


def _jarvis_server_artifact() -> dict[str, object]:
    install_spec = "/opt/wheels/clio_kit-2.3.1-py3-none-any.whl"
    return {
        "requested_command": jarvis_mcp_server(),
        "resolved_executable": "/home/user/.local/bin/clio-kit",
        "executable": {
            "path": "/home/user/.local/bin/clio-kit",
            "filename": "clio-kit",
            "sha256": "clio-kit-launcher-sha256",
            "size_bytes": 1,
        },
        "install_spec": install_spec,
        "install_source": "uv-tool",
        "install_artifact_sha256": "d" * 64,
        "input_files": [
            {
                "path": install_spec,
                "filename": "clio_kit-2.3.1-py3-none-any.whl",
                "sha256": "d" * 64,
                "size_bytes": 3,
            }
        ],
        "launcher_artifact_verified": True,
        "python_distribution_runtime": {
            "schema_version": "clio-relay.python-distribution-runtime.v1",
            "distribution": "clio-kit",
            "distribution_version": "2.3.1",
            "entry_point": "clio-kit",
            "runtime_closure_verified": True,
            "direct_url": {"url": "file:///opt/wheels/clio_kit-2.3.1-py3-none-any.whl"},
        },
        "nested_launcher": True,
        "nested_runtime": {
            "schema_version": "clio-kit.locked-server.v4",
            "server_name": "jarvis",
            "persistent_tool": True,
            "locked_runtime_verified": True,
        },
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
