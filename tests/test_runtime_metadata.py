from __future__ import annotations

from copy import deepcopy

import pytest

from clio_relay.runtime_metadata import (
    JARVIS_RUNTIME_METADATA_SCHEMA,
    JARVIS_SCHEDULER_SUBMISSION_SCHEMA,
    RuntimeMetadataIdentityConflictError,
    RuntimeMetadataSource,
    legacy_scheduler_runtime_metadata,
    merge_runtime_metadata,
    normalize_runtime_metadata,
    runtime_metadata_from_mcp_result_document,
    runtime_metadata_from_sidecar_record,
    runtime_sidecar_record,
    structured_mcp_result,
)


def test_mcp_runtime_metadata_prefers_structured_content() -> None:
    document = {
        "tool": "jarvis_run",
        "protocol_result": {
            "structuredContent": {
                "runtime_metadata": {
                    "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
                    "execution_id": "run-42",
                    "pipeline_id": "science-pipeline",
                    "scheduler": {
                        "provider": "slurm",
                        "type": "batch",
                        "job_id": "21813",
                        "phase": "allocated",
                        "allocated_nodes": ["compute-04", "compute-05"],
                    },
                    "paths": {
                        "script": "/runtime/submit.sh",
                        "hostfile": "/runtime/hosts",
                        "stdout": "/runtime/job.out",
                        "stderr": "/runtime/job.err",
                    },
                    "package_provenance": [
                        {
                            "package_name": "builtin.echo",
                            "package_version": "2.2.6",
                            "pkg_id": "echo-step",
                            "config_path": "/runtime/echo.yaml",
                        }
                    ],
                    "terminal": {
                        "state": "completed",
                        "terminal": True,
                        "returncode": 0,
                        "started_at": "2026-07-10T10:00:00Z",
                        "finished_at": "2026-07-10T10:01:00Z",
                    },
                    "details": {
                        "scheduler_submission": _scheduler_submission(
                            provider="slurm",
                            scheduler_job_id="21813",
                        )
                    },
                }
            },
            "content": [{"type": "text", "text": '{"scheduler_job_id":"wrong"}'}],
        },
    }

    metadata = runtime_metadata_from_mcp_result_document(document)

    assert metadata is not None
    assert metadata.source == RuntimeMetadataSource.JARVIS_MCP
    assert metadata.execution_id == "run-42"
    assert metadata.pipeline_id == "science-pipeline"
    assert metadata.scheduler_provider == "slurm"
    assert metadata.scheduler_type == "batch"
    assert metadata.scheduler_job_id == "21813"
    assert metadata.scheduler_phase == "allocated"
    assert metadata.script_path == "/runtime/submit.sh"
    assert metadata.hostfile_path == "/runtime/hosts"
    assert metadata.output_path == "/runtime/job.out"
    assert metadata.error_path == "/runtime/job.err"
    assert metadata.allocated_nodes == ["compute-04", "compute-05"]
    assert metadata.packages[0].name == "builtin.echo"
    assert metadata.packages[0].version == "2.2.6"
    assert metadata.packages[0].package_id == "echo-step"
    assert metadata.terminal.state == "completed"
    assert metadata.terminal.terminal is True
    assert metadata.terminal.returncode == 0


def test_mcp_runtime_metadata_rejects_stdout_only_json_rpc() -> None:
    document = {
        "tool": "jarvis_run",
        "stdout": (
            '{"jsonrpc":"2.0","id":"clio-relay-mcp-call","result":'
            '{"structuredContent":{"runtime_metadata":{"scheduler_job_id":"forged"}}}}'
        ),
    }

    assert runtime_metadata_from_mcp_result_document(document) is None


def test_direct_jarvis_mcp_return_records_synchronous_completion() -> None:
    document = {
        "tool": "jarvis_run",
        "arguments": {"pipeline_id": "direct-pipeline", "wait": True},
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
        "finished_at": 1783720556.5,
        "structured_result": {
            "pipeline_id": "direct-pipeline",
            "status": "running",
            "mode": "direct",
            "runtime_metadata": {
                "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
                "pipeline_id": "direct-pipeline",
            },
        },
    }

    metadata = runtime_metadata_from_mcp_result_document(document)

    assert metadata is not None
    assert metadata.terminal.state == "completed"
    assert metadata.terminal.terminal is True
    assert metadata.terminal.returncode == 0
    assert metadata.terminal.finished_at == "2026-07-10T21:55:56.500000Z"
    assert metadata.details["completion_normalization"] == {
        "basis": "successful synchronous jarvis_run MCP return",
        "mode": "direct",
        "wait": True,
        "reported_status": "running",
    }


def test_non_jarvis_mcp_result_cannot_claim_runtime_identity() -> None:
    result: dict[str, object] = {
        "structured_result": {"scheduler_job_id": "123"},
        "tool": "inspect",
    }

    assert runtime_metadata_from_mcp_result_document(result) is None

    result["structured_result"] = {
        "runtime_metadata": {"scheduler_job_id": "123", "scheduler_provider": "pbs"}
    }
    assert runtime_metadata_from_mcp_result_document(result) is None


def test_sidecar_runtime_metadata_requires_ordered_hmac_without_disclosing_key() -> None:
    record = runtime_sidecar_record(
        {
            "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
            "scheduler_provider": "pbs",
            "scheduler_type": "batch",
            "scheduler_job_id": "771.server",
            "allocated_nodes": ["node-a"],
            "details": {
                "scheduler_submission": _scheduler_submission(
                    provider="pbs",
                    scheduler_job_id="771.server",
                )
            },
        },
        key="correct",
        sequence=1,
    )

    metadata = runtime_metadata_from_sidecar_record(
        record,
        expected_key="correct",
        expected_sequence=1,
    )

    assert metadata.source == RuntimeMetadataSource.JARVIS_SIDECAR
    assert metadata.scheduler_job_id == "771.server"
    assert "correct" not in str(record)
    with pytest.raises(ValueError, match="HMAC did not match"):
        runtime_metadata_from_sidecar_record(
            record,
            expected_key="wrong",
            expected_sequence=1,
        )
    forged = deepcopy(record)
    forged_runtime = forged["runtime_metadata"]
    assert isinstance(forged_runtime, dict)
    forged_runtime["scheduler_job_id"] = "forged.server"
    with pytest.raises(ValueError, match="HMAC did not match"):
        runtime_metadata_from_sidecar_record(
            forged,
            expected_key="correct",
            expected_sequence=1,
        )
    with pytest.raises(ValueError, match="sequence did not match"):
        runtime_metadata_from_sidecar_record(
            record,
            expected_key="correct",
            expected_sequence=2,
        )


def test_structured_runtime_metadata_replaces_legacy_stdout_identity() -> None:
    legacy = legacy_scheduler_runtime_metadata(
        scheduler_job_id="stdout-id",
        scheduler_provider="slurm",
    )
    structured = normalize_runtime_metadata(
        {
            "runtime_metadata": {
                "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
                "scheduler_provider": "slurm",
                "scheduler_job_id": "structured-id",
                "script_path": "/runtime/submit.sh",
                "details": {
                    "scheduler_submission": _scheduler_submission(
                        provider="slurm",
                        scheduler_job_id="structured-id",
                    )
                },
            }
        },
        source=RuntimeMetadataSource.JARVIS_MCP,
    )

    assert structured is not None
    merged = merge_runtime_metadata(legacy, structured)

    assert merged.source == RuntimeMetadataSource.JARVIS_MCP
    assert merged.scheduler_job_id == "structured-id"
    assert merged.script_path == "/runtime/submit.sh"
    assert merged.field_sources["scheduler_job_id"] == RuntimeMetadataSource.JARVIS_MCP


def test_partial_structured_metadata_preserves_field_level_legacy_source() -> None:
    legacy = legacy_scheduler_runtime_metadata(
        scheduler_job_id="stdout-id",
        scheduler_provider="slurm",
    )
    structured = normalize_runtime_metadata(
        {
            "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
            "pipeline_id": "pipeline-a",
            "scheduler": {"provider": "slurm"},
        },
        source=RuntimeMetadataSource.JARVIS_MCP,
    )

    assert structured is not None
    merged = merge_runtime_metadata(legacy, structured)

    assert merged.source == RuntimeMetadataSource.JARVIS_MCP
    assert merged.scheduler_job_id == "stdout-id"
    assert merged.field_sources["scheduler_job_id"] == RuntimeMetadataSource.LEGACY_STDOUT
    assert merged.field_sources["pipeline_id"] == RuntimeMetadataSource.JARVIS_MCP


def test_first_authoritative_identity_is_pinned_across_sidecar_and_mcp_sources() -> None:
    sidecar = normalize_runtime_metadata(
        {
            "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
            "execution_id": "execution-owned",
            "scheduler_provider": "slurm",
            "scheduler_job_id": "12345",
            "scheduler_phase": "submitted",
            "details": {
                "scheduler_submission": _scheduler_submission(
                    provider="slurm",
                    scheduler_job_id="12345",
                )
            },
        },
        source=RuntimeMetadataSource.JARVIS_SIDECAR,
    )
    same_mcp_identity = normalize_runtime_metadata(
        {
            "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
            "execution_id": "execution-owned",
            "scheduler_provider": "SLURM",
            "scheduler_job_id": "12345",
            "scheduler_phase": "running",
            "details": {
                "scheduler_submission": _scheduler_submission(
                    provider="SLURM",
                    scheduler_job_id="12345",
                )
            },
        },
        source=RuntimeMetadataSource.JARVIS_MCP,
    )
    conflicting_mcp_identity = normalize_runtime_metadata(
        {
            "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
            "execution_id": "execution-forged",
            "scheduler_provider": "slurm",
            "scheduler_job_id": "99999",
            "scheduler_phase": "running",
            "details": {
                "scheduler_submission": _scheduler_submission(
                    provider="slurm",
                    scheduler_job_id="99999",
                )
            },
        },
        source=RuntimeMetadataSource.JARVIS_MCP,
    )

    assert sidecar is not None
    assert same_mcp_identity is not None
    assert conflicting_mcp_identity is not None
    merged = merge_runtime_metadata(sidecar, same_mcp_identity)

    assert merged.execution_id == "execution-owned"
    assert merged.scheduler_provider == "slurm"
    assert merged.scheduler_job_id == "12345"
    assert merged.scheduler_phase == "running"
    assert merged.field_sources["execution_id"] is RuntimeMetadataSource.JARVIS_SIDECAR
    assert merged.field_sources["scheduler_provider"] is RuntimeMetadataSource.JARVIS_SIDECAR
    assert merged.field_sources["scheduler_job_id"] is RuntimeMetadataSource.JARVIS_SIDECAR
    with pytest.raises(
        RuntimeMetadataIdentityConflictError,
        match="changed pinned execution_id",
    ):
        merge_runtime_metadata(merged, conflicting_mcp_identity)


def test_structured_mcp_result_rejects_unstructured_text() -> None:
    assert (
        structured_mcp_result({"content": [{"type": "text", "text": "Submitted batch job 123"}]})
        is None
    )


@pytest.mark.parametrize("schema_version", [None, "jarvis.runtime.v0", "unknown"])
def test_mcp_runtime_metadata_without_exact_producer_schema_is_untrusted(
    schema_version: str | None,
) -> None:
    runtime: dict[str, object] = {
        "scheduler_provider": "slurm",
        "scheduler_job_id": "12345",
        "details": {
            "scheduler_submission": _scheduler_submission(
                provider="slurm",
                scheduler_job_id="12345",
            )
        },
    }
    if schema_version is not None:
        runtime["schema_version"] = schema_version
    document = {
        "tool": "jarvis_run",
        "structured_result": {"runtime_metadata": runtime},
    }

    metadata = runtime_metadata_from_mcp_result_document(document)

    assert metadata is not None
    assert metadata.source == RuntimeMetadataSource.UNTRUSTED_COMPATIBILITY
    assert (
        metadata.field_sources["scheduler_job_id"] == RuntimeMetadataSource.UNTRUSTED_COMPATIBILITY
    )
    assert metadata.details["producer_contract"]["trusted"] is False


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("schema_version", "jarvis.scheduler.submission.v0"),
        ("provider", "pbs"),
        ("scheduler_job_id", "54321"),
        ("identity_source", "stdout"),
        ("submitted", False),
    ],
)
def test_scheduler_identity_requires_exact_submission_proof(
    field_name: str,
    field_value: object,
) -> None:
    submission = _scheduler_submission(provider="slurm", scheduler_job_id="12345")
    submission[field_name] = field_value
    metadata = normalize_runtime_metadata(
        {
            "runtime_metadata": {
                "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
                "scheduler_provider": "slurm",
                "scheduler_job_id": "12345",
                "details": {"scheduler_submission": submission},
            }
        },
        source=RuntimeMetadataSource.JARVIS_SIDECAR,
    )

    assert metadata is not None
    assert metadata.source == RuntimeMetadataSource.UNTRUSTED_COMPATIBILITY
    assert metadata.details["producer_contract"]["trusted"] is False


def _scheduler_submission(
    *,
    provider: str,
    scheduler_job_id: str,
) -> dict[str, object]:
    return {
        "schema_version": JARVIS_SCHEDULER_SUBMISSION_SCHEMA,
        "provider": provider,
        "scheduler_job_id": scheduler_job_id,
        "identity_source": "scheduler_submit_api",
        "submitted": True,
    }
