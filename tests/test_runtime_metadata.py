from __future__ import annotations

from copy import deepcopy
from typing import cast

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


def test_mcp_runtime_metadata_prefers_exact_native_execution_documents() -> None:
    structured = _native_execution_envelope(mode="direct", state="completed")
    runtime_projection = cast(dict[str, object], structured["runtime_metadata"])
    runtime_projection["output_path"] = "/runs/pipeline-a/stdout.log"
    runtime_projection["error_path"] = "/runs/pipeline-a/stderr.log"
    runtime_projection["package_provenance"] = [
        {
            "pkg_id": "render",
            "pkg_type": "builtin.paraview",
            "global_id": "builtin.paraview.render",
            "config_path": "/runs/pipeline-a/render.yaml",
        },
        {
            "pkg_id": "analysis",
            "pkg_type": "builtin.gray_scott",
            "config_path": "/runs/pipeline-a/analysis.yaml",
        },
    ]
    document = {
        "tool": "jarvis_run",
        "structured_result": structured,
    }

    metadata = runtime_metadata_from_mcp_result_document(document)

    assert metadata is not None
    assert metadata.source is RuntimeMetadataSource.JARVIS_MCP
    assert metadata.execution_id == "native-execution"
    assert metadata.pipeline_id == "pipeline-a"
    assert metadata.scheduler_provider is None
    assert metadata.scheduler_job_id is None
    assert metadata.scheduler_phase is None
    assert metadata.terminal.state == "completed"
    assert metadata.terminal.terminal is True
    assert metadata.terminal.returncode == 0
    assert metadata.output_path == "/runs/pipeline-a/stdout.log"
    assert metadata.error_path == "/runs/pipeline-a/stderr.log"
    assert metadata.packages[0].name == "builtin.paraview"
    assert metadata.packages[0].package_id == "render"
    assert metadata.packages[0].path == "/runs/pipeline-a/render.yaml"
    assert metadata.packages[0].metadata == {
        "global_id": "builtin.paraview.render",
        "progress_event_count": 1,
    }
    assert metadata.packages[1].name == "builtin.gray_scott"
    assert metadata.packages[1].package_id == "analysis"
    assert metadata.details["environment"] == {
        "schema_version": "jarvis.environment.v1",
        "spack_specs": ["paraview"],
    }
    assert metadata.details["producer_contract"]["contract_kind"] == "native_execution"
    assert metadata.details["producer_contract"]["trusted"] is True
    assert metadata.details["producer_contract"]["runtime_projection_merged"] is True
    native_progress = metadata.details["native_execution"]["progress"]
    assert native_progress["packages"][0]["latest"]["current"] is None


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("execution_id", "different-execution", "execution_id did not match"),
        ("pipeline_id", "different-pipeline", "pipeline_id did not match"),
        ("scheduler_phase", "running", "scheduler_phase did not match"),
    ],
)
def test_native_runtime_projection_rejects_authoritative_identity_or_lifecycle_drift(
    field_name: str,
    value: object,
    message: str,
) -> None:
    structured = _native_execution_envelope(mode="direct", state="completed")
    runtime_projection = cast(dict[str, object], structured["runtime_metadata"])
    runtime_projection[field_name] = value

    with pytest.raises(ValueError, match=message):
        runtime_metadata_from_mcp_result_document(
            {"tool": "jarvis_run", "structured_result": structured}
        )


def test_native_runtime_projection_rejects_terminal_and_detail_document_drift() -> None:
    structured = _native_execution_envelope(mode="direct", state="completed")
    runtime_projection = cast(dict[str, object], structured["runtime_metadata"])
    terminal = cast(dict[str, object], runtime_projection["terminal"])
    terminal["terminal"] = False

    with pytest.raises(ValueError, match=r"terminal\.terminal did not match"):
        runtime_metadata_from_mcp_result_document(
            {"tool": "jarvis_run", "structured_result": structured}
        )

    structured = _native_execution_envelope(mode="direct", state="completed")
    runtime_projection = cast(dict[str, object], structured["runtime_metadata"])
    details = cast(dict[str, object], runtime_projection["details"])
    details["execution_handle"] = {"forged": True}

    with pytest.raises(ValueError, match=r"details\.execution_handle did not match"):
        runtime_metadata_from_mcp_result_document(
            {"tool": "jarvis_run", "structured_result": structured}
        )


def test_native_runtime_projection_is_required_and_structurally_validated() -> None:
    structured = _native_execution_envelope(mode="direct", state="completed")
    structured.pop("runtime_metadata")

    with pytest.raises(ValueError, match="omitted structured runtime_metadata"):
        runtime_metadata_from_mcp_result_document(
            {"tool": "jarvis_run", "structured_result": structured}
        )

    structured = _native_execution_envelope(mode="direct", state="completed")
    runtime_projection = cast(dict[str, object], structured["runtime_metadata"])
    runtime_projection["package_provenance"] = [{"pkg_id": "missing-package-type"}]

    with pytest.raises(ValueError, match="package provenance contained an invalid entry"):
        runtime_metadata_from_mcp_result_document(
            {"tool": "jarvis_run", "structured_result": structured}
        )


def test_native_scheduler_documents_bind_provider_native_id_without_stdout() -> None:
    document = {
        "tool": "jarvis_run",
        "structured_result": _native_execution_envelope(
            mode="scheduler",
            state="submitted",
        ),
    }

    metadata = runtime_metadata_from_mcp_result_document(document)

    assert metadata is not None
    assert metadata.scheduler_provider == "slurm"
    assert metadata.scheduler_job_id == "24680"
    assert metadata.scheduler_phase == "submitted"
    assert metadata.terminal.terminal is False
    assert metadata.field_sources["scheduler_job_id"] is RuntimeMetadataSource.JARVIS_MCP


def test_native_execution_documents_reject_cross_document_identity_drift() -> None:
    structured = _native_execution_envelope(mode="direct", state="completed")
    progress = structured["progress"]
    assert isinstance(progress, dict)
    progress["execution_id"] = "different-execution"

    with pytest.raises(ValueError, match="execution identity did not match"):
        runtime_metadata_from_mcp_result_document(
            {"tool": "jarvis_run", "structured_result": structured}
        )


def test_native_execution_documents_reject_partial_envelope() -> None:
    structured = _native_execution_envelope(mode="direct", state="completed")
    structured.pop("execution_record")

    with pytest.raises(ValueError, match="omitted documents"):
        runtime_metadata_from_mcp_result_document(
            {"tool": "jarvis_run", "structured_result": structured}
        )


def test_native_scheduler_documents_reject_submission_cluster_drift() -> None:
    structured = _native_execution_envelope(mode="scheduler", state="submitted")
    record = cast(dict[str, object], structured["execution_record"])
    metadata = cast(dict[str, object], record["metadata"])
    submission = cast(dict[str, object], metadata["submission"])
    submission["scheduler_cluster"] = "different-cluster"

    with pytest.raises(ValueError, match="submission cluster did not match"):
        runtime_metadata_from_mcp_result_document(
            {"tool": "jarvis_run", "structured_result": structured}
        )


def test_native_execution_documents_reject_nonportable_identity() -> None:
    structured = _native_execution_envelope(mode="direct", state="completed")
    for key in ("execution_handle", "execution_record", "progress"):
        document = cast(dict[str, object], structured[key])
        document["execution_id"] = "CON"
    package = cast(
        list[dict[str, object]],
        cast(dict[str, object], structured["progress"])["packages"],
    )[0]
    cast(dict[str, object], package["latest"])["execution_id"] = "CON"

    with pytest.raises(ValueError, match="portable ASCII identity"):
        runtime_metadata_from_mcp_result_document(
            {"tool": "jarvis_run", "structured_result": structured}
        )


def test_native_runtime_merge_rejects_lifecycle_regression() -> None:
    completed = runtime_metadata_from_mcp_result_document(
        {
            "tool": "jarvis_run",
            "structured_result": _native_execution_envelope(
                mode="direct",
                state="completed",
            ),
        }
    )
    running = runtime_metadata_from_mcp_result_document(
        {
            "tool": "jarvis_run",
            "structured_result": _native_execution_envelope(
                mode="direct",
                state="running",
            ),
        }
    )
    assert completed is not None
    assert running is not None

    with pytest.raises(RuntimeMetadataIdentityConflictError, match="lifecycle regressed"):
        merge_runtime_metadata(completed, running)


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


def _native_execution_envelope(*, mode: str, state: str) -> dict[str, object]:
    execution_id = "native-execution"
    pipeline_id = "pipeline-a"
    scheduler = mode == "scheduler"
    terminal = state in {"scripted", "completed", "failed", "canceled"}
    native_id = "24680" if scheduler else None
    provider = "slurm" if scheduler else None
    cluster = "linux" if scheduler else None
    submission = {
        "schema_version": JARVIS_SCHEDULER_SUBMISSION_SCHEMA,
        "execution_id": execution_id,
        "provider": provider,
        "scheduler_job_id": native_id,
        "scheduler_cluster": cluster,
        "identity_source": "scheduler_submit_api",
        "submitted": True,
        "script_path": "/tmp/submit.sh",
        "hostfile_path": "/tmp/hosts",
    }
    metadata = {"submission": submission, "script_path": "/tmp/submit.sh"} if scheduler else {}
    handle = {
        "schema_version": "jarvis.execution.handle.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "mode": mode,
        "scheduler_provider": provider,
        "scheduler_native_id": native_id,
        "cluster": cluster,
    }
    record = {
        "schema_version": "jarvis.execution.record.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_id,
        "mode": mode,
        "scheduler_provider": provider,
        "scheduler_native_id": native_id,
        "cluster": cluster,
        "state": state,
        "submitted": scheduler,
        "terminal": terminal,
        "created_at": "2026-07-12T10:00:00Z",
        "updated_at": "2026-07-12T10:00:01Z",
        "return_code": 0 if state == "completed" else None,
        "error": None,
        "metadata": metadata,
    }
    latest = {
        "schema_version": "jarvis.progress.v1",
        "package_name": "builtin.paraview",
        "package_id": "render",
        "execution_id": execution_id,
        "label": "server readiness",
        "state": "ready",
        "sequence": 0,
        "observed_at_epoch": 1_789_000_000.0,
        "determinate": False,
        "metadata": {"mode": "server"},
    }
    progress = {
        "schema_version": "jarvis.execution.progress.v1",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "execution_state": state,
        "terminal": terminal,
        "packages": [
            {
                "package_id": "render",
                "package_name": "builtin.paraview",
                "event_count": 1,
                "latest": latest,
            }
        ],
    }
    documents: dict[str, object] = {
        "execution_handle": handle,
        "execution_record": record,
        "progress": progress,
    }
    documents["runtime_metadata"] = {
        "schema_version": JARVIS_RUNTIME_METADATA_SCHEMA,
        "source": "jarvis_mcp",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "mode": mode,
        "scheduler_provider": provider,
        "scheduler_native_id": native_id,
        "cluster": cluster,
        "scheduler_type": provider,
        "scheduler_job_id": native_id,
        "scheduler_phase": state if scheduler else None,
        "script_path": "/tmp/submit.sh" if scheduler else None,
        "hostfile_path": "/tmp/hosts" if scheduler else None,
        "output_path": None,
        "error_path": None,
        "package_provenance": [
            {
                "pkg_id": "render",
                "pkg_type": "builtin.paraview",
            }
        ],
        "terminal": {
            "state": state,
            "terminal": terminal,
            "returncode": 0 if state == "completed" else None,
            "reason": None,
            "started_at": "2026-07-12T10:00:00Z",
            "finished_at": "2026-07-12T10:00:01Z" if terminal else None,
        },
        "details": {
            "execution_owner": "jarvis_cd.execution_record",
            "submit": True if scheduler else None,
            "wait": None if scheduler else True,
            "environment": {
                "schema_version": "jarvis.environment.v1",
                "spack_specs": ["paraview"],
            },
            "execution_handle": deepcopy(handle),
            "execution_record": deepcopy(record),
            "scheduler_submission": deepcopy(submission) if scheduler else None,
        },
    }
    return documents
