"""Normalize exact native JARVIS execution documents into runtime metadata.

Extracted from ``runtime_metadata.py`` (clio-relay split/runtime-metadata-w2):
:func:`native_execution_documents` parses the exact ``execution_handle`` /
``execution_record`` / ``progress`` triple when a producer emits it (failing
closed on a partial envelope), and
:func:`runtime_metadata_from_native_documents` normalizes that trusted triple
into a :class:`~clio_relay.runtime_metadata_core_model.JarvisRuntimeMetadata`
document without scraping process output. :func:`_merge_native_runtime_projection`
then validates the clio-kit runtime projection against those same native
documents field-by-field and merges in only the non-authoritative detail
(paths, package provenance) it adds -- any disagreement with the native
record is a hard error, never a silent override.
"""

from __future__ import annotations

from typing import Any, cast

from clio_relay.runtime_metadata_coercion import _first_str, _mapping, _package_provenance
from clio_relay.runtime_metadata_core_model import (
    JarvisRuntimeMetadata,
    PackageProvenance,
    TerminalRuntimeMetadata,
    _field_sources,
)
from clio_relay.runtime_metadata_native_documents import (
    JarvisExecutionRecordDocument,
    JarvisNativeExecutionDocuments,
    _JarvisRuntimeProjectionDocument,
)
from clio_relay.runtime_metadata_native_validators import _validate_native_text
from clio_relay.runtime_metadata_types import RuntimeMetadataSource


def native_execution_documents(
    payload: dict[str, Any],
) -> JarvisNativeExecutionDocuments | None:
    """Parse an exact native JARVIS result envelope when one is present.

    A producer that emits any native document must emit all three. Partial
    envelopes fail closed instead of falling back to the legacy synthesized
    runtime contract.
    """
    keys = {"execution_handle", "execution_record", "progress"}
    present = keys & set(payload)
    if not present:
        return None
    if present != keys:
        missing = sorted(keys - present)
        raise ValueError(f"native JARVIS result omitted documents: {missing}")
    return JarvisNativeExecutionDocuments.model_validate(
        {key: payload[key] for key in sorted(keys)}
    )


def runtime_metadata_from_native_documents(
    documents: JarvisNativeExecutionDocuments,
    *,
    source: RuntimeMetadataSource,
) -> JarvisRuntimeMetadata:
    """Normalize exact JARVIS-owned documents without scraping process output."""
    handle = documents.execution_handle
    record = documents.execution_record
    progress = documents.progress
    submission = _mapping(record.metadata.get("submission"))
    packages = [
        PackageProvenance(
            name=package.package_name,
            package_type=package.package_name,
            package_id=package.package_id,
            metadata={"progress_event_count": package.event_count},
        )
        for package in progress.packages
    ]
    metadata = JarvisRuntimeMetadata(
        source=source,
        execution_id=record.execution_id,
        pipeline_id=record.pipeline_id,
        scheduler_provider=record.scheduler_provider,
        scheduler_type=record.scheduler_provider,
        scheduler_job_id=record.scheduler_native_id,
        scheduler_phase=_native_scheduler_phase(record),
        script_path=_first_str(record.metadata, "script_path")
        or (_first_str(submission, "script_path") if submission is not None else None),
        hostfile_path=(_first_str(submission, "hostfile_path") if submission is not None else None),
        output_path=(
            _first_str(submission, "output_path", "output") if submission is not None else None
        ),
        error_path=(
            _first_str(submission, "error_path", "error") if submission is not None else None
        ),
        packages=packages,
        terminal=TerminalRuntimeMetadata(
            state=record.state,
            terminal=record.terminal,
            returncode=record.return_code,
            reason=record.error,
            started_at=record.created_at,
            finished_at=record.updated_at if record.terminal else None,
        ),
        details={
            "execution_mode": handle.mode,
            "submitted": record.submitted,
            "scheduler_cluster": record.cluster,
            "native_execution": documents.model_dump(mode="json"),
            "producer_contract": {
                "requested_source": source.value,
                "contract_kind": "native_execution",
                "producer_schema_version": record.schema_version,
                "handle_schema_version": handle.schema_version,
                "progress_schema_version": progress.schema_version,
                "trusted": True,
                "reason": "exact native JARVIS execution documents matched",
            },
        },
    )
    return metadata.model_copy(update={"field_sources": _field_sources(metadata, source)})


def _native_scheduler_phase(record: JarvisExecutionRecordDocument) -> str | None:
    """Return lifecycle state only when a scheduler owns a submitted native job."""

    if (
        record.mode != "scheduler"
        or record.submitted is not True
        or record.scheduler_provider is None
        or record.scheduler_native_id is None
    ):
        return None
    return record.state


def _merge_native_runtime_projection(
    metadata: JarvisRuntimeMetadata,
    documents: JarvisNativeExecutionDocuments,
    value: object,
) -> JarvisRuntimeMetadata:
    """Validate and merge non-authoritative detail from clio-kit's runtime projection."""
    if not isinstance(value, dict):
        raise ValueError("native JARVIS result omitted structured runtime_metadata")
    projection = _JarvisRuntimeProjectionDocument.model_validate(value)
    handle = documents.execution_handle
    record = documents.execution_record
    authoritative = {
        "execution_id": record.execution_id,
        "pipeline_id": record.pipeline_id,
        "mode": record.mode,
        "scheduler_provider": record.scheduler_provider,
        "scheduler_native_id": record.scheduler_native_id,
        "cluster": record.cluster,
        "scheduler_type": record.scheduler_provider,
        "scheduler_job_id": record.scheduler_native_id,
        "scheduler_phase": _native_scheduler_phase(record),
    }
    for field_name, expected in authoritative.items():
        if getattr(projection, field_name) != expected:
            raise ValueError(
                "native JARVIS runtime projection "
                f"{field_name} did not match authoritative execution documents"
            )

    terminal = projection.terminal
    authoritative_terminal = {
        "state": record.state,
        "terminal": record.terminal,
        "returncode": record.return_code,
        "reason": record.error,
        "started_at": record.created_at,
        "finished_at": record.updated_at if record.terminal else None,
    }
    for field_name, expected in authoritative_terminal.items():
        if getattr(terminal, field_name) != expected:
            raise ValueError(
                "native JARVIS runtime projection terminal."
                f"{field_name} did not match authoritative execution documents"
            )

    for field_name in ("script_path", "hostfile_path"):
        if getattr(projection, field_name) != getattr(metadata, field_name):
            raise ValueError(
                "native JARVIS runtime projection "
                f"{field_name} did not match authoritative execution documents"
            )

    handle_document = handle.model_dump(mode="json")
    record_document = record.model_dump(mode="json")
    runtime_details = projection.details
    required_detail_documents = {
        "execution_handle": handle_document,
        "execution_record": record_document,
        "scheduler_submission": record.metadata.get("submission"),
    }
    for field_name, expected in required_detail_documents.items():
        if field_name not in runtime_details or runtime_details[field_name] != expected:
            raise ValueError(
                "native JARVIS runtime projection details."
                f"{field_name} did not match authoritative execution documents"
            )

    output_path = _enriched_native_path(
        metadata.output_path,
        projection.output_path,
        field_name="output_path",
    )
    error_path = _enriched_native_path(
        metadata.error_path,
        projection.error_path,
        field_name="error_path",
    )
    packages = _merge_native_package_provenance(
        metadata.packages,
        projection.package_provenance,
    )

    merged_details = dict(runtime_details)
    for field_name, value in metadata.details.items():
        if field_name in merged_details and merged_details[field_name] != value:
            raise ValueError(
                "native JARVIS runtime projection details."
                f"{field_name} attempted to override authoritative relay metadata"
            )
        merged_details[field_name] = value
    producer_contract = cast(dict[str, Any], merged_details["producer_contract"])
    merged_details["producer_contract"] = {
        **producer_contract,
        "runtime_projection_schema_version": projection.schema_version,
        "runtime_projection_merged": True,
    }
    merged = metadata.model_copy(
        update={
            "output_path": output_path,
            "error_path": error_path,
            "packages": packages,
            "details": merged_details,
        }
    )
    return merged.model_copy(
        update={"field_sources": _field_sources(merged, RuntimeMetadataSource.JARVIS_MCP)}
    )


def _enriched_native_path(
    authoritative: str | None,
    projected: str | None,
    *,
    field_name: str,
) -> str | None:
    """Use a richer producer path only when it does not conflict with native evidence."""
    if authoritative is not None and projected not in {None, authoritative}:
        raise ValueError(
            f"native JARVIS runtime projection {field_name} conflicted with native evidence"
        )
    return authoritative or projected


def _merge_native_package_provenance(
    native_packages: list[PackageProvenance],
    projected_items: list[dict[str, Any]],
) -> list[PackageProvenance]:
    """Enrich progress identities with JARVIS package provenance without identity drift."""
    projected_packages = _package_provenance(projected_items)
    if len(projected_packages) != len(projected_items):
        raise ValueError("native JARVIS runtime package provenance contained an invalid entry")
    native_by_id = {
        package.package_id: package for package in native_packages if package.package_id is not None
    }
    consumed_native_ids: set[str] = set()
    observed_projected_ids: set[str] = set()
    merged: list[PackageProvenance] = []
    for raw_item, projected in zip(projected_items, projected_packages, strict=True):
        if projected.package_id is not None:
            if projected.package_id in observed_projected_ids:
                raise ValueError("native JARVIS runtime package provenance repeated a package_id")
            observed_projected_ids.add(projected.package_id)
        native = (
            native_by_id.get(projected.package_id) if projected.package_id is not None else None
        )
        metadata = dict(projected.metadata)
        global_id = raw_item.get("global_id")
        if global_id is not None:
            if not isinstance(global_id, str):
                raise ValueError("native JARVIS runtime package global_id was invalid")
            _validate_native_text(global_id, "runtime package global_id", maximum=256)
            metadata["global_id"] = global_id
        if native is not None:
            if projected.name != native.name or projected.package_type not in {
                None,
                native.package_type,
                native.name,
            }:
                raise ValueError(
                    "native JARVIS runtime package provenance conflicted with progress identity"
                )
            consumed_native_ids.add(cast(str, native.package_id))
            metadata = {**metadata, **native.metadata}
            projected = projected.model_copy(
                update={
                    "name": native.name,
                    "package_type": projected.package_type or native.package_type,
                    "metadata": metadata,
                }
            )
        else:
            projected = projected.model_copy(update={"metadata": metadata})
        merged.append(projected)
    merged.extend(
        package
        for package in native_packages
        if package.package_id is None or package.package_id not in consumed_native_ids
    )
    return merged
