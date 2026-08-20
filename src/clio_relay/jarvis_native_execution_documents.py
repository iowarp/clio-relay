"""Validation for the native JARVIS execution handle/record/progress documents.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). Pure
leaf validation -- no facade reach-back needed.
"""

from __future__ import annotations

from typing import Any, cast

from clio_relay.constants import (
    _JARVIS_EXECUTION_STATES,
    _JARVIS_PROGRESS_STATES,
    _JARVIS_TERMINAL_STATES,
    _WINDOWS_RESERVED_COMPONENTS,
    MCP_JARVIS_EXECUTION_HANDLE_SCHEMA,
    MCP_JARVIS_EXECUTION_PROGRESS_SCHEMA,
    MCP_JARVIS_EXECUTION_RECORD_SCHEMA,
    MCP_JARVIS_PROGRESS_EVENT_SCHEMA,
)
from clio_relay.protocol_messages import (
    _bounded_finite_json,
    _finite_progress_number,
    _McpProtocolFailure,
)


def _validated_native_execution_documents(
    value: dict[str, Any],
) -> dict[str, dict[str, Any]] | None:
    """Validate an exact native JARVIS handle/record/progress result envelope."""
    keys = {"execution_handle", "execution_record", "progress"}
    present = keys & set(value)
    if not present:
        return None
    if present != keys:
        raise _McpProtocolFailure("MCP native JARVIS result omitted execution documents")
    handle = _validated_native_execution_handle(value["execution_handle"])
    record = _validated_native_execution_record(value["execution_record"])
    progress = _validated_native_progress_snapshot(value["progress"])
    identity_fields = (
        "execution_id",
        "pipeline_id",
        "mode",
        "scheduler_provider",
        "scheduler_native_id",
        "cluster",
    )
    if any(handle[field] != record[field] for field in identity_fields):
        raise _McpProtocolFailure("MCP native JARVIS handle and record identities did not match")
    if (
        progress["execution_id"] != record["execution_id"]
        or progress["pipeline_id"] != record["pipeline_id"]
        or progress["execution_state"] != record["state"]
        or progress["terminal"] is not record["terminal"]
    ):
        raise _McpProtocolFailure("MCP native JARVIS record and progress did not match")
    return {
        "execution_handle": handle,
        "execution_record": record,
        "progress": progress,
    }


def _validated_native_execution_handle(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP native JARVIS execution_handle must be an object")
    typed = dict(cast(dict[str, Any], value))
    expected = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "mode",
        "scheduler_provider",
        "scheduler_native_id",
        "cluster",
    }
    if set(typed) != expected or typed.get("schema_version") != MCP_JARVIS_EXECUTION_HANDLE_SCHEMA:
        raise _McpProtocolFailure("MCP native JARVIS execution_handle schema was invalid")
    _native_identity(typed.get("execution_id"), "execution_id")
    _native_identity(typed.get("pipeline_id"), "pipeline_id")
    mode = typed.get("mode")
    if mode not in {"direct", "scheduler"}:
        raise _McpProtocolFailure("MCP native JARVIS execution mode was invalid")
    for field_name in ("scheduler_provider", "scheduler_native_id", "cluster"):
        field_value = typed.get(field_name)
        if field_value is not None:
            _native_text(field_value, field_name)
    if mode == "direct" and any(
        typed.get(field_name) is not None
        for field_name in ("scheduler_provider", "scheduler_native_id", "cluster")
    ):
        raise _McpProtocolFailure("MCP native direct execution claimed scheduler identity")
    if mode == "scheduler" and typed.get("scheduler_provider") is None:
        raise _McpProtocolFailure("MCP native scheduler execution omitted its provider")
    if typed.get("scheduler_provider") == "slurm":
        native_id = typed.get("scheduler_native_id")
        cluster = typed.get("cluster")
        if native_id is not None and (
            len(cast(str, native_id)) > 64
            or not cast(str, native_id).isascii()
            or not cast(str, native_id).isdigit()
        ):
            raise _McpProtocolFailure("MCP native SLURM identity was invalid")
        if cluster is not None and (
            len(cast(str, cluster)) > 255
            or any(
                not (character.isascii() and (character.isalnum() or character in "._-"))
                for character in cast(str, cluster)
            )
        ):
            raise _McpProtocolFailure("MCP native SLURM cluster was invalid")
    return typed


def _validated_native_execution_record(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP native JARVIS execution_record must be an object")
    typed = dict(cast(dict[str, Any], value))
    expected = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "pipeline_name",
        "mode",
        "scheduler_provider",
        "scheduler_native_id",
        "cluster",
        "state",
        "submitted",
        "terminal",
        "created_at",
        "updated_at",
        "return_code",
        "error",
        "metadata",
    }
    if set(typed) != expected or typed.get("schema_version") != MCP_JARVIS_EXECUTION_RECORD_SCHEMA:
        raise _McpProtocolFailure("MCP native JARVIS execution_record schema was invalid")
    handle_projection = {
        "schema_version": MCP_JARVIS_EXECUTION_HANDLE_SCHEMA,
        **{
            key: typed[key]
            for key in (
                "execution_id",
                "pipeline_id",
                "mode",
                "scheduler_provider",
                "scheduler_native_id",
                "cluster",
            )
        },
    }
    _validated_native_execution_handle(handle_projection)
    if typed.get("pipeline_name") != typed.get("pipeline_id"):
        raise _McpProtocolFailure("MCP native JARVIS pipeline identity did not match")
    state = typed.get("state")
    if state not in _JARVIS_EXECUTION_STATES:
        raise _McpProtocolFailure("MCP native JARVIS execution state was invalid")
    submitted = typed.get("submitted")
    terminal = typed.get("terminal")
    if not isinstance(submitted, bool) or not isinstance(terminal, bool):
        raise _McpProtocolFailure("MCP native JARVIS lifecycle flags must be boolean")
    if terminal and state not in _JARVIS_TERMINAL_STATES:
        raise _McpProtocolFailure("MCP native terminal execution state was invalid")
    if state in {"completed", "failed", "canceled"} and terminal is not True:
        raise _McpProtocolFailure("MCP native terminal state omitted terminal=true")
    return_code = typed.get("return_code")
    if return_code is not None and (
        isinstance(return_code, bool) or not isinstance(return_code, int)
    ):
        raise _McpProtocolFailure("MCP native JARVIS return_code was invalid")
    if state == "completed" and return_code != 0:
        raise _McpProtocolFailure("MCP native completed execution requires return_code=0")
    if state == "failed" and (return_code is None or return_code == 0):
        raise _McpProtocolFailure("MCP native failed execution requires a nonzero return_code")
    _native_timestamp(typed.get("created_at"), "created_at")
    _native_timestamp(typed.get("updated_at"), "updated_at")
    error = typed.get("error")
    if error is not None:
        _native_text(error, "error", maximum=16_384, allow_newlines=True)
    metadata_value = typed.get("metadata")
    if not isinstance(metadata_value, dict):
        raise _McpProtocolFailure("MCP native JARVIS execution metadata must be an object")
    metadata_document = cast(dict[str, Any], metadata_value)
    _bounded_finite_json(metadata_document, "native JARVIS execution metadata", 48_000)
    native_id = typed.get("scheduler_native_id")
    raw_submission = metadata_document.get("submission")
    if raw_submission is None:
        if native_id is not None or submitted is True:
            raise _McpProtocolFailure("MCP native scheduler identity omitted submission proof")
        return typed
    if not isinstance(raw_submission, dict):
        raise _McpProtocolFailure("MCP native scheduler submission proof must be an object")
    if typed["mode"] != "scheduler":
        raise _McpProtocolFailure("MCP native direct execution carried scheduler submission proof")
    submission_document = cast(dict[str, Any], raw_submission)
    submission_submitted = submission_document.get("submitted")
    if (
        submission_document.get("schema_version") != "jarvis.scheduler.submission.v1"
        or submission_document.get("execution_id") != typed.get("execution_id")
        or submission_document.get("provider") != typed.get("scheduler_provider")
        or submission_document.get("scheduler_job_id") != native_id
        or submission_document.get("scheduler_cluster") != typed.get("cluster")
        or not isinstance(submission_submitted, bool)
        or submission_submitted is not submitted
    ):
        raise _McpProtocolFailure("MCP native scheduler submission proof did not match")
    identity_source = submission_document.get("identity_source")
    if native_id is not None and (
        identity_source != "scheduler_submit_api" or submission_submitted is not True
    ):
        raise _McpProtocolFailure("MCP native scheduler submission identity was not authoritative")
    if native_id is None and identity_source is not None:
        raise _McpProtocolFailure("MCP native scheduler submission source claimed no identity")
    for field_name in (
        "script_path",
        "hostfile_path",
        "pipeline_snapshot_path",
        "pipeline_input_path",
        "execution_root_path",
        "output_path",
        "error_path",
    ):
        field_value = submission_document.get(field_name)
        if field_value is not None:
            _native_text(field_value, field_name, maximum=16_384)
    return typed


def _validated_native_progress_snapshot(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP native JARVIS progress must be an object")
    typed = dict(cast(dict[str, Any], value))
    expected = {
        "schema_version",
        "execution_id",
        "pipeline_id",
        "execution_state",
        "terminal",
        "packages",
    }
    if (
        set(typed) != expected
        or typed.get("schema_version") != MCP_JARVIS_EXECUTION_PROGRESS_SCHEMA
    ):
        raise _McpProtocolFailure("MCP native JARVIS progress snapshot schema was invalid")
    execution_id = _native_identity(typed.get("execution_id"), "execution_id")
    _native_identity(typed.get("pipeline_id"), "pipeline_id")
    if typed.get("execution_state") not in _JARVIS_EXECUTION_STATES:
        raise _McpProtocolFailure("MCP native JARVIS progress state was invalid")
    terminal = typed.get("terminal")
    if not isinstance(terminal, bool):
        raise _McpProtocolFailure("MCP native JARVIS progress terminal flag was invalid")
    if terminal and typed["execution_state"] not in _JARVIS_TERMINAL_STATES:
        raise _McpProtocolFailure("MCP native JARVIS terminal progress state was invalid")
    if typed["execution_state"] in {"completed", "failed", "canceled"} and not terminal:
        raise _McpProtocolFailure("MCP native JARVIS terminal progress omitted terminal=true")
    raw_packages = typed.get("packages")
    if not isinstance(raw_packages, list):
        raise _McpProtocolFailure("MCP native JARVIS progress packages must be an array")
    packages: list[dict[str, Any]] = []
    package_ids: set[str] = set()
    for raw_package in cast(list[object], raw_packages):
        if not isinstance(raw_package, dict):
            raise _McpProtocolFailure("MCP native JARVIS package progress must be an object")
        package = dict(cast(dict[str, Any], raw_package))
        if set(package) != {"package_id", "package_name", "event_count", "latest"}:
            raise _McpProtocolFailure("MCP native JARVIS package progress fields were invalid")
        package_id = _native_text(package.get("package_id"), "package_id", maximum=256)
        package_name = _native_text(package.get("package_name"), "package_name", maximum=256)
        event_count = package.get("event_count")
        if isinstance(event_count, bool) or not isinstance(event_count, int) or event_count < 0:
            raise _McpProtocolFailure("MCP native JARVIS event_count was invalid")
        if package_id in package_ids:
            raise _McpProtocolFailure("MCP native JARVIS progress repeated a package_id")
        package_ids.add(package_id)
        latest_value = package.get("latest")
        latest = None if latest_value is None else _validated_native_progress_event(latest_value)
        if (event_count == 0) is not (latest is None):
            raise _McpProtocolFailure("MCP native JARVIS event_count did not match latest")
        if latest is not None and (
            latest["package_id"] != package_id
            or latest["package_name"] != package_name
            or latest["execution_id"] != execution_id
        ):
            raise _McpProtocolFailure("MCP native JARVIS package event identity did not match")
        package["latest"] = latest
        packages.append(package)
    typed["packages"] = packages
    return typed


def _validated_native_progress_event(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP native JARVIS progress event must be an object")
    typed = dict(cast(dict[str, Any], value))
    required = {
        "schema_version",
        "package_name",
        "package_id",
        "execution_id",
        "label",
        "state",
        "sequence",
        "observed_at_epoch",
        "determinate",
        "metadata",
    }
    optional = {"current", "total", "unit", "message"}
    if (
        not required.issubset(typed)
        or not set(typed).issubset(required | optional)
        or typed.get("schema_version") != MCP_JARVIS_PROGRESS_EVENT_SCHEMA
    ):
        raise _McpProtocolFailure("MCP native JARVIS progress event schema was invalid")
    for field_name in ("package_name", "package_id", "execution_id", "label"):
        _native_text(typed.get(field_name), field_name, maximum=256)
    if typed.get("state") not in _JARVIS_PROGRESS_STATES:
        raise _McpProtocolFailure("MCP native JARVIS progress event state was invalid")
    sequence = typed.get("sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        raise _McpProtocolFailure("MCP native JARVIS progress event sequence was invalid")
    observed = _finite_progress_number(typed.get("observed_at_epoch"))
    if observed is None or observed < 0:
        raise _McpProtocolFailure("MCP native JARVIS progress timestamp was invalid")
    raw_current = typed.get("current")
    raw_total = typed.get("total")
    current = None if raw_current is None else _finite_progress_number(raw_current)
    total = None if raw_total is None else _finite_progress_number(raw_total)
    if raw_current is not None and (current is None or current < 0):
        raise _McpProtocolFailure("MCP native JARVIS progress current was invalid")
    if raw_total is not None and (
        total is None or total <= 0 or current is None or current > total
    ):
        raise _McpProtocolFailure("MCP native JARVIS progress total was invalid")
    if typed.get("determinate") is not (current is not None and total is not None):
        raise _McpProtocolFailure("MCP native JARVIS determinate flag was invalid")
    if typed.get("unit") is not None:
        _native_text(typed.get("unit"), "unit", maximum=256)
    if typed.get("message") is not None:
        _native_text(typed.get("message"), "message")
    metadata_value = typed.get("metadata")
    if not isinstance(metadata_value, dict):
        raise _McpProtocolFailure("MCP native JARVIS progress metadata must be an object")
    _bounded_finite_json(
        cast(dict[str, Any], metadata_value),
        "native JARVIS progress metadata",
        48_000,
    )
    return typed


def _native_identity(value: object, field_name: str) -> str:
    rendered = _native_text(value, field_name, maximum=128)
    reserved_stem = rendered.split(".", 1)[0].upper()
    if (
        not rendered[0].isalnum()
        or rendered.endswith(".")
        or reserved_stem in _WINDOWS_RESERVED_COMPONENTS
        or any(
            not (character.isascii() and (character.isalnum() or character in "._-"))
            for character in rendered
        )
    ):
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} was not portable")
    return rendered


def _native_text(
    value: object,
    field_name: str,
    *,
    maximum: int = 4096,
    allow_newlines: bool = False,
) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum:
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} was invalid")
    allowed_controls: set[str] = {"\n", "\r", "\t"} if allow_newlines else set()
    if any(
        (ord(character) < 32 and character not in allowed_controls) or ord(character) == 127
        for character in value
    ):
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} contained controls")
    return value


def _native_timestamp(value: object, field_name: str) -> str:
    rendered = _native_text(value, field_name, maximum=64)
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(rendered.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} was invalid") from exc
    if parsed.tzinfo is None:
        raise _McpProtocolFailure(f"MCP native JARVIS {field_name} omitted timezone")
    return rendered
