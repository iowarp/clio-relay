"""Authenticate one MCP progress stream and append relay-sidecar records.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3).
``_McpProgressBridge`` is one cohesive, security-sensitive class handling both
the legacy compatibility envelope and the native JARVIS progress transport --
splitting it further would separate methods that share invariants and mutable
instance state, so it stays here as a single ~530-line module (over the
150-500 sweet spot, still well under the 800-line hard cap; the same
single-class exception the wave-1 split used for
``mcp_wheel_snapshot.py``/566 lines). Nothing here is individually
monkeypatched by ``tests/test_mcp_call_runner.py`` beyond direct construction
(``runner._McpProgressBridge(...)``), so no facade reach-back is needed.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import stat
from pathlib import Path
from typing import Any, cast

from clio_relay.constants import (
    _JARVIS_REACHABLE_STATES,
    _QUERY_CONTRACTS,
    MCP_JARVIS_EXECUTION_PROGRESS_SCHEMA,
    MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA,
    MCP_JARVIS_RUNTIME_SCHEMA,
    MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA,
    MCP_PACKAGE_PROGRESS_MAX_NOTIFICATION_BYTES,
    MCP_PACKAGE_PROGRESS_MAX_NOTIFICATIONS,
    MCP_PACKAGE_PROGRESS_MAX_TOTAL_BYTES,
    MCP_PACKAGE_PROGRESS_SCHEMA,
    PROGRESS_SIDECAR_RECORD_SCHEMA,
)
from clio_relay.jarvis_native_execution_documents import (
    _validated_native_execution_documents,
    _validated_native_progress_snapshot,
)
from clio_relay.protocol_messages import (
    _finite_progress_number,
    _McpProtocolFailure,
    _nonempty_bounded_text,
    _reject_duplicate_json_keys,
)


class _McpProgressBridge:
    """Authenticate one MCP progress stream and append relay-sidecar records."""

    def __init__(
        self,
        *,
        path: Path,
        relay_token: str,
        expected_server_artifact_digest: str,
        observed_server_artifact_digest: str,
        expected_pipeline_id: str,
    ) -> None:
        self.path = path
        self.relay_token = relay_token
        self.expected_server_artifact_digest = expected_server_artifact_digest
        self.observed_server_artifact_digest = observed_server_artifact_digest
        self.expected_pipeline_id = expected_pipeline_id
        self.progress_token = secrets.token_urlsafe(32)
        self.notification_count = 0
        self.notification_bytes = 0
        self.last_sequence = 0
        self.sidecar_sequence = 0
        self.bound_execution_id: str | None = None
        self.bound_provider: dict[str, Any] | None = None
        self.acceptance_candidates: list[dict[str, Any]] = []
        self.native_mode: bool | None = None
        self.native_transport_sequence = 0
        self.native_execution_state: str | None = None
        self.native_execution_terminal: bool | None = None
        self.native_scripted_activation_observed = False
        self.native_package_names: dict[str, str] = {}
        self.native_package_sequences: dict[str, int] = {}
        self.native_package_event_counts: dict[str, int] = {}
        self.native_latest_candidates: dict[str, dict[str, Any]] = {}
        self.execution_validated = False

    def observe(self, message: dict[str, Any]) -> None:
        """Validate and bridge one package-progress notification immediately."""
        raw_params = message.get("params")
        if not isinstance(raw_params, dict):
            raise _McpProtocolFailure("MCP progress notification params must be an object")
        params = cast(dict[str, Any], raw_params)
        token = params.get("progressToken")
        if not isinstance(token, str) or not secrets.compare_digest(token, self.progress_token):
            raise _McpProtocolFailure("MCP progress notification token did not match")
        raw_message = params.get("message")
        if not isinstance(raw_message, str):
            raise _McpProtocolFailure("MCP package progress message must be schema-versioned JSON")
        encoded_size = len(raw_message.encode("utf-8"))
        if encoded_size > MCP_PACKAGE_PROGRESS_MAX_NOTIFICATION_BYTES:
            raise _McpProtocolFailure("MCP package progress notification exceeded its byte limit")
        self.notification_count += 1
        self.notification_bytes += encoded_size
        if self.notification_count > MCP_PACKAGE_PROGRESS_MAX_NOTIFICATIONS:
            raise _McpProtocolFailure("MCP package progress exceeded its notification limit")
        if self.notification_bytes > MCP_PACKAGE_PROGRESS_MAX_TOTAL_BYTES:
            raise _McpProtocolFailure("MCP package progress exceeded its total byte limit")
        try:
            envelope = json.loads(raw_message, object_pairs_hook=_reject_duplicate_json_keys)
        except (json.JSONDecodeError, ValueError) as exc:
            raise _McpProtocolFailure(f"MCP package progress JSON was invalid: {exc}") from exc
        typed_envelope = cast(dict[str, Any], envelope) if isinstance(envelope, dict) else None
        if (
            typed_envelope is not None
            and typed_envelope.get("schema_version") == MCP_JARVIS_EXECUTION_PROGRESS_SCHEMA
        ):
            self._observe_native_progress(typed_envelope, params=params)
            return
        if self.native_mode is True:
            raise _McpProtocolFailure("MCP progress producer changed from native to compatibility")
        self.native_mode = False
        validated = self._validated_envelope(typed_envelope, params=params)
        self._append_record(validated, execution_validated=False)
        if validated["provider_acceptance_validated"] is True:
            self.acceptance_candidates.append(validated)

    def finalize(self, structured_result: dict[str, Any] | None) -> None:
        """Bind accepted observations to the final JARVIS execution result."""
        if structured_result is None:
            if self.notification_count == 0:
                return
            raise _McpProtocolFailure(
                "MCP package progress had no structured JARVIS result for execution binding"
            )
        native_documents = _validated_native_execution_documents(structured_result)
        if native_documents is not None:
            if self.native_mode is False:
                raise _McpProtocolFailure(
                    "MCP compatibility progress result changed to native execution documents"
                )
            self._finalize_native_progress(native_documents)
            return
        if self.native_mode is True:
            raise _McpProtocolFailure(
                "MCP native progress result omitted native JARVIS execution documents"
            )
        if self.notification_count == 0:
            return
        raw_runtime = structured_result.get("runtime_metadata")
        if not isinstance(raw_runtime, dict):
            raise _McpProtocolFailure(
                "MCP package progress result omitted structured JARVIS runtime metadata"
            )
        runtime = cast(dict[str, Any], raw_runtime)
        if runtime.get("schema_version") != MCP_JARVIS_RUNTIME_SCHEMA:
            raise _McpProtocolFailure(
                "MCP package progress result omitted the JARVIS runtime producer schema"
            )
        if runtime.get("execution_id") != self.bound_execution_id:
            raise _McpProtocolFailure("MCP package progress execution id did not match the result")
        if runtime.get("pipeline_id") != self.expected_pipeline_id:
            raise _McpProtocolFailure("MCP package progress pipeline id did not match the result")
        package_name = (
            self.bound_provider.get("package_name") if self.bound_provider is not None else None
        )
        raw_provenance = runtime.get("package_provenance")
        if not isinstance(raw_provenance, list) or not any(
            isinstance(item, dict) and cast(dict[str, Any], item).get("pkg_type") == package_name
            for item in cast(list[object], raw_provenance)
        ):
            raise _McpProtocolFailure(
                "MCP package progress provider package was absent from runtime provenance"
            )
        self.execution_validated = True
        for candidate in self.acceptance_candidates:
            self._append_record(candidate, execution_validated=True)

    def result_metadata(self) -> dict[str, Any]:
        """Return non-secret progress-bridge provenance for ``mcp-result.json``."""
        if self.native_mode is True:
            return {
                "schema_version": MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA,
                "notification_count": self.notification_count,
                "notification_bytes": self.notification_bytes,
                "execution_id": self.bound_execution_id,
                "pipeline_id": self.expected_pipeline_id,
                "package_sequences": dict(sorted(self.native_package_sequences.items())),
                "expected_server_artifact_digest": self.expected_server_artifact_digest,
                "observed_server_artifact_digest": self.observed_server_artifact_digest,
                "execution_validated": self.execution_validated,
            }
        return {
            "schema_version": MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA,
            "notification_count": self.notification_count,
            "notification_bytes": self.notification_bytes,
            "execution_id": self.bound_execution_id,
            "pipeline_id": self.expected_pipeline_id,
            "provider": self.bound_provider,
            "expected_server_artifact_digest": self.expected_server_artifact_digest,
            "observed_server_artifact_digest": self.observed_server_artifact_digest,
            "execution_validated": self.execution_validated,
        }

    def _observe_native_progress(
        self,
        snapshot_value: dict[str, Any],
        *,
        params: dict[str, Any],
    ) -> None:
        """Validate one native snapshot without treating MCP progress as workload percent."""
        if self.native_mode is False:
            raise _McpProtocolFailure("MCP progress producer changed from compatibility to native")
        self.native_mode = True
        transport_value = _finite_progress_number(params.get("progress"))
        if (
            transport_value is None
            or not transport_value.is_integer()
            or int(transport_value) != self.native_transport_sequence + 1
        ):
            raise _McpProtocolFailure("MCP native progress transport sequence was not monotonic")
        self.native_transport_sequence = int(transport_value)
        snapshot = _validated_native_progress_snapshot(snapshot_value)
        if snapshot["pipeline_id"] != self.expected_pipeline_id:
            raise _McpProtocolFailure("MCP native progress pipeline id did not match the request")
        execution_id = cast(str, snapshot["execution_id"])
        if self.bound_execution_id is None:
            self.bound_execution_id = execution_id
        elif self.bound_execution_id != execution_id:
            raise _McpProtocolFailure("MCP native progress execution id changed")
        self._observe_native_execution_lifecycle(snapshot)
        packages = cast(list[dict[str, Any]], snapshot["packages"])
        package_ids = {cast(str, package["package_id"]) for package in packages}
        if not set(self.native_package_names).issubset(package_ids):
            raise _McpProtocolFailure("MCP native progress dropped a package identity")
        for package in packages:
            self._observe_native_package(
                snapshot,
                package,
                transport_sequence=self.native_transport_sequence,
            )

    def _observe_native_package(
        self,
        snapshot: dict[str, Any],
        package: dict[str, Any],
        *,
        transport_sequence: int,
    ) -> None:
        """Append a package's new latest event while recording skipped snapshot events."""
        package_id = cast(str, package["package_id"])
        package_name = cast(str, package["package_name"])
        prior_name = self.native_package_names.get(package_id)
        if prior_name is not None and prior_name != package_name:
            raise _McpProtocolFailure("MCP native package progress name changed")
        self.native_package_names[package_id] = package_name
        event_count = cast(int, package["event_count"])
        prior_count = self.native_package_event_counts.get(package_id, 0)
        if event_count < prior_count:
            raise _McpProtocolFailure("MCP native progress event count regressed")
        self.native_package_event_counts[package_id] = event_count
        latest = cast(dict[str, Any] | None, package["latest"])
        if latest is None:
            return
        event_sequence = cast(int, latest["sequence"])
        prior_sequence = self.native_package_sequences.get(package_id, -1)
        if event_sequence < prior_sequence:
            raise _McpProtocolFailure("MCP native package progress sequence regressed")
        if event_sequence == prior_sequence:
            if event_count != prior_count:
                raise _McpProtocolFailure(
                    "MCP native package progress count changed without a new event"
                )
            return
        if prior_sequence >= 0 and event_count == prior_count:
            raise _McpProtocolFailure(
                "MCP native package progress event changed without increasing its count"
            )
        candidate = {
            "snapshot": snapshot,
            "package": package,
            "event": latest,
            "transport_sequence": transport_sequence,
            "skipped_event_count": max(0, event_count - prior_count - 1),
        }
        self.native_package_sequences[package_id] = event_sequence
        self.native_latest_candidates[package_id] = candidate
        self._append_native_record(candidate, execution_validated=False)

    def _observe_native_execution_lifecycle(self, snapshot: dict[str, Any]) -> None:
        """Require each sampled execution state to be reachable without regression."""
        state = cast(str, snapshot["execution_state"])
        terminal = cast(bool, snapshot["terminal"])
        previous_state = self.native_execution_state
        previous_terminal = self.native_execution_terminal
        if previous_state is None:
            self.native_execution_state = state
            self.native_execution_terminal = terminal
            return
        if state == previous_state:
            if terminal is not previous_terminal:
                raise _McpProtocolFailure("MCP native progress terminal flag changed in place")
            return
        if state not in _JARVIS_REACHABLE_STATES[previous_state]:
            raise _McpProtocolFailure("MCP native progress execution state regressed")
        if previous_terminal is True and previous_state != "scripted":
            raise _McpProtocolFailure("MCP native terminal execution changed state")
        if previous_state == "scripted" and state != "failed":
            self.native_scripted_activation_observed = True
        self.native_execution_state = state
        self.native_execution_terminal = terminal

    def _finalize_native_progress(self, documents: dict[str, dict[str, Any]]) -> None:
        """Bind native observations to exact matching final execution documents."""
        self.native_mode = True
        handle = documents["execution_handle"]
        progress = documents["progress"]
        record = documents["execution_record"]
        execution_id = cast(str, handle["execution_id"])
        if cast(str, handle["pipeline_id"]) != self.expected_pipeline_id:
            raise _McpProtocolFailure("MCP native execution pipeline id did not match the request")
        if self.bound_execution_id is not None and self.bound_execution_id != execution_id:
            raise _McpProtocolFailure("MCP native progress execution id did not match the result")
        self.bound_execution_id = execution_id
        self._observe_native_execution_lifecycle(progress)
        if self.native_scripted_activation_observed and (
            handle["mode"] != "scheduler"
            or handle["scheduler_native_id"] is None
            or record["submitted"] is not True
        ):
            raise _McpProtocolFailure(
                "MCP native scripted execution activation lacked scheduler identity"
            )
        final_packages = {
            cast(str, package["package_id"]): package
            for package in cast(list[dict[str, Any]], progress["packages"])
        }
        if not set(self.native_package_names).issubset(final_packages):
            raise _McpProtocolFailure("MCP native final progress dropped a package identity")
        for package_id, candidate in self.native_latest_candidates.items():
            final_package = final_packages.get(package_id)
            if final_package is None:
                raise _McpProtocolFailure(
                    "MCP native progress package was absent from final result"
                )
            candidate_event = cast(dict[str, Any], candidate["event"])
            final_event = cast(dict[str, Any] | None, final_package["latest"])
            if final_event is None or cast(int, final_event["sequence"]) < cast(
                int, candidate_event["sequence"]
            ):
                raise _McpProtocolFailure("MCP native progress result regressed a package event")
            if (
                cast(int, final_event["sequence"]) == cast(int, candidate_event["sequence"])
                and final_event != candidate_event
            ):
                raise _McpProtocolFailure("MCP native progress changed an existing package event")
        final_candidates: list[tuple[str, str, int, int, dict[str, Any] | None]] = []
        for package in final_packages.values():
            package_id = cast(str, package["package_id"])
            package_name = cast(str, package["package_name"])
            previous_name = self.native_package_names.get(package_id)
            if previous_name is not None and previous_name != package_name:
                raise _McpProtocolFailure("MCP native final package progress name changed")
            latest = cast(dict[str, Any] | None, package["latest"])
            if latest is None:
                final_candidates.append((package_id, package_name, 0, -1, None))
                continue
            event_count = cast(int, package["event_count"])
            previous_count = self.native_package_event_counts.get(package_id, 0)
            if event_count < previous_count:
                raise _McpProtocolFailure("MCP native final progress event count regressed")
            previous_sequence = self.native_package_sequences.get(package_id, -1)
            final_sequence = cast(int, latest["sequence"])
            if final_sequence == previous_sequence and event_count != previous_count:
                raise _McpProtocolFailure(
                    "MCP native final progress count changed without a new event"
                )
            if (
                previous_sequence >= 0
                and final_sequence > previous_sequence
                and (event_count == previous_count)
            ):
                raise _McpProtocolFailure(
                    "MCP native final progress event changed without increasing its count"
                )
            candidate = {
                "snapshot": progress,
                "package": package,
                "event": latest,
                "transport_sequence": self.native_transport_sequence,
                "skipped_event_count": max(0, event_count - previous_count - 1),
            }
            final_candidates.append(
                (package_id, package_name, event_count, final_sequence, candidate)
            )
        self.execution_validated = True
        for package_id, package_name, event_count, final_sequence, candidate in final_candidates:
            self.native_package_names[package_id] = package_name
            self.native_package_event_counts[package_id] = event_count
            if candidate is None:
                continue
            self.native_package_sequences[package_id] = final_sequence
            self._append_native_record(candidate, execution_validated=True)

    def _append_native_record(
        self,
        candidate: dict[str, Any],
        *,
        execution_validated: bool,
    ) -> None:
        """Project one exact native event into the relay progress record transport."""
        snapshot = cast(dict[str, Any], candidate["snapshot"])
        package = cast(dict[str, Any], candidate["package"])
        event = cast(dict[str, Any], candidate["event"])
        metadata = dict(cast(dict[str, Any], event["metadata"]))
        metadata["mcp_native_progress_bridge"] = {
            "schema_version": MCP_JARVIS_NATIVE_PROGRESS_BRIDGE_SCHEMA,
            "execution_id": snapshot["execution_id"],
            "pipeline_id": snapshot["pipeline_id"],
            "execution_state": snapshot["execution_state"],
            "terminal": snapshot["terminal"],
            "transport_sequence": candidate["transport_sequence"],
            "package_name": package["package_name"],
            "package_id": package["package_id"],
            "event_count": package["event_count"],
            "event_schema_version": event["schema_version"],
            "event_sequence": event["sequence"],
            "event_state": event["state"],
            "observed_at_epoch": event["observed_at_epoch"],
            "determinate": event["determinate"],
            "skipped_event_count": candidate["skipped_event_count"],
            "expected_server_artifact_digest": self.expected_server_artifact_digest,
            "observed_server_artifact_digest": self.observed_server_artifact_digest,
            "execution_validated": execution_validated,
        }
        record: dict[str, Any] = {
            "label": event["label"],
            "message": event.get("message") or event["label"],
            "metadata": metadata,
        }
        for field_name in ("current", "total", "unit"):
            if field_name in event:
                record[field_name] = event[field_name]
        self._append_progress_payload(record)

    def _validated_envelope(
        self,
        envelope: object,
        *,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(envelope, dict):
            raise _McpProtocolFailure("MCP package progress envelope must be an object")
        typed = dict(cast(dict[str, Any], envelope))
        required = {
            "schema_version",
            "execution_id",
            "pipeline_id",
            "notification_sequence",
            "source_authority",
            "provider",
            "provider_acceptance_validated",
            "record",
        }
        if set(typed) != required or typed.get("schema_version") != MCP_PACKAGE_PROGRESS_SCHEMA:
            raise _McpProtocolFailure("MCP package progress envelope schema was invalid")
        execution_id = _nonempty_bounded_text(typed.get("execution_id"), "execution_id")
        pipeline_id = _nonempty_bounded_text(typed.get("pipeline_id"), "pipeline_id")
        if pipeline_id != self.expected_pipeline_id:
            raise _McpProtocolFailure("MCP package progress pipeline id did not match the request")
        sequence = typed.get("notification_sequence")
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence != self.last_sequence + 1
        ):
            raise _McpProtocolFailure("MCP package progress sequence was not monotonic")
        self.last_sequence = sequence
        source_authority = typed.get("source_authority")
        if source_authority not in {"package_log", "jarvis_stdout_fallback"}:
            raise _McpProtocolFailure("MCP package progress source authority was invalid")
        provider = _validated_progress_provider(typed.get("provider"))
        record = _validated_progress_record(typed.get("record"))
        metadata = cast(dict[str, Any], record["metadata"])
        for key, expected in (
            ("adapter", provider["adapter"]),
            ("package_name", provider["package_name"]),
            ("package_version", provider["package_version"]),
            ("run_id", execution_id),
            ("execution_id", execution_id),
        ):
            if metadata.get(key) != expected:
                raise _McpProtocolFailure(f"MCP package progress metadata {key} did not match")
        current = _finite_progress_number(params.get("progress"))
        if current is None or current != record["current"]:
            raise _McpProtocolFailure("MCP package progress current did not match its record")
        notification_total = params.get("total")
        record_total = record.get("total")
        if notification_total is None:
            if record_total is not None:
                raise _McpProtocolFailure("MCP package progress total did not match its record")
        elif _finite_progress_number(notification_total) != record_total:
            raise _McpProtocolFailure("MCP package progress total did not match its record")
        provider_acceptance = typed.get("provider_acceptance_validated")
        if not isinstance(provider_acceptance, bool):
            raise _McpProtocolFailure("MCP package progress provider acceptance must be boolean")
        binding = {
            "execution_id": execution_id,
            "provider": provider,
        }
        if self.bound_execution_id is None:
            self.bound_execution_id = execution_id
            self.bound_provider = provider
        elif binding != {
            "execution_id": self.bound_execution_id,
            "provider": self.bound_provider,
        }:
            raise _McpProtocolFailure("MCP package progress execution or provider changed")
        typed["execution_id"] = execution_id
        typed["pipeline_id"] = pipeline_id
        typed["provider"] = provider
        typed["record"] = record
        return typed

    def _append_record(
        self,
        envelope: dict[str, Any],
        *,
        execution_validated: bool,
    ) -> None:
        record = dict(cast(dict[str, Any], envelope["record"]))
        metadata = dict(cast(dict[str, Any], record["metadata"]))
        metadata["mcp_progress_bridge"] = {
            "schema_version": MCP_PACKAGE_PROGRESS_BRIDGE_SCHEMA,
            "execution_id": envelope["execution_id"],
            "pipeline_id": envelope["pipeline_id"],
            "notification_sequence": envelope["notification_sequence"],
            "source_authority": envelope["source_authority"],
            "provider": envelope["provider"],
            "provider_acceptance_validated": envelope["provider_acceptance_validated"],
            "expected_server_artifact_digest": self.expected_server_artifact_digest,
            "observed_server_artifact_digest": self.observed_server_artifact_digest,
            "execution_validated": execution_validated,
        }
        record["metadata"] = metadata
        self._append_progress_payload(record)

    def _append_progress_payload(self, record: dict[str, Any]) -> None:
        """Sign and append one relay-shaped progress payload."""
        sequence = self.sidecar_sequence + 1
        signed = {
            "schema_version": PROGRESS_SIDECAR_RECORD_SCHEMA,
            "sequence": sequence,
            "progress": record,
        }
        canonical = json.dumps(
            signed,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        sidecar_record = {
            **signed,
            "progress_hmac": hmac.new(
                self.relay_token.encode("utf-8"),
                canonical,
                hashlib.sha256,
            ).hexdigest(),
        }
        payload = (
            json.dumps(
                sidecar_record,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        )
        if len(payload.encode("utf-8")) > MCP_PACKAGE_PROGRESS_MAX_NOTIFICATION_BYTES:
            raise _McpProtocolFailure("bridged MCP package progress exceeded its byte limit")
        _append_progress_sidecar(self.path, payload)
        self.sidecar_sequence = sequence


def _validated_progress_provider(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP package progress provider must be an object")
    typed = {str(key): item for key, item in cast(dict[object, object], value).items()}
    required = {
        "entry_point",
        "entry_point_value",
        "distribution",
        "distribution_version",
        "adapter",
        "package_name",
        "package_version",
    }
    allowed = required | {"application_profile"}
    if not required.issubset(typed) or not set(typed).issubset(allowed):
        raise _McpProtocolFailure("MCP package progress provider identity was incomplete")
    for field_name in required:
        typed[field_name] = _nonempty_bounded_text(typed[field_name], field_name)
    profile = typed.get("application_profile")
    if profile is not None:
        typed["application_profile"] = _nonempty_bounded_text(
            profile,
            "application_profile",
        )
    return typed


def _validated_progress_record(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _McpProtocolFailure("MCP package progress record must be an object")
    typed = {str(key): item for key, item in cast(dict[object, object], value).items()}
    allowed = {"label", "current", "total", "unit", "message", "metadata"}
    if not {"label", "current", "metadata"}.issubset(typed) or not set(typed).issubset(allowed):
        raise _McpProtocolFailure("MCP package progress record fields were invalid")
    typed["label"] = _nonempty_bounded_text(typed["label"], "label")
    current = _finite_progress_number(typed["current"])
    if current is None:
        raise _McpProtocolFailure("MCP package progress current must be finite")
    typed["current"] = current
    if typed.get("total") is not None:
        total = _finite_progress_number(typed["total"])
        if total is None:
            raise _McpProtocolFailure("MCP package progress total must be finite")
        typed["total"] = total
    for field_name in ("unit", "message"):
        if typed.get(field_name) is not None:
            typed[field_name] = _nonempty_bounded_text(typed[field_name], field_name)
    metadata = typed.get("metadata")
    if not isinstance(metadata, dict):
        raise _McpProtocolFailure("MCP package progress metadata must be an object")
    typed["metadata"] = {
        str(key): item for key, item in cast(dict[object, object], metadata).items()
    }
    try:
        json.dumps(typed, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise _McpProtocolFailure(f"MCP package progress record was not JSON-safe: {exc}") from exc
    return typed


def _append_progress_sidecar(path: Path, payload: str) -> None:
    encoded = payload.encode("utf-8")
    flags = (
        os.O_WRONLY
        | os.O_APPEND
        | int(getattr(os, "O_BINARY", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        os.set_inheritable(descriptor, False)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise _McpProtocolFailure("relay progress sidecar is not a regular file")
        if opened.st_nlink != 1:
            raise _McpProtocolFailure("relay progress sidecar hardlink count changed")
        if os.name != "nt" and (
            opened.st_uid != os.getuid() or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise _McpProtocolFailure("relay progress sidecar ownership or mode changed")
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise _McpProtocolFailure("relay progress sidecar append made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _package_progress_bridge_from_invocation(
    *,
    operation: str,
    tool: str | None,
    arguments: dict[str, Any],
    expected_server_artifact_digest: str | None,
    expected_registered_contract: str | None,
    expected_jarvis_cd_lock_binding: dict[str, str] | None,
    observed_server_artifact_digest: str,
    server_artifact: dict[str, Any],
) -> _McpProgressBridge | None:
    """Create a private bridge only for a recognized artifact-bound JARVIS call."""
    progress_path = os.environ.get("CLIO_RELAY_PROGRESS_FILE")
    relay_token = os.environ.get("CLIO_RELAY_PROGRESS_TOKEN")
    if progress_path is None and relay_token is None:
        return None
    if progress_path is None or relay_token is None or not relay_token:
        raise ValueError("relay progress sidecar path and token must be configured together")
    if operation != "tools/call" or tool != "jarvis_run":
        return None
    registered_route = (
        expected_registered_contract in _QUERY_CONTRACTS and expected_jarvis_cd_lock_binding is None
    )
    built_in_route = (
        expected_registered_contract is None and expected_jarvis_cd_lock_binding is not None
    )
    if (
        expected_server_artifact_digest is None
        or not (registered_route or built_in_route)
        or observed_server_artifact_digest != expected_server_artifact_digest
        or server_artifact.get("verified") is not True
    ):
        return None
    pipeline_id = arguments.get("pipeline_id")
    if not isinstance(pipeline_id, str) or not pipeline_id:
        raise ValueError("artifact-bound jarvis_run progress requires pipeline_id")
    return _McpProgressBridge(
        path=Path(progress_path).expanduser(),
        relay_token=relay_token,
        expected_server_artifact_digest=expected_server_artifact_digest,
        observed_server_artifact_digest=observed_server_artifact_digest,
        expected_pipeline_id=pipeline_id,
    )
