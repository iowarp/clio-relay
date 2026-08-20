"""Accumulate and construct live validation reports (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). This module owns the
report-construction concern: :class:`ValidationRecorder` accumulates checks,
resources, and structured transport-probe cleanup evidence into a
:class:`~clio_relay.validation_schema.LiveValidationReport` as a live
validation run proceeds, and :func:`new_live_validation_report` builds the
seeded report a run starts from (package/install-source identity plus
evidence-trust provenance).

Several call sites still reach back into :mod:`clio_relay.validation_report`
for concerns not yet extracted to their own owner module (install-source
detection, invocation redaction, and the durable report/Markdown writers).
Those imports are function-scoped -- not module-scoped -- because
:mod:`clio_relay.validation_report` imports :class:`ValidationRecorder` from
here for its own re-export surface; a module-scope import in the other
direction would be a load-order circular import (the proven idiom -- see the
module docstring precedent in :mod:`clio_relay.session_wire_models`). Each
function-scoped import gets re-pointed at the real owner module as that
concern is extracted in turn (acceptance-line fact classification already
moved to :mod:`clio_relay.acceptance_facts`, imported at module scope here
since that module has no back-reference of its own).
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from pydantic import ValidationError

from clio_relay.acceptance_facts import acceptance_scope, line_proves_success
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import logical_filesystem_text
from clio_relay.identifiers import DurableRecordId
from clio_relay.validation_limits import TRANSPORT_PROBE_EVIDENCE_KEY
from clio_relay.validation_schema import (
    EvidenceOrigin,
    EvidenceReference,
    EvidenceTrust,
    LiveValidationReport,
    TransportProbeEvidence,
    ValidationCheck,
    ValidationResource,
    ValidationStatus,
    parse_transport_probe_evidence,
)


class ValidationRecorder:
    """Accumulate checks and resources, then atomically persist a report."""

    def __init__(self, report: LiveValidationReport) -> None:
        self.report = report
        self._active_check: str | None = None
        self._job_ids_by_scope: dict[str, str] = {}
        self._scheduler_providers_by_scope: dict[str, str] = {}
        self._transport_probe_ids: set[str] = set()

    @property
    def transport_probe_count(self) -> int:
        """Return the number of distinct structured transport probes observed."""
        return len(self._transport_probe_ids)

    @contextmanager
    def check(self, check_id: str, summary: str) -> Generator[list[EvidenceReference]]:
        """Record a passed or failed check around a block of live work."""
        if self._active_check is not None:
            raise RuntimeError(f"validation check already active: {self._active_check}")
        self._active_check = check_id
        started_at = datetime.now(UTC)
        evidence: list[EvidenceReference] = []
        try:
            yield evidence
        except Exception as exc:
            self.report.checks.append(
                ValidationCheck(
                    check_id=check_id,
                    summary=summary,
                    status=ValidationStatus.FAILED,
                    started_at=started_at,
                    completed_at=datetime.now(UTC),
                    evidence=evidence,
                    error=logical_filesystem_text(f"{type(exc).__name__}: {exc}"),
                )
            )
            raise
        else:
            self.report.checks.append(
                ValidationCheck(
                    check_id=check_id,
                    summary=summary,
                    status=ValidationStatus.PASSED,
                    started_at=started_at,
                    completed_at=datetime.now(UTC),
                    evidence=evidence,
                )
            )
        finally:
            self._active_check = None

    def add_resource(self, resource: ValidationResource) -> None:
        """Add or merge a resource without duplicating its stable identity."""
        for existing in self.report.resources:
            if existing.kind == resource.kind and existing.resource_id == resource.resource_id:
                merged = existing.model_copy(
                    update={
                        "role": resource.role or existing.role,
                        "cluster": resource.cluster or existing.cluster,
                        "state": resource.state or existing.state,
                        "provider": resource.provider or existing.provider,
                        "references": list(
                            dict.fromkeys([*existing.references, *resource.references])
                        ),
                        "metadata": {**existing.metadata, **resource.metadata},
                    }
                )
                self.report.resources[self.report.resources.index(existing)] = merged
                return
        self.report.resources.append(resource)

    def observe_line(self, line: str) -> None:
        """Convert a verified human-facing acceptance fact into structured evidence."""
        key, separator, value = line.partition("=")
        if not separator:
            if line == "live acceptance passed":
                self._record_passed_fact("live-test.completed", line, line)
            return
        if key == TRANSPORT_PROBE_EVIDENCE_KEY:
            self._observe_transport_probe_evidence(parse_transport_probe_evidence(value))
            return
        if line_proves_success(key, value):
            self._record_passed_fact(key, key.replace(".", " "), line)
        scope = acceptance_scope(key)
        if key.endswith("job_id") and value.startswith("job_"):
            role = key.removeprefix("acceptance.").removesuffix("_job_id").strip(".")
            role = role or "primary"
            self._job_ids_by_scope[scope] = value
            self.add_resource(
                ValidationResource(
                    kind="relay_job",
                    resource_id=value,
                    role=role,
                    cluster=self.report.cluster,
                )
            )
        if key.endswith("job_state") or key.endswith("state"):
            job_id = self._job_ids_by_scope.get(scope)
            if job_id is not None:
                self.add_resource(
                    ValidationResource(
                        kind="relay_job",
                        resource_id=job_id,
                        cluster=self.report.cluster,
                        state=value,
                    )
                )
        if key == "transport.protocol" and value not in self.report.transport_modes:
            self.report.transport_modes.append(value)
        if key == "transport.protocol":
            if value == "ssh_forward":
                self._record_passed_fact("transport.ssh", "SSH-forward transport", line)
            else:
                self._record_passed_fact("transport.relay", "relay transport", line)
        if key == "direct_transport.result" and value == "xtcp":
            self._record_passed_fact("transport.direct", "direct XTCP transport", line)
        if key.endswith(".runtime_scheduler_provider"):
            self._scheduler_providers_by_scope[scope] = value
        if key.endswith(".runtime_scheduler_job_id"):
            self.add_resource(
                ValidationResource(
                    kind="scheduler_job",
                    resource_id=value,
                    role=scope,
                    cluster=self.report.cluster,
                    provider=self._scheduler_providers_by_scope.get(scope),
                    metadata={"metadata_source": "structured_runtime"},
                )
            )
        if key.endswith(".runtime_metadata_artifact"):
            self.add_resource(
                ValidationResource(
                    kind="artifact",
                    resource_id=value,
                    role="runtime_metadata",
                    cluster=self.report.cluster,
                )
            )
        if key.endswith(".structured_runtime_scheduler_identity") and value == "ok":
            self._record_passed_fact(
                "scheduler.structured-metadata",
                "structured runtime scheduler identity",
                line,
            )
        if key == "worker.running" and value == "passed":
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    state="running",
                )
            )
        if key == "worker.artifact-version":
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={"clio_relay_version": value},
                )
            )
        if key == "worker.artifact-sha256":
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={"artifact_sha256": value},
                )
            )
        if key == "worker.source-identity":
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={"source_identity": value},
                )
            )
        if key == "worker.components":
            try:
                raw_components = cast(object, json.loads(value))
            except json.JSONDecodeError as exc:
                raise ConfigurationError("worker.components must be valid JSON") from exc
            if not isinstance(raw_components, dict) or not all(
                isinstance(name, str) and isinstance(component, str)
                for name, component in cast(dict[object, object], raw_components).items()
            ):
                raise ConfigurationError("worker.components must be a string object")
            typed_components = cast(dict[str, str], raw_components)
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={"components": dict(typed_components)},
                )
            )
        if key in {"worker.component-artifacts", "worker.component-runtime"}:
            try:
                component_value = cast(object, json.loads(value))
            except json.JSONDecodeError as exc:
                raise ConfigurationError(f"{key} must be valid JSON") from exc
            if not isinstance(component_value, dict):
                raise ConfigurationError(f"{key} must be an object")
            metadata_key = (
                "component_artifacts"
                if key == "worker.component-artifacts"
                else "component_runtime"
            )
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={
                        metadata_key: {
                            str(name): item
                            for name, item in cast(dict[object, object], component_value).items()
                        }
                    },
                )
            )
        if key.endswith(("stdout_bytes", "stderr_bytes")):
            job_id = self._job_ids_by_scope.get(scope)
            if job_id is not None:
                stream = "stdout" if key.endswith("stdout_bytes") else "stderr"
                self.report.artifacts.append(
                    EvidenceReference(
                        kind="log",
                        reference=(
                            f"relay-log://{self.report.cluster}/{job_id}/{stream}?bytes={value}"
                        ),
                    )
                )
        if key.endswith("artifacts"):
            job_id = self._job_ids_by_scope.get(scope)
            if job_id is not None:
                for kind in value.split(","):
                    self.report.artifacts.append(
                        EvidenceReference(
                            kind=kind,
                            reference=f"relay-artifact://{self.report.cluster}/{job_id}/{kind}",
                        )
                    )

    def _observe_transport_probe_evidence(self, evidence: TransportProbeEvidence) -> None:
        if evidence.cluster != self.report.cluster:
            raise ConfigurationError(
                "transport probe evidence cluster does not match the validation report"
            )
        self._transport_probe_ids.add(evidence.probe_id)
        self.report.cleanup.requested = True
        if self.report.cleanup.mode == "not_requested":
            self.report.cleanup.mode = evidence.cleanup_mode
        for cleanup_resource in evidence.resources:
            metadata = {
                **cleanup_resource.metadata,
                "transport_probe_id": evidence.probe_id,
                "cleanup_mode": evidence.cleanup_mode,
                "action": cleanup_resource.action,
                "ownership_verified": cleanup_resource.ownership_verified,
                "verified_after_operation": cleanup_resource.verified_after_operation,
                "observed_state": cleanup_resource.observed_state,
                "residual": cleanup_resource.residual,
                "detail": cleanup_resource.detail,
            }
            validation_resource = ValidationResource(
                kind=cleanup_resource.kind,
                resource_id=cleanup_resource.resource_id,
                role=cleanup_resource.role,
                cluster=evidence.cluster,
                state=cleanup_resource.outcome,
                provider=cleanup_resource.provider,
                references=(
                    [cleanup_resource.location] if cleanup_resource.location is not None else []
                ),
                metadata=metadata,
            )
            self.add_resource(validation_resource)
            action = cleanup_resource.model_dump(mode="json")
            action.update(
                {
                    "probe_id": evidence.probe_id,
                    "cluster": evidence.cluster,
                    "cleanup_mode": evidence.cleanup_mode,
                }
            )
            action_identity = (
                cleanup_resource.kind,
                cleanup_resource.resource_id,
                cleanup_resource.action,
                evidence.probe_id,
            )
            for index, existing in enumerate(self.report.cleanup.actions):
                existing_identity = (
                    existing.get("kind"),
                    existing.get("resource_id"),
                    existing.get("action"),
                    existing.get("probe_id"),
                )
                if existing_identity == action_identity:
                    self.report.cleanup.actions[index] = action
                    break
            else:
                self.report.cleanup.actions.append(action)

            remaining_identity = (
                cleanup_resource.kind,
                cleanup_resource.resource_id,
                evidence.probe_id,
            )
            matching_remaining = [
                item
                for item in self.report.cleanup.remaining_resources
                if (
                    item.kind,
                    item.resource_id,
                    item.metadata.get("transport_probe_id"),
                )
                == remaining_identity
            ]
            if cleanup_resource.residual:
                if matching_remaining:
                    index = self.report.cleanup.remaining_resources.index(matching_remaining[0])
                    self.report.cleanup.remaining_resources[index] = validation_resource
                else:
                    self.report.cleanup.remaining_resources.append(validation_resource)
            elif matching_remaining:
                self.report.cleanup.remaining_resources.remove(matching_remaining[0])

    def record_failure(self, check_id: str, summary: str, error: BaseException) -> None:
        """Record a terminal failure that occurred outside a check context."""
        now = datetime.now(UTC)
        self.report.checks.append(
            ValidationCheck(
                check_id=check_id,
                summary=summary,
                status=ValidationStatus.FAILED,
                started_at=now,
                completed_at=now,
                error=logical_filesystem_text(f"{type(error).__name__}: {error}"),
            )
        )

    def finish(self, error: BaseException | None = None) -> None:
        """Set terminal report state without hiding the original exception."""
        self.report.completed_at = datetime.now(UTC)
        self.report.error = (
            None if error is None else logical_filesystem_text(f"{type(error).__name__}: {error}")
        )
        self.report.status = (
            ValidationStatus.PASSED
            if error is None
            and self.report.checks
            and all(check.status is ValidationStatus.PASSED for check in self.report.checks)
            else ValidationStatus.FAILED
        )

    def write(self, json_path: Path, markdown_path: Path | None = None) -> None:
        """Atomically write stable JSON and optional Markdown evidence."""
        from clio_relay.validation_report import (
            _atomic_write_text,
            render_validation_markdown,
            write_validation_report,
        )

        write_validation_report(self.report, json_path)
        if markdown_path is not None:
            _atomic_write_text(markdown_path, render_validation_markdown(self.report))

    def _record_passed_fact(self, check_id: str, summary: str, line: str) -> None:
        now = datetime.now(UTC)
        evidence = EvidenceReference(kind="acceptance_output", excerpt=line)
        for index, existing in enumerate(self.report.checks):
            if existing.check_id != check_id:
                continue
            merged = existing.model_copy(
                update={"evidence": [*existing.evidence, evidence], "completed_at": now}
            )
            self.report.checks[index] = merged
            return
        self.report.checks.append(
            ValidationCheck(
                check_id=check_id,
                summary=summary,
                status=ValidationStatus.PASSED,
                started_at=now,
                completed_at=now,
                evidence=[evidence],
            )
        )


def new_live_validation_report(
    *,
    scenario: str,
    cluster: str,
    transport_modes: Iterable[str] = (),
    launcher: str | None = None,
    install_source: str | None = None,
    artifact_sha256: str | None = None,
    report_id: DurableRecordId | None = None,
) -> LiveValidationReport:
    """Create a report seeded with package, source, and invocation provenance."""
    from clio_relay.validation_report import (
        _redacted_invocation,
        detect_install_source,
        detect_software_identity,
    )

    return LiveValidationReport(
        report_id=(report_id if report_id is not None else f"validation_{uuid4().hex}"),
        scenario=scenario,
        cluster=cluster,
        transport_modes=list(dict.fromkeys(transport_modes)),
        evidence_trust=_validation_evidence_trust(cluster),
        software=detect_software_identity(),
        install_source=detect_install_source(
            launcher=launcher,
            source_override=install_source,
            artifact_sha256=artifact_sha256,
        ),
        invocation=_redacted_invocation([str(item) for item in sys.orig_argv]),
    )


def _validation_evidence_trust(cluster: str) -> EvidenceTrust:
    """Build producer provenance only from explicit validation-run inputs."""
    login = os.environ.get("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN")
    raw_github_id = os.environ.get("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID")
    invocation_id = os.environ.get("CLIO_RELAY_VALIDATION_INVOCATION_ID")
    github_id: int | None = None
    if raw_github_id is not None:
        if re.fullmatch(r"[1-9][0-9]*", raw_github_id) is None:
            raise ConfigurationError(
                "CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID must be a positive integer"
            )
        github_id = int(raw_github_id)
    try:
        return EvidenceTrust(
            origin=(
                EvidenceOrigin.LOCAL_PROCESS
                if cluster == "local"
                else EvidenceOrigin.OPERATOR_GENERATED
            ),
            producer_github_login=login,
            producer_github_id=github_id,
            invocation_id=invocation_id,
        )
    except ValidationError as exc:
        raise ConfigurationError(f"invalid validation producer identity: {exc}") from exc
