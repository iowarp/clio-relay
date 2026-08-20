"""Worker environment identity and scheduler naming/status normalization
(iowarp/clio-relay#231).

Owner module for three small, independent leaf concerns bundled together
because none is individually large enough to earn its own file:

- Worker environment identity: ``_worker_installation_snapshot`` (the
  package/receipt identity a worker loaded, degrading to an explicit
  unverified marker rather than raising), ``_worker_process_identity``
  (exact Linux process-generation evidence for durable endpoint records),
  ``bootstrap_cluster_environment`` (create endpoint directories and verify
  required executables), ``_bounded_output_event_chunks`` (split persisted
  output into byte-bounded queue events without splitting a UTF-8
  codepoint), ``_file_summary`` (existence/size/digest summary for one
  file).
- Scheduler status/naming normalization: ``_scheduler_status_is_not_found``,
  ``_normalized_scheduler_status`` (bind a provider's status to the
  requested identity and bound all durable free-text fields),
  ``_configured_scheduler_provider_name``/``_validate_scheduler_launch_
  provider``, and the pipeline-YAML scheduler-name resolution chain
  (``_scheduler_name_from_job``/``_jarvis_pipeline_name``/``_scheduler_
  name_from_yaml``/``_scheduler_name_from_document``).
- Bounded value coercion: ``_optional_str``/``_optional_float``/
  ``_optional_metadata``.

Depends only on stdlib and already-stable clio_relay modules (models,
scheduler_providers, command_evidence, filesystem_paths, installation,
storage_runtime, jarvis_provider) -- none of this module's own callers or
callees live in any other ``endpoint_*.py`` owner module, so it is a pure
leaf with no cross-owner-module dependency at all.
"""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path
from typing import cast

import yaml

from clio_relay.command_evidence import bounded_error_detail
from clio_relay.config import RelaySettings
from clio_relay.endpoint_sidecar_types import OUTPUT_EVENT_MAX_BYTES
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.installation import installation_info
from clio_relay.jarvis_provider import JarvisCdProvider
from clio_relay.models import JarvisRunSpec, JobKind, RelayJob, SchedulerPhase, SchedulerStatus
from clio_relay.scheduler_providers import SchedulerProvider
from clio_relay.storage_runtime import storage_managed_queue


def _worker_installation_snapshot() -> dict[str, object]:
    """Capture the package/receipt identity loaded by this worker process."""
    try:
        return installation_info()
    except ConfigurationError as exc:
        return {
            "schema_version": "clio-relay.installation-info.unverified",
            "receipt_matches_install": False,
            "error": str(exc),
        }


def _worker_process_identity() -> dict[str, object] | None:
    """Return exact Linux process-generation evidence for durable endpoint records."""
    if os.name != "posix" or not hasattr(os, "getuid"):
        return None
    try:
        boot_id = (
            Path("/proc/sys/kernel/random/boot_id")
            .read_text(
                encoding="ascii",
            )
            .strip()
        )
        raw_stat = Path("/proc/self/stat").read_bytes()
    except OSError:
        return None
    if not boot_id or len(boot_id) > 128 or len(raw_stat) > 4096:
        return None
    closing_parenthesis = raw_stat.rfind(b")")
    fields = raw_stat[closing_parenthesis + 1 :].split()
    if closing_parenthesis < 0 or len(fields) <= 19:
        return None
    try:
        start_ticks = int(fields[19])
    except ValueError:
        return None
    return {
        "schema_version": "clio-relay.process-identity.v1",
        "boot_id": boot_id,
        "start_ticks": start_ticks,
        "uid": os.getuid(),
        "pid": os.getpid(),
    }


def bootstrap_cluster_environment(settings: RelaySettings) -> None:
    """Create endpoint directories and verify required executables are configured."""
    internal_filesystem_path(settings.core_dir, force_extended=True).mkdir(
        parents=True,
        exist_ok=True,
    )
    internal_filesystem_path(settings.spool_dir, force_extended=True).mkdir(
        parents=True,
        exist_ok=True,
    )
    queue = storage_managed_queue(settings)
    queue.storage_runtime.ensure_new_intake_allowed()
    provider = JarvisCdProvider(
        jarvis_bin=settings.jarvis_bin,
        agent_bin=settings.agent_bin,
        agent_adapter=settings.agent_adapter,
        agent_args=settings.agent_args,
    )
    provider.require_available()
    if settings.frps_addr is None or settings.frp_token is None:
        raise ConfigurationError("CLIO_RELAY_FRPS_ADDR and CLIO_RELAY_FRP_TOKEN are required")


def _bounded_output_event_chunks(text: str) -> list[str]:
    """Split persisted output into queue events with a strict UTF-8 byte bound."""
    if text == "":
        return []
    payload = text.encode("utf-8")
    chunks: list[str] = []
    offset = 0
    while offset < len(payload):
        end = min(offset + OUTPUT_EVENT_MAX_BYTES, len(payload))
        while end > offset:
            try:
                chunk = payload[offset:end].decode("utf-8")
            except UnicodeDecodeError as exc:
                end = offset + exc.start
                continue
            chunks.append(chunk)
            offset = end
            break
        else:
            raise RuntimeError("could not split valid UTF-8 output into bounded events")
    return chunks


def _file_summary(path: Path) -> dict[str, object]:
    storage_path = internal_filesystem_path(path)
    if not storage_path.exists():
        return {"path": str(path), "exists": False}
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": storage_path.stat().st_size,
        "sha256": hashlib.sha256(storage_path.read_bytes()).hexdigest(),
    }


def _extract_scheduler_job_id(line: str) -> str | None:
    explicit = re.search(r"\bscheduler_job_id=(?P<job_id>[A-Za-z0-9_.-]+)\b", line)
    if explicit is not None:
        return explicit.group("job_id")
    submitted = re.search(r"\bSubmitted batch job (?P<job_id>[A-Za-z0-9_.-]+)\b", line)
    if submitted is not None:
        return submitted.group("job_id")
    return None


def _scheduler_status_is_not_found(status: SchedulerStatus) -> bool:
    """Recognize a provider's exact-job not-found terminal observation."""
    return status.phase is SchedulerPhase.UNKNOWN and status.record_found is False


_SCHEDULER_STATUS_TEXT_FIELDS = (
    "raw_state",
    "reason",
    "partition",
    "qos",
    "user",
    "memory",
    "submit_time",
    "eligible_time",
    "start_time",
    "elapsed",
    "time_limit",
    "queue_position_scope",
    "queue_position_note",
)


def _normalized_scheduler_status(
    status: SchedulerStatus,
    *,
    expected_scheduler: str,
    expected_scheduler_job_id: str,
) -> SchedulerStatus:
    """Bind provider status to the requested identity and bound all durable text."""
    if (
        status.scheduler != expected_scheduler
        or status.scheduler_job_id != expected_scheduler_job_id
    ):
        detail = bounded_error_detail(
            "scheduler provider returned mismatched identity: "
            f"expected scheduler={expected_scheduler!r} "
            f"job_id={expected_scheduler_job_id!r}; "
            f"observed scheduler={status.scheduler!r} job_id={status.scheduler_job_id!r}"
        )
        return SchedulerStatus(
            scheduler=expected_scheduler,
            scheduler_job_id=expected_scheduler_job_id,
            phase=SchedulerPhase.UNKNOWN,
            reason="scheduler provider response identity mismatch",
            queue_position_note=detail,
            observed_at=status.observed_at,
        )
    payload = status.model_dump(mode="python")
    for field_name in _SCHEDULER_STATUS_TEXT_FIELDS:
        value = payload.get(field_name)
        if isinstance(value, str):
            payload[field_name] = bounded_error_detail(value)
    return SchedulerStatus.model_validate(payload)


def _configured_scheduler_provider_name(provider: SchedulerProvider | None) -> str:
    raw_name = "external" if provider is None else provider.name
    normalized = raw_name.strip().lower().replace("_", "-")
    if normalized in {"none", "unmanaged"}:
        return "external"
    if not normalized:
        raise ConfigurationError("configured worker scheduler provider must be non-empty")
    return normalized


def _validate_scheduler_launch_provider(*, requested: str | None, configured: str) -> None:
    if requested is None:
        return
    normalized_requested = requested.strip().lower().replace("_", "-")
    if normalized_requested in {"none", "unmanaged"}:
        normalized_requested = "external"
    if not normalized_requested:
        raise ConfigurationError("JARVIS scheduler provider must be non-empty")
    if normalized_requested != configured:
        raise ConfigurationError(
            "JARVIS pipeline scheduler provider does not match the configured worker provider: "
            f"{normalized_requested} != {configured}; no JARVIS execution was launched"
        )
    if normalized_requested != "slurm":
        raise ConfigurationError(
            "clio-relay 1.0 supports scheduled JARVIS execution only through slurm; "
            f"requested {normalized_requested}; no JARVIS execution was launched"
        )


def _scheduler_name_from_job(job: RelayJob) -> str | None:
    if not isinstance(job.spec, JarvisRunSpec):
        return None
    if job.spec.pipeline_yaml is not None:
        return _scheduler_name_from_yaml(job.spec.pipeline_yaml)
    if job.spec.pipeline_path is not None:
        try:
            pipeline_yaml = internal_filesystem_path(Path(job.spec.pipeline_path)).read_text(
                encoding="utf-8"
            )
        except OSError:
            return None
        return _scheduler_name_from_yaml(pipeline_yaml)
    return None


def _jarvis_pipeline_name(job: RelayJob) -> str | None:
    if job.kind == JobKind.JARVIS and isinstance(job.spec, JarvisRunSpec):
        return job.spec.pipeline_name
    return None


def _scheduler_name_from_yaml(pipeline_yaml: str) -> str | None:
    try:
        loaded = yaml.safe_load(pipeline_yaml)
    except yaml.YAMLError:
        return None
    return _scheduler_name_from_document(loaded)


def _scheduler_name_from_document(document: object) -> str | None:
    if not isinstance(document, dict):
        return None
    typed = cast(dict[str, object], document)
    scheduler = typed.get("scheduler")
    if isinstance(scheduler, dict):
        typed_scheduler = cast(dict[str, object], scheduler)
        name = typed_scheduler.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    config = typed.get("config")
    if isinstance(config, dict):
        config_scheduler = _scheduler_name_from_document(cast(dict[str, object], config))
        if config_scheduler is not None:
            return config_scheduler
    experiments = typed.get("experiments")
    if isinstance(experiments, list):
        for experiment in cast(list[object], experiments):
            experiment_scheduler = _scheduler_name_from_document(experiment)
            if experiment_scheduler is not None:
                return experiment_scheduler
    return None


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value != "" else None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("numeric progress fields cannot be booleans")
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str) and value != "":
        return float(value)
    raise ValueError("progress numeric field must be a number")


def _optional_metadata(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("progress metadata must be an object")
    typed = cast(dict[object, object], value)
    return {str(key): item for key, item in typed.items()}
