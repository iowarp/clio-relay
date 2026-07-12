"""JARVIS-owned scheduled execution adapters.

Scheduler providers observe and cancel jobs; they never submit application work.
This boundary invokes JARVIS to materialize the execution plan and emits the
structured runtime contract consumed by the relay worker.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

from clio_relay.errors import ConfigurationError
from clio_relay.process_containment import (
    BROKER_CREDENTIAL_FD_ENV,
    BROKER_READY_FD_ENV,
)

ScheduledJarvisCommandFactory = Callable[[str, Path], list[str]]
PROGRESS_FILE_ENV = "CLIO_RELAY_PROGRESS_FILE"
PROGRESS_TOKEN_ENV = "CLIO_RELAY_PROGRESS_TOKEN"
RUNTIME_METADATA_FILE_ENV = "CLIO_RELAY_RUNTIME_METADATA_FILE"
RUNTIME_METADATA_TOKEN_ENV = "CLIO_RELAY_RUNTIME_METADATA_TOKEN"
RUNTIME_METADATA_ANCHOR_ENV = "CLIO_RELAY_RUNTIME_METADATA_ANCHOR"
RUNTIME_SUBMISSION_INTENT_ENV = "CLIO_RELAY_RUNTIME_SUBMISSION_INTENT"
RUNTIME_DIRECT_PROOF_ENV = "CLIO_RELAY_RUNTIME_DIRECT_PROOF"
JARVIS_CREDENTIAL_SCHEMA = "clio-relay.jarvis-private-credential.v1"
RUNTIME_CREDENTIAL_MAX_BYTES = 8 * 1024


def sanitized_jarvis_environment(
    environment: Mapping[str, str] | None,
) -> dict[str, str]:
    """Drop every relay sidecar capability from a JARVIS launch environment."""
    sanitized = dict(environment or {})
    for name in (
        PROGRESS_FILE_ENV,
        PROGRESS_TOKEN_ENV,
        RUNTIME_METADATA_FILE_ENV,
        RUNTIME_METADATA_TOKEN_ENV,
        RUNTIME_METADATA_ANCHOR_ENV,
        RUNTIME_SUBMISSION_INTENT_ENV,
        RUNTIME_DIRECT_PROOF_ENV,
        BROKER_CREDENTIAL_FD_ENV,
        BROKER_READY_FD_ENV,
    ):
        sanitized.pop(name, None)
    return sanitized


def jarvis_private_credential_channel(
    environment: Mapping[str, str] | None,
) -> tuple[dict[str, str], str]:
    """Move all JARVIS sidecar credentials into one private broker payload."""
    source = dict(environment or {})
    progress_file = source.get(PROGRESS_FILE_ENV)
    progress_token = source.get(PROGRESS_TOKEN_ENV)
    runtime_file = source.get(RUNTIME_METADATA_FILE_ENV)
    runtime_token = source.get(RUNTIME_METADATA_TOKEN_ENV)
    raw_anchor = source.get(RUNTIME_METADATA_ANCHOR_ENV)
    raw_intent = source.get(RUNTIME_SUBMISSION_INTENT_ENV)
    runtime_direct_proof = source.get(RUNTIME_DIRECT_PROOF_ENV)
    sanitized = sanitized_jarvis_environment(source)
    if (
        not progress_file
        or not progress_token
        or not runtime_file
        or not runtime_token
        or not raw_anchor
        or not raw_intent
        or not runtime_direct_proof
    ):
        raise ConfigurationError("JARVIS execution requires private sidecar credentials")
    try:
        decoded_anchor = cast(object, json.loads(raw_anchor))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("scheduled JARVIS runtime sidecar anchor is invalid") from exc
    if not isinstance(decoded_anchor, dict):
        raise ConfigurationError("scheduled JARVIS runtime sidecar anchor did not match")
    runtime_anchor = cast(dict[str, Any], decoded_anchor)
    if set(runtime_anchor) != {"device", "inode", "owner", "link_count", "mode"} or any(
        isinstance(value, bool) or not isinstance(value, int) for value in runtime_anchor.values()
    ):
        raise ConfigurationError("scheduled JARVIS runtime sidecar anchor did not match")
    try:
        decoded_intent = cast(object, json.loads(raw_intent))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("scheduled JARVIS submission intent is invalid") from exc
    if not isinstance(decoded_intent, dict):
        raise ConfigurationError("scheduled JARVIS submission intent did not match")
    runtime_intent = cast(dict[str, Any], decoded_intent)
    if (
        set(runtime_intent)
        != {
            "schema_version",
            "execution_id",
            "marker",
            "created_at",
            "scheduler_user",
            "scheduler_expected",
            "direct_proof_sha256",
        }
        or runtime_intent.get("schema_version") != "clio-relay.scheduler-submission-intent.v1"
        or any(
            not isinstance(runtime_intent.get(field), str) or not runtime_intent[field]
            for field in ("execution_id", "marker", "created_at", "scheduler_user")
        )
        or runtime_intent.get("scheduler_expected") not in {False, True, "unknown"}
        or not isinstance(runtime_intent.get("direct_proof_sha256"), str)
        or len(runtime_intent["direct_proof_sha256"]) != 64
        or not runtime_intent["marker"].startswith("clio-relay-")
        or hashlib.sha256(runtime_direct_proof.encode("utf-8")).hexdigest()
        != runtime_intent["direct_proof_sha256"]
    ):
        raise ConfigurationError("scheduled JARVIS submission intent did not match")
    payload = json.dumps(
        {
            "schema_version": JARVIS_CREDENTIAL_SCHEMA,
            "progress_file": progress_file,
            "progress_token": progress_token,
            "runtime_file": runtime_file,
            "runtime_token": runtime_token,
            "runtime_anchor": runtime_anchor,
            "runtime_intent": runtime_intent,
            "runtime_direct_proof": runtime_direct_proof,
        },
        separators=(",", ":"),
        allow_nan=False,
    )
    if len(payload.encode("utf-8")) > RUNTIME_CREDENTIAL_MAX_BYTES:
        raise ConfigurationError("scheduled JARVIS runtime credential exceeds its byte limit")
    return sanitized, payload


def scheduled_runtime_credential_channel(
    environment: Mapping[str, str] | None,
) -> tuple[dict[str, str], str]:
    """Compatibility alias for the generalized private JARVIS channel."""
    return jarvis_private_credential_channel(environment)


def scheduled_jarvis_command(
    scheduler_name: str,
    *,
    python_bin: str,
    pipeline_path: Path,
) -> list[str]:
    """Return the explicit JARVIS execution adapter for a scheduled pipeline."""
    normalized = scheduler_name.strip().lower().replace("_", "-")
    factory = _SCHEDULED_COMMAND_FACTORIES.get(normalized)
    if factory is None:
        raise ConfigurationError(
            f"JARVIS scheduled execution adapter is not registered: {scheduler_name}"
        )
    if normalized not in _CREDENTIAL_CONSUMING_ADAPTERS:
        raise ConfigurationError(
            f"JARVIS scheduled execution adapter did not declare credential consumption: "
            f"{scheduler_name}"
        )
    return factory(python_bin, pipeline_path)


def named_jarvis_command(*, python_bin: str, pipeline_name: str) -> list[str]:
    """Return the hardened wrapper that resolves a named pipeline after credential closure."""
    if not pipeline_name.strip():
        raise ConfigurationError("pipeline_name must be non-empty")
    return _credential_wrapper_command(python_bin, "named", pipeline_name)


def yaml_jarvis_command(*, python_bin: str, pipeline_path: Path) -> list[str]:
    """Return the hardened in-process wrapper for any YAML pipeline."""
    return _credential_wrapper_command(python_bin, "yaml", str(pipeline_path))


def register_scheduled_jarvis_command(
    scheduler_name: str,
    factory: ScheduledJarvisCommandFactory,
    *,
    consumes_runtime_credential: bool,
    replace: bool = False,
) -> None:
    """Register a JARVIS scheduled-execution adapter independently of observation."""
    normalized = scheduler_name.strip().lower().replace("_", "-")
    if not normalized:
        raise ConfigurationError("scheduler name must be non-empty")
    if normalized in _SCHEDULED_COMMAND_FACTORIES and not replace:
        raise ConfigurationError(
            f"JARVIS scheduled execution adapter is already registered: {normalized}"
        )
    if consumes_runtime_credential is not True:
        raise ConfigurationError(
            "scheduled JARVIS adapters must explicitly consume the private runtime credential"
        )
    _SCHEDULED_COMMAND_FACTORIES[normalized] = factory
    _CREDENTIAL_CONSUMING_ADAPTERS.add(normalized)


def _slurm_command(python_bin: str, pipeline_path: Path) -> list[str]:
    return yaml_jarvis_command(python_bin=python_bin, pipeline_path=pipeline_path)


def _credential_wrapper_command(
    python_bin: str,
    launch_mode: str,
    launch_target: str,
) -> list[str]:
    return [
        python_bin,
        "-I",
        "-S",
        "-c",
        _JARVIS_SLURM_EXECUTION_ADAPTER,
        launch_mode,
        launch_target,
    ]


_SCHEDULED_COMMAND_FACTORIES: dict[str, ScheduledJarvisCommandFactory] = {
    "slurm": _slurm_command,
}
_CREDENTIAL_CONSUMING_ADAPTERS = {"slurm"}


_JARVIS_SLURM_EXECUTION_ADAPTER = """
from __future__ import annotations

import getpass
import hashlib
import hmac
import json
import os
import stat
import sys
import sysconfig
import time
from datetime import datetime, timezone
from pathlib import Path

# Isolated/no-site startup prevents PYTHONPATH, .pth, sitecustomize, and
# usercustomize execution before credential closure. Add only known installed
# roots as plain paths, then apply the shared secret-memory gate before the
# broker pipe can place key material in this process.
module_roots = set()
for key in ("purelib", "platlib"):
    candidate = sysconfig.get_path(key)
    if isinstance(candidate, str):
        module_roots.add(candidate)
python_version = f"python{sys.version_info.major}.{sys.version_info.minor}"
environment_root = Path(sys.executable).absolute().parent.parent
module_roots.add(str(environment_root / "lib" / python_version / "site-packages"))
module_roots.add(str(environment_root / "lib64" / python_version / "site-packages"))
module_roots.add(str(environment_root / "Lib" / "site-packages"))
for module_root in sorted(module_roots):
    if Path(module_root).is_dir() and module_root not in sys.path:
        sys.path.append(module_root)

from clio_relay.process_containment import enforce_linux_secret_memory_gate

enforce_linux_secret_memory_gate()

# The containment broker passes this capability through a one-shot pipe. The
# initial process environment and command line contain only a non-secret file
# descriptor number. Read and close it before importing JARVIS/package code.
credential_fd_text = os.environ.pop("CLIO_RELAY_BROKER_CREDENTIAL_FD", None)
ready_fd_text = os.environ.pop("CLIO_RELAY_BROKER_READY_FD", None)
if (
    credential_fd_text is None
    or not credential_fd_text.isdecimal()
    or ready_fd_text is None
    or not ready_fd_text.isdecimal()
    or ready_fd_text == credential_fd_text
):
    raise RuntimeError("scheduled JARVIS broker credential descriptor is unavailable")
credential_fd = int(credential_fd_text)
ready_fd = int(ready_fd_text)

def fail_before_ready(message, cause=None):
    try:
        os.close(ready_fd)
    except OSError:
        pass
    if cause is None:
        raise RuntimeError(message)
    raise RuntimeError(message) from cause

os.set_inheritable(credential_fd, False)
os.set_inheritable(ready_fd, False)
credential_chunks = []
credential_size = 0
try:
    while True:
        chunk = os.read(credential_fd, 64 * 1024)
        if not chunk:
            break
        credential_size += len(chunk)
        if credential_size > 8 * 1024:
            fail_before_ready("scheduled JARVIS broker credential exceeded its byte limit")
        credential_chunks.append(chunk)
finally:
    os.close(credential_fd)
try:
    credential = json.loads(b"".join(credential_chunks))
except (UnicodeDecodeError, json.JSONDecodeError) as exc:
    fail_before_ready("scheduled JARVIS broker credential was invalid", exc)
if (
    not isinstance(credential, dict)
    or credential.get("schema_version") != "clio-relay.jarvis-private-credential.v1"
    or not isinstance(credential.get("progress_file"), str)
    or not credential["progress_file"]
    or not isinstance(credential.get("progress_token"), str)
    or not credential["progress_token"]
    or not isinstance(credential.get("runtime_file"), str)
    or not credential["runtime_file"]
    or not isinstance(credential.get("runtime_token"), str)
    or not credential["runtime_token"]
    or not isinstance(credential.get("runtime_anchor"), dict)
    or set(credential["runtime_anchor"]) != {"device", "inode", "owner", "link_count", "mode"}
    or any(
        isinstance(value, bool) or not isinstance(value, int)
        for value in credential["runtime_anchor"].values()
    )
    or not isinstance(credential.get("runtime_intent"), dict)
    or set(credential["runtime_intent"])
    != {
        "schema_version",
        "execution_id",
        "marker",
        "created_at",
        "scheduler_user",
        "scheduler_expected",
        "direct_proof_sha256",
    }
    or credential["runtime_intent"].get("schema_version")
    != "clio-relay.scheduler-submission-intent.v1"
    or any(
        not isinstance(credential["runtime_intent"].get(field), str)
        or not credential["runtime_intent"][field]
        for field in ("execution_id", "marker", "created_at", "scheduler_user")
    )
    or credential["runtime_intent"].get("scheduler_expected") not in {False, True, "unknown"}
    or not isinstance(credential["runtime_intent"].get("direct_proof_sha256"), str)
    or len(credential["runtime_intent"]["direct_proof_sha256"]) != 64
    or not credential["runtime_intent"]["marker"].startswith("clio-relay-")
    or not isinstance(credential.get("runtime_direct_proof"), str)
    or not credential["runtime_direct_proof"]
    or hashlib.sha256(credential["runtime_direct_proof"].encode("utf-8")).hexdigest()
    != credential["runtime_intent"]["direct_proof_sha256"]
):
    fail_before_ready("JARVIS broker credential contract did not match")
progress_file = credential["progress_file"]
progress_token = credential["progress_token"]
runtime_file = credential["runtime_file"]
runtime_token = credential["runtime_token"]
runtime_anchor = credential["runtime_anchor"]
runtime_intent = credential["runtime_intent"]
runtime_direct_proof = credential["runtime_direct_proof"]
if getpass.getuser() != runtime_intent["scheduler_user"]:
    fail_before_ready("JARVIS effective user did not match submission intent")
runtime_sequence = 0
RUNTIME_METADATA_MAX_RECORD_BYTES = 256 * 1024
RUNTIME_METADATA_MAX_TOTAL_BYTES = 4 * 1024 * 1024

# These are absent from the initial process environment. Expose them only now,
# after the non-dumpable gate and one-shot credential validation, so trusted
# JARVIS packages can emit ordered signed progress records.
os.environ["CLIO_RELAY_PROGRESS_FILE"] = progress_file
os.environ["CLIO_RELAY_PROGRESS_TOKEN"] = progress_token

try:
    acknowledgement_written = os.write(ready_fd, b"1")
finally:
    os.close(ready_fd)
if acknowledgement_written != 1:
    raise RuntimeError("scheduled JARVIS broker readiness acknowledgement was incomplete")

from jarvis_cd.core.pipeline_test import load_yaml_auto
from clio_relay.models import SchedulerPhase
from clio_relay.scheduler_providers import provider_for_scheduler

if len(sys.argv) != 3 or sys.argv[1] not in {"yaml", "named"}:
    raise RuntimeError("scheduled JARVIS wrapper launch contract did not match")
if sys.argv[1] == "yaml":
    _, obj = load_yaml_auto(sys.argv[2])
else:
    from jarvis_cd.core.pipeline import Pipeline

    obj = Pipeline(sys.argv[2])
    obj.load()
scheduler = getattr(obj, "scheduler", None)
direct_execution = not scheduler
if direct_execution and runtime_intent["scheduler_expected"] is True:
    raise RuntimeError("JARVIS object lost its expected scheduler contract")
if not direct_execution and runtime_intent["scheduler_expected"] is False:
    raise RuntimeError("JARVIS object acquired an unexpected scheduler contract")
if not direct_execution and not isinstance(scheduler, dict):
    raise RuntimeError("Scheduled JARVIS object did not retain its scheduler contract")
# Scheduled submissions do not execute packages in this process. Remove the
# progress capability before JARVIS launches any scheduler CLI child; only
# direct package execution retains it for signed progress emission.
if not direct_execution:
    os.environ.pop("CLIO_RELAY_PROGRESS_FILE", None)
    os.environ.pop("CLIO_RELAY_PROGRESS_TOKEN", None)
scheduler_name = None if direct_execution else scheduler.get("name")
if not direct_execution and (not isinstance(scheduler_name, str) or not scheduler_name):
    raise RuntimeError("Scheduled JARVIS object has no scheduler provider name")
if not direct_execution and scheduler_name.strip().lower().replace("_", "-") != "slurm":
    raise RuntimeError(
        "named scheduled JARVIS execution requires an explicitly registered secure adapter"
    )
submit = None if direct_execution else getattr(obj, "submit")
pipeline_id = getattr(obj, "name", None)
execution_id = runtime_intent["execution_id"]
started_at = runtime_intent["created_at"]
submission_marker = runtime_intent["marker"]
previous_job_name = None if direct_execution else scheduler.get("job_name")
if not direct_execution:
    scheduler["job_name"] = submission_marker
submission_intent = None if direct_execution else {
    "schema_version": "clio-relay.scheduler-submission-intent.v1",
    "execution_id": execution_id,
    "provider": scheduler_name,
    "marker": submission_marker,
    "created_at": started_at,
    "scheduler_user": runtime_intent["scheduler_user"],
    "scheduler_expected": runtime_intent["scheduler_expected"],
    "direct_proof_sha256": runtime_intent["direct_proof_sha256"],
    "previous_job_name": previous_job_name if isinstance(previous_job_name, str) else None,
}
package_provenance = []
for package in getattr(obj, "packages", []):
    if not isinstance(package, dict):
        continue
    package_provenance.append(
        {
            key: str(package[key])
            for key in ("pkg_id", "pkg_type", "global_id", "config_path")
            if package.get(key) is not None
        }
    )

def append_runtime_record(runtime_metadata: dict[str, object]) -> None:
    global runtime_sequence
    runtime_sequence += 1
    signed = {
        "schema_version": "clio-relay.runtime-sidecar-record.v1",
        "sequence": runtime_sequence,
        "runtime_metadata": runtime_metadata,
    }
    canonical = json.dumps(
        signed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    record = {
        **signed,
        "runtime_metadata_hmac": hmac.new(
            runtime_token.encode("utf-8"),
            canonical,
            hashlib.sha256,
        ).hexdigest(),
    }
    payload = json.dumps(record, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\\n"
    if len(payload) > RUNTIME_METADATA_MAX_RECORD_BYTES:
        raise RuntimeError("JARVIS runtime metadata record exceeded its byte limit")
    if runtime_file is None:
        return
    flags = (
        os.O_WRONLY
        | os.O_APPEND
        | int(getattr(os, "O_BINARY", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
    )
    descriptor = os.open(runtime_file, flags, 0o600)
    try:
        os.set_inheritable(descriptor, False)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise RuntimeError("JARVIS runtime metadata sidecar is not a regular file")
        observed_anchor = {
            "device": int(opened.st_dev),
            "inode": int(opened.st_ino),
            "owner": int(opened.st_uid),
            "link_count": int(opened.st_nlink),
            "mode": stat.S_IMODE(opened.st_mode),
        }
        if observed_anchor != runtime_anchor:
            raise RuntimeError("JARVIS runtime metadata sidecar identity changed")
        if opened.st_nlink != 1:
            raise RuntimeError("JARVIS runtime metadata sidecar hardlink count changed")
        if os.name != "nt":
            if opened.st_uid != os.getuid():
                raise RuntimeError("JARVIS runtime metadata sidecar ownership changed")
            if stat.S_IMODE(opened.st_mode) != 0o600:
                raise RuntimeError("JARVIS runtime metadata sidecar mode changed")
        if opened.st_size + len(payload) > RUNTIME_METADATA_MAX_TOTAL_BYTES:
            raise RuntimeError("JARVIS runtime metadata sidecar exceeded its total byte limit")
        written = os.write(descriptor, payload)
        if written != len(payload):
            raise RuntimeError("JARVIS runtime metadata sidecar append was incomplete")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def emit_runtime_metadata(
    phase: str,
    *,
    terminal: bool,
    returncode: int | None = None,
    reason: str | None = None,
    status: object | None = None,
) -> None:
    if runtime_file is None or runtime_token is None:
        return
    runtime_metadata = {
            "schema_version": "jarvis.runtime.v1",
            "source": "jarvis_sidecar",
            "execution_id": execution_id,
            "pipeline_id": pipeline_id if isinstance(pipeline_id, str) else None,
            "scheduler_provider": scheduler_name,
            "scheduler_type": scheduler_name,
            "scheduler_job_id": scheduler_job_id,
            "scheduler_phase": phase,
            "script_path": str(script_path),
            "hostfile_path": (
                str(scheduler["hostfile"]) if scheduler.get("hostfile") is not None else None
            ),
            "output_path": (
                str(scheduler["output"]) if scheduler.get("output") is not None else None
            ),
            "error_path": (
                str(scheduler["error"]) if scheduler.get("error") is not None else None
            ),
            "package_provenance": package_provenance,
            "terminal": {
                "state": phase,
                "terminal": terminal,
                "returncode": returncode,
                "reason": reason,
                "started_at": started_at,
                "finished_at": (
                    datetime.now(timezone.utc).isoformat() if terminal else None
                ),
            },
            "details": {
                "execution_owner": "jarvis_cd.pipeline.submit",
                "identity_source": submission_metadata["identity_source"],
                "scheduler_submission": submission_metadata,
                "scheduler_submission_intent": submission_intent,
                "scheduler_status": (
                    status.model_dump(mode="json") if status is not None else None
                ),
                "wait": "relay_scheduler_provider",
            },
    }
    append_runtime_record(runtime_metadata)


def emit_direct_runtime_metadata(
    phase: str,
    *,
    terminal: bool,
    returncode: int | None = None,
    reason: str | None = None,
) -> None:
    append_runtime_record(
        {
            "schema_version": "jarvis.runtime.v1",
            "source": "jarvis_sidecar",
            "execution_id": execution_id,
            "pipeline_id": pipeline_id if isinstance(pipeline_id, str) else None,
            "scheduler_phase": phase,
            "package_provenance": package_provenance,
            "terminal": {
                "state": phase,
                "terminal": terminal,
                "returncode": returncode,
                "reason": reason,
                "started_at": started_at,
                "finished_at": (
                    datetime.now(timezone.utc).isoformat() if terminal else None
                ),
            },
            "details": {
                "execution_owner": "jarvis_cd.pipeline.run",
                "execution_mode": "direct",
                "scheduler_expected": False,
                "direct_execution_proof": runtime_direct_proof,
            },
        }
    )


if direct_execution:
    run = getattr(obj, "run", None)
    if not callable(run):
        raise RuntimeError("named JARVIS object has no direct run operation")
    emit_direct_runtime_metadata("direct_running", terminal=False)
    try:
        run()
    except BaseException as exc:
        emit_direct_runtime_metadata(
            "direct_failed",
            terminal=True,
            returncode=1,
            reason=f"{type(exc).__name__}: {exc}",
        )
        raise
    emit_direct_runtime_metadata("direct_completed", terminal=True, returncode=0)
    raise SystemExit(0)


# Persist an HMAC-authenticated intent before the scheduler side effect. The
# unique provider-native job name lets the worker reconcile only an exact
# single match if this adapter dies during or immediately after submit().
append_runtime_record(
    {
        "schema_version": "jarvis.runtime.v1",
        "source": "jarvis_sidecar",
        "execution_id": execution_id,
        "pipeline_id": pipeline_id if isinstance(pipeline_id, str) else None,
        "scheduler_provider": scheduler_name,
        "scheduler_type": scheduler_name,
        "scheduler_phase": "submission_intent",
        "package_provenance": package_provenance,
        "terminal": {
            "state": "submission_intent",
            "terminal": False,
            "started_at": started_at,
        },
        "details": {
            "execution_owner": "jarvis_cd.pipeline.submit",
            "scheduler_submission_intent": submission_intent,
        },
    }
)


# JARVIS owns script generation and scheduler submission. It submits without a
# blocking scheduler wait so the provider-owned identity is available before
# the workload completes. clio-relay then observes that exact identity through
# its explicit scheduler provider; it never submits application work itself.
script_path = submit(submit=True, wait=False)
if script_path is None:
    raise RuntimeError("Scheduled JARVIS object did not return a scheduler script path")

submission_metadata = getattr(obj, "last_submission", None)
if not isinstance(submission_metadata, dict):
    raise RuntimeError("JARVIS-CD did not return structured scheduler submission metadata")
if submission_metadata.get("schema_version") != "jarvis.scheduler.submission.v1":
    raise RuntimeError("JARVIS-CD scheduler submission schema is unsupported")
if submission_metadata.get("provider") != scheduler_name:
    raise RuntimeError("JARVIS-CD scheduler submission provider did not match")
if submission_metadata.get("script_path") != str(script_path):
    raise RuntimeError("JARVIS-CD scheduler submission script did not match")
if submission_metadata.get("identity_source") != "scheduler_submit_api":
    raise RuntimeError("JARVIS-CD scheduler identity source is not authoritative")
if submission_metadata.get("submitted") is not True:
    raise RuntimeError("JARVIS-CD did not confirm scheduler submission")
if submission_metadata.get("reconciliation_marker") != submission_marker:
    raise RuntimeError("JARVIS-CD scheduler reconciliation marker did not match")
scheduler_job_id = submission_metadata.get("scheduler_job_id")
if not isinstance(scheduler_job_id, str) or not scheduler_job_id:
    raise RuntimeError("JARVIS-CD did not return a scheduler job identity")
if scheduler_name == "slurm" and not scheduler_job_id.isdecimal():
    raise RuntimeError("JARVIS-CD returned an invalid SLURM job identity")

provider = provider_for_scheduler(scheduler_name)
emit_runtime_metadata("submitted", terminal=False)
last_observation = None
while True:
    status = provider.poll(scheduler_job_id)
    observation = (
        status.phase.value,
        status.raw_state,
        status.reason,
        status.nodes,
        status.start_time,
    )
    if observation != last_observation:
        terminal = status.phase in {
            SchedulerPhase.COMPLETED,
            SchedulerPhase.FAILED,
            SchedulerPhase.CANCELED,
        }
        emit_runtime_metadata(
            status.phase.value,
            terminal=terminal,
            returncode=(0 if status.phase is SchedulerPhase.COMPLETED else 1 if terminal else None),
            reason=status.reason,
            status=status,
        )
        last_observation = observation
    if status.phase is SchedulerPhase.COMPLETED:
        break
    if status.phase in {SchedulerPhase.FAILED, SchedulerPhase.CANCELED}:
        raise RuntimeError(
            f"Scheduler job {scheduler_job_id} ended in {status.phase.value}: "
            f"{status.reason or status.raw_state or 'no reason reported'}"
        )
    time.sleep(5)
"""
