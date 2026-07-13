"""JARVIS-owned scheduled execution adapters.

Scheduler providers observe and cancel jobs; they never submit application work.
This boundary invokes JARVIS to materialize the execution plan and emits the
structured runtime contract consumed by the relay worker.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
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
RUNTIME_SCHEDULER_PROVIDER_ENV = "CLIO_RELAY_RUNTIME_SCHEDULER_PROVIDER"
JARVIS_CREDENTIAL_SCHEMA = "clio-relay.jarvis-private-credential.v1"
RUNTIME_CREDENTIAL_MAX_BYTES = 8 * 1024


def _read_native_execution(
    obj: Any,
    *,
    execution_id: str,
    pipeline_id: str,
    direct_execution: bool,
    scheduler_name: str | None,
    submission_marker: str,
    sleep: Callable[[float], None],
) -> tuple[dict[str, Any], Any]:
    """Read one coherent, validated JARVIS handle/record/progress snapshot."""
    from clio_relay.runtime_metadata import native_execution_documents

    for _attempt in range(8):
        record = obj.get_execution(execution_id)
        progress = obj.get_execution_progress(execution_id)
        record_document = cast(dict[str, Any], record.to_dict())
        progress_document = cast(dict[str, Any], progress.to_dict())
        if progress_document.get("execution_state") != record_document.get(
            "state"
        ) or progress_document.get("terminal") is not record_document.get("terminal"):
            sleep(0.01)
            continue
        envelope: dict[str, Any] = {
            "execution_handle": record.handle.to_dict(),
            "execution_record": record_document,
            "progress": progress_document,
        }
        documents = native_execution_documents(envelope)
        if documents is None:
            raise RuntimeError("JARVIS native execution documents were unavailable")
        handle = documents.execution_handle
        if (
            handle.execution_id != execution_id
            or handle.pipeline_id != pipeline_id
            or handle.mode != ("direct" if direct_execution else "scheduler")
        ):
            raise RuntimeError("JARVIS native execution identity did not match relay intent")
        if not direct_execution:
            assert scheduler_name is not None
            assert handle.scheduler_provider is not None
            normalized_provider = handle.scheduler_provider.strip().lower().replace("_", "-")
            if normalized_provider != scheduler_name:
                raise RuntimeError("JARVIS native scheduler provider did not match")
            submission = documents.execution_record.metadata.get("submission")
            if submission is not None:
                if not isinstance(submission, dict):
                    raise RuntimeError("JARVIS scheduler submission proof was invalid")
                typed_submission = cast(dict[str, Any], submission)
                if typed_submission.get("reconciliation_marker") != submission_marker:
                    raise RuntimeError("JARVIS scheduler reconciliation marker did not match")
        return envelope, record
    raise RuntimeError("JARVIS execution changed during every bounded snapshot read")


def _validate_operation_handle(
    handle: object,
    *,
    execution_id: str,
    pipeline_id: str,
    direct_execution: bool,
) -> None:
    """Require a top-level JARVIS operation to return its exact native handle."""
    from clio_relay.runtime_metadata import JarvisExecutionHandleDocument

    to_dict = getattr(handle, "to_dict", None)
    if not callable(to_dict):
        raise RuntimeError("JARVIS execution did not return an ExecutionHandle")
    document = JarvisExecutionHandleDocument.model_validate(to_dict())
    if (
        document.execution_id != execution_id
        or document.pipeline_id != pipeline_id
        or document.mode != ("direct" if direct_execution else "scheduler")
    ):
        raise RuntimeError("JARVIS returned an execution handle for another operation")


def run_native_jarvis_broker(
    obj: Any,
    *,
    runtime_intent: Mapping[str, Any],
    runtime_direct_proof: str,
    configured_scheduler_provider: str,
    append_runtime_record: Callable[[dict[str, Any]], None],
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Run one contained JARVIS operation and relay only authoritative lifecycle documents."""
    scheduler = getattr(obj, "scheduler", None)
    direct_execution = not scheduler
    scheduler_expected = runtime_intent["scheduler_expected"]
    if direct_execution and scheduler_expected is True:
        raise RuntimeError("JARVIS object lost its expected scheduler contract")
    if not direct_execution and scheduler_expected is False:
        raise RuntimeError("JARVIS object acquired an unexpected scheduler contract")
    if not direct_execution and not isinstance(scheduler, dict):
        raise RuntimeError("Scheduled JARVIS object did not retain its scheduler contract")
    pipeline_id = getattr(obj, "name", None)
    if not isinstance(pipeline_id, str) or not pipeline_id:
        raise RuntimeError("JARVIS object has no pipeline identity")

    def refuse_scheduler_launch(
        *,
        reason: str,
        scheduler_name: str,
        normalized_configured_provider: str,
    ) -> None:
        append_runtime_record(
            {
                "schema_version": "jarvis.runtime.v1",
                "source": "jarvis_sidecar",
                "execution_id": runtime_intent["execution_id"],
                "pipeline_id": pipeline_id,
                "scheduler_provider": scheduler_name,
                "scheduler_phase": "launch_refused",
                "terminal": {
                    "state": "launch_refused",
                    "terminal": True,
                    "returncode": 2,
                    "reason": reason,
                },
                "details": {
                    "execution_owner": "jarvis_cd.pipeline.preflight",
                    "execution_mode": "scheduler",
                    "scheduler_expected": runtime_intent["scheduler_expected"],
                    "scheduler_submission_attempted": False,
                    "scheduler_launch_refused": True,
                    "scheduler_provider": scheduler_name,
                    "configured_scheduler_provider": normalized_configured_provider,
                    "scheduler_launch_refusal_proof": runtime_direct_proof,
                },
            }
        )
        raise RuntimeError(reason)

    scheduler_name: str | None = None
    scheduler_config: dict[str, Any] | None = None
    if not direct_execution:
        scheduler_config = cast(dict[str, Any], scheduler)
        raw_scheduler_name = cast(object, scheduler_config.get("name"))
        if not isinstance(raw_scheduler_name, str) or not raw_scheduler_name:
            raise RuntimeError("Scheduled JARVIS object has no scheduler provider name")
        scheduler_name = raw_scheduler_name.strip().lower().replace("_", "-")
        normalized_configured_provider = (
            configured_scheduler_provider.strip().lower().replace("_", "-")
        )
        if normalized_configured_provider in {"none", "unmanaged"}:
            normalized_configured_provider = "external"
        if not normalized_configured_provider:
            raise RuntimeError("configured worker scheduler provider is empty")
        if scheduler_name != normalized_configured_provider:
            refuse_scheduler_launch(
                reason=(
                    "JARVIS scheduler provider does not match the configured worker provider: "
                    f"{scheduler_name} != {normalized_configured_provider}"
                ),
                scheduler_name=scheduler_name,
                normalized_configured_provider=normalized_configured_provider,
            )
        if scheduler_name != "slurm":
            refuse_scheduler_launch(
                reason="clio-relay 1.0 supports scheduled JARVIS execution only through slurm",
                scheduler_name=scheduler_name,
                normalized_configured_provider=normalized_configured_provider,
            )
    execution_id = cast(str, runtime_intent["execution_id"])
    submission_marker = cast(str, runtime_intent["marker"])
    if not direct_execution:
        assert scheduler_config is not None
        scheduler_config["job_name"] = submission_marker
    elif scheduler_expected == "unknown":
        append_runtime_record(
            {
                "schema_version": "jarvis.runtime.v1",
                "source": "jarvis_sidecar",
                "execution_id": execution_id,
                "pipeline_id": pipeline_id,
                "details": {
                    "execution_owner": "jarvis_cd.pipeline.run",
                    "execution_mode": "direct",
                    "scheduler_expected": False,
                    "direct_execution_proof": runtime_direct_proof,
                },
            }
        )

    operation_results: list[object] = []
    operation_errors: list[BaseException] = []

    def execute_jarvis_operation() -> None:
        try:
            if direct_execution:
                run = getattr(obj, "run", None)
                if not callable(run):
                    raise RuntimeError("named JARVIS object has no direct run operation")
                # A detached direct process would escape the relay-owned broker
                # tree, so direct work remains blocking in this contained process.
                operation_results.append(run(execution_id=execution_id, wait=True))
            else:
                submit = getattr(obj, "submit", None)
                if not callable(submit):
                    raise RuntimeError("scheduled JARVIS object has no submit operation")
                operation_results.append(submit(submit=True, wait=False, execution_id=execution_id))
        except BaseException as exc:
            operation_errors.append(exc)

    operation = threading.Thread(
        target=execute_jarvis_operation,
        name=f"clio-relay-jarvis-{execution_id}",
        daemon=True,
    )
    operation.start()
    last_payload: str | None = None
    last_record: Any = None
    while True:
        try:
            payload, observed_record = _read_native_execution(
                obj,
                execution_id=execution_id,
                pipeline_id=pipeline_id,
                direct_execution=direct_execution,
                scheduler_name=scheduler_name,
                submission_marker=submission_marker,
                sleep=sleep,
            )
        except FileNotFoundError:
            if not operation.is_alive():
                break
        else:
            canonical = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            if canonical != last_payload:
                append_runtime_record(payload)
                last_payload = canonical
            last_record = observed_record
            if observed_record.terminal is True:
                break
        if not operation.is_alive() and (operation_errors or direct_execution):
            break
        sleep(0.25)

    operation.join()
    try:
        final_payload, final_record = _read_native_execution(
            obj,
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            direct_execution=direct_execution,
            scheduler_name=scheduler_name,
            submission_marker=submission_marker,
            sleep=sleep,
        )
    except FileNotFoundError:
        final_payload = None
        final_record = None
    if final_payload is not None:
        final_canonical = json.dumps(
            final_payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        if final_canonical != last_payload:
            append_runtime_record(final_payload)
        last_record = final_record

    if operation_errors:
        raise operation_errors[0]
    if len(operation_results) != 1:
        raise RuntimeError("JARVIS execution did not return exactly one handle")
    _validate_operation_handle(
        operation_results[0],
        execution_id=execution_id,
        pipeline_id=pipeline_id,
        direct_execution=direct_execution,
    )
    if last_record is None or last_record.terminal is not True:
        raise RuntimeError("JARVIS execution ended without an authoritative terminal record")
    if last_record.state != "completed":
        native_id = last_record.scheduler_native_id
        identity = f" {native_id}" if isinstance(native_id, str) else ""
        diagnostic = last_record.error or "no reason reported"
        raise RuntimeError(f"JARVIS execution{identity} ended in {last_record.state}: {diagnostic}")


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
        RUNTIME_SCHEDULER_PROVIDER_ENV,
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
    configured_scheduler_provider = source.get(RUNTIME_SCHEDULER_PROVIDER_ENV)
    sanitized = sanitized_jarvis_environment(source)
    if (
        not progress_file
        or not progress_token
        or not runtime_file
        or not runtime_token
        or not raw_anchor
        or not raw_intent
        or not runtime_direct_proof
        or not configured_scheduler_provider
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
            "scheduler_provider": configured_scheduler_provider,
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
    if normalized != "slurm":
        raise ConfigurationError(
            "clio-relay 1.0 supports scheduled JARVIS execution only through slurm; "
            f"requested {scheduler_name}"
        )
    return _slurm_command(python_bin, pipeline_path)


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
    """Reject custom scheduled execution adapters outside the hardened SLURM broker."""
    del factory, consumes_runtime_credential, replace
    normalized = scheduler_name.strip().lower().replace("_", "-")
    requested = normalized or "<empty>"
    raise ConfigurationError(
        "custom JARVIS scheduled-execution adapters are not supported for clio-relay 1.0; "
        f"the built-in slurm broker cannot be registered or replaced (requested {requested})"
    )


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
    or not isinstance(credential.get("scheduler_provider"), str)
    or not credential["scheduler_provider"].strip()
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
configured_scheduler_provider = credential["scheduler_provider"]
if getpass.getuser() != runtime_intent["scheduler_user"]:
    fail_before_ready("JARVIS effective user did not match submission intent")
runtime_sequence = 0
# A native progress snapshot may use the full 4 MiB producer bound. Reserve
# space for its execution handle, record, sidecar envelope, and HMAC.
RUNTIME_METADATA_MAX_RECORD_BYTES = 5 * 1024 * 1024
RUNTIME_METADATA_MAX_TOTAL_BYTES = 64 * 1024 * 1024

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

from clio_relay.jarvis_execution import run_native_jarvis_broker
from jarvis_cd.core.pipeline_test import load_yaml_auto

if len(sys.argv) != 3 or sys.argv[1] not in {"yaml", "named"}:
    raise RuntimeError("scheduled JARVIS wrapper launch contract did not match")
if sys.argv[1] == "yaml":
    _, obj = load_yaml_auto(sys.argv[2])
else:
    from jarvis_cd.core.pipeline import Pipeline

    obj = Pipeline(sys.argv[2])
    obj.load()
# Scheduled submissions do not execute packages in this process. Remove the
# progress capability before JARVIS launches any scheduler CLI child; only
# direct package execution retains it for signed progress emission.
if getattr(obj, "scheduler", None):
    os.environ.pop("CLIO_RELAY_PROGRESS_FILE", None)
    os.environ.pop("CLIO_RELAY_PROGRESS_TOKEN", None)

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


run_native_jarvis_broker(
    obj,
    runtime_intent=runtime_intent,
    runtime_direct_proof=runtime_direct_proof,
    configured_scheduler_provider=configured_scheduler_provider,
    append_runtime_record=append_runtime_record,
)
"""
