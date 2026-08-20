"""Embedded remote shell/Python scripts for scheduler-submission tracking.

Extracted from ``service_runtime.py`` (#231 rework slice): the SSH-delivered
script generators that reserve, capture, and verify one exact scheduler
submission on a remote host through a durable, race-safe sidecar --
``_submit_script`` (reserve the durable submission intent, then run the
command behind a bounded output anchor), ``_remote_submission_record_script``
(promote the anchored output into the durable submission record),
``_template_command_script`` (fill a scheduler-job-id template into one
shell-quoted command line), and ``_remote_scheduler_script`` (status/cancel/
connector-placement queries against the cluster's own scheduler CLI). Every
returned string is Python source embedded as heredoc text the remote host
runs, not live code in this module -- see the doc's B4 note on why a naive
``grep '^def '`` sweep over this class of function is unreliable.

Depends only on ``clio_relay.cluster_config``/``clio_relay.remote_cli`` --
never on any other piece of the service-runtime split.
"""

from __future__ import annotations

import base64
import json
import shlex
from collections.abc import Sequence
from typing import Literal

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.remote_cli import remote_env

_MAX_SUBMISSION_OUTPUT_BYTES = 262_144
_REMOTE_SUBMISSION_VERIFICATION_SCHEMA = "clio-relay.gateway-submission-verification.v1"


def _submit_script(
    command: Sequence[str],
    *,
    session_id: str,
    submission_id: str,
    scheduler_provider: str,
    submission_marker: str,
) -> str:
    """Run a submission behind an exact durable intent and bounded output anchor."""
    encoded_command = base64.b64encode(
        json.dumps(list(command), separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return f"""set -euo pipefail
umask 077
session_id={shlex.quote(session_id)}
submission_id={shlex.quote(submission_id)}
scheduler_provider={shlex.quote(scheduler_provider)}
submission_marker={shlex.quote(submission_marker)}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
mkdir -p "$session_dir/submissions"
record_file="$session_dir/submissions/$submission_id.json"
output_file="$session_dir/submissions/$submission_id.out"
output_meta="$session_dir/submissions/$submission_id.out.json"
intent_file="$session_dir/submissions/$submission_id.intent.json"
python3 - "$intent_file" "$session_id" "$submission_id" "$scheduler_provider" \
  "$submission_marker" <<'__CLIO_RESERVE_SUBMISSION__'
import json
import os
import stat
import sys
from pathlib import Path

path_raw, session_id, submission_id, provider, marker = sys.argv[1:]
path = Path(path_raw)
expected = {{
    "schema_version": "clio-relay.gateway-submission-intent.v1",
    "session_id": session_id,
    "submission_id": submission_id,
    "scheduler_provider": provider,
    "submission_marker": marker,
}}
if path.exists():
    before = os.lstat(path)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_mode & 0o077
        or before.st_size > 65536
    ):
        raise RuntimeError("scheduler submission intent is not a private bounded file")
    payload = path.read_bytes()
    after = os.lstat(path)
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError("scheduler submission intent changed while reading")
    if json.loads(payload) != expected:
        raise RuntimeError("scheduler submission intent identity mismatch")
    raise SystemExit(0)
temporary = path.with_name(f".{{path.name}}.{{os.getpid()}}.tmp")
with temporary.open("w", encoding="utf-8") as handle:
    json.dump(expected, handle, sort_keys=True)
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, path)
directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
__CLIO_RESERVE_SUBMISSION__
python3 - "$record_file" "$output_file" "$output_meta" "$intent_file" \
  "$session_id" "$submission_id" "$scheduler_provider" "$submission_marker" \
  {shlex.quote(encoded_command)} {int(_MAX_SUBMISSION_OUTPUT_BYTES)} \
  <<'__CLIO_CAPTURE_SUBMISSION__'
import base64
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

(
    record_raw,
    output_raw,
    meta_raw,
    intent_raw,
    session_id,
    submission_id,
    provider,
    marker,
    encoded_command,
    maximum_raw,
) = sys.argv[1:]
maximum = int(maximum_raw)
expected_intent = {{
    "schema_version": "clio-relay.gateway-submission-intent.v1",
    "session_id": session_id,
    "submission_id": submission_id,
    "scheduler_provider": provider,
    "submission_marker": marker,
}}
intent_path = Path(intent_raw)
before = os.lstat(intent_path)
if (
    not stat.S_ISREG(before.st_mode)
    or before.st_nlink != 1
    or before.st_mode & 0o077
    or before.st_size > 65536
):
    raise RuntimeError("scheduler submission intent is not a private bounded file")
intent_payload = intent_path.read_bytes()
after = os.lstat(intent_path)
if (before.st_ino, before.st_size, before.st_mtime_ns) != (
    after.st_ino,
    after.st_size,
    after.st_mtime_ns,
):
    raise RuntimeError("scheduler submission intent changed while reading")
intent = json.loads(intent_payload)
if intent != expected_intent:
    raise RuntimeError("scheduler submission intent changed before execution")
record_exists = os.path.lexists(record_raw)
output_exists = os.path.lexists(output_raw)
meta_exists = os.path.lexists(meta_raw)
if output_exists != meta_exists:
    raise RuntimeError("scheduler submission output anchor is incomplete")
if record_exists and not (output_exists and meta_exists):
    raise RuntimeError("scheduler submission record is missing its output anchor")
if output_exists and meta_exists:
    raise SystemExit(0)
command = json.loads(base64.b64decode(encoded_command).decode("utf-8"))
if (
    not isinstance(command, list)
    or not command
    or not all(isinstance(item, str) for item in command)
):
    raise RuntimeError("scheduler submission command is invalid")
process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
temporary_output = Path(output_raw).with_name(f".{{Path(output_raw).name}}.{{os.getpid()}}.tmp")
observed = 0
persisted = 0
with temporary_output.open("wb") as handle:
    assert process.stdout is not None
    while True:
        chunk = process.stdout.read(65536)
        if not chunk:
            break
        observed += len(chunk)
        if persisted < maximum + 1:
            selected = chunk[: maximum + 1 - persisted]
            handle.write(selected)
            persisted += len(selected)
    handle.flush()
    os.fsync(handle.fileno())
returncode = process.wait()
os.chmod(temporary_output, 0o600)
os.replace(temporary_output, output_raw)
output = Path(output_raw).read_bytes()
truncated = observed > maximum
effective_returncode = returncode if returncode != 0 else (75 if truncated else 0)
meta = {{
    **expected_intent,
    "schema_version": "clio-relay.gateway-submission-output.v1",
    "returncode": effective_returncode,
    "output_sha256": hashlib.sha256(output).hexdigest(),
    "output_size": len(output),
    "observed_output_size": observed,
    "output_truncated": truncated,
}}
meta_path = Path(meta_raw)
temporary_meta = meta_path.with_name(f".{{meta_path.name}}.{{os.getpid()}}.tmp")
with temporary_meta.open("w", encoding="utf-8") as handle:
    json.dump(meta, handle, sort_keys=True)
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary_meta, 0o600)
os.replace(temporary_meta, meta_path)
directory = os.open(meta_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
__CLIO_CAPTURE_SUBMISSION__
python3 - "$record_file" "$output_file" "$output_meta" "$intent_file" \
  "$session_id" "$submission_id" "$scheduler_provider" "$submission_marker" \
  {int(_MAX_SUBMISSION_OUTPUT_BYTES)} <<'__CLIO_RECORD_SUBMISSION__'
import hashlib
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    record_raw,
    output_raw,
    meta_raw,
    intent_raw,
    session_id,
    submission_id,
    provider,
    marker,
    maximum_raw,
) = sys.argv[1:]
maximum = int(maximum_raw)

def read_private(path_raw, maximum_bytes):
    path = Path(path_raw)
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_mode & 0o077:
        raise RuntimeError(f"submission sidecar is not a private regular file: {{path}}")
    if before.st_size > maximum_bytes:
        raise RuntimeError(f"submission sidecar exceeds its bound: {{path}}")
    data = path.read_bytes()
    after = os.lstat(path)
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError(f"submission sidecar changed while reading: {{path}}")
    return data

expected = {{
    "session_id": session_id,
    "submission_id": submission_id,
    "scheduler_provider": provider,
    "submission_marker": marker,
}}
intent = json.loads(read_private(intent_raw, 65536))
if (
    intent.get("schema_version") != "clio-relay.gateway-submission-intent.v1"
    or any(intent.get(k) != v for k, v in expected.items())
):
    raise RuntimeError("scheduler submission intent identity mismatch")
output = read_private(output_raw, maximum + 1)
meta = json.loads(read_private(meta_raw, 65536))
if (
    meta.get("schema_version") != "clio-relay.gateway-submission-output.v1"
    or any(meta.get(k) != v for k, v in expected.items())
):
    raise RuntimeError("scheduler submission output identity mismatch")
if (
    meta.get("output_sha256") != hashlib.sha256(output).hexdigest()
    or meta.get("output_size") != len(output)
):
    raise RuntimeError("scheduler submission output digest mismatch")
record = {{
    "schema_version": "clio-relay.gateway-submission-sidecar.v1",
    **expected,
    "returncode": int(meta["returncode"]),
    "output": output.decode("utf-8"),
    "output_sha256": meta["output_sha256"],
    "output_size": len(output),
    "output_truncated": meta.get("output_truncated") is True,
    "recorded_at": datetime.now(timezone.utc).isoformat(),
}}
record_path = Path(record_raw)
if record_path.exists():
    existing = json.loads(read_private(record_raw, maximum + 65536))
    if any(existing.get(k) != v for k, v in record.items() if k != "recorded_at"):
        raise RuntimeError("scheduler submission record conflicts with anchored output")
else:
    temporary = record_path.with_name(f".{{record_path.name}}.{{os.getpid()}}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(record, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, record_path)
    directory = os.open(record_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
print(record["output"], end="")
raise SystemExit(record["returncode"])
__CLIO_RECORD_SUBMISSION__
"""


def _remote_submission_record_script(
    *,
    session_id: str,
    submission_id: str,
    scheduler_provider: str,
    submission_marker: str,
) -> str:
    """Validate and promote one exact anchored scheduler-submission output."""
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
submission_id={shlex.quote(submission_id)}
scheduler_provider={shlex.quote(scheduler_provider)}
submission_marker={shlex.quote(submission_marker)}
root="$HOME/.local/share/clio-relay/service-sessions/$session_id/submissions"
record_file="$root/$submission_id.json"
output_file="$root/$submission_id.out"
output_meta="$root/$submission_id.out.json"
intent_file="$root/$submission_id.intent.json"
python3 - "$record_file" "$output_file" "$output_meta" "$intent_file" \
  "$session_id" "$submission_id" "$scheduler_provider" "$submission_marker" \
  {int(_MAX_SUBMISSION_OUTPUT_BYTES)} <<'__CLIO_READ_SUBMISSION__'
import hashlib
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    record_raw,
    output_raw,
    meta_raw,
    intent_raw,
    session_id,
    submission_id,
    provider,
    marker,
    maximum_raw,
) = sys.argv[1:]
maximum = int(maximum_raw)

verification_schema = {_REMOTE_SUBMISSION_VERIFICATION_SCHEMA!r}
identity_fields = (
    "schema_version",
    "session_id",
    "submission_id",
    "scheduler_provider",
    "submission_marker",
    "returncode",
    "output_truncated",
)

def bounded_scalar(value, maximum_characters=256):
    if isinstance(value, str):
        return value[:maximum_characters]
    if isinstance(value, (int, bool)):
        return value
    return None

def emit_retryable(error_code, *, anchored=False):
    print(json.dumps({{
        "schema_version": verification_schema,
        "present": False,
        "anchored": anchored,
        "verification_outcome": "retryable",
        "error_code": error_code,
    }}, sort_keys=True))
    raise SystemExit(0)

def emit_invalid(
    error_code,
    error,
    component,
    *,
    observed=None,
    observed_output=None,
    recorded_output_sha256=None,
):
    observed_mapping = observed if isinstance(observed, dict) else {{}}
    payload = {{
        "schema_version": verification_schema,
        "present": True,
        "verification_outcome": "definitive_invalid",
        "failure_kind": "relay_integrity_failure",
        "error_code": error_code,
        "error": error[:1024],
        "invalid_component": component,
        "observed_identity": {{
            field: bounded_scalar(observed_mapping.get(field))
            for field in identity_fields
        }},
    }}
    if isinstance(recorded_output_sha256, str):
        payload["output_sha256"] = recorded_output_sha256[:128]
    if isinstance(observed_output, bytes):
        payload["observed_output_sha256"] = hashlib.sha256(observed_output).hexdigest()
        payload["output_size"] = len(observed_output)
    print(json.dumps(payload, sort_keys=True))
    raise SystemExit(0)

def read_private(path_raw, maximum_bytes, component):
    path = Path(path_raw)
    try:
        before = os.lstat(path)
    except FileNotFoundError:
        emit_retryable(component + "_disappeared", anchored=True)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_mode & 0o077:
        emit_invalid(
            component + "_unsafe_file",
            "scheduler submission " + component + " is not a private regular file",
            component,
        )
    if before.st_size > maximum_bytes:
        emit_invalid(
            component + "_oversized",
            "scheduler submission " + component + " exceeds its size bound",
            component,
        )
    try:
        data = path.read_bytes()
        after = os.lstat(path)
    except FileNotFoundError:
        emit_retryable(component + "_disappeared", anchored=True)
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        emit_retryable(component + "_changed_while_reading", anchored=True)
    return data

def read_json_private(path_raw, maximum_bytes, component):
    data = read_private(path_raw, maximum_bytes, component)
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        emit_invalid(
            component + "_json_invalid",
            "scheduler submission " + component + " JSON is invalid",
            component,
        )

expected = {{
    "session_id": session_id,
    "submission_id": submission_id,
    "scheduler_provider": provider,
    "submission_marker": marker,
}}
intent_path = Path(intent_raw)
if not intent_path.exists():
    emit_retryable("intent_absent")
intent = read_json_private(intent_raw, 65536, "intent")
if (
    not isinstance(intent, dict)
    or
    intent.get("schema_version") != "clio-relay.gateway-submission-intent.v1"
    or any(intent.get(k) != v for k, v in expected.items())
):
    emit_invalid(
        "intent_identity_mismatch",
        "scheduler submission intent identity mismatch",
        "intent",
        observed=intent,
    )
record_path = Path(record_raw)
if record_path.exists():
    record = read_json_private(record_raw, maximum + 65536, "record")
else:
    output_exists = Path(output_raw).exists()
    meta_exists = Path(meta_raw).exists()
    if not output_exists and not meta_exists:
        emit_retryable("output_absent", anchored=True)
    if output_exists != meta_exists:
        emit_retryable("output_incomplete", anchored=True)
    output = read_private(output_raw, maximum + 1, "output")
    meta = read_json_private(meta_raw, 65536, "output_metadata")
    if (
        not isinstance(meta, dict)
        or
        meta.get("schema_version") != "clio-relay.gateway-submission-output.v1"
        or any(meta.get(k) != v for k, v in expected.items())
    ):
        emit_invalid(
            "output_identity_mismatch",
            "scheduler submission output identity mismatch",
            "output_metadata",
            observed=meta,
            observed_output=output,
        )
    if (
        meta.get("output_sha256") != hashlib.sha256(output).hexdigest()
        or meta.get("output_size") != len(output)
    ):
        emit_invalid(
            "output_digest_mismatch",
            "scheduler submission output digest mismatch",
            "output_metadata",
            observed=meta,
            observed_output=output,
            recorded_output_sha256=meta.get("output_sha256"),
        )
    returncode = meta.get("returncode")
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        emit_invalid(
            "output_returncode_invalid",
            "scheduler submission output return code is invalid",
            "output_metadata",
            observed=meta,
            observed_output=output,
        )
    try:
        decoded_output = output.decode("utf-8")
    except UnicodeDecodeError:
        emit_invalid(
            "output_encoding_invalid",
            "scheduler submission output is not valid UTF-8",
            "output",
            observed=meta,
            observed_output=output,
        )
    record = {{
        "schema_version": "clio-relay.gateway-submission-sidecar.v1",
        **expected,
        "returncode": returncode,
        "output": decoded_output,
        "output_sha256": meta["output_sha256"],
        "output_size": len(output),
        "output_truncated": meta.get("output_truncated") is True,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }}
    temporary = record_path.with_name(f".{{record_path.name}}.{{os.getpid()}}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(record, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, record_path)
    directory = os.open(record_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
if (
    not isinstance(record, dict)
    or
    record.get("schema_version") != "clio-relay.gateway-submission-sidecar.v1"
    or any(record.get(k) != v for k, v in expected.items())
):
    emit_invalid(
        "record_identity_mismatch",
        "scheduler submission record identity mismatch",
        "record",
        observed=record,
    )
output = record.get("output")
if not isinstance(output, str) or len(output.encode("utf-8")) > maximum + 1:
    emit_invalid(
        "record_output_invalid",
        "scheduler submission record output is invalid",
        "record",
        observed=record,
    )
output_bytes = output.encode("utf-8")
if (
    record.get("output_sha256") != hashlib.sha256(output_bytes).hexdigest()
    or record.get("output_size") != len(output_bytes)
):
    emit_invalid(
        "record_output_digest_mismatch",
        "scheduler submission record output digest mismatch",
        "record",
        observed=record,
        observed_output=output_bytes,
        recorded_output_sha256=record.get("output_sha256"),
    )
record["present"] = True
print(json.dumps(record))
__CLIO_READ_SUBMISSION__
"""


def _template_command_script(command: Sequence[str], scheduler_job_id: str) -> str:
    templated = [part.format(scheduler_job_id=scheduler_job_id) for part in command]
    return "set -euo pipefail\n" + shlex.join(templated) + "\n"


def _remote_scheduler_script(
    *,
    definition: ClusterDefinition,
    operation: Literal["status", "cancel", "connector-placement"],
    provider: str,
    scheduler_job_id: str,
) -> str:
    command = [
        "clio-relay",
        "scheduler",
        operation,
        scheduler_job_id,
        "--cluster",
        definition.name,
        "--provider",
        provider,
    ]
    return f"set -euo pipefail\n{remote_env(definition)} {shlex.join(command)}\n"
