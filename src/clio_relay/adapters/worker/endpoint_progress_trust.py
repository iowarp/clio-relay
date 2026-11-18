"""Package-progress observation trust (iowarp/clio-relay#231).

Owner module for two related layers of progress-observation trust:

- Bounded, race-tolerant sidecar-record reading:
  ``_read_bounded_sidecar_record`` (one JSONL record with a strict byte
  bound, preserving an incomplete writer's in-progress line rather than
  treating it as corrupt), ``_progress_log_checkpoint``/``_progress_log_
  checkpoint_matches`` (tail-hash a package progress log so a later reopen
  can detect the source was truncated or replaced), and ``_progress_from_
  sidecar_record`` (verify one ordered HMAC-authenticated observation).
- Provider trust: ``_trusted_provider_metadata`` stamps a provider candidate
  without trusting plugin-supplied provenance; ``_trusted_sidecar_metadata``
  stamps the regex-adapter fallback path.

Depends on ``endpoint_sidecar_types.py`` (schema constants), a leaf relative
to this module, so it stays acyclic. ``EndpointWorker`` (still resident in
``endpoint.py``) is this module's main caller.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
from typing import BinaryIO, cast

from clio_relay.adapters.worker.endpoint_sidecar_types import (
    PROGRESS_SIDECAR_RECORD_SCHEMA,
    SIDECAR_DRAIN_CHUNK_BYTES,
    _PackageProgressLogState,
)
from clio_relay.adapters.worker.progress_adapters import (
    PackageProgressProvider,
)
from clio_relay.errors import ConfigurationError
from clio_relay.progress_provenance import (
    PROTECTED_PROGRESS_METADATA_KEYS,
    PackageProgressSourceAuthority,
    package_progress_metadata,
    package_progress_provider_metadata,
)


def _read_bounded_sidecar_record(
    handle: BinaryIO,
    *,
    max_bytes: int,
    allow_final_record: bool,
) -> tuple[bytes | None, str]:
    """Read one bounded JSONL record and preserve incomplete writer output."""
    record_start = handle.tell()
    line = handle.readline(max_bytes + 1)
    if line == b"":
        return None, "eof"
    if len(line) > max_bytes:
        while not line.endswith(b"\n"):
            fragment = handle.readline(SIDECAR_DRAIN_CHUNK_BYTES)
            if fragment == b"" or fragment.endswith(b"\n"):
                break
        return None, "oversized"
    if not line.endswith(b"\n") and not allow_final_record:
        handle.seek(record_start)
        return None, "incomplete"
    return line, "record"


def _progress_log_checkpoint(
    handle: BinaryIO,
    offset: int,
    *,
    path: Path,
) -> tuple[int, str | None]:
    if offset <= 0:
        return 0, None
    checkpoint_offset = max(0, offset - 4096)
    expected_length = offset - checkpoint_offset
    original_offset = handle.tell()
    try:
        handle.seek(checkpoint_offset)
        payload = handle.read(expected_length)
    except OSError as exc:
        raise ConfigurationError(
            f"could not checkpoint package progress log {path}: {exc}"
        ) from exc
    finally:
        handle.seek(original_offset)
    if len(payload) != expected_length:
        raise ConfigurationError(f"package progress log changed while it was checkpointed: {path}")
    return checkpoint_offset, hashlib.sha256(payload).hexdigest()


def _progress_log_checkpoint_matches(
    state: _PackageProgressLogState,
    handle: BinaryIO,
) -> bool:
    if state.checkpoint_sha256 is None:
        return True
    expected_length = state.offset - state.checkpoint_offset
    original_offset = handle.tell()
    try:
        handle.seek(state.checkpoint_offset)
        payload = handle.read(expected_length)
    except OSError as exc:
        raise ConfigurationError(
            f"could not verify package progress log checkpoint {state.path}: {exc}"
        ) from exc
    finally:
        handle.seek(original_offset)
    return (
        len(payload) == expected_length
        and hashlib.sha256(payload).hexdigest() == state.checkpoint_sha256
    )


def _progress_from_sidecar_record(
    record: object,
    *,
    expected_key: str,
    expected_sequence: int,
) -> dict[str, object]:
    """Verify one ordered HMAC-authenticated package-progress observation."""
    if not isinstance(record, dict):
        raise ValueError("progress sidecar record must be an object")
    typed = cast(dict[str, object], record)
    if set(typed) != {"schema_version", "sequence", "progress", "progress_hmac"}:
        raise ValueError("progress sidecar record fields did not match")
    if typed.get("schema_version") != PROGRESS_SIDECAR_RECORD_SCHEMA:
        raise ValueError("progress sidecar record schema did not match")
    sequence = typed.get("sequence")
    if isinstance(sequence, bool) or sequence != expected_sequence:
        raise ValueError("progress sidecar sequence did not match")
    progress = typed.get("progress")
    if not isinstance(progress, dict):
        raise ValueError("progress sidecar omitted its progress object")
    typed_progress = {
        str(key): value for key, value in cast(dict[object, object], progress).items()
    }
    observed_hmac = typed.get("progress_hmac")
    if not isinstance(observed_hmac, str) or len(observed_hmac) != 64:
        raise ValueError("progress sidecar HMAC was invalid")
    signed = {
        "schema_version": PROGRESS_SIDECAR_RECORD_SCHEMA,
        "sequence": expected_sequence,
        "progress": typed_progress,
    }
    canonical = json.dumps(
        signed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    expected_hmac = hmac.new(
        expected_key.encode("utf-8"),
        canonical,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(observed_hmac, expected_hmac):
        raise ValueError("progress sidecar HMAC did not match")
    return typed_progress


def _trusted_provider_metadata(
    metadata: dict[str, object],
    *,
    job_id: str,
    provider: PackageProgressProvider,
    source_authority: PackageProgressSourceAuthority,
    acceptance_validated: bool,
) -> dict[str, object]:
    """Stamp a provider candidate without trusting plugin-supplied provenance."""
    identity = provider.identity
    return package_progress_provider_metadata(
        metadata,
        package_name=identity.package_name,
        package_version=identity.package_version,
        run_id=job_id,
        adapter_name=identity.adapter_name,
        provider_entry_point=identity.entry_point_name,
        provider_entry_point_value=identity.entry_point_value,
        provider_distribution=identity.distribution_name,
        provider_distribution_version=identity.distribution_version,
        source_authority=source_authority,
        application_profile=identity.application_profile,
        provider_validated=True,
        acceptance_validated=acceptance_validated,
    )


def _trusted_sidecar_metadata(metadata: dict[str, object], *, job_id: str) -> dict[str, object]:
    preserved = {
        key: value for key, value in metadata.items() if key not in PROTECTED_PROGRESS_METADATA_KEYS
    }
    preserved["adapter"] = "regex"
    return package_progress_metadata(
        preserved,
        package_name="clio_relay.bounded_command",
        package_version="builtin",
        run_id=job_id,
    )
