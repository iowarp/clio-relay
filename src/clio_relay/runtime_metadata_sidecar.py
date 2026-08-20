"""Authenticated, ordered runtime metadata sidecar record codec.

Extracted from ``runtime_metadata.py`` (clio-relay split/runtime-metadata-w2):
the worker-owned runtime sidecar appends one HMAC-signed, strictly ordered
record per observation; :func:`runtime_sidecar_record` builds one without
disclosing its HMAC key, and :func:`runtime_metadata_from_sidecar_record`
verifies the exact sequence and signature before normalizing the enclosed
payload (preferring the exact native execution documents when present, the
same rule ``runtime_metadata_mcp_normalize.py`` applies to MCP results).
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, cast

from clio_relay.runtime_metadata_coercion import _mapping
from clio_relay.runtime_metadata_core_model import JarvisRuntimeMetadata
from clio_relay.runtime_metadata_mcp_normalize import normalize_runtime_metadata
from clio_relay.runtime_metadata_native_normalize import (
    native_execution_documents,
    runtime_metadata_from_native_documents,
)
from clio_relay.runtime_metadata_types import RUNTIME_SIDECAR_RECORD_SCHEMA, RuntimeMetadataSource


def runtime_metadata_from_sidecar_record(
    record: object,
    *,
    expected_key: str,
    expected_sequence: int,
) -> JarvisRuntimeMetadata:
    """Verify one ordered HMAC-authenticated JARVIS runtime sidecar record."""
    if not isinstance(record, dict):
        raise ValueError("runtime metadata sidecar record must be an object")
    typed = cast(dict[str, Any], record)
    if set(typed) != {
        "schema_version",
        "sequence",
        "runtime_metadata",
        "runtime_metadata_hmac",
    }:
        raise ValueError("runtime metadata sidecar record fields did not match")
    if typed.get("schema_version") != RUNTIME_SIDECAR_RECORD_SCHEMA:
        raise ValueError("runtime metadata sidecar record schema did not match")
    sequence = typed.get("sequence")
    if isinstance(sequence, bool) or sequence != expected_sequence:
        raise ValueError("runtime metadata sidecar sequence did not match")
    payload = _mapping(typed.get("runtime_metadata"))
    if payload is None:
        raise ValueError("runtime metadata sidecar record omitted runtime metadata")
    observed_hmac = typed.get("runtime_metadata_hmac")
    if not isinstance(observed_hmac, str) or len(observed_hmac) != 64:
        raise ValueError("runtime metadata sidecar HMAC was invalid")
    expected_hmac = _runtime_sidecar_hmac(
        payload,
        key=expected_key,
        sequence=expected_sequence,
    )
    if not hmac.compare_digest(observed_hmac, expected_hmac):
        raise ValueError("runtime metadata sidecar HMAC did not match")
    native = native_execution_documents(payload)
    metadata = (
        runtime_metadata_from_native_documents(
            native,
            source=RuntimeMetadataSource.JARVIS_SIDECAR,
        )
        if native is not None
        else normalize_runtime_metadata(payload, source=RuntimeMetadataSource.JARVIS_SIDECAR)
    )
    if metadata is None:
        raise ValueError("runtime metadata sidecar did not contain runtime fields")
    return metadata


def runtime_sidecar_record(
    runtime_metadata: dict[str, Any],
    *,
    key: str,
    sequence: int,
) -> dict[str, object]:
    """Build one canonical ordered sidecar record without disclosing its HMAC key."""
    if not key:
        raise ValueError("runtime metadata sidecar key must not be empty")
    if sequence < 1:
        raise ValueError("runtime metadata sidecar sequence must be positive")
    return {
        "schema_version": RUNTIME_SIDECAR_RECORD_SCHEMA,
        "sequence": sequence,
        "runtime_metadata": runtime_metadata,
        "runtime_metadata_hmac": _runtime_sidecar_hmac(
            runtime_metadata,
            key=key,
            sequence=sequence,
        ),
    }


def _runtime_sidecar_hmac(
    runtime_metadata: dict[str, Any],
    *,
    key: str,
    sequence: int,
) -> str:
    signed = {
        "schema_version": RUNTIME_SIDECAR_RECORD_SCHEMA,
        "sequence": sequence,
        "runtime_metadata": runtime_metadata,
    }
    try:
        canonical = json.dumps(
            signed,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("runtime metadata sidecar payload was not canonical JSON") from exc
    return hmac.new(key.encode("utf-8"), canonical, hashlib.sha256).hexdigest()
