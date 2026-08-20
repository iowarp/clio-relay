"""JARVIS run-dispatch intent and resume-checkpoint minting (iowarp/
clio-relay#231 continuation): idempotency-key derivation and the
checkpoints written before and after a dispatch attempt."""

from __future__ import annotations

import hashlib
import json
from typing import Any, cast
from uuid import uuid4

import clio_relay.cli_jarvis_artifact_io as cli_jarvis_artifact_io
from clio_relay.errors import RelayError


def _canonical_jarvis_validation_digest(value: object) -> str:
    """Hash one finite JSON value using the checkpoint's canonical encoding."""
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RelayError("JARVIS validation checkpoint evidence must be finite JSON") from exc
    return hashlib.sha256(payload).hexdigest()


def _new_jarvis_validation_idempotency_key(
    *,
    cluster: str,
    profile: str,
    arguments: dict[str, Any],
) -> str:
    """Create a run-specific stable key before crossing the stdio dispatch boundary."""
    intent_digest = _canonical_jarvis_validation_digest(
        {
            "cluster": cluster,
            "profile": profile,
            "tool": "jarvis_run",
            "arguments": arguments,
        }
    )
    return f"validation:jarvis-run:{cluster}:{intent_digest}:{uuid4().hex}"


def _jarvis_run_execution_intent(
    *,
    cluster: str,
    profile: str,
    arguments: dict[str, Any],
    idempotency_key: str,
) -> dict[str, object]:
    """Return the exact replayable virtual-tool request, including relay idempotency."""
    return {
        "cluster": cluster,
        "profile": profile,
        "tool": "jarvis_run",
        "arguments": {
            "cluster": cluster,
            **arguments,
            "idempotency_key": idempotency_key,
        },
    }


def _new_jarvis_intent_resume_checkpoint(
    *,
    execution_intent: dict[str, object],
    pre_dispatch_inputs: dict[str, Any],
) -> dict[str, Any]:
    """Persist an idempotent intent before a relay receipt is observable."""
    import clio_relay.cli as cli

    cluster = cast(str, execution_intent["cluster"])
    profile = cast(str, execution_intent["profile"])
    arguments = cast(dict[str, object], execution_intent["arguments"])
    idempotency_key = cast(str, arguments["idempotency_key"])
    pipeline_id = cast(str, arguments["pipeline_id"])
    selector: dict[str, object] = {
        "cluster": cluster,
        "pipeline_id": pipeline_id,
        "relay_job_id": None,
        "idempotency_key": idempotency_key,
        "idempotency_key_sha256": hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest(),
        "execution_intent_sha256": _canonical_jarvis_validation_digest(execution_intent),
        "pre_dispatch_inputs_sha256": _canonical_jarvis_validation_digest(pre_dispatch_inputs),
        "call_response_sha256": None,
        "dispatch_evidence_sha256": None,
    }
    return {
        "schema_version": cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA,
        "phase": cli._JARVIS_VALIDATION_PHASE_INTENT,
        "profile": profile,
        "retry_selector": selector,
        "execution_intent": execution_intent,
        "pre_dispatch_inputs": pre_dispatch_inputs,
    }


def _promote_jarvis_intent_to_dispatch_checkpoint(
    intent_checkpoint: dict[str, Any],
    *,
    job_id: str,
    builder_inputs: dict[str, Any],
) -> dict[str, Any]:
    """Bind a pre-dispatch intent to the one durable relay receipt it returned."""
    import clio_relay.cli as cli

    checkpoint = dict(intent_checkpoint)
    selector = dict(cast(dict[str, object], checkpoint["retry_selector"]))
    call_response = builder_inputs.get("call_response")
    typed_call_response = (
        cast(dict[str, Any], call_response) if isinstance(call_response, dict) else None
    )
    if (
        typed_call_response is None
        or cli_jarvis_artifact_io._mcp_response_job_id(typed_call_response) != job_id
    ):
        raise RelayError("JARVIS validation dispatch response changed its relay job identity")
    selector.update(
        {
            "relay_job_id": job_id,
            "call_response_sha256": _canonical_jarvis_validation_digest(typed_call_response),
            "dispatch_evidence_sha256": _canonical_jarvis_validation_digest(builder_inputs),
        }
    )
    checkpoint.update(
        {
            "phase": cli._JARVIS_VALIDATION_PHASE_DISPATCH,
            "retry_selector": selector,
            "builder_inputs": builder_inputs,
        }
    )
    return checkpoint
