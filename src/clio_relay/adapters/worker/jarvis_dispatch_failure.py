"""Typed JARVIS dispatch refusals carried by a durable ``jarvis_run`` result.

The registered JARVIS user contract returns a durable execution handle from
``jarvis_run`` and resolves every requested Spack runtime *before* direct or
scheduler execution. An explicit ``isError`` answer therefore settles ownership:
no execution handle was issued, so there is no durable execution for the relay
to query or adopt. The worker records that answer as a typed refusal and
terminalizes the durable job with it instead of entering the lost-response
recovery loop, which can only ever fail for an execution that was never created.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

JARVIS_DISPATCH_REFUSAL_SCHEMA = "clio-relay.jarvis-dispatch-refusal.v1"
"""Schema of the durable event payload recording one refusal."""

JARVIS_ERROR_PAYLOAD_SCHEMA = "jarvis.error.v1"
"""Schema of the typed error document the JARVIS MCP server returns."""

JARVIS_DISPATCH_REFUSAL_RESOLUTION = "dispatch_refusal"
"""Recovery-intent resolution recorded when the dispatch itself answered."""

UNTYPED_REFUSAL_CODE = "jarvis_tool_error"
"""Code used when an ``isError`` answer carries no typed JARVIS payload."""

MAX_REFUSAL_MESSAGE_CHARS = 2_000


@dataclass(frozen=True)
class JarvisDispatchRefusal:
    """One explicit JARVIS tool error returned by a durable run dispatch."""

    code: str
    message: str
    pipeline_id: str | None
    execution_id: str | None
    payload_schema_version: str | None

    def as_payload(self) -> dict[str, object]:
        """Return the durable event payload describing this refusal."""
        return {
            "schema_version": JARVIS_DISPATCH_REFUSAL_SCHEMA,
            "code": self.code,
            "message": self.message,
            "pipeline_id": self.pipeline_id,
            "execution_id": self.execution_id,
            "payload_schema_version": self.payload_schema_version,
        }

    def as_error_detail(self) -> str:
        """Return the typed reason recorded on the terminal durable job."""
        return f"{self.code}: {self.message}"


@dataclass(frozen=True)
class McpRuntimeIngestOutcome:
    """Result of reading one durable MCP dispatch result for runtime metadata."""

    ingested: bool
    refusal: JarvisDispatchRefusal | None = None


def jarvis_dispatch_refusal(document: object) -> JarvisDispatchRefusal | None:
    """Return the typed refusal when a JARVIS dispatch answered with a tool error.

    Args:
        document: The persisted ``mcp-result.json`` document. Its route identity
            must already have been matched against the durable job spec.

    Returns:
        The typed refusal, or ``None`` when the dispatch did not return an
        explicit tool error. A dispatch that timed out is never a refusal: the
        response was lost rather than answered, so ownership stays unresolved.
    """
    if not isinstance(document, dict):
        return None
    typed = cast(dict[str, object], document)
    if typed.get("timed_out") is True:
        return None
    protocol_result = typed.get("protocol_result")
    if not isinstance(protocol_result, dict):
        return None
    if cast(dict[str, object], protocol_result).get("isError") is not True:
        return None
    structured = typed.get("structured_result")
    if isinstance(structured, dict):
        payload = cast(dict[str, object], structured)
        error = payload.get("error")
        if payload.get("schema_version") == JARVIS_ERROR_PAYLOAD_SCHEMA and isinstance(error, dict):
            fields = cast(dict[str, object], error)
            code = fields.get("code")
            message = fields.get("message")
            if isinstance(code, str) and code and isinstance(message, str) and message:
                return JarvisDispatchRefusal(
                    code=code,
                    message=message[:MAX_REFUSAL_MESSAGE_CHARS],
                    pipeline_id=_optional_text(fields.get("pipeline_id")),
                    execution_id=_optional_text(fields.get("execution_id")),
                    payload_schema_version=JARVIS_ERROR_PAYLOAD_SCHEMA,
                )
    protocol_error = typed.get("protocol_error")
    message = (
        protocol_error
        if isinstance(protocol_error, str) and protocol_error
        else "the JARVIS MCP tool returned isError without a typed payload"
    )
    return JarvisDispatchRefusal(
        code=UNTYPED_REFUSAL_CODE,
        message=message[:MAX_REFUSAL_MESSAGE_CHARS],
        pipeline_id=None,
        execution_id=None,
        payload_schema_version=None,
    )


def _optional_text(value: object) -> str | None:
    """Return one non-empty string field, or ``None`` when it is absent."""
    return value if isinstance(value, str) and value else None
