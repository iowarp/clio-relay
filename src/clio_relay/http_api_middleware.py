"""ASGI middleware for the private owned-session input-artifact ingest route.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``http_api.py`` -- ``InputArtifactBodyLimitMiddleware`` never referenced any
of ``create_app()``'s local closures, so it is an unmodified, atomic move.
``http_api.py`` re-exports the class under its original name so external
imports (``from clio_relay.http_api import InputArtifactBodyLimitMiddleware``)
and the ``tests/test_door_errors.py`` structural AST scan (which now walks
this file as part of the split module set) keep resolving it.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from typing import cast

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from clio_relay import door_error_adapters, door_errors
from clio_relay.job_identity import OWNER_SESSION_ID_HEADER, SESSION_GENERATION_ID_HEADER


class InputArtifactBodyLimitMiddleware:
    """Reject an oversized private ingest body before request-model parsing."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        max_body_bytes: int,
        api_token: str | None,
        owner_session_id: str | None,
        session_generation_id: str | None,
    ) -> None:
        if max_body_bytes < 1:
            raise ValueError("input artifact request body limit must be positive")
        self.app = app
        self.max_body_bytes = max_body_bytes
        self._api_token = None if api_token is None else api_token.encode("utf-8")
        self._owner_session_id = (
            None if owner_session_id is None else owner_session_id.encode("utf-8")
        )
        self._session_generation_id = (
            None if session_generation_id is None else session_generation_id.encode("utf-8")
        )
        self._body_slot = asyncio.Semaphore(1)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] != "http"
            or scope.get("method") != "POST"
            or scope.get("path") != "/input-artifacts/ingest"
        ):
            await self.app(scope, receive, send)
            return

        raw_headers = scope.get("headers", [])
        if not isinstance(raw_headers, list):
            await self._send_error(send, "http_request_malformed", "invalid HTTP request headers")
            return
        headers = cast(list[tuple[bytes, bytes]], raw_headers)
        authentication_error = self._authentication_error(headers)
        if authentication_error is not None:
            reason, detail = authentication_error
            await self._send_error(send, reason, detail)
            return

        async with self._body_slot:
            await self._buffer_and_dispatch(scope, headers, receive, send)

    async def _buffer_and_dispatch(
        self,
        scope: Scope,
        headers: list[tuple[bytes, bytes]],
        receive: Receive,
        send: Send,
    ) -> None:
        content_length_values = self._header_values(headers, b"content-length")
        if len(content_length_values) > 1:
            await self._send_error(send, "http_request_malformed", "invalid Content-Length header")
            return
        raw_content_length = content_length_values[0] if content_length_values else None
        if raw_content_length is not None:
            try:
                content_length = int(raw_content_length.decode("ascii"))
            except (UnicodeDecodeError, ValueError):
                await self._send_error(
                    send, "http_request_malformed", "invalid Content-Length header"
                )
                return
            if content_length < 0:
                await self._send_error(
                    send, "http_request_malformed", "invalid Content-Length header"
                )
                return
            if content_length > self.max_body_bytes:
                await self._send_too_large(send)
                return

        body = bytearray()
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "http.disconnect":
                return
            if message_type != "http.request":
                await self._send_error(
                    send, "http_request_malformed", "invalid HTTP request body stream"
                )
                return
            chunk = message.get("body", b"")
            if not isinstance(chunk, bytes):
                await self._send_error(
                    send, "http_request_malformed", "invalid HTTP request body chunk"
                )
                return
            if len(body) + len(chunk) > self.max_body_bytes:
                await self._send_too_large(send)
                return
            body.extend(chunk)
            if message.get("more_body") is not True:
                break

        replayed = False

        async def replay_receive() -> Message:
            nonlocal replayed
            if replayed:
                return {"type": "http.disconnect"}
            replayed = True
            return {"type": "http.request", "body": bytes(body), "more_body": False}

        await self.app(scope, replay_receive, send)

    def _authentication_error(
        self,
        headers: list[tuple[bytes, bytes]],
    ) -> tuple[str, str] | None:
        """Authenticate the private route before allocating its bounded body."""
        if (
            self._api_token is None
            or self._owner_session_id is None
            or self._session_generation_id is None
        ):
            return "input_ingest_unavailable", "owned-session input artifact ingest is unavailable"

        header_tokens = self._header_values(headers, b"x-clio-relay-token")
        authorizations = self._header_values(headers, b"authorization")
        supplied: bytes | None = None
        if header_tokens:
            if len(header_tokens) != 1 or not header_tokens[0]:
                return "authentication_required", "missing or invalid relay API token"
            supplied = header_tokens[0]
        elif authorizations:
            if len(authorizations) != 1:
                return "authentication_required", "missing or invalid relay API token"
            scheme, separator, token = authorizations[0].partition(b" ")
            if separator != b" " or scheme.lower() != b"bearer" or not token:
                return "authentication_required", "missing or invalid relay API token"
            supplied = token
        if supplied is None or not secrets.compare_digest(supplied, self._api_token):
            return "authentication_required", "missing or invalid relay API token"

        session_ids = self._header_values(headers, OWNER_SESSION_ID_HEADER.lower().encode("ascii"))
        generation_ids = self._header_values(
            headers,
            SESSION_GENERATION_ID_HEADER.lower().encode("ascii"),
        )
        if len(session_ids) != 1 or len(generation_ids) != 1:
            return (
                "session_binding_headers_required",
                "exact owner session and generation headers are required",
            )
        if not (
            secrets.compare_digest(session_ids[0], self._owner_session_id)
            and secrets.compare_digest(generation_ids[0], self._session_generation_id)
        ):
            return (
                "session_binding_identity_mismatch",
                "owner session or generation does not match this API process",
            )
        return None

    @staticmethod
    def _header_values(
        headers: list[tuple[bytes, bytes]],
        name: bytes,
    ) -> list[bytes]:
        return [value for key, value in headers if key.lower() == name]

    async def _send_too_large(self, send: Send) -> None:
        await self._send_error(
            send,
            "payload_too_large",
            f"input artifact request body exceeds the {self.max_body_bytes}-byte limit",
        )

    @staticmethod
    async def _send_error(send: Send, reason: str, detail: str) -> None:
        fault = door_errors.fault_for_reason(reason, detail)
        payload = json.dumps(
            door_error_adapters.as_http_problem(fault),
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": fault.http_status,
                "headers": [
                    (b"content-type", b"application/problem+json"),
                    (b"content-length", str(len(payload)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload})
