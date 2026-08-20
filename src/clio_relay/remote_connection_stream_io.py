"""Raw wire mechanics for one identity-bound HTTP stream to a remote relay.

:class:`~clio_relay.remote_connection.RemoteConnection` composes these
primitives; nothing here owns the *channel* the stream rides (see
:mod:`clio_relay.control_channel`) or which stream is pooled/reused (see
:mod:`clio_relay.remote_connection`). This module owns exactly the three
steps every owned-session HTTP exchange goes through: proving a freshly
opened stream against the connection's bring-up identity before any
credential is sent (:func:`_open_identity_bound_stream`,
:func:`verify_session_identity`), issuing one non-reconnecting JSON request
over an already-proven stream (:func:`_request_json_on_stream`), and reading
one bounded JSON response off the wire (:func:`read_json_response`).

clio-relay#213: :func:`_is_stale_stream_error` is the narrow, OS-observed
dead-pooled-stream signature ``RemoteConnection.request_json`` retries
exactly once -- deliberately narrow so an HTTP-status failure or a
slow-but-live server (``ObservationTimeoutError``) never retries under a
second identity-bound stream.
"""

# _is_stale_stream_error/_open_identity_bound_stream/_request_json_on_stream
# are leaf primitives called only from remote_connection.py's RemoteConnection
# class (whose methods stay resident there after this split), or from tests --
# never from within this file -- matching storage_ledger_codec.py's own
# leaf-primitive precedent.
# pyright: reportUnusedFunction=false

from __future__ import annotations

import hmac
import http.client
import json
import urllib.parse
from typing import Final, cast

from clio_relay.bounded_payload import describe_delivery_refusal, is_delivery_refusal
from clio_relay.control_channel import ChannelEndpoint
from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.job_identity import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
)

MAX_SESSION_API_RESPONSE_BYTES: Final = 8 * 1024 * 1024

# clio-relay#213: the closed set of exceptions `_request_json_on_stream` wraps and
# chains that mean the *stream* died at the OS level (idle-closed, reset, a bad
# status line) rather than the request being genuinely rejected. Deliberately
# narrow: an HTTP-status failure or a slow-but-live server (ObservationTimeoutError)
# must never retry under a second identity-bound stream.
_STALE_STREAM_ERROR_TYPES: Final = (ConnectionError, http.client.BadStatusLine)


def _is_stale_stream_error(exc: BaseException) -> bool:
    """Return True only for the narrow, OS-observed dead-pooled-stream signature."""
    return isinstance(exc.__cause__, _STALE_STREAM_ERROR_TYPES)


_IDENTITY_FIELDS: Final = (
    "schema_version",
    "cluster",
    "session_id",
    "session_generation_id",
    "nonce",
)


def _open_identity_bound_stream(
    *,
    endpoint: ChannelEndpoint,
    nonce: str,
    expected_identity: dict[str, object],
    timeout_seconds: float,
) -> http.client.HTTPConnection:
    """Prove one non-reconnecting TCP stream before any credential is sent."""
    stream = http.client.HTTPConnection(endpoint.host, endpoint.port, timeout=timeout_seconds)
    try:
        stream.connect()
        stream.auto_open = 0
        stream.request(
            "GET",
            "/session-identity?" + urllib.parse.urlencode({"nonce": nonce}),
            headers={"Accept": "application/json", "Connection": "keep-alive"},
        )
        proof_response = stream.getresponse()
        proof_document = read_json_response(proof_response, label="session identity challenge")
        if proof_response.status != 200 or not isinstance(proof_document, dict):
            raise RelayError("owned session API did not return a valid server identity challenge")
        verify_session_identity(
            cast(dict[str, object], proof_document),
            expected=expected_identity,
        )
        if proof_response.will_close or stream.sock is None:
            raise RelayError(
                "owned session API closed the identity-proven connection before authentication"
            )
        return stream
    except (OSError, http.client.HTTPException) as exc:
        stream.close()
        raise RelayError("owned session API identity challenge failed") from exc
    except BaseException:
        stream.close()
        raise


def _request_json_on_stream(
    *,
    stream: http.client.HTTPConnection,
    method: str,
    path: str,
    query: dict[str, object] | None,
    body: dict[str, object] | None,
    api_token: str,
    session_id: str,
    generation_id: str,
    response_timeout_seconds: float | None,
) -> object:
    """Issue one request without permitting HTTPConnection to reconnect."""
    encoded_query = "" if query is None else "?" + urllib.parse.urlencode(query)
    encoded_body = None if body is None else json.dumps(body).encode("utf-8")
    proven_socket = stream.sock
    prior_connection_timeout = stream.timeout
    prior_socket_timeout: float | None = None
    response_timeout_applied = False
    try:
        if response_timeout_seconds is not None:
            if proven_socket is None:
                raise RelayError("owned session API identity-proven connection is not open")
            prior_socket_timeout = proven_socket.gettimeout()
            stream.timeout = response_timeout_seconds
            proven_socket.settimeout(response_timeout_seconds)
            response_timeout_applied = True
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {api_token}",
            OWNER_SESSION_ID_HEADER: session_id,
            SESSION_GENERATION_ID_HEADER: generation_id,
        }
        if encoded_body is not None:
            headers["Content-Type"] = "application/json"
        stream.request(
            method,
            path + encoded_query,
            body=encoded_body,
            headers=headers,
        )
        response = stream.getresponse()
        document = read_json_response(response, label=f"{method} {path}")
        if not 200 <= response.status < 300:
            # A1 (#231 R6 review): door_errors.as_http_problem spreads the
            # original T2 refusal document's fields (doc §6.4) into the 413
            # problem body (fault.data, F4) -- recognized here so its own
            # typed code/message surfaces instead of the generic "HTTP
            # {status}: {raw json blob}" that discards the structure.
            typed_document = (
                cast(dict[str, object], document) if isinstance(document, dict) else None
            )
            if typed_document is not None and is_delivery_refusal(typed_document):
                code = cast(dict[str, object], typed_document.get("delivery", {})).get("code")
                raise RelayError(
                    f"owned session API request refused delivery ({code}): "
                    f"{describe_delivery_refusal(typed_document)}"
                )
            detail = json.dumps(document, ensure_ascii=False)[:2_000]
            raise RelayError(
                f"owned session API request failed: {method} {path}: "
                f"HTTP {response.status}: {detail}"
            )
        return document
    except (OSError, http.client.HTTPException) as exc:
        if response_timeout_applied and isinstance(exc, TimeoutError):
            raise ObservationTimeoutError(
                f"owned session API response observation timed out for {method} {path}"
            ) from exc
        raise RelayError(
            f"owned session API identity-bound request failed for {method} {path}: {exc}"
        ) from exc
    finally:
        if response_timeout_seconds is not None:
            stream.timeout = prior_connection_timeout
            if (
                response_timeout_applied
                and stream.sock is proven_socket
                and proven_socket is not None
            ):
                try:
                    proven_socket.settimeout(prior_socket_timeout)
                except OSError:
                    # The response may have closed the proven socket.  Never let
                    # HTTPConnection reconnect it under the authenticated client.
                    stream.close()


def read_json_response(response: http.client.HTTPResponse, *, label: str) -> object:
    """Read one bounded JSON document from a channel response."""
    payload = response.read(MAX_SESSION_API_RESPONSE_BYTES + 1)
    if len(payload) > MAX_SESSION_API_RESPONSE_BYTES:
        raise RelayError(f"owned session API {label} response exceeded its byte limit")
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelayError(f"owned session API {label} response was not UTF-8 JSON") from exc


def verify_session_identity(
    observed: dict[str, object],
    *,
    expected: dict[str, object],
) -> None:
    """Require the reached listener to be the bring-up-proven session.

    ``expected`` is proven under whatever identity anchor this connection's
    mode declares (§8.3) -- the ssh-authenticated bootstrap act in
    ``ssh_forward`` mode, the weaker ``preshared_link_secret`` anchor in
    modes (a)/(b) -- never assumed to be ssh-specific here.
    """
    if any(observed.get(field) != expected.get(field) for field in _IDENTITY_FIELDS):
        raise RelayError(
            "owned session API server identity did not match the bring-up-proven session"
        )
    observed_signature = observed.get("hmac_sha256")
    expected_signature = expected.get("hmac_sha256")
    if (
        not isinstance(observed_signature, str)
        or not isinstance(expected_signature, str)
        or len(observed_signature) != 64
        or len(expected_signature) != 64
        or not hmac.compare_digest(observed_signature, expected_signature)
    ):
        raise RelayError("owned session API server identity HMAC did not verify")
