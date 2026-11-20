"""The held link to one placed worker, from relay's side.

One frp ``stcp`` visitor through the rendezvous carries every call
(:class:`~clio_relay.frp_transport.BrokeredTcpTransport`). Bring-up fetches the
worker's identity and status over it. After that, each HTTP stream proves the
worker's identity before it carries the bearer token: the worker signs a fresh
nonce with the session owner token, which relay minted at placement and keeps
in the link file, so relay checks the signature itself.

A worker that does not report :data:`WORKER_PROTOCOL` was placed by an older
relay. The link refuses it with :class:`WorkerOutdated`, and the adapter asks
for a sign-in, which places a current worker in its stead.
"""

from __future__ import annotations

import hmac
import http.client
import json
import secrets
import threading
import time
import urllib.parse
from typing import Final, cast

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.control_channel import ChannelDropped, ChannelEndpoint
from clio_relay.errors import RelayError
from clio_relay.frp_transport import BrokeredTcpTransport
from clio_relay.session_api import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
    session_identity_document,
)

WORKER_PROTOCOL: Final = 3
"""3: the site's local adapter hands package jobs to ``jarvis ppl submit``."""
MAX_RESPONSE_BYTES: Final = 8 * 1024 * 1024
_IDLE_STREAMS: Final = 4
# The site keeps an idle stream open for 300 s; relay drops its own sooner.
_IDLE_STREAM_S: Final = 60.0
_STREAM_CONNECT_S: Final = 30.0


class WorkerOutdated(RelayError):
    """The worker was placed by an older relay and has to be placed again."""


class WorkerConnection:
    """One held link to one placed worker."""

    def __init__(
        self,
        *,
        definition: ClusterDefinition,
        session_id: str,
        generation_id: str,
        remote_api_port: int,
        api_token: str,
        session_owner_token: str,
        frpc_bin: str,
    ) -> None:
        self._definition = definition
        self._cluster = definition.name
        self._session_id = session_id
        self._generation_id = generation_id
        self._port = remote_api_port
        self._api_token = api_token
        self._owner_token = session_owner_token
        self._frpc_bin = frpc_bin
        self._transport: BrokeredTcpTransport | None = None
        self._endpoint: ChannelEndpoint | None = None
        self._lock = threading.Lock()
        self._idle: list[tuple[float, http.client.HTTPConnection]] = []

    @property
    def cluster(self) -> str:
        return self._cluster

    @property
    def connected(self) -> bool:
        transport = self._transport
        return transport is not None and transport.is_alive()

    def connect(self) -> None:
        """Hold the visitor, then check the worker's identity and status."""
        transport = BrokeredTcpTransport(
            definition=self._definition,
            cluster=self._cluster,
            session_id=self._session_id,
            session_generation_id=self._generation_id,
            remote_api_port=self._port,
            api_token=self._api_token,
            identity_anchor="preshared_link_secret",
            frpc_bin=self._frpc_bin,
        )
        nonce = secrets.token_hex(32)
        try:
            channel = transport.establish(nonce=nonce)
            self._check_identity(channel.bootstrap.identity, nonce)
            self._check_status(channel.bootstrap.status)
        except BaseException:
            transport.close()
            raise
        self._transport = transport
        self._endpoint = channel.control_endpoint

    def _check_identity(self, observed: dict[str, object], nonce: str) -> None:
        expected = session_identity_document(
            owner_token=self._owner_token,
            cluster=self._cluster,
            session_id=self._session_id,
            generation_id=self._generation_id,
            nonce=nonce,
        )
        signature = observed.get("hmac_sha256")
        if (
            any(
                observed.get(key) != value
                for key, value in expected.items()
                if key != "hmac_sha256"
            )
            or not isinstance(signature, str)
            or not hmac.compare_digest(signature, expected["hmac_sha256"])
        ):
            raise RelayError(f"the worker for {self._cluster} did not prove its identity")

    def _check_status(self, status: dict[str, object]) -> None:
        if status.get("worker_protocol") != WORKER_PROTOCOL:
            raise WorkerOutdated(
                f"the worker for {self._cluster} was placed by an older relay "
                f"(protocol {status.get('worker_protocol')!r}, this relay speaks "
                f"{WORKER_PROTOCOL}); sign in again to place a current one"
            )
        expected = {
            "cluster": self._cluster,
            "session_id": self._session_id,
            "session_generation_id": self._generation_id,
            "remote_api_port": self._port,
            "running": True,
        }
        wrong = sorted(key for key, value in expected.items() if status.get(key) != value)
        if wrong:
            raise RelayError(
                f"the worker for {self._cluster} is not the placed session: {', '.join(wrong)}"
            )

    # --- streams --------------------------------------------------------

    def _new_stream(self) -> http.client.HTTPConnection:
        endpoint = self._endpoint
        if endpoint is None or not self.connected:
            raise ChannelDropped(f"the link to the worker for {self._cluster} is down")
        stream = http.client.HTTPConnection(endpoint.host, endpoint.port, timeout=_STREAM_CONNECT_S)
        try:
            stream.connect()
            stream.auto_open = 0
            nonce = secrets.token_hex(32)
            stream.request(
                "GET",
                "/session-identity?" + urllib.parse.urlencode({"nonce": nonce}),
                headers={"Accept": "application/json", "Connection": "keep-alive"},
            )
            response = stream.getresponse()
            document = _read_json(response, label="identity challenge")
            if response.status != 200 or not isinstance(document, dict):
                raise RelayError("the worker did not answer its identity challenge")
            self._check_identity(cast("dict[str, object]", document), nonce)
            if response.will_close or stream.sock is None:
                raise RelayError("the worker closed the stream before authentication")
        except (OSError, http.client.HTTPException) as exc:
            stream.close()
            raise RelayError(f"the worker's identity challenge failed: {exc}") from exc
        except BaseException:
            stream.close()
            raise
        return stream

    def _take_stream(self) -> http.client.HTTPConnection:
        now = time.monotonic()
        with self._lock:
            while self._idle:
                since, stream = self._idle.pop()
                if now - since < _IDLE_STREAM_S and stream.sock is not None:
                    return stream
                stream.close()
        return self._new_stream()

    def _return_stream(self, stream: http.client.HTTPConnection) -> None:
        with self._lock:
            if len(self._idle) < _IDLE_STREAMS and self._transport is not None:
                self._idle.append((time.monotonic(), stream))
                return
        stream.close()

    def request_json(
        self,
        *,
        method: str,
        path: str,
        body: dict[str, object] | None = None,
        response_timeout_seconds: float | None = None,
    ) -> object:
        """Send one authenticated JSON request over the link and return its answer."""
        stream = self._take_stream()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._api_token}",
            OWNER_SESSION_ID_HEADER: self._session_id,
            SESSION_GENERATION_ID_HEADER: self._generation_id,
        }
        payload = None
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        keep = False
        try:
            if stream.sock is not None:
                stream.sock.settimeout(response_timeout_seconds)
            stream.request(method, path, body=payload, headers=headers)
            response = stream.getresponse()
            document = _read_json(response, label=f"{method} {path}")
            keep = not response.will_close
            if not 200 <= response.status < 300:
                detail = json.dumps(document, ensure_ascii=False)[:2_000]
                raise RelayError(
                    f"the worker answered {method} {path} with {response.status}: {detail}"
                )
            return document
        except (OSError, http.client.HTTPException) as exc:
            keep = False
            if not self.connected:
                raise ChannelDropped(f"the link to the worker for {self._cluster} dropped") from exc
            raise RelayError(f"{method} {path} to the worker failed: {exc}") from exc
        finally:
            if keep:
                self._return_stream(stream)
            else:
                stream.close()

    def close(self, *, at_exit: bool = False) -> None:  # noqa: ARG002 - same call at exit
        """Release the streams and the visitor."""
        with self._lock:
            idle, self._idle = self._idle, []
        for _, stream in idle:
            stream.close()
        transport, self._transport = self._transport, None
        self._endpoint = None
        if transport is not None:
            transport.close()


def _read_json(response: http.client.HTTPResponse, *, label: str) -> object:
    data = response.read(MAX_RESPONSE_BYTES + 1)
    if len(data) > MAX_RESPONSE_BYTES:
        raise RelayError(f"the worker's {label} answer is over {MAX_RESPONSE_BYTES} bytes")
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelayError(f"the worker's {label} answer is not JSON") from exc
