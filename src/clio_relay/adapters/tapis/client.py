"""The one place relay calls `tapipy` 25.4.0.

Every Tapis call goes through :func:`call`, which names the service and the
`tapipy` operation id, so the ledger's list of operations can be read off the
callers. `tapipy` is synchronous; :func:`acall` runs it in a thread.

`tapipy` turns a JSON result into ``TapisResult`` objects and returns ``None``
for some results. Relay asks for its debug data and reads the plain JSON
``result`` from the response instead, so callers get dicts and lists.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, cast

from clio_relay.jobs.refusals import Refusal, RefusalReason


class TapisCallError(Exception):
    """A Tapis call that failed. ``status`` is the HTTP status, or ``None``."""

    def __init__(self, operation: str, status: int | None, message: str) -> None:
        super().__init__(operation, status, message)
        self.operation = operation
        self.status = status
        self.message = message

    def __str__(self) -> str:
        code = "" if self.status is None else f" ({self.status})"
        return f"Tapis {self.operation}{code}: {self.message}"

    @property
    def not_found(self) -> bool:
        return self.status == 404

    @property
    def unauthorized(self) -> bool:
        return self.status == 401

    def refusal(self, reason: RefusalReason = RefusalReason.UNREACHABLE, **data: Any) -> Refusal:
        """Return the typed refusal relay shows for this failure."""
        if self.unauthorized:
            reason = RefusalReason.NEEDS_SIGN_IN
        return Refusal(
            reason,
            str(self),
            tapis_operation=self.operation,
            tapis_status=self.status,
            tapis_message=self.message,
            **data,
        )


_lock = threading.Lock()


def make_client(base_url: str, tenant_id: str, access_token: str | None = None) -> Any:
    """Build a ``tapipy`` client. Passing ``tenant_id`` keeps it off the Tenants API."""
    try:
        from tapipy.tapis import Tapis  # pyright: ignore[reportMissingTypeStubs]
    except ModuleNotFoundError as exc:
        raise Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            "the tapis adapter needs tapipy: install clio-relay with the tapis extra",
            missing=exc.name,
        ) from exc
    kwargs: dict[str, Any] = {
        "base_url": base_url.rstrip("/"),
        "tenant_id": tenant_id,
        "debug_prints": False,
    }
    if access_token is not None:
        kwargs["access_token"] = access_token
    # tapipy fills module globals from its bundled specs on first use; keep that single-threaded.
    with _lock:
        return cast(Any, Tapis)(**kwargs)


def _message(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    if response is not None:
        try:
            body: object = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict):
            text = cast(dict[str, Any], body).get("message")
            if text:
                return str(text)
    return str(getattr(exc, "message", None) or exc)


def call(client: Any, service: str, operation: str, **kwargs: Any) -> Any:
    """Run ``client.<service>.<operation>(**kwargs)`` and return the plain result."""
    from tapipy.errors import BaseTapyException  # pyright: ignore[reportMissingTypeStubs]

    name = f"{service}.{operation}"
    op = getattr(getattr(client, service), operation)
    try:
        out: object = op(_tapis_debug=True, **kwargs)
    except BaseTapyException as exc:  # pyright: ignore[reportUnknownVariableType]
        error = cast(BaseException, exc)
        status = getattr(getattr(error, "response", None), "status_code", None)
        raise TapisCallError(name, status, _message(error)) from None
    except OSError as exc:
        raise TapisCallError(name, None, f"could not reach Tapis: {exc}") from None
    if isinstance(out, tuple):
        pair = cast(tuple[Any, ...], out)
        response = getattr(pair[-1], "response", None) if len(pair) == 2 else None
        if response is not None:
            content_type = str(response.headers.get("content-type") or "").lower()
            if content_type.startswith("application/json"):
                body: object = response.json()
                if isinstance(body, dict):
                    return cast(dict[str, Any], body).get("result")
                return body
            return bytes(response.content)
    return out


async def acall(client: Any, service: str, operation: str, **kwargs: Any) -> Any:
    """Run :func:`call` in a thread."""
    return await asyncio.to_thread(call, client, service, operation, **kwargs)
