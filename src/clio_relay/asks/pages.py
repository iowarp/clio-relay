"""Relay's page: single-use, expiring addresses and the HTML behind them (RL28).

An address carries a random token. Only its SHA-256 is written down, under
``<state>/pages/``, so a second relay process (``clio-relay asks page``) can
mint an address the serving process accepts, and nothing on disk opens a page.
An address dies when it expires or when its ask is finished. The ask itself
never expires: a fresh address is one call away.

The form token that guards each POST is an HMAC of the page token under a key
kept in the same private directory.
"""

from __future__ import annotations

import hashlib
import hmac
import html
import json
import os
import re
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from filelock import FileLock

from clio_relay.adapters.hooks import Ask

DEFAULT_PAGE_TTL_S = 24 * 3600.0
MAX_LIVE_TOKENS = 8
_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

TokenCheck = Literal["ok", "expired", "unknown"]


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class PageTokens:
    """Page tokens, stored as hashes with their expiry."""

    def __init__(self, root: Path, ttl_s: float = DEFAULT_PAGE_TTL_S) -> None:
        self.root = root
        self.ttl_s = ttl_s
        self._key: bytes | None = None

    def _ensure(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        (self.root / ".locks").mkdir(exist_ok=True, mode=0o700)

    def _path(self, ask_id: str) -> Path:
        if not _ID.match(ask_id):
            raise ValueError(f"not an ask id: {ask_id!r}")
        return self.root / f"{ask_id}.json"

    def _lock(self, ask_id: str) -> FileLock:
        self._ensure()
        return FileLock(str(self.root / ".locks" / f"{ask_id}.lock"), timeout=30)

    def _read(self, ask_id: str) -> list[dict[str, Any]]:
        try:
            value: object = json.loads(self._path(ask_id).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return []
        if not isinstance(value, dict):
            return []
        tokens = value.get("tokens")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if not isinstance(tokens, list):
            return []
        return [item for item in tokens if isinstance(item, dict)]  # pyright: ignore[reportUnknownVariableType]

    def _write(self, ask_id: str, tokens: list[dict[str, Any]]) -> None:
        path = self._path(ask_id)
        tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}")
        tmp.write_text(json.dumps({"tokens": tokens}), encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)

    def mint(self, ask_id: str) -> str:
        """Return a fresh token for ``ask_id``."""
        token = secrets.token_urlsafe(24)
        now = time.time()
        with self._lock(ask_id):
            tokens = [item for item in self._read(ask_id) if float(item.get("exp", 0)) > now]
            tokens.append({"h": _hash(token), "exp": now + self.ttl_s})
            self._write(ask_id, tokens[-MAX_LIVE_TOKENS:])
        return token

    def check(self, ask_id: str, token: str) -> TokenCheck:
        """Return whether ``token`` opens ``ask_id``'s page."""
        if not token or not _ID.match(ask_id):
            return "unknown"
        wanted = _hash(token)
        for item in self._read(ask_id):
            if hmac.compare_digest(str(item.get("h", "")), wanted):
                return "ok" if float(item.get("exp", 0)) > time.time() else "expired"
        return "unknown"

    def spend(self, ask_id: str) -> None:
        """Kill every address of ``ask_id``."""
        if not _ID.match(ask_id):
            return
        with self._lock(ask_id):
            self._path(ask_id).unlink(missing_ok=True)

    def _secret(self) -> bytes:
        if self._key is None:
            self._ensure()
            path = self.root / ".key"
            try:
                self._key = bytes.fromhex(path.read_text(encoding="ascii").strip())
            except (OSError, ValueError):
                key = secrets.token_bytes(32)
                with FileLock(str(self.root / ".locks" / "key.lock"), timeout=30):
                    if not path.exists():
                        tmp = path.with_name(f".key.{secrets.token_hex(4)}")
                        tmp.write_text(key.hex(), encoding="ascii")
                        os.chmod(tmp, 0o600)
                        os.replace(tmp, path)
                    self._key = bytes.fromhex(path.read_text(encoding="ascii").strip())
        return self._key

    def form_token(self, ask_id: str, token: str) -> str:
        """Return the form token a POST from ``token``'s page must carry."""
        message = f"{ask_id}:{token}".encode()
        return hmac.new(self._secret(), message, hashlib.sha256).hexdigest()

    def check_form(self, ask_id: str, token: str, form: str) -> bool:
        """Return whether ``form`` is the form token for that page."""
        return bool(form) and hmac.compare_digest(self.form_token(ask_id, token), form)


@dataclass
class PageResponse:
    """What a page handler answers, for either server."""

    status: int
    body: str
    headers: dict[str, str] = field(default_factory=dict[str, str])


SECURITY_HEADERS: dict[str, str] = {
    "Content-Type": "text/html; charset=utf-8",
    "Cache-Control": "no-store",
    "Pragma": "no-cache",
    "X-Frame-Options": "DENY",
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy": (
        "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
        "frame-ancestors 'none'; base-uri 'none'"
    ),
    "Referrer-Policy": "same-origin",
}

_STYLE = (
    "body{font:16px/1.45 system-ui,sans-serif;max-width:40rem;margin:2rem auto;padding:0 1rem}"
    "table{border-collapse:collapse;margin:1rem 0}th,td{text-align:left;padding:.25rem .75rem"
    " .25rem 0;vertical-align:top}th{font-weight:600}button{font:inherit;padding:.4rem 1rem;"
    "margin:.25rem .5rem .25rem 0}input[type=password],input[type=text]{font:inherit;"
    "padding:.3rem;width:100%;box-sizing:border-box}.note{color:#555}.code{font:600 1.6rem "
    "ui-monospace,monospace;letter-spacing:.15rem}"
)


def e(value: object) -> str:
    """Escape for HTML text and attributes."""
    return html.escape(str(value), quote=True)


def page(title: str, body: str, *, refresh_s: int | None = None) -> str:
    """Return a whole page."""
    meta = f"<meta http-equiv='refresh' content='{refresh_s}'>" if refresh_s else ""
    return (
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"{meta}<title>{e(title)}</title><style>{_STYLE}</style></head><body>"
        f"<h1>{e(title)}</h1>{body}</body></html>"
    )


def response(status: int, title: str, body: str, *, refresh_s: int | None = None) -> PageResponse:
    """Return a page response with relay's headers."""
    return PageResponse(status, page(title, body, refresh_s=refresh_s), dict(SECURITY_HEADERS))


def hidden(ask: Ask, token: str, form: str) -> str:
    """Return the hidden fields every form on a page carries."""
    return (
        f"<input type='hidden' name='t' value='{e(token)}'>"
        f"<input type='hidden' name='csrf' value='{e(form)}'>"
        f"<input type='hidden' name='op' value='{e(ask.operation_id or '')}'>"
        f"<input type='hidden' name='bd' value='{e(ask.binding_digest)}'>"
    )


def _charge_rows(binding: dict[str, Any]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    charge = binding.get("charge")
    if isinstance(charge, dict):
        labels = {
            "queue": "Queue",
            "account": "Account",
            "nodes": "Nodes",
            "walltime_s": "Walltime",
            "node_hours": "Most it may use",
            "amount": "Most it may cost",
        }
        for key, label in labels.items():
            value = charge.get(key)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if value is None:
                continue
            if key == "walltime_s" and isinstance(value, (int, float)):
                text = f"{float(value) / 60:g} min"
            elif key == "node_hours":
                text = f"{value} {charge.get('unit', 'node-hours')}"  # pyright: ignore[reportUnknownMemberType]
            else:
                text = str(value)  # pyright: ignore[reportUnknownArgumentType]
            rows.append((label, text))
    return rows


def binding_table(ask: Ask) -> str:
    """Return what an approval approves, as a table."""
    rows: list[tuple[str, str]] = []
    if ask.operation_id:
        rows.append(("Operation", ask.operation_id))
    rows.append(("Site", str(ask.binding.get("site", ask.site))))
    digest = ask.binding.get("spec_digest")
    if digest:
        rows.append(("Job hash", str(digest)))
    package = ask.binding.get("package_hash")
    if package:
        rows.append(("Package hash", str(package)))
    rows.extend(_charge_rows(ask.binding))
    cells = "".join(f"<tr><th>{e(key)}</th><td>{e(value)}</td></tr>" for key, value in rows)
    return f"<table>{cells}</table>"


def options(ask: Ask) -> list[str]:
    """Return a choose ask's options."""
    properties = (ask.requested_schema or {}).get("properties")
    if not isinstance(properties, dict):
        return []
    for prop in properties.values():  # pyright: ignore[reportUnknownVariableType]
        if isinstance(prop, dict):
            values = prop.get("enum")  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if isinstance(values, list):
                return [str(item) for item in values]  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
    return []


def field_name(ask: Ask) -> str:
    """Return a choose ask's field name."""
    properties = (ask.requested_schema or {}).get("properties")
    if isinstance(properties, dict) and properties:
        return str(next(iter(properties)))  # pyright: ignore[reportUnknownArgumentType]
    return "value"
