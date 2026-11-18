from __future__ import annotations

import base64
import hashlib
import hmac
import html
import os
import re
import struct
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import pytest

from clio_relay.asks import askpass
from clio_relay.asks.provider import RelayAskProvider
from clio_relay.identity import store as token_store
from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

FIXTURES_ENV = Path(
    os.environ.get(
        "CLIO_RELAY_FIXTURES_ENV",
        str(Path.home() / "clio-relay-rework" / "state" / "w2-fixtures" / "fixtures.env"),
    )
)

# The fx-otp run needs W2's fixtures; without their env file it is not collected.
collect_ignore = [] if FIXTURES_ENV.is_file() else ["test_askpass_otp.py"]


@pytest.fixture(autouse=True)
def _private_keyring(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("CLIO_RELAY_KEYRING", "memory")
    for key in list(os.environ):
        if key.startswith(("CLIO_RELAY_GRANT_FILE_", "CLIO_RELAY_GLOBUS_", "CLIO_RELAY_GITHUB_")):
            monkeypatch.delenv(key)
    monkeypatch.delenv("CLIO_RELAY_PUBLIC_URL", raising=False)
    monkeypatch.delenv("CLIO_RELAY_OPERATOR_TOKEN", raising=False)
    memory = token_store._shared_memory  # pyright: ignore[reportPrivateUsage]
    memory._values.clear()  # pyright: ignore[reportPrivateUsage]
    yield
    memory._values.clear()  # pyright: ignore[reportPrivateUsage]


@pytest.fixture
def relay_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RelayPaths:
    paths = RelayPaths(config_dir=tmp_path / "config", state_dir=tmp_path / "state")
    monkeypatch.setenv("CLIO_RELAY_CONFIG_DIR", str(paths.config_dir))
    monkeypatch.setenv("CLIO_RELAY_STATE_DIR", str(paths.state_dir))
    return paths


def add_site(
    paths: RelayPaths,
    site_id: str = "fx",
    *,
    config: dict[str, Any] | None = None,
    spend: str | float = "never",
    identity: str | None = None,
) -> SiteEntry:
    values: dict[str, Any] = {
        "id": site_id,
        "adapter": "fixture",
        "config": config if config is not None else {"queue_s": 0.2, "run_s": 0.3},
        "policy": "open",
        "spend": {"ask": spend},
    }
    if identity is not None:
        values["identity"] = identity
    return SiteStore(paths.sites_file).add(SiteEntry.model_validate(values))


def provider_of(service: RelayService) -> RelayAskProvider:
    provider = service.ask_provider
    assert isinstance(provider, RelayAskProvider)
    return provider


_HIDDEN = re.compile(r"<input type='hidden' name='([a-z_]+)' value='([^']*)'>")


def hidden_fields(text: str) -> dict[str, str]:
    """Return the first value of each hidden field on a page."""
    found: dict[str, str] = {}
    for name, value in _HIDDEN.findall(text):
        found.setdefault(name, html.unescape(value))
    return found


def origin_of(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def submit(
    url: str,
    action: str,
    value: str | None = None,
    *,
    extra: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    client: httpx.Client | None = None,
) -> httpx.Response:
    """Load a page and post its form the way a browser does: every hidden field and an Origin."""
    own = client is None
    http = client or httpx.Client(timeout=30)
    try:
        page = http.get(url)
        assert page.status_code == 200, page.text
        form = hidden_fields(page.text)
        assert "t" in form and "csrf" in form, page.text
        form["action"] = action
        if value is not None:
            form["value"] = value
        form.update(extra or {})
        sent = {"Origin": origin_of(url), "Sec-Fetch-Site": "same-origin", **(headers or {})}
        return http.post(url.split("?", 1)[0], data=form, headers=sent)
    finally:
        if own:
            http.close()


def eventually[T](check: Callable[[], T | None], what: str, limit_s: float = 60.0) -> T:
    """Wait for ``check`` to return something. The limit only stops a broken test."""
    deadline = time.monotonic() + limit_s
    while True:
        found = check()
        if found is not None and found is not False:
            return found
        if time.monotonic() > deadline:
            raise AssertionError(f"never saw {what}")
        time.sleep(0.05)


def totp(secret_b32: str, at: float | None = None) -> str:
    padded = secret_b32 + "=" * (-len(secret_b32) % 8)
    key = base64.b32decode(padded, casefold=True)
    step = int(time.time() if at is None else at) // 30
    digest = hmac.new(key, struct.pack(">Q", step), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    code = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return f"{code % 1_000_000:06d}"


@pytest.fixture
def no_listeners() -> Iterator[None]:
    """Leave the process's askpass broker as it was found."""
    broker = askpass.broker()
    before = list(broker._listeners)  # pyright: ignore[reportPrivateUsage]
    yield
    broker._listeners[:] = before  # pyright: ignore[reportPrivateUsage]
